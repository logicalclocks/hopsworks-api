#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import io
import json
import os
import queue
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from hopsworks_common.core import feature_logging_file as flf


posix_only = pytest.mark.skipif(
    sys.platform == "win32", reason="the writer pipe uses select() on pipe descriptors"
)


def _stream(rows: int, start: int = 0) -> bytes:
    batch = pa.record_batch(
        {"customer_id": list(range(start, start + rows)), "score": [0.5] * rows}
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class _Uploads:
    def __init__(self, fail_first: int = 0):
        self.paths = []
        self.fail_first = fail_first
        self.triggers = 0

    def _upload(self, path):
        if self.fail_first:
            self.fail_first -= 1
            raise OSError("registry down")
        self.paths.append(path.name)

    def _maybe_trigger_commit(self):
        self.triggers += 1
        return False


def _options(tmp_path, **overrides):
    return flf._FileLogOptions(
        "view",
        2,
        schema_id="abc",
        buffer_dir=str(tmp_path),
        deployment="dep",
        revision="rev1",
        pod="pod-a",
        **overrides,
    )


class TestSegmentWriter:
    def test_batches_land_in_one_segment_until_it_rotates(self, tmp_path):
        uploads = _Uploads()
        writer = flf._SegmentWriter(_options(tmp_path), uploader=uploads)

        assert writer._append(_stream(3), 3)
        assert writer._append(_stream(2, 3), 2)
        assert not writer._has_ready()
        assert writer._snapshot()["rows_written"] == 5
        assert writer._snapshot()["bytes_current"] > 0

        assert writer._rotate()
        ready = list(writer.ready_dir.glob("*.arrow"))
        assert [p.name for p in ready] == [f"pod-a-{writer.boot_id}-00000000.arrow"]
        with pa.ipc.open_stream(ready[0].read_bytes()) as reader:
            table = reader.read_all()
        assert table.num_rows == 5
        assert table.schema.metadata[b"chunk_id"] == ready[0].stem.encode()
        assert table.schema.metadata[b"schema_id"] == b"abc"
        assert table.schema.metadata[b"feature_view"] == b"view_2"

        assert writer._upload_ready()
        assert uploads.paths == [ready[0].name]
        assert not writer._has_ready()
        assert writer._snapshot()["chunks_uploaded"] == 1

    def test_size_and_age_rotate_and_a_schema_change_starts_a_new_segment(
        self, tmp_path
    ):
        writer = flf._SegmentWriter(
            _options(tmp_path, flush_bytes=1, flush_interval_seconds=0),
            uploader=_Uploads(),
        )
        writer._append(_stream(1), 1)
        # Over flush_bytes: rotated at once, nothing left open.
        assert writer._snapshot()["bytes_current"] == 0
        assert writer.chunks_rotated == 1

        big = _options(tmp_path, flush_interval_seconds=0)
        aged = flf._SegmentWriter(big, uploader=_Uploads())
        aged._append(_stream(1), 1)
        assert aged._due()

        other = pa.record_batch({"different": [1]})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, other.schema) as w:
            w.write_batch(other)
        aged._append(sink.getvalue().to_pybytes(), 1)
        assert aged.chunks_rotated == 1  # the first schema's segment was closed
        assert aged._snapshot()["bytes_current"] > 0

    def test_a_full_buffer_drops_and_counts(self, tmp_path):
        writer = flf._SegmentWriter(
            _options(tmp_path, max_buffer_bytes=10), uploader=_Uploads()
        )
        assert not writer._append(_stream(4), 4)
        snapshot = writer._snapshot()
        assert snapshot["rows_dropped"] == 4
        assert snapshot["rows_received"] == 4
        assert snapshot["rows_written"] == 0

    def test_a_new_writer_adopts_what_the_old_one_left_open(self, tmp_path):
        first = flf._SegmentWriter(_options(tmp_path), uploader=_Uploads())
        first._append(_stream(2), 2)
        leftover = first._segment_path
        assert leftover.exists()  # never rotated: the process "died" here

        second = flf._SegmentWriter(_options(tmp_path), uploader=_Uploads())
        assert not leftover.exists()
        assert [p.name for p in second.ready_dir.glob("*.arrow")] == [leftover.name]

    def test_a_failed_upload_keeps_the_segment_and_counts(self, tmp_path):
        uploads = _Uploads(fail_first=1)
        writer = flf._SegmentWriter(_options(tmp_path), uploader=uploads)
        writer._append(_stream(1), 1)
        writer._rotate()

        assert not writer._upload_ready()
        assert writer._has_ready()
        assert writer.upload_failures == 1
        assert writer._upload_ready()
        assert not writer._has_ready()


class TestAccounting:
    def test_rows_and_upload_durations_follow_each_segment(self, tmp_path):
        uploads = _Uploads(fail_first=1)
        writer = flf._SegmentWriter(_options(tmp_path), uploader=uploads)
        writer._append(_stream(3), 3)
        writer._rotate()
        writer._append(_stream(2), 2)
        assert writer._snapshot()["rows_buffered"] == 5
        assert not writer._upload_ready()  # the first attempt fails
        assert writer._snapshot()["rows_uploaded"] == 0
        assert writer._upload_ready()
        snapshot = writer._snapshot()
        assert snapshot["rows_uploaded"] == 3
        assert snapshot["rows_buffered"] == 2  # the open segment
        assert snapshot["upload_count"] == 1
        assert snapshot["upload_buckets"][-1] == 1  # under 30 s
        assert snapshot["upload_seconds_sum"] > 0

    def test_a_new_writer_counts_the_rows_it_adopts(self, tmp_path):
        first = flf._SegmentWriter(_options(tmp_path), uploader=_Uploads())
        first._append(_stream(4), 4)
        second = flf._SegmentWriter(_options(tmp_path), uploader=_Uploads())
        assert second._snapshot()["rows_buffered"] == 4
        assert second._upload_ready()
        assert second._snapshot()["rows_uploaded"] == 4

    def test_the_collector_publishes_the_sidecars_metric_names(self, tmp_path):
        pytest.importorskip("prometheus_client")
        from prometheus_client import CollectorRegistry, generate_latest

        transport = SimpleNamespace(
            rows_failed=2,
            frames_sent=5,
            frames_failed=1,
            writer_restarts=1,
            _stats=lambda: {
                "rows_received": 40,
                "rows_uploaded": 30,
                "rows_dropped": 3,
                "rows_buffered": 7,
                "bytes_current": 100,
                "bytes_ready": 900,
                "chunks_uploaded": 4,
                "upload_failures": 1,
                "upload_count": 4,
                "upload_seconds_sum": 3.5,
                "upload_buckets": [0, 0, 1, 2, 4, 4, 4, 4],
                "commit_triggers": 1,
            },
        )
        worker = SimpleNamespace(dropped=5, failed=1)
        registry = CollectorRegistry()
        registry.register(flf._FileLogCollector(transport, worker))
        text = generate_latest(registry).decode()
        expected = {
            'hopsworks_feature_log_rows_total{outcome="accepted"} 40.0',
            'hopsworks_feature_log_rows_total{outcome="produced"} 30.0',
            'hopsworks_feature_log_rows_total{outcome="failed"} 3.0',
            'hopsworks_feature_log_rows_total{outcome="dropped"} 8.0',
            'hopsworks_feature_log_events_total{outcome="accepted",type="arrow"} 5.0',
            "hopsworks_feature_log_inflight_rows 7.0",
            "hopsworks_feature_log_inflight_bytes 1000.0",
            "hopsworks_feature_log_pending_bytes 900.0",
            'hopsworks_feature_log_produce_duration_seconds_bucket{le="1.0"} 2.0',
            'hopsworks_feature_log_produce_duration_seconds_bucket{le="+Inf"} 4.0',
            "hopsworks_feature_log_produce_duration_seconds_sum 3.5",
            'hopsworks_feature_log_chunks_total{outcome="failed"} 1.0',
            "hopsworks_feature_log_commit_triggers_total 1.0",
            "hopsworks_feature_log_writer_restarts_total 1.0",
        }
        lines = set(text.splitlines())
        assert expected <= lines, expected - lines

    def test_exposing_twice_registers_once(self, tmp_path):
        pytest.importorskip("prometheus_client")
        from prometheus_client import REGISTRY

        transport = SimpleNamespace(
            rows_failed=0,
            frames_sent=0,
            frames_failed=0,
            writer_restarts=0,
            _stats=dict,
        )
        collector = flf._expose_metrics(transport)
        try:
            assert collector is not None
            assert flf._expose_metrics(transport) is None
        finally:
            if collector is not None:
                REGISTRY.unregister(collector)


class TestFrameQueue:
    def test_the_queue_blocks_the_reader_at_its_byte_bound(self):
        frames = flf._FrameQueue(15)
        frames._put((flf._KIND_BATCH, 1, b"x" * 10))
        blocked = threading.Event()
        released = threading.Event()

        def producer():
            blocked.set()
            frames._put((flf._KIND_BATCH, 1, b"y" * 10))  # 20 > 15: waits for a get
            released.set()

        threading.Thread(target=producer, daemon=True).start()
        assert blocked.wait(1) and not released.wait(0.2)
        assert frames._get(timeout=1)[2] == b"x" * 10
        assert released.wait(1)
        assert len(frames) == 1
        with pytest.raises(queue.Empty):
            flf._FrameQueue(1)._get(timeout=0.01)

    def test_one_frame_larger_than_the_bound_still_passes_when_empty(self):
        frames = flf._FrameQueue(4)
        frames._put((flf._KIND_BATCH, 1, b"z" * 100))
        assert frames._get(timeout=0.1)[1] == 1


class TestWriterMain:
    def test_frames_in_status_out_and_stop_uploads_everything(self, tmp_path, mocker):
        uploads = _Uploads()
        mocker.patch.object(flf, "_DatasetUploader", return_value=uploads)
        payload = _stream(3)
        frames = (
            flf._FRAME.pack(flf._KIND_BATCH, 3, len(payload))
            + payload
            + flf._FRAME.pack(flf._KIND_ROTATE, 0, 0)
            + flf._FRAME.pack(flf._KIND_STOP, 0, 0)
        )
        read_fd, write_fd = os.pipe()
        os.write(write_fd, frames)
        os.close(write_fd)
        out = io.StringIO()
        with os.fdopen(read_fd, "rb", buffering=0) as stdin:
            flf._writer_main(
                _options(tmp_path, shutdown_seconds=5), stdin=stdin, stdout=out
            )
        statuses = [json.loads(line) for line in out.getvalue().splitlines()]
        assert statuses[-1]["rows_written"] == 3
        assert statuses[-1]["bytes_ready"] == 0
        assert statuses[-1]["chunks_uploaded"] == 1
        assert len(uploads.paths) == 1


@posix_only
class TestFileLogTransport:
    def test_the_predictor_end_feeds_a_real_writer_process(self, tmp_path, mocker):
        # A real child process that only writes and rotates; this process
        # uploads through the project handed to the transport.
        (tmp_path / "uploaded").mkdir()
        options = _options(
            tmp_path / "buffer", shutdown_seconds=5, upload_in_writer=False
        )
        transport = flf._FileLogTransport(options, project=_LocalProject(tmp_path))
        try:
            transport._submit(_stream(2), 2)
            transport._submit(_stream(2, 2), 2)
            assert transport.rows_sent == 4
            deadline = threading.Event()
            for _ in range(200):
                if transport._stats().get("rows_written") == 4:
                    break
                deadline.wait(0.05)
            assert transport._stats()["rows_written"] == 4
        finally:
            assert transport._close(timeout=10)
        uploaded = list((tmp_path / "uploaded").glob("*.arrow"))
        assert len(uploaded) == 1
        assert not list((tmp_path / "buffer" / "ready").glob("*.arrow"))
        assert not list((tmp_path / "buffer" / "current").glob("*.arrow"))
        with pa.ipc.open_stream(uploaded[0].read_bytes()) as reader:
            assert reader.read_all().num_rows == 4


@posix_only
class TestBlockedWriter:
    def test_a_writer_that_stops_reading_neither_blocks_logging_nor_shutdown(
        self, tmp_path, monkeypatch
    ):
        import subprocess
        import sys
        import time

        # A child that never reads its stdin stands in for a writer stuck in
        # an upload; frames beyond the pipe buffer must fail within the handoff
        # budget and _close must return within its own, killing the child.
        real_popen = subprocess.Popen

        def stuck(*args, **kwargs):
            return real_popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                stdin=kwargs["stdin"],
                stdout=kwargs["stdout"],
            )

        monkeypatch.setattr(flf.subprocess, "Popen", stuck)
        options = _options(tmp_path / "buffer", handoff_seconds=1, shutdown_seconds=1)
        transport = flf._FileLogTransport(options, project=_LocalProject(tmp_path))
        payload = b"x" * (8 << 20)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            transport._submit(payload, 5)
        assert time.monotonic() - started < 5
        assert transport.rows_failed == 5 and transport.rows_sent == 0

        started = time.monotonic()
        assert transport._close(timeout=1) is True  # nothing was ever written
        assert time.monotonic() - started < 5
        assert transport._process.poll() is not None


class _LocalDatasetApi:
    """A Dataset API over a local directory, enough for the uploader."""

    def __init__(self, root):
        self.root = root

    def exists(self, path):
        return (self.root / path).exists()

    def mkdir(self, path):
        (self.root / path).mkdir(parents=True, exist_ok=True)

    def upload(self, local_path, upload_path, overwrite=False):
        target = self.root / upload_path / os.path.basename(local_path)
        target.write_bytes(Path(local_path).read_bytes())
        return str(target.relative_to(self.root))

    def move(self, source, destination, overwrite=False):
        (self.root / source).rename(
            self.root / "uploaded" / os.path.basename(destination)
        )


class _LocalProject:
    def __init__(self, root):
        self._api = _LocalDatasetApi(root)

    def get_dataset_api(self):
        return self._api


@pytest.mark.parametrize(
    "name",
    [
        "flush_bytes",
        "flush_interval_seconds",
        "max_buffer_bytes",
        "shutdown_seconds",
    ],
)
def test_options_come_from_the_environment_and_round_trip(name, mocker):
    mocker.patch.dict(
        os.environ,
        {
            "HOPSWORKS_FEATURE_LOGGER_FLUSH_BYTES": "2048",
            "HOPSWORKS_FEATURE_LOGGER_FLUSH_INTERVAL_SECONDS": "7",
            "HOPSWORKS_FEATURE_LOGGER_MAX_BUFFER_BYTES": "4096",
            "HOPSWORKS_FEATURE_LOGGER_SHUTDOWN_SECONDS": "3",
            "DEPLOYMENT_NAME": "churn",
            "K_REVISION": "churn-00002",
            "HOSTNAME": "pod-x",
        },
    )
    options = flf._FileLogOptions("view", 1)
    assert (
        getattr(options, name)
        == {
            "flush_bytes": 2048,
            "flush_interval_seconds": 7,
            "max_buffer_bytes": 4096,
            "shutdown_seconds": 3,
        }[name]
    )
    restored = flf._FileLogOptions.from_dict(options.to_dict())
    assert restored.to_dict() == options.to_dict()
    assert restored.staging_dir == "Resources/feature_logging/view_1"
    assert flf._commit_job_name("view", 1) == "view_1_log_feature_log_commit"
