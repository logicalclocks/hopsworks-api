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
"""The `job` feature logging transport: a file buffer on the pod, staged to HopsFS.

The predictor hands serialized Arrow IPC streams to a writer process.
The writer appends them to one open segment, rotates the segment on size or age, and uploads rotated segments to the feature view's staging directory in HopsFS, where the commit job turns them into Delta commits.
Only the writer renames the open segment, so a segment is either being written or complete, never both.
"""

from __future__ import annotations

import collections
import json
import logging
import os
import queue
import select
import struct
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from hopsworks_common.core.feature_logging_buffer import _positive_env


_logger = logging.getLogger(__name__)

STAGING_ROOT = "Resources/feature_logging"
CHUNK_SUFFIX = ".arrow"
OPTIONS_ENV_VAR = "HOPSWORKS_FEATURE_LOG_WRITER_OPTIONS"

# One frame per handoff on the writer's stdin: kind, rows, payload length.
_FRAME = struct.Struct("!BIQ")
_KIND_BATCH, _KIND_ROTATE, _KIND_STOP = 1, 2, 3


# Upload duration histogram bounds, in seconds: a Dataset API upload of one
# segment takes about a second on a quiet cluster.
UPLOAD_SECONDS_BOUNDS = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


def _staging_dir(feature_view_name: str, feature_view_version: int) -> str:
    return f"{STAGING_ROOT}/{feature_view_name}_{feature_view_version}"


def _commit_job_name(feature_view_name: str, feature_view_version: int) -> str:
    return f"{feature_view_name}_{feature_view_version}_log_feature_log_commit"


def _mkdirs(dataset_api, path: str) -> None:
    """Create `path` and its parents; the Dataset API creates one level at a time."""
    parts = [p for p in path.split("/") if p]
    for depth in range(1, len(parts) + 1):
        current = "/".join(parts[:depth])
        if not dataset_api.exists(current):
            dataset_api.mkdir(current)


def _trigger_commit_job(project, name: str) -> bool:
    """Start the commit job unless an execution is already running; `True` when started."""
    job = project.get_job_api().get_job(name)
    if job is None:
        return False
    if any(e.success is None for e in job.get_executions() or []):
        return False
    job.run(await_termination=False)
    return True


class _FileLogOptions:
    """Where the buffer lives, when a segment rotates, and who wrote it."""

    def __init__(
        self,
        feature_view_name: str,
        feature_view_version: int,
        schema_id: str | None = None,
        **overrides,
    ):
        env = os.environ
        self.feature_view_name = feature_view_name
        self.feature_view_version = int(feature_view_version)
        self.schema_id = schema_id
        self.buffer_dir = env.get(
            "HOPSWORKS_FEATURE_LOG_BUFFER_DIR", "/tmp/feature-log-buffer"
        )
        self.flush_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_FLUSH_BYTES", 1024 * 1024
        )
        self.flush_interval_seconds = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_FLUSH_INTERVAL_SECONDS", 300
        )
        self.max_buffer_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_MAX_BUFFER_BYTES", 64 * 1024 * 1024
        )
        # The deployment's shutdown budget, the same variable the backend sets
        # from shutdown_seconds; it covers the drain, the upload and the trigger.
        self.shutdown_seconds = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_SHUTDOWN_SECONDS", 20
        )
        # Longest a logging worker waits to hand a frame to the writer.
        self.handoff_seconds = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_HANDOFF_SECONDS", 5
        )
        # Uploaded bytes that ask the commit job to run before its schedule.
        self.commit_bytes = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_COMMIT_BYTES", 32 * 1024 * 1024
        )
        self.commit_trigger_interval_seconds = _positive_env(
            "HOPSWORKS_FEATURE_LOGGER_COMMIT_TRIGGER_INTERVAL_SECONDS", 300
        )
        # The writer process uploads by default; a pod whose environment cannot
        # log in twice lets the predictor process upload rotated segments.
        self.upload_in_writer = env.get(
            "HOPSWORKS_FEATURE_LOGGER_UPLOAD_IN_WRITER", "true"
        ).strip().lower() not in ("false", "0", "no")
        self.deployment = env.get("DEPLOYMENT_NAME", "deployment")
        self.revision = env.get("K_REVISION") or env.get("DEPLOYMENT_VERSION") or "0"
        self.pod = env.get("HOSTNAME", "pod")
        for name, value in overrides.items():
            if not hasattr(self, name):
                raise TypeError(f"Unknown file log option {name!r}")
            setattr(self, name, value)

    @property
    def staging_dir(self) -> str:
        return _staging_dir(self.feature_view_name, self.feature_view_version)

    @property
    def commit_job_name(self) -> str:
        return _commit_job_name(self.feature_view_name, self.feature_view_version)

    def to_dict(self) -> dict:
        return dict(vars(self))

    @classmethod
    def from_dict(cls, values: dict) -> _FileLogOptions:
        values = dict(values)
        return cls(
            values.pop("feature_view_name"),
            values.pop("feature_view_version"),
            values.pop("schema_id", None),
            **values,
        )


class _DatasetUploader:
    """Moves a complete segment into the staging directory's `pending/`.

    The file lands in `uploading/` first and is renamed into `pending/`, so the commit job never lists a partial chunk.
    """

    def __init__(self, options: _FileLogOptions, project=None):
        self._options = options
        self._staging_dir = options.staging_dir
        self._project = project
        self._api = None
        self._bytes_since_trigger = 0
        self._last_trigger = 0.0

    def _connect(self):
        if self._project is None:
            import hopsworks

            self._project = hopsworks._connected_project or hopsworks.login(
                engine="python"
            )
        return self._project

    def _dataset_api(self):
        if self._api is None:
            api = self._connect().get_dataset_api()
            for name in ("uploading", "pending"):
                _mkdirs(api, f"{self._staging_dir}/{name}")
            self._api = api
        return self._api

    def _upload(self, local_path: Path) -> None:
        api = self._dataset_api()
        uploading = f"{self._staging_dir}/uploading"
        api.upload(str(local_path), uploading, overwrite=True)
        api.move(
            f"{uploading}/{local_path.name}",
            f"{self._staging_dir}/pending/{local_path.name}",
            overwrite=True,
        )
        self._bytes_since_trigger += local_path.stat().st_size

    def _maybe_trigger_commit(self) -> bool:
        """Ask the commit job to run once enough has been staged; `True` when asked."""
        due = (
            self._bytes_since_trigger >= self._options.commit_bytes
            and time.monotonic() - self._last_trigger
            >= self._options.commit_trigger_interval_seconds
        )
        if not due:
            return False
        try:
            started = _trigger_commit_job(
                self._connect(), self._options.commit_job_name
            )
        except Exception as error:  # noqa: BLE001 - the schedule covers it
            _logger.warning(
                "Feature log commit job not triggered: %s", type(error).__name__
            )
            started = False
        self._bytes_since_trigger = 0
        self._last_trigger = time.monotonic()
        return started


class _SegmentWriter:
    """Appends Arrow batches to the open segment and rotates it into `ready/`."""

    def __init__(self, options: _FileLogOptions, uploader=None):
        self._options = options
        root = Path(options.buffer_dir)
        self.current_dir = root / "current"
        self.ready_dir = root / "ready"
        self.current_dir.mkdir(parents=True, exist_ok=True)
        self.ready_dir.mkdir(parents=True, exist_ok=True)
        self._uploader = uploader or _DatasetUploader(options)
        self.boot_id = uuid.uuid4().hex[:8]
        self.sequence = 0
        self._file = None
        self._writer = None
        self._schema = None
        self._segment_path: Path | None = None
        self._segment_started: float | None = None
        self.rows_received = 0
        self.rows_written = 0
        self.rows_dropped = 0
        self.chunks_rotated = 0
        self.chunks_uploaded = 0
        self.bytes_uploaded = 0
        self.upload_failures = 0
        self.commit_triggers = 0
        self.rows_uploaded = 0
        self.upload_count = 0
        self.upload_seconds_sum = 0.0
        self.upload_buckets = [0] * len(UPLOAD_SECONDS_BOUNDS)
        self._segment_rows = 0
        self._chunk_rows: dict[str, int] = {}
        self._adopt_leftovers()

    def _adopt_leftovers(self) -> None:
        # Exactly one writer exists at a time, so whatever the previous one
        # left open is complete as far as it goes and can be uploaded as is.
        for path in sorted(self.current_dir.glob("*" + CHUNK_SUFFIX)):
            os.rename(path, self.ready_dir / path.name)
        for path in sorted(self.ready_dir.glob("*" + CHUNK_SUFFIX)):
            self._chunk_rows[path.name] = _rows_in(path)

    def _chunk_id(self) -> str:
        # The pod name already carries the deployment and revision.
        chunk_id = f"{self._options.pod}-{self.boot_id}-{self.sequence:08d}"
        self.sequence += 1
        return chunk_id

    def _ready_bytes(self) -> int:
        total = 0
        for path in self.ready_dir.glob("*" + CHUNK_SUFFIX):
            try:
                total += path.stat().st_size
            except FileNotFoundError:  # uploaded and removed meanwhile
                continue
        return total

    def _segment_bytes(self) -> int:
        if self._file is None:
            return 0
        return os.fstat(self._file.fileno()).st_size

    def _append(self, payload: bytes, rows: int) -> bool:
        """Append one IPC stream; `False` when the buffer is full and it was dropped."""
        import pyarrow as pa

        self.rows_received += rows
        if (
            self._ready_bytes() + self._segment_bytes() + len(payload)
            > self._options.max_buffer_bytes
        ):
            self.rows_dropped += rows
            return False
        with pa.ipc.open_stream(payload) as reader:
            batches = list(reader)
        for batch in batches:
            if self._writer is not None and not batch.schema.equals(
                self._schema, check_metadata=False
            ):
                self._rotate()
            if self._writer is None:
                self._open(batch.schema)
            self._writer.write_batch(batch)
            self._file.flush()
            os.fsync(self._file.fileno())
        self.rows_written += rows
        self._segment_rows += rows
        if self._segment_bytes() >= self._options.flush_bytes:
            self._rotate()
        return True

    def _open(self, schema) -> None:
        import pyarrow as pa

        chunk_id = self._chunk_id()
        metadata = dict(schema.metadata or {})
        metadata.update(
            {
                b"chunk_id": chunk_id.encode(),
                b"feature_view": (
                    f"{self._options.feature_view_name}_"
                    f"{self._options.feature_view_version}"
                ).encode(),
                b"deployment": self._options.deployment.encode(),
            }
        )
        if self._options.schema_id:
            metadata[b"schema_id"] = str(self._options.schema_id).encode()
        self._schema = schema
        self._segment_path = self.current_dir / (chunk_id + CHUNK_SUFFIX)
        self._file = open(self._segment_path, "wb")  # noqa: SIM115 - closed by _rotate()
        self._writer = pa.ipc.new_stream(self._file, schema.with_metadata(metadata))
        self._segment_started = time.monotonic()

    def _due(self) -> bool:
        return (
            self._segment_started is not None
            and time.monotonic() - self._segment_started
            >= self._options.flush_interval_seconds
        )

    def _rotate(self) -> bool:
        """Close the open segment and move it into `ready/`."""
        if self._writer is None:
            return False
        self._writer.close()
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        os.rename(self._segment_path, self.ready_dir / self._segment_path.name)
        self._chunk_rows[self._segment_path.name] = self._segment_rows
        self._segment_rows = 0
        self._file = self._writer = self._schema = None
        self._segment_path = self._segment_started = None
        self.chunks_rotated += 1
        return True

    def _upload_ready(self) -> bool:
        """Upload every complete segment, oldest first; `False` after the first failure."""
        for path in sorted(self.ready_dir.glob("*" + CHUNK_SUFFIX)):
            size = path.stat().st_size
            started = time.monotonic()
            try:
                self._uploader._upload(path)
            except Exception as error:  # noqa: BLE001 - retried with backoff
                self.upload_failures += 1
                _logger.warning(
                    "Feature log chunk upload failed (%s): %s",
                    type(error).__name__,
                    path.name,
                    exc_info=error,
                )
                return False
            os.remove(path)
            self._observe_upload(time.monotonic() - started)
            self.chunks_uploaded += 1
            self.bytes_uploaded += size
            self.rows_uploaded += self._chunk_rows.pop(path.name, 0)
        if self._uploader._maybe_trigger_commit():
            self.commit_triggers += 1
        return True

    def _has_ready(self) -> bool:
        return any(self.ready_dir.glob("*" + CHUNK_SUFFIX))

    def _observe_upload(self, seconds: float) -> None:
        self.upload_count += 1
        self.upload_seconds_sum += seconds
        for index, bound in enumerate(UPLOAD_SECONDS_BOUNDS):
            if seconds <= bound:
                self.upload_buckets[index] += 1

    def _snapshot(self) -> dict:
        return {
            "rows_received": self.rows_received,
            "rows_written": self.rows_written,
            "rows_dropped": self.rows_dropped,
            "rows_uploaded": self.rows_uploaded,
            "rows_buffered": self._segment_rows + sum(self._chunk_rows.values()),
            "bytes_current": self._segment_bytes(),
            "bytes_ready": self._ready_bytes(),
            "chunks_rotated": self.chunks_rotated,
            "chunks_uploaded": self.chunks_uploaded,
            "bytes_uploaded": self.bytes_uploaded,
            "upload_failures": self.upload_failures,
            "upload_count": self.upload_count,
            "upload_seconds_sum": self.upload_seconds_sum,
            "upload_buckets": list(self.upload_buckets),
            "commit_triggers": self.commit_triggers,
        }


def _rows_in(path: Path) -> int:
    """Rows in a complete or truncated IPC segment; a file with no readable batch counts none."""
    import pyarrow as pa

    rows = 0
    try:
        with open(path, "rb") as file, pa.ipc.open_stream(file) as reader:
            for batch in reader:
                rows += batch.num_rows
    except (OSError, pa.ArrowInvalid):
        pass
    return rows


def _read_exact(stream, size: int) -> bytes:
    chunks = []
    while size:
        chunk = stream.read(size)
        if not chunk:
            return b""
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


class _FrameQueue:
    """Frames read from the pipe, bounded by payload bytes.

    Once `limit` bytes wait for the writer the reader blocks, the pipe fills behind it and the predictor's handoff deadline turns the pressure into counted failures instead of memory growth in this process.
    A frame is always admitted into an empty queue, so one frame larger than the limit still passes.
    """

    def __init__(self, limit: int):
        self._limit = limit
        self._items: collections.deque = collections.deque()
        self._bytes = 0
        self._changed = threading.Condition()

    def _put(self, item) -> None:
        size = len(item[2])
        with self._changed:
            while self._items and self._bytes + size > self._limit:
                self._changed.wait()
            self._items.append(item)
            self._bytes += size
            self._changed.notify_all()

    def _get(self, timeout: float | None = None):
        with self._changed:
            if not self._items and not self._changed.wait_for(
                lambda: bool(self._items), timeout
            ):
                raise queue.Empty
            item = self._items.popleft()
            self._bytes -= len(item[2])
            self._changed.notify_all()
            return item

    def __len__(self) -> int:
        with self._changed:
            return len(self._items)


def _read_frames(stdin, frames) -> None:
    """Reader thread of the writer process: frames from stdin into the bounded queue.

    Reading never waits on a file or an upload, only on the queue's byte bound, so the predictor's pipe drains as fast as the buffer allows.
    """
    while True:
        header = _read_exact(stdin, _FRAME.size)
        if not header:
            frames._put((_KIND_STOP, 0, b""))
            return
        kind, rows, length = _FRAME.unpack(header)
        payload = _read_exact(stdin, length) if length else b""
        frames._put((kind, rows, payload))
        if kind == _KIND_STOP:
            return


class _Uploader(threading.Thread):
    """Upload thread of the writer process, so appends never wait on HopsFS."""

    def __init__(self, writer: _SegmentWriter):
        super().__init__(name="feature-log-upload", daemon=True)
        self._writer = writer
        self.wake = threading.Event()
        self.stopping = threading.Event()
        self.lock = threading.Lock()

    def run(self) -> None:
        backoff = 30.0
        while not self.stopping.is_set():
            self.wake.wait(1.0)
            self.wake.clear()
            if not self._writer._has_ready():
                continue
            with self.lock:
                ok = self._writer._upload_ready()
            if ok:
                backoff = 30.0
            else:
                self.stopping.wait(backoff)
                backoff = min(600.0, backoff * 2)

    def _drain(self, deadline: float) -> bool:
        """Upload what is ready until the deadline; `True` when nothing is left."""
        while self._writer._has_ready() and time.monotonic() < deadline:
            with self.lock:
                if not self._writer._upload_ready():
                    time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        return not self._writer._has_ready()


def _writer_main(options: _FileLogOptions, stdin=None, stdout=None) -> None:
    """The writer process: frames in on stdin, status lines out on stdout."""
    # Unbuffered: the reader thread hands frames over as they arrive, and a
    # Python-side buffer would hide the next one from the queue.
    stdin = stdin or sys.stdin.buffer.raw
    stdout = stdout or sys.stdout
    writer = _SegmentWriter(options)
    frames = _FrameQueue(options.max_buffer_bytes)
    threading.Thread(
        target=_read_frames, args=(stdin, frames), name="feature-log-read", daemon=True
    ).start()
    uploader = _Uploader(writer) if options.upload_in_writer else None
    if uploader is not None:
        uploader.start()
    last_report = None

    def report():
        nonlocal last_report
        snapshot = writer._snapshot()
        if snapshot != last_report:
            stdout.write(json.dumps(snapshot) + "\n")
            stdout.flush()
            last_report = snapshot

    report()
    while True:
        try:
            kind, rows, payload = frames._get(timeout=1.0)
        except queue.Empty:
            kind = None
        if kind == _KIND_STOP:
            break
        if kind == _KIND_BATCH:
            writer._append(payload, rows)
        elif kind == _KIND_ROTATE:
            writer._rotate()
        if writer._due():
            writer._rotate()
        if writer._has_ready() and uploader is not None:
            uploader.wake.set()
        report()
    writer._rotate()
    if uploader is not None:
        uploader.stopping.set()
        uploader._drain(time.monotonic() + options.shutdown_seconds)
    report()


class _FileLogTransport:
    """The predictor's end: a writer process fed one frame per Arrow post."""

    def __init__(self, options: _FileLogOptions, project=None):
        self._options = options
        self._project = project
        self._status: dict = {}
        self._status_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._stopping = threading.Event()
        self.rows_sent = 0
        self.rows_failed = 0
        self.frames_sent = 0
        self.frames_failed = 0
        self.writer_restarts = 0
        self._process = None
        self._start_writer()
        self._upload_lock = threading.Lock()
        self._uploader_thread = None
        if not options.upload_in_writer:
            self._uploader_thread = threading.Thread(
                target=self._upload_loop, name="hsml-feature-log-upload", daemon=True
            )
            self._uploader_thread.start()

    def _start_writer(self) -> None:
        env = dict(os.environ)
        env[OPTIONS_ENV_VAR] = json.dumps(self._options.to_dict())
        self._process = subprocess.Popen(
            [sys.executable, "-m", __name__],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env,
        )
        os.set_blocking(self._process.stdin.fileno(), False)
        threading.Thread(
            target=self._read_status,
            args=(self._process,),
            name="hsml-feature-log-writer",
            daemon=True,
        ).start()

    def _ensure_writer(self) -> None:
        # A writer that died is replaced; the new one adopts what it left open.
        if self._process.poll() is not None and not self._stopping.is_set():
            self.writer_restarts += 1
            _logger.warning(
                "Feature log writer exited with %s; starting another",
                self._process.returncode,
            )
            self._start_writer()

    def _read_status(self, process) -> None:
        for line in process.stdout:
            try:
                status = json.loads(line)
            except ValueError:
                continue
            with self._status_lock:
                self._status = status

    def _write_all(self, data: bytes, deadline: float) -> None:
        """Write to the writer's pipe without blocking past the deadline."""
        fd = self._process.stdin.fileno()
        view = memoryview(data)
        while view:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("feature log writer is not reading")
            _, writable, _ = select.select([], [fd], [], remaining)
            if not writable:
                continue
            try:
                written = os.write(fd, view)
            except BlockingIOError:
                continue
            view = view[written:]

    def _send(
        self, kind: int, payload: bytes = b"", rows: int = 0, timeout=None
    ) -> None:
        deadline = time.monotonic() + (
            self._options.handoff_seconds if timeout is None else timeout
        )
        if not self._write_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise TimeoutError("feature log writer handoff is busy")
        try:
            self._ensure_writer()
            try:
                self._write_all(
                    _FRAME.pack(kind, rows, len(payload)) + payload, deadline
                )
            except (TimeoutError, OSError):
                # A frame written in part leaves the pipe mid-frame and the
                # writer would take the next header as payload: this writer is
                # finished, the next submission starts another.
                if self._process.poll() is None:
                    self._process.kill()
                raise
        finally:
            self._write_lock.release()

    def _submit(self, payload: bytes, rows: int) -> None:
        """Hand one serialized IPC stream to the writer."""
        try:
            self._send(_KIND_BATCH, payload, rows)
        except (BrokenPipeError, ValueError, OSError, TimeoutError):
            self.rows_failed += rows
            self.frames_failed += 1
            raise
        self.rows_sent += rows
        self.frames_sent += 1

    def _request_rotation(self) -> None:
        self._send(_KIND_ROTATE)

    def _stats(self) -> dict:
        with self._status_lock:
            status = dict(self._status)
        status["rows_sent"] = self.rows_sent
        status["rows_failed"] = self.rows_failed
        status["frames_sent"] = self.frames_sent
        status["frames_failed"] = self.frames_failed
        status["writer_restarts"] = self.writer_restarts
        status["writer_alive"] = self._process.poll() is None
        return status

    def _flush(self, timeout: float = 60.0) -> bool:
        """Rotate and upload everything the writer holds; `True` when nothing is left."""
        self._request_rotation()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self._stats()
            if (
                status.get("rows_written", -1) + status.get("rows_dropped", 0)
                >= self.rows_sent
                and status.get("bytes_current", 1) == 0
                and status.get("bytes_ready", 1) == 0
            ):
                return True
            if not status["writer_alive"]:
                return False
            time.sleep(0.05)
        return False

    def _upload_loop(self) -> None:
        backoff = 30.0
        while not self._stopping.wait(1.0):
            if self._ready_files():
                if self._upload_ready(None):
                    backoff = 30.0
                else:
                    self._stopping.wait(backoff)
                    backoff = min(600.0, backoff * 2)

    def _ready_files(self) -> list[Path]:
        ready = Path(self._options.buffer_dir) / "ready"
        return sorted(ready.glob("*" + CHUNK_SUFFIX)) if ready.exists() else []

    def _upload_ready(self, deadline: float | None) -> bool:
        """Upload every complete segment from this process; `False` after a failure or at the deadline."""
        with self._upload_lock:
            uploader = _DatasetUploader(self._options, self._project)
            for path in self._ready_files():
                if deadline is not None and time.monotonic() >= deadline:
                    return False
                error = _upload_within(uploader, path, deadline)
                if error is not None:
                    _logger.warning(
                        "Feature log chunk upload failed in the predictor (%s): %s",
                        type(error).__name__,
                        path.name,
                        exc_info=error,
                    )
                    return False
                os.remove(path)
        return True

    def _close(self, timeout: float | None = None) -> bool:
        """Stop the writer, then upload what it left behind from this process.

        One deadline covers the handoff of the stop frame, the writer's own drain, its termination and the fallback upload, so a stuck upload cannot hold the pod past its budget.

        Returns:
            `True` when every segment reached HopsFS.
        """
        deadline = time.monotonic() + (
            self._options.shutdown_seconds if timeout is None else timeout
        )
        self._stopping.set()
        try:
            self._send(_KIND_STOP, timeout=max(0.1, deadline - time.monotonic()))
            self._process.stdin.close()
        except (BrokenPipeError, ValueError, OSError, TimeoutError):
            pass
        try:
            self._process.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
        if self._uploader_thread is not None:
            self._uploader_thread.join(timeout=max(0.1, deadline - time.monotonic()))
        # The writer is gone, so this process is the only one left that can
        # rename: adopt its open segment too and upload from the main thread.
        root = Path(self._options.buffer_dir)
        ready, current = root / "ready", root / "current"
        ready.mkdir(parents=True, exist_ok=True)
        for path in (
            sorted(current.glob("*" + CHUNK_SUFFIX)) if current.exists() else []
        ):
            os.rename(path, ready / path.name)
        if not self._ready_files():
            return True
        if time.monotonic() >= deadline or not self._upload_ready(deadline):
            _logger.error("Feature log chunks left behind on stop under %s", ready)
            return False
        return True


def _upload_within(uploader, path: Path, deadline: float | None):
    """Upload one segment, giving up at the deadline rather than holding the pod past its budget.

    The Dataset API call cannot be interrupted, so it runs on a thread that is abandoned when the deadline passes; the segment stays on disk either way.

    Returns:
        `None` on success, otherwise the error, a `TimeoutError` when the deadline passed.
    """
    outcome: list = []

    def run():
        try:
            uploader._upload(path)
            outcome.append(None)
        except Exception as error:  # noqa: BLE001 - reported to the caller
            outcome.append(error)

    worker = threading.Thread(target=run, name="hsml-feature-log-fallback", daemon=True)
    worker.start()
    worker.join(None if deadline is None else max(0.0, deadline - time.monotonic()))
    if worker.is_alive():
        return TimeoutError(
            "feature log upload did not finish within the shutdown budget"
        )
    return outcome[0]


class _FileLogCollector:
    """Prometheus collector publishing the job transport's counters under the inference logger's metric names.

    KServe serves prometheus_client's default registry on the predictor's metrics endpoint, which the queue-proxy aggregates and Prometheus scrapes with the pod's labels, so the Feature Logging dashboard draws a job-transport deployment from the same queries it uses for the sidecar.
    Every value is read from the transport when scraped, so nothing is counted twice and a writer restart cannot leave a stale series.
    """

    def __init__(self, transport: _FileLogTransport, worker=None):
        self._transport = transport
        # The predictor's log worker owns admission drops and failed log calls.
        self._worker = worker

    def collect(self):
        from prometheus_client.core import (
            CounterMetricFamily,
            GaugeMetricFamily,
            HistogramMetricFamily,
        )

        status = self._transport._stats()
        worker_dropped = getattr(self._worker, "dropped", 0)
        worker_failed = getattr(self._worker, "failed", 0)
        rows = CounterMetricFamily(
            "hopsworks_feature_log_rows",
            "Feature log rows by outcome",
            labels=["outcome"],
        )
        rows.add_metric(["accepted"], status.get("rows_received", 0))
        rows.add_metric(["produced"], status.get("rows_uploaded", 0))
        rows.add_metric(["failed"], self._transport.rows_failed + worker_failed)
        rows.add_metric(["dropped"], status.get("rows_dropped", 0) + worker_dropped)
        yield rows
        events = CounterMetricFamily(
            "hopsworks_feature_log_events",
            "Arrow batches handed to the writer by outcome",
            labels=["type", "outcome"],
        )
        events.add_metric(["arrow", "accepted"], self._transport.frames_sent)
        events.add_metric(["arrow", "rejected"], self._transport.frames_failed)
        yield events
        yield GaugeMetricFamily(
            "hopsworks_feature_log_inflight_rows",
            "Rows written to the pod's buffer and not yet uploaded",
            value=status.get("rows_buffered", 0),
        )
        yield GaugeMetricFamily(
            "hopsworks_feature_log_inflight_bytes",
            "Bytes in the pod's buffer, open segment and rotated segments",
            value=status.get("bytes_current", 0) + status.get("bytes_ready", 0),
        )
        yield GaugeMetricFamily(
            "hopsworks_feature_log_pending_bytes",
            "Bytes of rotated segments awaiting upload",
            value=status.get("bytes_ready", 0),
        )
        histogram = HistogramMetricFamily(
            "hopsworks_feature_log_produce_duration_seconds",
            "Upload duration of one segment to HopsFS",
        )
        counts = status.get("upload_buckets") or [0] * len(UPLOAD_SECONDS_BOUNDS)
        buckets = [
            (str(bound), float(count))
            for bound, count in zip(UPLOAD_SECONDS_BOUNDS, counts, strict=False)
        ]
        buckets.append(("+Inf", float(status.get("upload_count", 0))))
        histogram.add_metric(
            [], buckets, sum_value=float(status.get("upload_seconds_sum", 0.0))
        )
        yield histogram
        chunks = CounterMetricFamily(
            "hopsworks_feature_log_chunks",
            "Segments by upload outcome",
            labels=["outcome"],
        )
        chunks.add_metric(["uploaded"], status.get("chunks_uploaded", 0))
        chunks.add_metric(["failed"], status.get("upload_failures", 0))
        yield chunks
        yield CounterMetricFamily(
            "hopsworks_feature_log_commit_triggers",
            "Commit job runs asked for by this pod",
            value=status.get("commit_triggers", 0),
        )
        yield CounterMetricFamily(
            "hopsworks_feature_log_writer_restarts",
            "Writer processes started after the first",
            value=self._transport.writer_restarts,
        )


def _expose_metrics(transport: _FileLogTransport, worker=None):
    """Register the transport's collector with prometheus_client when it is installed.

    Returns:
        The collector, or `None` when prometheus_client is missing or the registry already holds one.
    """
    try:
        from prometheus_client import REGISTRY
    except ImportError:
        return None
    collector = _FileLogCollector(transport, worker)
    try:
        REGISTRY.register(collector)
    except Exception as error:  # noqa: BLE001 - metrics never stop the predictor
        # ValueError: a reload registered one already.
        _logger.warning("Feature log metrics not exposed: %s", type(error).__name__)
        return None
    return collector


def _main() -> None:
    options = _FileLogOptions.from_dict(json.loads(os.environ[OPTIONS_ENV_VAR]))
    _writer_main(options)


if __name__ == "__main__":
    _main()
