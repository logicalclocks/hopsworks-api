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

import json
import os
import pathlib
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from hsfs.core import feature_log_commit_job as job


def _chunk(path: Path, rows: int, chunk_id: str, truncate: bool = False) -> Path:
    batch = pa.record_batch({"customer_id": list(range(rows)), "score": [0.5] * rows})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(
        sink, batch.schema.with_metadata({b"chunk_id": chunk_id.encode()})
    ) as writer:
        writer.write_batch(batch)
        writer.write_batch(batch)
    data = sink.getvalue().to_pybytes()
    if truncate:
        data = data[: len(data) - 40]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


class TestChunks:
    def test_a_truncated_tail_costs_only_the_last_batch(self, tmp_path):
        whole, truncated = job._read_chunk(_chunk(tmp_path / "a.arrow", 3, "c1"))
        assert whole.num_rows == 6 and not truncated
        table, truncated = job._read_chunk(
            _chunk(tmp_path / "b.arrow", 3, "c2", truncate=True)
        )
        assert truncated
        assert table.num_rows == 3
        assert job._chunk_id_of(table, tmp_path / "b.arrow") == "c2"
        assert job._chunk_id_of(pa.table({"x": [1]}), tmp_path / "noid.arrow") == "noid"

    def test_log_ids_are_derived_so_a_rerun_produces_the_same_ones(self, tmp_path):
        table, _ = job._read_chunk(_chunk(tmp_path / "a.arrow", 2, "chunk-7"))
        first = job._with_log_ids(table, "chunk-7")["log_id"].to_pylist()
        second = job._with_log_ids(table, "chunk-7")["log_id"].to_pylist()
        assert first == second
        assert len(set(first)) == 4
        assert first[0] == str(uuid.uuid5(job.LOG_ID_NAMESPACE, "chunk-7:0"))
        assert job._with_log_ids(table, "chunk-8")["log_id"].to_pylist() != first

    def test_a_frame_keeps_the_chunk_s_arrow_types(self):
        # What the engine infers from the frame has to be the chunk's own schema, or
        # an integer list with a null becomes doubles, a decimal takes the precision
        # of its values and an all-null date becomes a timestamp.
        from decimal import Decimal

        table = pa.table(
            {
                "request_parameters": pa.array([None, None], pa.string()),
                "td_version": pa.array([None, None], pa.int64()),
                "items": pa.array([[2**60 + 1, None], None], pa.list_(pa.int64())),
                "price": pa.array([Decimal("123.45"), None], pa.decimal128(12, 2)),
                "day": pa.array([None, None], pa.date32()),
                "attributes": pa.array(
                    [{"a": 1}, None], pa.map_(pa.string(), pa.int32())
                ),
            }
        )
        frame = job._frame(table)
        assert pa.Table.from_pandas(frame, preserve_index=False).schema.equals(
            table.schema
        )
        assert frame["items"].iloc[0] == [2**60 + 1, None]


class _LocalDatasetApi:
    def __init__(self, root: Path):
        self.root = root
        self.fail_on = None  # ("upload"|"move", occurrence) to inject a crash

    def _p(self, path):
        return self.root / path.lstrip("/")

    def _maybe_fail(self, operation):
        if self.fail_on and self.fail_on[0] == operation:
            self.fail_on = (operation, self.fail_on[1] - 1)
            if self.fail_on[1] < 0:
                self.fail_on = None
                raise OSError(f"injected {operation} failure")

    def exists(self, path):
        return self._p(path).exists()

    def mkdir(self, path):
        self._p(path).mkdir(parents=True, exist_ok=True)

    def list(self, path, offset=0, limit=1000):
        # posix, as the Dataset API answers, so the comparisons hold on Windows too.
        entries = sorted(
            p.relative_to(self.root).as_posix() for p in self._p(path).iterdir()
        )
        return entries[offset : offset + limit]

    def move(self, source, destination, overwrite=False):
        self._maybe_fail("move")
        self._p(source).rename(self._p(destination))

    def upload(self, local_path, upload_path, overwrite=False):
        self._maybe_fail("upload")
        target = self._p(upload_path) / os.path.basename(local_path)
        shutil.copy(local_path, target)
        return target.relative_to(self.root).as_posix()

    def read_content(self, path):
        # The released client answers with a streaming Response.
        return SimpleNamespace(content=self._p(path).read_bytes())

    def download(self, path, local_path=None, overwrite=False):
        target = Path(local_path) / os.path.basename(path)
        shutil.copy(self._p(path), target)
        return str(target)

    def remove(self, path):
        shutil.rmtree(self._p(path))

    def _get(self, path):
        # The released client reports modificationTime as ISO text with a Z suffix.
        modified = datetime.fromtimestamp(
            self._p(path).stat().st_mtime, tz=timezone.utc
        )
        return {
            "attributes": {
                "modificationTime": modified.isoformat().replace("+00:00", "Z")
            }
        }


class _Feature:
    def __init__(self, name, type_="bigint"):
        self.name = name
        self.type = type_


class _FeatureGroup:
    id = 42
    feature_store_id = 1
    feature_store_name = "fs"
    name = "view_1_log"

    def __init__(self, names=("customer_id", "score", "log_id")):
        self.features = [_Feature(n) for n in names]
        self.inserts = []

    def insert(self, dataframe, storage=None, write_options=None):
        self.inserts.append((dataframe, storage, write_options))


class _Transactions:
    """Stands in for the Delta log: remembers which chunks were committed."""

    def __init__(self, committed=()):
        self.committed = set(committed)

    def __call__(self, feature_group):
        return self

    def _committed(self, chunk_id):
        return chunk_id in self.committed


def _committed_chunks(feature_group):
    """The chunk ids each of the group's inserts recorded, in commit order."""
    return [
        [
            t["app_id"].rsplit("/", 1)[-1]
            for t in options["commit_properties"]["transactions"]
        ]
        for _frame, _storage, options in feature_group.inserts
    ]


def _summary():
    return dict.fromkeys(
        (
            "claims_recovered",
            "chunks_already_applied",
            "chunks_claimed",
            "chunks_truncated",
            "chunks_rejected",
            "rows_committed",
            "commits",
        ),
        0,
    )


def _staging(tmp_path, chunks):
    api = _LocalDatasetApi(tmp_path)
    staging = job._Staging(api, "view", 1)
    for name, rows, chunk_id in chunks:
        _chunk(tmp_path / staging.root / "pending" / name, rows, chunk_id)
    return api, staging


def time_far_ahead():
    return 4102444800.0  # 2100-01-01


class TestClaims:
    def test_pending_chunks_are_claimed_committed_once_and_released(
        self, tmp_path, mocker
    ):
        api, staging = _staging(
            tmp_path, [("a.arrow", 2, "dep-1"), ("b.arrow", 1, "dep-2")]
        )
        fg = _FeatureGroup()
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        summary = _summary()

        claim = staging._claim("exec-0", staging._pending())
        assert staging._pending() == []
        manifest = json.loads(
            (tmp_path / staging.root / "claimed/exec-0/claim.json").read_text()
        )
        assert manifest["chunks"] == ["a.arrow", "b.arrow"]
        job._process_claim(fg, staging, claim, summary)

        assert summary["rows_committed"] == 6 and summary["commits"] == 1
        frame, storage, write_options = fg.inserts[0]
        assert storage == "offline"
        assert write_options["mode"] == "append"
        assert write_options["commit_properties"] == {
            "transactions": [
                {"app_id": "hopsworks_feature_log_42/chunk/dep-1", "version": 1},
                {"app_id": "hopsworks_feature_log_42/chunk/dep-2", "version": 1},
            ]
        }
        assert len(frame) == 6
        assert not (tmp_path / staging.root / "claimed/exec-0").exists()

    def test_a_chunk_the_table_records_is_not_committed_again(self, tmp_path, mocker):
        # The same chunk under any claim: a retried claim whose commit landed, or a
        # second copy staged by an upload retry after a lost acknowledgement.
        api, staging = _staging(
            tmp_path, [("a.arrow", 2, "dep-1"), ("b.arrow", 1, "dep-2")]
        )
        fg = _FeatureGroup()
        mocker.patch.object(job, "_DeltaTransactions", _Transactions({"dep-1"}))
        summary = _summary()
        claim = staging._claim("exec-retry", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert _committed_chunks(fg) == [["dep-2"]]
        assert summary["chunks_already_applied"] == 1
        assert summary["rows_committed"] == 2
        assert not (tmp_path / staging.root / "claimed/exec-retry").exists()

    def test_an_older_uncommitted_claim_survives_a_newer_commit(self, tmp_path, mocker):
        # Execution A claims and dies; B claims, commits and is released; A's
        # rows must still be committed when A is recovered, which a shared
        # high-water mark would have counted as already applied.
        api, staging = _staging(
            tmp_path, [("a.arrow", 2, "dep-a"), ("b.arrow", 2, "dep-b")]
        )
        fg = _FeatureGroup()
        transactions = _Transactions()
        mocker.patch.object(job, "_DeltaTransactions", transactions)
        claim_a = staging._claim("exec-a", staging._pending()[:1])
        claim_b = staging._claim("exec-b", staging._pending())
        summary = _summary()
        job._process_claim(fg, staging, claim_b, summary)
        transactions.committed.add("dep-b")
        assert summary["rows_committed"] == 4

        job._process_claim(fg, staging, claim_a, summary)
        assert summary["rows_committed"] == 8
        assert summary["chunks_already_applied"] == 0
        assert _committed_chunks(fg) == [["dep-b"], ["dep-a"]]

    def test_a_crash_between_manifest_and_moves_is_finished_by_recovery(
        self, tmp_path, mocker
    ):
        api, staging = _staging(
            tmp_path, [("a.arrow", 1, "dep-1"), ("b.arrow", 1, "dep-2")]
        )
        api.fail_on = ("move", 1)  # the second move dies
        with pytest.raises(OSError):
            staging._claim("exec-crash", staging._pending())
        assert staging._pending() == [f"{staging.root}/pending/b.arrow"]
        assert (tmp_path / staging.root / "claimed/exec-crash/a.arrow").exists()

        assert staging._recoverable_claims() == []  # too young to touch
        mocker.patch.object(job.time, "time", return_value=time_far_ahead())
        (claim,) = staging._recoverable_claims()
        assert claim["claim_id"] == "exec-crash"
        assert staging._pending() == []
        assert sorted(p.rsplit("/", 1)[-1] for p in staging._chunks(claim)) == [
            "a.arrow",
            "b.arrow",
        ]

    def test_a_crash_before_the_manifest_leaves_pending_untouched(self, tmp_path):
        api, staging = _staging(tmp_path, [("a.arrow", 1, "dep-1")])
        api.fail_on = ("upload", 0)
        with pytest.raises(OSError):
            staging._claim("exec-nomanifest", staging._pending())
        assert staging._pending() == [f"{staging.root}/pending/a.arrow"]
        assert (tmp_path / staging.root / "claimed/exec-nomanifest").exists()

        assert staging._recoverable_claims() == []
        # The empty directory is swept; the file was never moved.
        assert not (tmp_path / staging.root / "claimed/exec-nomanifest").exists()
        assert staging._pending() == [f"{staging.root}/pending/a.arrow"]

    def test_a_chunk_of_another_schema_is_parked_and_the_rest_committed(
        self, tmp_path, mocker
    ):
        api, staging = _staging(tmp_path, [("a.arrow", 2, "dep-1")])
        other = pa.record_batch({"unexpected": [1]})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, other.schema) as writer:
            writer.write_batch(other)
        (tmp_path / staging.root / "pending/z.arrow").write_bytes(
            sink.getvalue().to_pybytes()
        )
        fg = _FeatureGroup()
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        summary = _summary()
        claim = staging._claim("exec-1", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert summary["chunks_rejected"] == 1
        assert summary["rows_committed"] == 4
        assert (tmp_path / staging.root / "failed/exec-1/z.arrow").exists()

    def test_a_run_refuses_a_stream_group(self, mocker):
        """A stream group is not this job's to append to.

        A view switched back to realtime resolves the same name to the group
        Kafka writes, and appending there bypasses Kafka and the online copy.
        """
        import pytest

        fg = _FeatureGroup()
        fg.stream = True
        project = mocker.Mock()
        project.get_feature_store.return_value.get_feature_group.return_value = fg
        mocker.patch.dict(
            "sys.modules", {"hopsworks": mocker.Mock(login=lambda: project)}
        )

        with pytest.raises(RuntimeError, match="stream group that Kafka writes"):
            job._run("view", 1)

    def test_a_chunk_without_one_whole_batch_is_parked_not_dropped(
        self, tmp_path, mocker
    ):
        api, staging = _staging(tmp_path, [("a.arrow", 2, "dep-1")])
        whole = (tmp_path / staging.root / "pending/a.arrow").read_bytes()
        # Short of the first batch's end: nothing at all can be read back.
        (tmp_path / staging.root / "pending/z.arrow").write_bytes(whole[:60])
        fg = _FeatureGroup()
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        summary = _summary()
        claim = staging._claim("exec-short", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert summary["chunks_rejected"] == 1
        assert summary["rows_committed"] == 4
        # The claim's release takes its directory, so the chunk has to be out of it.
        assert (tmp_path / staging.root / "failed/exec-short/z.arrow").exists()

    def test_a_claim_is_committed_in_bounded_parts(self, tmp_path, mocker):
        api, staging = _staging(
            tmp_path,
            [("a.arrow", 4, "dep-1"), ("b.arrow", 4, "dep-2"), ("c.arrow", 4, "dep-3")],
        )
        fg = _FeatureGroup()
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        mocker.patch.object(job, "MAX_PART_BYTES", 1)  # every chunk is its own part
        summary = _summary()
        claim = staging._claim("exec-parts", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert summary["commits"] == 3 and summary["rows_committed"] == 24
        assert _committed_chunks(fg) == [["dep-1"], ["dep-2"], ["dep-3"]]


class TestRealBuilder:
    def test_the_predictors_batch_is_what_the_job_commits(self, tmp_path, mocker):
        # The parity fixture drives the real Arrow builder; a chunk it writes
        # must carry every logging-group column but log_id, including the ones
        # the inference logger otherwise reads from the event headers.
        from tests.core import test_feature_logging_arrow as parity

        fixture, rows = parity._fixture()
        builder, frame, predictions, extra = fixture["_inputs"]
        batch = builder._build_batch(
            frame, predictions, extra, "request-1", parity._TIME
        )
        completed = builder._complete(
            batch,
            {
                "model_name": "fraud",
                "model_version": "2",
                "td_version": 3,
                "deployment_name": "dep",
                "deployment_version": 4,
                "deployment_schema_id": "abc",
            },
        )
        group_columns = {f.name for f in builder._features}
        assert set(completed.schema.names) == group_columns - {"log_id"}
        assert completed.column("model_version").to_pylist() == ["2"] * len(rows)

        api = _LocalDatasetApi(tmp_path)
        staging = job._Staging(api, "view", 1)
        (tmp_path / staging.root / "pending/real.arrow").write_bytes(
            builder._serialize(completed)
        )
        fg = _FeatureGroup()
        fg.features = builder._features
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        summary = _summary()
        claim = staging._claim("real", staging._pending())
        job._process_claim(fg, staging, claim, summary)

        assert summary["chunks_rejected"] == 0
        assert summary["rows_committed"] == len(rows)
        (frame_out, _, _) = fg.inserts[0]
        assert set(frame_out.columns) == group_columns

    def test_a_model_less_deployment_writes_the_none_version_marker(self):
        from tests.core import test_feature_logging_arrow as parity

        fixture, rows = parity._fixture()
        builder, frame, predictions, extra = fixture["_inputs"]
        batch = builder._build_batch(frame, predictions, extra, "r", parity._TIME)
        completed = builder._complete(
            batch, {"model_version": "None", "td_version": None}
        )
        assert completed.column("model_version").to_pylist() == ["None"] * len(rows)
        assert completed.column("td_version").null_count == len(rows)
        assert completed.column("model_name").null_count == len(rows)


def test_the_deployment_version_reaches_the_batch_as_an_integer():
    # DEPLOYMENT_VERSION arrives from the environment as text; the group declares an int.
    from tests.core import test_feature_logging_arrow as parity

    fixture, rows = parity._fixture()
    builder, frame, predictions, extra = fixture["_inputs"]
    batch = builder._build_batch(frame, predictions, extra, "r", parity._TIME)
    declared = {feature.name: feature.type for feature in builder._features}
    if "deployment_version" not in declared:
        pytest.skip("the parity fixture declares no deployment_version column")
    completed = builder._complete(batch, {"deployment_version": "4"})
    assert completed.column("deployment_version").to_pylist() == [4] * len(rows)


def test_a_complete_chunk_stranded_in_uploading_is_adopted_once_stale(tmp_path, mocker):
    # A pod killed between the end of its upload and the rename leaves a whole file
    # behind; an upload still in progress has a .temp companion and is recent.
    api, staging = _staging(tmp_path, [])
    uploading = tmp_path / staging.root / "uploading"
    uploading.mkdir()
    _chunk(uploading / "stranded.arrow", 1, "dep-1")
    _chunk(uploading / "inflight.arrow", 1, "dep-2")
    (uploading / "inflight.arrow.temp").write_bytes(b"")
    assert staging._adopt_stale_uploads() == []  # too young to be sure it is whole
    mocker.patch.object(job.time, "time", return_value=time_far_ahead())
    assert staging._adopt_stale_uploads() == ["stranded.arrow"]
    assert staging._pending() == [f"{staging.root}/pending/stranded.arrow"]
    assert sorted(p.name for p in uploading.iterdir()) == [
        "inflight.arrow",
        "inflight.arrow.temp",
    ]


def test_modification_times_are_read_in_both_wire_forms():
    assert job._modified_at(
        {"modificationTime": "2026-09-12T15:23:31.669Z"}
    ) == pytest.approx(1789226611.669)
    assert job._modified_at({"modificationTime": 1789226611669}) == pytest.approx(
        1789226611.669
    )


def test_the_follow_up_run_ignores_the_execution_asking_for_it():
    # The execution that hits its claim limit is itself still running.
    running = SimpleNamespace(id=77, success=None)
    job_obj = SimpleNamespace(
        get_executions=lambda: [running, SimpleNamespace(id=70, success=True)],
        run=lambda await_termination=False: None,
    )
    project = SimpleNamespace(
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: job_obj)
    )
    assert not job._trigger(project, "j")
    assert job._trigger(project, "j", running_execution="77")


def test_an_old_manifest_naming_paths_is_read_as_names(tmp_path):
    api, staging = _staging(tmp_path, [])
    directory = tmp_path / staging.root / "claimed/old-0"
    directory.mkdir(parents=True)
    (directory / "claim.json").write_text(
        json.dumps(
            {
                "execution_id": "old-0",
                "chunks": [f"{staging.root}/claimed/old-0/x.arrow"],
                "version": 1,
                "claimed_at": 0,
            }
        )
    )
    _chunk(directory / "x.arrow", 1, "x")
    (claim,) = staging._recoverable_claims()
    assert claim["chunks"] == ["x.arrow"]
    assert staging._chunks(claim) == [f"{staging.root}/claimed/old-0/x.arrow"]


def test_dataset_reads_take_absolute_paths():
    assert job._absolute("Resources/x") == "/Resources/x"
    assert job._absolute("/Resources/x") == "/Resources/x"


def test_manifest_content_accepts_dicts_and_responses():
    assert job._manifest_content({"content": '{"a": 1}'}) == '{"a": 1}'
    assert job._manifest_content(SimpleNamespace(content=b'{"b": 2}')) == '{"b": 2}'
    assert job._manifest_content(None) == ""


def test_timestamps_survive_the_round_trip(tmp_path):
    when = datetime(2026, 9, 12, 10, 0, tzinfo=timezone.utc)
    batch = pa.record_batch({"log_time": pa.array([when], pa.timestamp("us"))})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    path = tmp_path / "t.arrow"
    path.write_bytes(sink.getvalue().to_pybytes())
    table, truncated = job._read_chunk(path)
    assert not truncated and table.column("log_time").type == pa.timestamp("us")


class _CheckpointingGroup(_FeatureGroup):
    """A feature group whose delta_checkpoint records the calls, or fails."""

    def __init__(self, error=None):
        super().__init__()
        self.checkpoints = 0
        self._error = error

    def delta_checkpoint(self):
        self.checkpoints += 1
        if self._error is not None:
            raise self._error
        return {"version": 7}


class TestCheckpoint:
    """The log is checkpointed once per execution that committed, never otherwise."""

    def test_a_run_that_committed_checkpoints_once(self):
        fg = _CheckpointingGroup()
        summary = _summary()
        summary["commits"] = 3
        job._checkpoint(fg, summary)
        assert fg.checkpoints == 1
        assert summary["checkpoint"] == {"version": 7}

    def test_a_run_that_committed_nothing_does_not(self):
        fg = _CheckpointingGroup()
        summary = _summary()
        job._checkpoint(fg, summary)
        assert fg.checkpoints == 0
        assert "checkpoint" not in summary

    def test_a_failed_checkpoint_does_not_fail_the_run(self):
        fg = _CheckpointingGroup(error=RuntimeError("log unreadable"))
        summary = _summary()
        summary["commits"] = 1
        job._checkpoint(fg, summary)
        assert summary["checkpoint_error"] == "RuntimeError"
        assert "checkpoint" not in summary


class _MaintainedGroup(_FeatureGroup):
    """A feature group recording the maintenance calls the job makes on it."""

    def __init__(
        self, active_files=1, last_optimize_at=None, fail=None, date_partition=None
    ):
        super().__init__()
        self.calls = []
        self._state = {
            "active_files": active_files,
            "last_optimize_at": last_optimize_at,
            "date_partition": date_partition,
        }
        self._fail = fail
        group = self

        class _Engine:
            @staticmethod
            def _delta_maintenance_state(feature_group):
                return group._state

        self._feature_group_engine = _Engine()

    def delta_optimize(self, after_ingest_date=None, max_concurrent_tasks=1):
        self.calls.append(("optimize", max_concurrent_tasks))
        self.compacted_after = after_ingest_date
        if self._fail == "optimize":
            raise RuntimeError("compaction failed")
        return {"numFilesAdded": 1, "numFilesRemoved": 120}

    def delta_vacuum(self, retention_hours=None):
        self.calls.append(("vacuum", retention_hours))
        return ["a.parquet", "b.parquet"]

    def delta_checkpoint(self):
        self.calls.append(("checkpoint", None))
        return {"version": 3}

    def delta_cleanup_metadata(self):
        self.calls.append(("cleanup", None))
        return {"version": 3}


def _at(day, hour):
    return datetime(2026, 9, day, hour, 30, tzinfo=timezone.utc).timestamp()


class TestCompactionPolicy:
    """When an execution compacts, and in what order it maintains the table."""

    def test_the_file_threshold_triggers_before_midnight(self):
        state = {
            "active_files": job.COMPACT_FILE_THRESHOLD,
            "last_optimize_at": _at(16, 1),
        }
        assert job._should_compact(state, _at(16, 12)) is not None

    def test_a_table_under_the_threshold_waits_for_the_next_day(self):
        state = {"active_files": 3, "last_optimize_at": _at(16, 1)}
        assert job._should_compact(state, _at(16, 12)) is None

    def test_the_first_run_of_the_day_compacts_a_quiet_table(self):
        state = {"active_files": 3, "last_optimize_at": _at(15, 23)}
        assert job._should_compact(state, _at(16, 0)) == "first run of the day"

    def test_a_table_never_compacted_compacts(self):
        state = {"active_files": 1, "last_optimize_at": None}
        assert job._should_compact(state, _at(16, 12)) == "first run of the day"

    def test_maintenance_compacts_then_reclaims_then_checkpoints(self):
        fg = _MaintainedGroup(active_files=200, last_optimize_at=_at(16, 1))
        summary = _summary()
        summary["commits"] = 2
        job._maintain(fg, summary, now=_at(16, 12))
        # Compact, record the compacted state, then the two deletions.
        assert [c[0] for c in fg.calls] == [
            "optimize",
            "checkpoint",
            "cleanup",
            "vacuum",
        ]
        assert ("vacuum", job.COMPACT_VACUUM_RETENTION_HOURS) in fg.calls
        assert ("optimize", job.COMPACT_CONCURRENT_TASKS) in fg.calls
        assert summary["vacuum_deleted"] == 2
        assert summary["active_files"] == 200

    def test_a_dated_group_compacts_only_what_changed(self):
        # Without a scope every daily run rewrites the whole table, including everything
        # earlier runs already compacted, and the cost grows with the log forever.
        fg = _MaintainedGroup(
            active_files=200, last_optimize_at=_at(16, 1), date_partition="log_date"
        )
        summary = _summary()
        summary["commits"] = 2
        job._maintain(fg, summary, now=_at(17, 0))
        # A day of slack before the last compaction, for a row that arrived late.
        assert fg.compacted_after == "2026-09-15"
        assert summary["compact_after"] == "2026-09-15"

    def test_a_group_with_no_date_partition_compacts_whole(self):
        # Only a partition column can select files without reading them, so there is
        # nothing narrower to ask for here.
        fg = _MaintainedGroup(active_files=200, last_optimize_at=_at(16, 1))
        summary = _summary()
        summary["commits"] = 2
        job._maintain(fg, summary, now=_at(17, 0))
        assert fg.compacted_after is None
        assert "compact_after" not in summary

    def test_a_table_never_compacted_compacts_whole(self):
        fg = _MaintainedGroup(
            active_files=200, last_optimize_at=None, date_partition="log_date"
        )
        summary = _summary()
        summary["commits"] = 2
        job._maintain(fg, summary, now=_at(17, 0))
        assert fg.compacted_after is None

    def test_no_commits_means_no_maintenance_at_all(self):
        fg = _MaintainedGroup(active_files=500)
        job._maintain(fg, _summary(), now=_at(16, 12))
        assert fg.calls == []

    def test_below_the_threshold_it_only_checkpoints(self):
        fg = _MaintainedGroup(active_files=3, last_optimize_at=_at(16, 1))
        summary = _summary()
        summary["commits"] = 1
        job._maintain(fg, summary, now=_at(16, 12))
        assert [c[0] for c in fg.calls] == ["checkpoint"]

    def test_a_failed_compaction_still_checkpoints(self):
        fg = _MaintainedGroup(active_files=500, fail="optimize")
        summary = _summary()
        summary["commits"] = 1
        job._maintain(fg, summary, now=_at(16, 12))
        assert summary["maintenance_error"] == "RuntimeError"
        # A failed compaction still checkpoints and still reclaims.
        assert [c[0] for c in fg.calls] == [
            "optimize",
            "checkpoint",
            "cleanup",
            "vacuum",
        ]


class TestTypedCommit:
    """What the predictor logged is what the table holds, type for type."""

    def test_declared_types_survive_the_commit_and_read_back(
        self, tmp_path, monkeypatch
    ):
        # Through the engine's own schema inference and compatibility check, then a
        # local Delta append: an integer list with a null, a decimal whose values do
        # not use its precision, an all-null date, a map, and a nested bigint.
        from decimal import Decimal

        # The fake goes first, then the skip: other tests leave one in sys.modules, and
        # importorskip would find it and not skip, leaving this to fail where there is no
        # real deltalake. hops-deltalake is linux and darwin-arm64 only.
        monkeypatch.delitem(sys.modules, "deltalake", raising=False)
        pytest.importorskip("deltalake")
        from deltalake import DeltaTable, write_deltalake
        from hsfs.core.delta_engine import DeltaEngine
        from hsfs.core.feature_group_base_engine import FeatureGroupBaseEngine
        from hsfs.engine.python import Engine

        declared = [
            ("items", "array<bigint>", pa.list_(pa.int64()), [2**60 + 1, None]),
            (
                "details",
                "struct<a:array<bigint>>",
                pa.struct([("a", pa.list_(pa.int64()))]),
                {"a": [2**60 + 1, None]},
            ),
            ("price", "decimal(12,2)", pa.decimal128(12, 2), Decimal("123.45")),
            ("day", "date", pa.date32(), None),
            (
                "attributes",
                "map<string,int>",
                pa.map_(pa.string(), pa.int32()),
                [("a", 1)],
            ),
            ("log_id", "string", pa.string(), "row"),
        ]
        table = pa.table(
            {name: pa.array([value, None], arrow) for name, _, arrow, value in declared}
        )
        features = [_Feature(name, type_) for name, type_, _, _ in declared]

        frame = job._frame(table)
        inferred = Engine._parse_schema_feature_group(None, frame, features=features)
        FeatureGroupBaseEngine._verify_schema_compatibility(None, features, inferred)
        location = str(tmp_path / "delta")
        write_deltalake(location, DeltaEngine._prepare_df_for_delta(frame))

        stored = DeltaTable(location).to_pyarrow_table().select(table.column_names)
        assert stored.to_pylist() == table.to_pylist()

    def test_a_part_the_group_rejects_is_parked_and_the_claim_released(
        self, tmp_path, mocker
    ):
        # The rejection is the same on every retry; raising it would hold every later
        # chunk behind the parked one.
        from hsfs.client.exceptions import FeatureStoreException

        api, staging = _staging(
            tmp_path, [("a.arrow", 2, "dep-1"), ("b.arrow", 1, "dep-2")]
        )
        fg = _FeatureGroup()

        def reject(*args, **kwargs):
            raise FeatureStoreException("Features are not compatible")

        fg.insert = reject
        mocker.patch.object(job, "_DeltaTransactions", _Transactions())
        summary = _summary()
        claim = staging._claim("exec-bad", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert summary["chunks_rejected"] == 2 and summary["commits"] == 0
        assert sorted(
            p.name for p in (tmp_path / staging.root / "failed/exec-bad").iterdir()
        ) == ["a.arrow", "b.arrow"]
        assert not (tmp_path / staging.root / "claimed/exec-bad").exists()


def test_a_chunk_staged_twice_is_committed_once(tmp_path, mocker, monkeypatch):
    """The same chunk under a later claim commits nothing, through a real Delta log.

    A second copy reaches `pending/` when a writer retries an upload the job had
    already claimed, or when a stale upload is adopted while its writer still retries.
    """
    monkeypatch.delitem(sys.modules, "deltalake", raising=False)
    pytest.importorskip("deltalake")
    from deltalake import DeltaTable, write_deltalake
    from hsfs.core.delta_engine import DeltaEngine

    api, staging = _staging(tmp_path, [("a.arrow", 1, "stable-chunk-id")])
    fg = _FeatureGroup()
    location = str(tmp_path / "delta")

    def insert(frame, storage, write_options):
        write_deltalake(
            location,
            DeltaEngine._prepare_df_for_delta(frame),
            mode="append",
            commit_properties=DeltaEngine._commit_properties(write_options),
        )

    fg.insert = insert

    class Transactions:
        def __init__(self, feature_group):
            pass

        def _committed(self, chunk_id):
            if not os.path.exists(location):
                return False
            app_id = job._chunk_app_id(fg, chunk_id)
            return DeltaTable(location).transaction_version(app_id) is not None

    mocker.patch.object(job, "_DeltaTransactions", Transactions)
    summary = _summary()
    job._process_claim(
        fg, staging, staging._claim("execution-1", staging._pending()), summary
    )
    _chunk(tmp_path / staging.root / "pending" / "a.arrow", 1, "stable-chunk-id")
    job._process_claim(
        fg, staging, staging._claim("execution-2", staging._pending()), summary
    )

    ids = DeltaTable(location).to_pyarrow_table()["log_id"].to_pylist()
    assert len(ids) == 2 and len(set(ids)) == 2
    assert summary["chunks_already_applied"] == 1
    assert summary["commits"] == 1


class TestMaintenanceLimits:
    """The limits must be settable on the job, which a scheduled run actually carries."""

    @staticmethod
    def _defaults():
        return (
            job.COMPACT_FILE_THRESHOLD,
            job.COMPACT_CONCURRENT_TASKS,
            job.COMPACT_VACUUM_RETENTION_HOURS,
            job.COMPACT_LOOKBACK_DAYS,
        )

    def test_arguments_override_and_omissions_keep_the_defaults(self, monkeypatch):
        before = self._defaults()
        try:
            job._maintenance_limits(
                SimpleNamespace(
                    compact_file_threshold=7,
                    compact_concurrent_tasks=None,
                    vacuum_retention_hours=96,
                    compact_lookback_days=None,
                )
            )
            assert job.COMPACT_FILE_THRESHOLD == 7
            assert job.COMPACT_VACUUM_RETENTION_HOURS == 96
            assert before[1] == job.COMPACT_CONCURRENT_TASKS
            assert before[3] == job.COMPACT_LOOKBACK_DAYS
        finally:
            (
                job.COMPACT_FILE_THRESHOLD,
                job.COMPACT_CONCURRENT_TASKS,
                job.COMPACT_VACUUM_RETENTION_HOURS,
                job.COMPACT_LOOKBACK_DAYS,
            ) = before

    def test_a_non_positive_argument_is_floored(self):
        before = self._defaults()
        try:
            job._maintenance_limits(
                SimpleNamespace(
                    compact_file_threshold=0,
                    compact_concurrent_tasks=-3,
                    vacuum_retention_hours=None,
                    compact_lookback_days=None,
                )
            )
            assert job.COMPACT_FILE_THRESHOLD == 1
            assert job.COMPACT_CONCURRENT_TASKS == 1
        finally:
            (
                job.COMPACT_FILE_THRESHOLD,
                job.COMPACT_CONCURRENT_TASKS,
                job.COMPACT_VACUUM_RETENTION_HOURS,
                job.COMPACT_LOOKBACK_DAYS,
            ) = before

    def test_the_names_carry_no_reserved_prefix(self):
        # HOPSWORKS_ is reserved, so a job given such a name is refused and the limit
        # would be settable by nobody.
        source = pathlib.Path(job.__file__).read_text()
        assert "HOPSWORKS_FEATURE_LOG_" not in source
        assert "FEATURE_LOG_COMPACT_FILE_THRESHOLD" in source
