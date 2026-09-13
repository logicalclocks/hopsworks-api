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
import shutil
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

    def test_a_frame_keeps_arrow_types_for_all_null_columns(self):
        table = pa.table(
            {
                "request_parameters": pa.array([None, None], pa.string()),
                "td_version": pa.array([None, None], pa.int64()),
                "customer_id": pa.array([1, 2], pa.int64()),
            }
        )
        group = SimpleNamespace(
            features=[
                SimpleNamespace(name="request_parameters", type="string"),
                SimpleNamespace(name="td_version", type="bigint"),
                SimpleNamespace(name="customer_id", type="bigint"),
            ]
        )
        frame = job._frame(table, group)
        kinds = {name: str(frame[name].dtype).lower() for name in table.column_names}
        # Neither route may hand Delta a `null`-typed column.
        assert "null" not in kinds.values()
        assert "object" not in kinds.values()

    def test_the_pandas_fallback_keeps_nullable_integers_integral(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def without_polars(name, *args, **kwargs):
            if name == "polars":
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", without_polars)
        table = pa.table(
            {
                "td_version": pa.array([1, None], pa.int64()),
                "score": pa.array([None, None], pa.null()),
                "customer_id": pa.array([1, 2], pa.int64()),
            }
        )
        group = SimpleNamespace(
            features=[
                SimpleNamespace(name="td_version", type="bigint"),
                SimpleNamespace(name="score", type="double"),
                SimpleNamespace(name="customer_id", type="bigint"),
            ]
        )
        frame = job._frame(table, group)
        assert str(frame["td_version"].dtype) == "Int64"  # not float64
        assert str(frame["score"].dtype) == "float64"
        assert str(frame["customer_id"].dtype) == "Int64"


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
        entries = sorted(str(p.relative_to(self.root)) for p in self._p(path).iterdir())
        return entries[offset : offset + limit]

    def move(self, source, destination, overwrite=False):
        self._maybe_fail("move")
        self._p(source).rename(self._p(destination))

    def upload(self, local_path, upload_path, overwrite=False):
        self._maybe_fail("upload")
        target = self._p(upload_path) / os.path.basename(local_path)
        shutil.copy(local_path, target)
        return str(target.relative_to(self.root))

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
    """Stands in for the Delta log: remembers which part transactions committed."""

    def __init__(self, committed=()):
        self.committed = set(committed)

    def __call__(self, feature_group):
        return self

    def _committed(self, app_id):
        return app_id in self.committed


def _summary():
    return dict.fromkeys(
        (
            "claims_recovered",
            "parts_already_applied",
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
            "app_id": "hopsworks_feature_log_42/exec-0/0",
            "version": 1,
        }
        assert len(frame) == 6
        assert not (tmp_path / staging.root / "claimed/exec-0").exists()

    def test_a_part_whose_transaction_landed_is_not_committed_again(
        self, tmp_path, mocker
    ):
        api, staging = _staging(tmp_path, [("a.arrow", 2, "dep-1")])
        fg = _FeatureGroup()
        mocker.patch.object(
            job,
            "_DeltaTransactions",
            _Transactions({"hopsworks_feature_log_42/exec-retry/0"}),
        )
        summary = _summary()
        claim = staging._claim("exec-retry", staging._pending())

        job._process_claim(fg, staging, claim, summary)

        assert fg.inserts == []
        assert summary["parts_already_applied"] == 1
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
        transactions.committed.add("hopsworks_feature_log_42/exec-b/0")
        assert summary["rows_committed"] == 4

        job._process_claim(fg, staging, claim_a, summary)
        assert summary["rows_committed"] == 8
        assert summary["parts_already_applied"] == 0
        assert [w["commit_properties"]["app_id"] for _, _, w in fg.inserts] == [
            "hopsworks_feature_log_42/exec-b/0",
            "hopsworks_feature_log_42/exec-a/0",
        ]

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
        assert [w["commit_properties"]["app_id"] for _, _, w in fg.inserts] == [
            f"hopsworks_feature_log_42/exec-parts/{i}" for i in range(3)
        ]


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
