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
"""Predicate deletes and the bounded commit read, on real local Delta tables."""

import json
import os
import sys
import types
from datetime import date

import hopsworks_common.connection
import pyarrow as pa
import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group
from hsfs.core.delta_engine import DeltaEngine
from hsfs.core.feature_group_engine import FeatureGroupEngine


@pytest.fixture
def deltalake(monkeypatch):
    """The installed deltalake, not a stand-in another test left in sys.modules."""
    for name in list(sys.modules):
        if name == "deltalake" or name.startswith("deltalake."):
            module = sys.modules[name]
            if not isinstance(module, types.ModuleType) or not getattr(
                module, "__file__", None
            ):
                monkeypatch.delitem(sys.modules, name)
    return pytest.importorskip("deltalake")


def _days(n_per_day, days):
    return pa.table(
        {
            "trace_id": [f"{d}-{i}" for d in days for i in range(n_per_day)],
            "day": pa.array([d for d in days for _ in range(n_per_day)], pa.date32()),
        }
    )


def _commit_time(path, name):
    """The commitInfo timestamp of one commit file."""
    with open(os.path.join(path, "_delta_log", name)) as f:
        return next(
            json.loads(line)["commitInfo"]["timestamp"]
            for line in f
            if '"commitInfo"' in line
        )


def _commit_files(path):
    log = os.path.join(path, "_delta_log")
    return sorted(f for f in os.listdir(log) if f.endswith(".json"))


def test_a_partition_delete_drops_files_and_is_read_back_as_a_delete_commit(
    deltalake, tmp_path
):
    path = str(tmp_path)
    deltalake.write_deltalake(
        path, _days(3, [date(2026, 11, 1), date(2026, 11, 2)]), partition_by=["day"]
    )

    metrics = deltalake.DeltaTable(path).delete("day < DATE '2026-11-02'")
    commit = DeltaEngine._get_last_commit_metadata(None, path)

    assert metrics["num_copied_rows"] == 0, "a partition-only delete rewrote rows"
    assert metrics["num_added_files"] == 0
    assert metrics["num_removed_files"] == 1
    assert deltalake.DeltaTable(path).to_pyarrow_table().num_rows == 3
    # delta-rs counts no rows when it drops whole files
    assert commit.rows_deleted == 0
    assert commit.rows_inserted == 0
    assert commit.commit_time is not None


def test_a_row_delete_is_read_back_with_its_deleted_rows(deltalake, tmp_path):
    path = str(tmp_path)
    deltalake.write_deltalake(path, _days(3, [date(2026, 11, 1)]), partition_by=["day"])

    deltalake.DeltaTable(path).delete("trace_id IN ('2026-11-01-0', '2026-11-01-2')")
    commit = DeltaEngine._get_last_commit_metadata(None, path)

    assert commit.rows_deleted == 2
    assert commit.rows_inserted == 0


def test_the_oldest_commit_is_the_oldest_log_file_left(deltalake, tmp_path):
    path = str(tmp_path)
    for day in (1, 2, 3):
        deltalake.write_deltalake(path, _days(1, [date(2026, 11, day)]), mode="append")
    # as a checkpoint and a log cleanup leave it: the first commit file gone
    deltalake.DeltaTable(path).create_checkpoint()
    os.remove(os.path.join(path, "_delta_log", _commit_files(path)[0]))
    first_left = _commit_files(path)[0]

    oldest = DeltaEngine._oldest_log_commit(deltalake.DeltaTable(path))

    assert oldest["version"] == 1
    assert oldest["timestamp"] == _commit_time(path, first_left)


def test_only_the_newest_commits_are_read_and_the_oldest_still_counts(
    deltalake, tmp_path, monkeypatch
):
    path = str(tmp_path)
    monkeypatch.setattr(DeltaEngine, "LAST_COMMIT_SEARCH", 3)
    for day in range(1, 8):
        deltalake.write_deltalake(path, _days(1, [date(2026, 11, day)]), mode="append")
    first = _commit_files(path)[0]
    read = []
    history = deltalake.DeltaTable.history

    def counted(self, limit=None):
        read.append(limit)
        return history(self, limit)

    monkeypatch.setattr(deltalake.DeltaTable, "history", counted)

    commit = DeltaEngine._get_last_commit_metadata(None, path)

    assert read == [3]
    assert commit.rows_inserted == 1
    assert commit.last_active_commit_time == _commit_time(path, first)


def test_a_write_after_the_delete_does_not_take_its_place(deltalake, tmp_path):
    path = str(tmp_path)
    deltalake.write_deltalake(path, _days(3, [date(2026, 11, 1)]), partition_by=["day"])
    table = deltalake.DeltaTable(path)
    before = table.version()
    table.delete("trace_id = '2026-11-01-0'")
    deleted_at = table.version()
    # the sidecar's append, between the delete and the commit read
    deltalake.write_deltalake(path, _days(2, [date(2026, 11, 2)]), mode="append")

    own = DeltaEngine._get_last_commit_metadata(
        None, path, after=before, operations=("DELETE",)
    )
    newest = DeltaEngine._get_last_commit_metadata(None, path)

    assert (own.rows_deleted, own.rows_inserted) == (1, 0)
    assert own.commit_time == _commit_time(path, f"{deleted_at:020}.json")
    assert (newest.rows_deleted, newest.rows_inserted) == (0, 2)


def test_a_delete_after_a_write_does_not_take_the_write_s_place(deltalake, tmp_path):
    path = str(tmp_path)
    deltalake.write_deltalake(path, _days(1, [date(2026, 11, 1)]), partition_by=["day"])
    before = deltalake.DeltaTable(path).version()
    deltalake.write_deltalake(path, _days(3, [date(2026, 11, 2)]), mode="append")
    # the daily job's delete, between the append and its commit read
    deltalake.DeltaTable(path).delete("day = DATE '2026-11-01'")

    own = DeltaEngine._get_last_commit_metadata(
        None, path, after=before, operations=("WRITE", "MERGE")
    )

    assert (own.rows_inserted, own.rows_deleted) == (3, 0)


def test_a_merge_delete_is_read_back_past_a_later_append(deltalake, tmp_path):
    path = str(tmp_path)
    deltalake.write_deltalake(path, _days(3, [date(2026, 11, 1)]), partition_by=["day"])
    table = deltalake.DeltaTable(path)
    before = table.version()
    table.merge(
        source=pa.table({"trace_id": ["2026-11-01-0"]}),
        predicate="s.trace_id = t.trace_id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete().execute()
    # another writer's append, between the merge and its commit read
    deltalake.write_deltalake(path, _days(2, [date(2026, 11, 2)]), mode="append")

    own = DeltaEngine._get_last_commit_metadata(
        None, path, after=before, operations=("MERGE",)
    )

    assert (own.rows_deleted, own.rows_inserted) == (1, 0)


def test_no_commit_is_registered_when_none_of_the_newest_is_a_data_commit(
    deltalake, tmp_path, monkeypatch
):
    path = str(tmp_path)
    monkeypatch.setattr(DeltaEngine, "LAST_COMMIT_SEARCH", 2)
    deltalake.write_deltalake(path, _days(1, [date(2026, 11, 1)]))
    table = deltalake.DeltaTable(path)
    table.alter.set_table_properties({"delta.logRetentionDuration": "interval 2 days"})
    table.alter.set_table_properties({"delta.enableChangeDataFeed": "false"})

    assert DeltaEngine._get_last_commit_metadata(None, path) is None
    assert (
        DeltaEngine._get_last_commit_metadata(None, path, after=table.version() - 2)
        is None
    )


def test_an_empty_predicate_is_refused_before_anything_is_read(monkeypatch):
    # FeatureGroup reads the engine type; without this the test passes only after another set it.
    monkeypatch.setattr(hopsworks_common.connection, "_hsfs_engine_type", "python")
    fg = feature_group.FeatureGroup(
        name="otel_spans",
        version=1,
        featurestore_id=1,
        primary_key=[],
        partition_key=[],
        time_travel_format="DELTA",
    )

    for predicate in ("", "   ", None):
        with pytest.raises(FeatureStoreException, match="needs a predicate"):
            FeatureGroupEngine._delta_delete(fg, predicate)
