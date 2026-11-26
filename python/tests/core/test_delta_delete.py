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

import os
import sys
import types
from datetime import date

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
    first_left = os.path.join(path, "_delta_log", _commit_files(path)[0])

    oldest = DeltaEngine._oldest_log_commit(deltalake.DeltaTable(path))

    assert oldest["version"] == 1
    assert oldest["timestamp"] == os.stat(first_left).st_mtime_ns // 1_000_000


def test_only_the_newest_commits_are_read_and_the_oldest_still_counts(
    deltalake, tmp_path, monkeypatch
):
    path = str(tmp_path)
    monkeypatch.setattr(DeltaEngine, "LAST_COMMIT_SEARCH", 3)
    for day in range(1, 8):
        deltalake.write_deltalake(path, _days(1, [date(2026, 11, day)]), mode="append")
    first = os.path.join(path, "_delta_log", _commit_files(path)[0])
    read = []
    history = deltalake.DeltaTable.history

    def counted(self, limit=None):
        read.append(limit)
        return history(self, limit)

    monkeypatch.setattr(deltalake.DeltaTable, "history", counted)

    commit = DeltaEngine._get_last_commit_metadata(None, path)

    assert read == [3]
    assert commit.rows_inserted == 1
    assert commit.last_active_commit_time == os.stat(first).st_mtime_ns // 1_000_000


def test_an_empty_predicate_is_refused_before_anything_is_read():
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
