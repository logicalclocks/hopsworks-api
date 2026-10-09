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
from __future__ import annotations

import datetime
import math

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from hsfs import feature_group
from hsfs.core import client_statistics_engine as cse
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.feature_group_commit import FeatureGroupCommit
from hsfs.statistics import Statistics
from hsfs.statistics_config import StatisticsConfig


pytest.importorskip("datasketches")


def _frame(start, stop):
    values = np.arange(start, stop)
    return pd.DataFrame(
        {
            "id": values,
            "amount": np.where(values % 7 == 0, np.nan, values * 1.5),
            "flag": values % 2 == 0,
            "name": [f"n{v % 13}" for v in values],
        }
    )


def _profile(table, name):
    column = table.column(name)
    return cse._ColumnState._of_arrow(column, cse._profile_type(column.type))


def _fds_of(profile):
    # what the backend hands back: the registered row, mergeable state in the extended statistics
    return FeatureDescriptiveStatistics._from_deequ_json(dict(profile))


class TestJavaRendering:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (1.0, "1.0"),
            (123.456, "123.456"),
            (0.001, "0.001"),
            (1e-4, "1.0E-4"),
            (1e7, "1.0E7"),
            (12345678.9, "1.23456789E7"),
            (-2.5e-7, "-2.5E-7"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (math.nan, "NaN"),
            (math.inf, "Infinity"),
            (-math.inf, "-Infinity"),
        ],
    )
    def test_java_double(self, value, expected):
        assert cse._java_double(value) == expected

    def test_java_float(self):
        # Float.toString prints the shortest digits of the single-precision value
        assert cse._java_double(np.float32(0.1), single=True) == "0.1"

    def test_spark_string(self):
        assert cse._spark_string(True, pa.bool_()) == "true"
        assert cse._spark_string(3, pa.int64()) == "3"
        assert (
            cse._spark_string(
                datetime.datetime(2024, 1, 2, 3, 4, 5, 500000), pa.timestamp("us")
            )
            == "2024-01-02 03:04:05.5"
        )
        assert (
            cse._spark_string(datetime.datetime(2024, 1, 2), pa.timestamp("us"))
            == "2024-01-02 00:00:00"
        )
        assert cse._spark_string(datetime.date(2024, 1, 2), pa.date32()) == "2024-01-02"


class TestColumnState:
    def test_merged_state_matches_the_profile_of_the_whole_frame(self):
        whole = pa.Table.from_pandas(_frame(0, 400), preserve_index=False)
        first = pa.Table.from_pandas(_frame(0, 150), preserve_index=False)
        second = pa.Table.from_pandas(_frame(150, 400), preserve_index=False)

        for name in ("id", "amount", "flag", "name"):
            want = _profile(whole, name)._to_profile(name)
            # through the registered row, as the next commit reads it
            previous = cse._ColumnState._of_fds(
                _fds_of(_profile(first, name)._to_profile(name))
            )
            got = previous._plus(_profile(second, name))._to_profile(name)

            assert got["dataType"] == want["dataType"]
            assert got["numRecordsNonNull"] == want["numRecordsNonNull"]
            assert got["numRecordsNull"] == want["numRecordsNull"]
            assert got["completeness"] == pytest.approx(want["completeness"])
            assert got["approximateNumDistinctValues"] == pytest.approx(
                want["approximateNumDistinctValues"], rel=0.05
            )
            for key in ("sum", "mean", "stdDev", "minimum", "maximum"):
                assert (key in got) == (key in want), key
                if key in want:
                    assert got[key] == pytest.approx(want[key], rel=1e-9), key
            if "approxPercentiles" in want:
                spread = want["approxPercentiles"][98] - want["approxPercentiles"][0]
                assert got["approxPercentiles"] == pytest.approx(
                    want["approxPercentiles"], abs=0.05 * spread
                )
            assert got["mergeable"]["format"] == "datasketches-native-v1"

    def test_the_numeric_values_match_pandas(self):
        frame = _frame(0, 200)
        profile = _profile(
            pa.Table.from_pandas(frame, preserve_index=False), "amount"
        )._to_profile("amount")
        amount = frame["amount"].dropna()

        assert profile["numRecordsNull"] == frame["amount"].isna().sum()
        assert profile["sum"] == pytest.approx(amount.sum())
        assert profile["mean"] == pytest.approx(amount.mean())
        assert profile["stdDev"] == pytest.approx(amount.std(ddof=0))
        assert profile["minimum"] == amount.min()
        assert profile["maximum"] == amount.max()
        assert len(profile["approxPercentiles"]) == 99

    def test_nan_follows_spark(self):
        # a NaN that reaches the table (Arrow, polars) is a value: it is the maximum and
        # makes the sum NaN, but it is not the minimum
        table = pa.table({"x": pa.array([1.0, math.nan, 3.0, None])})
        state = _profile(table, "x")
        profile = state._to_profile("x")

        assert state.count == 3 and state.nulls == 1
        assert math.isnan(state.maximum) and state.minimum == 1.0
        assert "sum" not in profile and "maximum" not in profile
        assert profile["minimum"] == 1.0
        assert profile["mergeable"]["moments"]["max"] == "NaN"
        assert math.isnan(cse._ColumnState._of_fds(_fds_of(profile)).maximum)

    def test_an_empty_column_has_the_spark_profilers_completeness(self):
        # the Spark profiler reports 0 for a column with no rows
        table = pa.table({"x": pa.array([], pa.int64())})
        assert _profile(table, "x")._to_profile("x")["completeness"] == 0.0

    def test_a_row_without_mergeable_state_is_not_merged(self):
        assert (
            cse._ColumnState._of_fds(FeatureDescriptiveStatistics(feature_name="x"))
            is None
        )


class TestClientStatisticsEngine:
    @pytest.fixture(autouse=True)
    def _setup(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        self.fg = mocker.Mock(spec=feature_group.FeatureGroup)
        self.fg.feature_store_id = 1
        self.fg.ENTITY_TYPE = "featuregroups"
        self.fg.statistics_config = StatisticsConfig(
            enabled=True, correlations=False, histograms=False, exact_uniqueness=False
        )
        self.fg.features = []
        for name in ("id", "amount", "flag", "name"):
            feature = mocker.Mock()
            feature.name = name
            self.fg.features.append(feature)
        self.engine = cse.ClientStatisticsEngine(self.fg)
        self.enabled = mocker.patch.object(
            self.engine._incremental, "_enabled", return_value=True
        )
        self.commits = mocker.patch.object(
            self.engine._feature_group_api, "_get_commit_details", return_value=[]
        )
        self.snapshot = mocker.patch.object(
            self.engine._incremental, "_previous_snapshot", return_value=None
        )
        self.save = mocker.patch.object(
            self.engine._statistics_engine, "_save_statistics"
        )
        self.compute = mocker.patch.object(
            self.engine._statistics_engine._statistics_api, "_compute"
        )

    def _registered_snapshot(self, end, frame):
        table = pa.Table.from_pandas(frame, preserve_index=False)
        return Statistics(
            computation_time=1,
            window_end_commit_time=end,
            feature_descriptive_statistics=[
                _fds_of(_profile(table, name)._to_profile(name))
                for name in ("id", "amount", "flag", "name")
            ],
        )

    def test_first_commit_registers_the_batch(self):
        prepared = self.engine._prepare(_frame(0, 50))
        assert prepared is not None and prepared.previous_end is None

        self.commits.return_value = [FeatureGroupCommit(commit_time=10)]
        self.engine._register_commit_statistics(
            prepared, FeatureGroupCommit(commit_time=10, rows_inserted=50)
        )

        statistics = self.save.call_args.args[0]
        assert statistics.window_end_commit_time == 10
        assert statistics.window_start_commit_time is None
        by_name = {
            fds.feature_name: fds for fds in statistics.feature_descriptive_statistics
        }
        assert by_name["id"].count == 50
        assert by_name["id"].sum == sum(range(50))
        assert "mergeable" in by_name["id"].extended_statistics
        self.compute.assert_not_called()

    def test_next_commit_merges_into_the_previous_snapshot(self):
        self.commits.return_value = [FeatureGroupCommit(commit_time=10)]
        self.snapshot.return_value = self._registered_snapshot(10, _frame(0, 50))
        prepared = self.engine._prepare(_frame(50, 120))
        assert prepared.previous_end == 10
        self.snapshot.assert_called_once_with(self.fg, 11)

        self.commits.return_value = [
            FeatureGroupCommit(commit_time=20),
            FeatureGroupCommit(commit_time=10),
        ]
        self.engine._register_commit_statistics(
            prepared, FeatureGroupCommit(commit_time=20, rows_inserted=70)
        )

        statistics = self.save.call_args.args[0]
        by_name = {
            fds.feature_name: fds for fds in statistics.feature_descriptive_statistics
        }
        assert statistics.window_end_commit_time == 20
        assert by_name["id"].count == 120
        assert by_name["id"].sum == sum(range(120))
        assert by_name["id"].mean == pytest.approx(np.mean(range(120)))
        self.compute.assert_not_called()

    @pytest.mark.parametrize(
        "config",
        [
            {"correlations": True},
            {"histograms": True},
            {"exact_uniqueness": True},
            {"kll": True},
            {"enabled": False},
        ],
    )
    def test_statistics_without_mergeable_state_stay_with_the_job(self, config):
        for key, value in config.items():
            setattr(self.fg.statistics_config, key, value)
        assert self.engine._prepare(_frame(0, 10)) is None

    def test_setting_off_stays_with_the_job(self):
        self.enabled.return_value = False
        assert self.engine._prepare(_frame(0, 10)) is None

    def test_large_frames_stay_with_the_job(self, mocker):
        mocker.patch.object(cse.ClientStatisticsEngine, "MAX_ROWS", 5)
        assert self.engine._prepare(_frame(0, 10)) is None

    def test_columns_the_spark_profiler_skips_are_skipped(self):
        # a merge emits only the batch's columns, so both profilers must cover the same ones
        frame = _frame(0, 10)
        frame["name"] = [[1, 2]] * 10
        frame["flag"] = pd.Timestamp("2024-01-01")
        prepared = self.engine._prepare(frame)
        assert prepared.names == ["id", "amount"]

    def test_decimal_columns_stay_with_the_job(self):
        import decimal

        frame = _frame(0, 10)
        frame["amount"] = [decimal.Decimal("1.50")] * 10
        assert self.engine._prepare(frame) is None

    def test_a_previous_commit_without_snapshot_stays_with_the_job(self):
        self.commits.return_value = [FeatureGroupCommit(commit_time=10)]
        assert self.engine._prepare(_frame(0, 10)) is None
        # a snapshot of an older commit does not describe the table either
        self.snapshot.return_value = self._registered_snapshot(5, _frame(0, 5))
        assert self.engine._prepare(_frame(0, 10)) is None

    def test_an_error_while_preparing_stays_with_the_job(self):
        self.commits.side_effect = RuntimeError("boom")
        assert self.engine._prepare(_frame(0, 10)) is None

    @pytest.mark.parametrize(
        "commit, history",
        [
            (FeatureGroupCommit(commit_time=20, rows_inserted=10, rows_updated=2), []),
            (FeatureGroupCommit(commit_time=20, rows_inserted=10, rows_deleted=1), []),
            (FeatureGroupCommit(commit_time=20, rows_inserted=9), []),
            # a concurrent writer committed before this commit
            (
                FeatureGroupCommit(commit_time=20, rows_inserted=10),
                [
                    FeatureGroupCommit(commit_time=20),
                    FeatureGroupCommit(commit_time=15),
                ],
            ),
        ],
    )
    def test_a_commit_the_profile_does_not_describe_starts_the_job(
        self, commit, history
    ):
        prepared = self.engine._prepare(_frame(0, 10))
        self.commits.return_value = history

        self.engine._register_commit_statistics(prepared, commit)

        self.save.assert_not_called()
        self.compute.assert_called_once_with(self.fg, end_commit_time=20)

    def test_a_failed_registration_starts_the_job(self):
        prepared = self.engine._prepare(_frame(0, 10))
        self.commits.return_value = [FeatureGroupCommit(commit_time=20)]
        self.save.side_effect = RuntimeError("boom")

        self.engine._register_commit_statistics(
            prepared, FeatureGroupCommit(commit_time=20, rows_inserted=10)
        )

        self.compute.assert_called_once_with(self.fg, end_commit_time=20)


class TestFeatureGroupCommit:
    def test_statistics_supplied_is_sent_only_when_set(self):
        assert "statisticsSupplied" not in FeatureGroupCommit(commit_time=1).to_dict()
        commit = FeatureGroupCommit(commit_time=1)
        commit.statistics_supplied = True
        assert commit.to_dict()["statisticsSupplied"] is True
