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

import json

import pytest
from hsfs import feature_group
from hsfs.core import incremental_statistics_engine
from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.feature_group_commit import FeatureGroupCommit
from hsfs.statistics import Statistics


MERGEABLE = {"format": "datasketches-native-v1", "hll": "AA==", "moments": {"n": 1}}


def _fg(mocker, time_travel_format="DELTA"):
    fg = mocker.Mock(spec=feature_group.FeatureGroup)
    fg.time_travel_format = time_travel_format
    fg.features = [mocker.Mock(name="a"), mocker.Mock(name="b")]
    fg.features[0].name, fg.features[1].name = "a", "b"
    fg.features[0].type, fg.features[1].type = "bigint", "double"
    return fg


def _window(window_type=mwc.WindowConfigType.ALL_TIME, row_percentage=1.0):
    window = mwc.MonitoringWindowConfig(window_config_type=window_type)
    window._row_percentage = row_percentage
    return window


def _snapshot(end, start=None, mergeable=True):
    return Statistics(
        computation_time=1,
        window_start_commit_time=start,
        window_end_commit_time=end,
        feature_descriptive_statistics=[
            FeatureDescriptiveStatistics(
                feature_name=name,
                count=10,
                extended_statistics={"mergeable": MERGEABLE} if mergeable else None,
            )
            for name in ("a", "b")
        ],
    )


def _commit(time, updated=0, deleted=0):
    return FeatureGroupCommit(
        commit_time=time, rows_inserted=10, rows_updated=updated, rows_deleted=deleted
    )


class TestIncrementalStatisticsEngine:
    @pytest.fixture(autouse=True)
    def _setup(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        self.statistics_engine = mocker.Mock()
        self.engine = incremental_statistics_engine.IncrementalStatisticsEngine(
            self.statistics_engine
        )
        self.variable = mocker.patch.object(
            self.engine._variable_api, "_get_variable", return_value="true"
        )

    # region applies
    def test_applies_to_an_all_time_commit_window_of_a_time_travel_feature_group(
        self, mocker
    ):
        assert self.engine._applies(_fg(mocker), _window(), {"kll": True}, None, 1000)
        assert self.engine._applies(_fg(mocker, "HUDI"), _window(), None, 0, 1000)

    def test_does_not_apply_when_the_setting_is_off(self, mocker):
        self.variable.return_value = "false"

        assert not self.engine._applies(_fg(mocker), _window(), None, None, 1000)

    def test_does_not_apply_when_the_setting_cannot_be_read(self, mocker):
        self.variable.side_effect = Exception("no such variable")

        assert not self.engine._applies(_fg(mocker), _window(), None, None, 1000)

    @pytest.mark.parametrize(
        "flags",
        [{"correlations": True}, {"histograms": True}, {"exact_uniqueness": True}],
    )
    def test_does_not_apply_to_statistics_without_mergeable_state(self, mocker, flags):
        assert not self.engine._applies(_fg(mocker), _window(), flags, None, 1000)

    def test_does_not_apply_to_other_windows_engines_or_formats(self, mocker):
        assert not self.engine._applies(
            _fg(mocker), _window(mwc.WindowConfigType.ROLLING_TIME), None, 500, 1000
        )
        assert not self.engine._applies(
            _fg(mocker), _window(row_percentage=0.5), None, None, 1000
        )
        assert not self.engine._applies(_fg(mocker), _window(), None, 500, 1000)
        assert not self.engine._applies(_fg(mocker), _window(), None, None, None)
        assert not self.engine._applies(
            _fg(mocker, "NONE"), _window(), None, None, 1000
        )
        mocker.patch("hsfs.engine._get_type", return_value="python")
        assert not self.engine._applies(_fg(mocker), _window(), None, None, 1000)

    # endregion

    # region compute
    def _previous(self, mocker, rows):
        self.statistics_engine._statistics_api._get_all_in_window.return_value = rows

    def _commits(self, mocker, commits):
        mocker.patch.object(
            self.engine._feature_group_api, "_get_commit_details", return_value=commits
        )

    def test_merges_the_new_rows_into_the_previous_snapshot(self, mocker):
        fg = _fg(mocker, "HUDI")
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(500)])
        spark_engine = mocker.patch("hsfs.engine._get_instance").return_value
        spark_engine._profile.return_value = '{"columns": []}'
        merger = (
            spark_engine._jvm.com.logicalclocks.hsfs.spark.engine.profile.ProfileMerger
        )
        merger.merge.return_value = (
            '{"columns": [{"column": "a", "numRecordsNonNull": 20}]}'
        )
        self.statistics_engine._parse_deequ_statistics.return_value = [
            FeatureDescriptiveStatistics(feature_name="a", count=20)
        ]

        saved = self.engine._compute(fg, ["a", "b"], 1000, kll=True, histogram_bins=20)

        # the delta is the rows after the previous snapshot, read up to the new commit
        fg.select.assert_called_once_with(["a", "b"])
        fg.select.return_value.as_of.assert_called_once_with(
            exclude_until=500, wallclock_time=1000
        )
        delta_df = fg.select.return_value.as_of.return_value.read.return_value
        spark_engine._profile.assert_called_once_with(
            delta_df, ["a", "b"], False, False, False, True, 20, mergeable_state=True
        )
        previous_json, delta_json = merger.merge.call_args.args
        assert [row["featureName"] for row in json.loads(previous_json)] == ["a", "b"]
        assert delta_json == '{"columns": []}'
        statistics = self.statistics_engine._save_statistics.call_args.args[0]
        assert statistics.window_end_commit_time == 1000
        assert statistics.window_start_commit_time is None
        assert statistics.feature_descriptive_statistics[0].count == 20
        assert saved is self.statistics_engine._save_statistics.return_value

    def _delta_log(self, mocker, versions):
        # version -> commit time recorded in its commitInfo
        delta = mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        delta.DELTA_SPARK_FORMAT = "delta"
        reader = delta.return_value
        reader._delta_version_at.side_effect = lambda location, ts: max(
            (v for v, t in versions.items() if t <= ts), default=None
        )
        reader._delta_commit_timestamp.side_effect = lambda jvm, fs, path, version: (
            versions[version]
        )
        return reader

    def test_reads_delta_commits_by_version_not_by_timestamp(self, mocker):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        self._delta_log(mocker, {0: 500, 1: 1000})
        spark = mocker.patch("hsfs.engine._get_instance").return_value._spark_session
        mocker.patch("pyspark.sql.functions.col")

        df = self.engine._read_commits_after(fg, ["a"], 500, 1000)

        # the change feed after the previous snapshot's version, up to the new commit's
        reader = spark.read.format.return_value
        chain = reader.option
        assert chain.call_args.args == ("readChangeFeed", "true")
        assert chain.return_value.option.call_args.args == ("startingVersion", 1)
        assert chain.return_value.option.return_value.option.call_args.args == (
            "endingVersion",
            1,
        )
        assert df is not None
        fg.select.assert_not_called()

    def test_delta_without_a_version_at_the_recorded_commit_time_falls_back(
        self, mocker
    ):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        # the backend recorded 1000, but the log has no commit at exactly that time
        self._delta_log(mocker, {0: 500, 1: 990})
        mocker.patch("hsfs.engine._get_instance")

        assert self.engine._read_commits_after(fg, ["a"], 500, 1000) is None

    def test_an_error_while_merging_falls_back(self, mocker):
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(500)])
        mocker.patch.object(
            self.engine, "_read_commits_after", side_effect=RuntimeError("no CDF")
        )

        assert self.engine._compute(_fg(mocker), ["a", "b"], 1000) is None

    def test_columns_the_spark_profiler_skips_do_not_block_the_merge(self, mocker):
        # an event-time column is never in a statistics row, mergeable or not
        fg = _fg(mocker, "HUDI")
        event_time = mocker.Mock()
        event_time.name, event_time.type = "ts", "timestamp"
        fg.features.append(event_time)
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(500)])
        spark_engine = mocker.patch("hsfs.engine._get_instance").return_value
        spark_engine._profile.return_value = '{"columns": []}'

        self.engine._compute(fg, None, 1000)

        fg.select.assert_called_once_with(["a", "b"])

    def test_falls_back_without_a_previous_snapshot(self, mocker):
        self._previous(mocker, [_snapshot(500, start=100)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_the_previous_snapshot_has_no_mergeable_state(self, mocker):
        self._previous(mocker, [_snapshot(500, mergeable=False)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_a_commit_in_between_updated_or_deleted_rows(self, mocker):
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(800, updated=3), _commit(500)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_too_many_commits_separate_the_snapshots(self, mocker):
        self._previous(mocker, [_snapshot(1)])
        many = [_commit(1000 - i) for i in range(self.engine.MAX_COMMITS)]
        self._commits(mocker, many)

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    # endregion
