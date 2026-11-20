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
from hsfs.statistics_config import StatisticsConfig


MERGEABLE = {"format": "datasketches-native-v1", "hll": "AA==", "moments": {"n": 1}}


def _fg(mocker, time_travel_format="DELTA", incremental=None):
    fg = mocker.Mock(spec=feature_group.FeatureGroup)
    fg.statistics_config = StatisticsConfig(incremental=incremental)
    fg.time_travel_format = time_travel_format
    fg.features = [mocker.Mock(name="a"), mocker.Mock(name="b")]
    fg.features[0].name, fg.features[1].name = "a", "b"
    fg.features[0].type, fg.features[1].type = "bigint", "double"
    return fg


def _window(window_type=mwc.WindowConfigType.ALL_TIME, row_percentage=1.0):
    window = mwc.MonitoringWindowConfig(window_config_type=window_type)
    window._row_percentage = row_percentage
    return window


def _snapshot(end, start=None, mergeable=True, merges=None):
    state = dict(MERGEABLE) if merges is None else {**MERGEABLE, "merges": merges}
    return Statistics(
        computation_time=1,
        window_start_commit_time=start,
        window_end_commit_time=end,
        feature_descriptive_statistics=[
            FeatureDescriptiveStatistics(
                feature_name=name,
                count=10,
                extended_statistics={"mergeable": state} if mergeable else None,
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
        self.variables = {
            "statistics_incremental_enabled": "true",
            "statistics_incremental_allowed": "true",
            "statistics_incremental_full_profile_every": "50",
        }

        def get_variable(name):
            value = self.variables[name]
            if isinstance(value, Exception):
                raise value
            return value

        mocker.patch.object(
            self.engine._variable_api, "_get_variable", side_effect=get_variable
        )

    # region applies
    def test_applies_to_an_all_time_commit_window_of_a_time_travel_feature_group(
        self, mocker
    ):
        assert self.engine._applies(_fg(mocker), _window(), {"kll": True}, None, 1000)
        assert self.engine._applies(_fg(mocker, "HUDI"), _window(), None, 0, 1000)

    def test_does_not_apply_when_the_setting_is_off(self, mocker):
        self.variables["statistics_incremental_enabled"] = "false"

        assert not self.engine._applies(_fg(mocker), _window(), None, None, 1000)

    def test_does_not_apply_when_the_setting_cannot_be_read(self, mocker):
        self.variables["statistics_incremental_enabled"] = Exception("no variable")

        assert not self.engine._applies(_fg(mocker), _window(), None, None, 1000)

    @pytest.mark.parametrize(
        ("flag", "enabled", "allowed", "expected"),
        [
            (None, "true", "true", True),
            (None, "false", "true", False),
            (True, "false", "true", True),
            (False, "true", "true", False),
            (None, "true", "false", False),
            (True, "true", "false", False),
            (False, "false", "false", False),
            (True, "false", Exception("missing"), False),
            (None, "true", Exception("missing"), False),
            (None, Exception("missing"), "true", False),
            (True, Exception("missing"), "true", True),
        ],
    )
    def test_effective_value_of_the_flag_and_the_cluster_variables(
        self, mocker, flag, enabled, allowed, expected
    ):
        self.variables["statistics_incremental_enabled"] = enabled
        self.variables["statistics_incremental_allowed"] = allowed

        assert self.engine._enabled(_fg(mocker, incremental=flag)) is expected

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
        # listed newest end first without content; a snapshot's content is read by its end commit
        api = self.statistics_engine._statistics_api
        api._get_all_in_window.return_value = sorted(
            rows, key=lambda row: row.window_end_commit_time, reverse=True
        )
        snapshots = {
            row.window_end_commit_time: row
            for row in rows
            if row.window_start_commit_time in (None, 0)
        }
        api._get.side_effect = lambda entity, end_commit_time, **kwargs: snapshots.get(
            end_commit_time
        )

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

    def _delta_log(self, mocker, versions, file_times=None):
        # version -> commit time recorded in its commitInfo, and the commit file's
        # modification time, which a Spark writer records and which defaults to later
        delta = mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        delta.DELTA_SPARK_FORMAT = "delta"
        reader = delta.return_value
        reader._delta_version_at.side_effect = lambda location, ts: max(
            (v for v, t in versions.items() if t <= ts), default=None
        )
        reader._delta_commit_timestamp.side_effect = lambda jvm, fs, path, version: (
            versions[version]
        )
        file_times = file_times or {v: t + 150 for v, t in versions.items()}
        reader._delta_commit_file_time.side_effect = lambda jvm, fs, path, version: (
            file_times[version]
        )
        return reader

    @staticmethod
    def _feed(spark, non_insert_rows, change_data_feed="true"):
        spark.sql.return_value.first.return_value = {
            "properties": {"delta.enableChangeDataFeed": change_data_feed}
        }
        reader = spark.read.format.return_value
        feed = reader.option.return_value.option.return_value.option.return_value.load.return_value
        feed.filter.return_value.limit.return_value.count.return_value = non_insert_rows
        return feed

    def test_a_change_feed_with_updated_or_deleted_rows_falls_back(self, mocker):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        # the commit times are in order, yet the feed between the versions holds an update
        self._delta_log(mocker, {0: 500, 1: 1000})
        spark = mocker.patch("hsfs.engine._get_instance").return_value._spark_session
        mocker.patch("pyspark.sql.functions.col")
        feed = self._feed(spark, non_insert_rows=1)

        assert self.engine._read_commits_after(fg, ["a"], 500, 1000) is None
        feed.filter.return_value.limit.assert_called_once_with(1)
        feed.filter.return_value.select.assert_not_called()

    @pytest.mark.parametrize("change_data_feed", ["false", None])
    def test_a_delta_table_without_a_change_data_feed_falls_back_quietly(
        self, mocker, caplog, change_data_feed
    ):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        reader = self._delta_log(mocker, {0: 500, 1: 1000})
        spark = mocker.patch("hsfs.engine._get_instance").return_value._spark_session
        feed = self._feed(spark, non_insert_rows=0, change_data_feed=change_data_feed)

        with caplog.at_level("DEBUG"):
            assert self.engine._read_commits_after(fg, ["a"], 500, 1000) is None

        spark.sql.assert_called_once_with("DESCRIBE DETAIL delta.`hopsfs://nn/fg`")
        reader._delta_version_at.assert_not_called()
        spark.read.format.return_value.option.assert_not_called()
        assert not [r for r in caplog.records if r.levelname == "WARNING"]
        assert feed is not None

    def test_reads_delta_commits_by_version_not_by_timestamp(self, mocker):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        self._delta_log(mocker, {0: 500, 1: 1000})
        spark = mocker.patch("hsfs.engine._get_instance").return_value._spark_session
        mocker.patch("pyspark.sql.functions.col")
        self._feed(spark, non_insert_rows=0)

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

    def test_reads_delta_commits_a_spark_writer_recorded_by_file_time(self, mocker):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        # a Spark writer records the commit file's modification time, which on HopsFS is
        # some hundred milliseconds after the commitInfo timestamp
        self._delta_log(mocker, {0: 500, 1: 1000}, file_times={0: 858, 1: 1152})
        spark = mocker.patch("hsfs.engine._get_instance").return_value._spark_session
        mocker.patch("pyspark.sql.functions.col")
        self._feed(spark, non_insert_rows=0)

        assert self.engine._read_commits_after(fg, ["a"], 858, 1152) is not None
        chain = spark.read.format.return_value.option
        assert chain.return_value.option.call_args.args == ("startingVersion", 1)
        assert chain.return_value.option.return_value.option.call_args.args == (
            "endingVersion",
            1,
        )

    def test_delta_without_a_version_at_the_recorded_commit_time_falls_back(
        self, mocker
    ):
        fg = _fg(mocker, "DELTA")
        fg.prepare_spark_location.return_value = "hopsfs://nn/fg"
        # the backend recorded 1000, but the log has no commit at exactly that time
        self._delta_log(mocker, {0: 500, 1: 990})
        mocker.patch("hsfs.engine._get_instance")

        assert self.engine._read_commits_after(fg, ["a"], 500, 1000) is None

    def test_the_previous_snapshot_is_the_newest_earlier_commit_with_one(self, mocker):
        # the commit at 800 has only a rolling window row, the one at 500 a snapshot
        self._previous(mocker, [_snapshot(800, start=100), _snapshot(500)])

        previous = self.engine._previous_snapshot(
            _fg(mocker), [_commit(800), _commit(500), _commit(200)]
        )

        assert previous.window_end_commit_time == 500
        api = self.statistics_engine._statistics_api
        listing = api._get_all_in_window.call_args.kwargs
        assert listing["end_commit_time"] == 800
        assert listing["with_content"] is False
        assert listing["row_percentage"] == 1.0
        # only the snapshot's content is read
        api._get.assert_called_once()
        assert api._get.call_args.kwargs["end_commit_time"] == 500
        assert api._get.call_args.kwargs["with_content"] is True

    def test_the_snapshot_content_is_read_with_no_start_bound_and_checked(self, mocker):
        # a rolling window registered at the snapshot's commit, at 100% of the rows
        self._previous(mocker, [_snapshot(500)])
        api = self.statistics_engine._statistics_api
        api._get.side_effect = None
        api._get.return_value = _snapshot(500, start=100)

        assert (
            self.engine._previous_snapshot(_fg(mocker), [_commit(800), _commit(500)])
            is None
        )
        assert api._get.call_args.kwargs["start_commit_time"] == 0

    def test_a_snapshot_many_commits_back_is_found(self, mocker):
        # a feature group committing faster than its statistics runs skips commits between snapshots
        commits = [_commit(1000 - i) for i in range(60)]
        self._previous(mocker, [_snapshot(941)])

        previous = self.engine._previous_snapshot(_fg(mocker), commits[1:])

        assert previous.window_end_commit_time == 941

    def test_rolling_window_rows_do_not_hide_the_previous_snapshot(self, mocker):
        # any number of other rows may end before the new commit, the lookup is by commit
        self._previous(
            mocker,
            [_snapshot(500)] + [_snapshot(900 - i, start=100) for i in range(10)],
        )
        self._commits(mocker, [_commit(1000), _commit(900), _commit(500)])
        spark_engine = mocker.patch("hsfs.engine._get_instance").return_value
        spark_engine._profile.return_value = '{"columns": []}'
        self.statistics_engine._parse_deequ_statistics.return_value = []

        assert (
            self.engine._merge(_fg(mocker, "HUDI"), ["a", "b"], 1000, False, None)
            is not None
        )

    def test_a_commit_at_the_previous_snapshot_is_not_part_of_the_new_rows(self):
        # the previous snapshot already holds the commit at 500, whatever it updated
        assert self.engine._append_only_after(
            [_commit(1000), _commit(500, updated=3)], 500
        )

    def test_a_commit_after_the_previous_snapshot_that_updated_rows_blocks_the_merge(
        self,
    ):
        assert not self.engine._append_only_after(
            [_commit(1000, updated=1), _commit(500)], 500
        )

    def test_a_scheduled_window_ending_at_wall_clock_time_merges_up_to_the_latest_commit(
        self, mocker
    ):
        fg = _fg(mocker, "HUDI")
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(500)])
        spark_engine = mocker.patch("hsfs.engine._get_instance").return_value
        spark_engine._profile.return_value = '{"columns": []}'
        self.statistics_engine._parse_deequ_statistics.return_value = []

        assert self.engine._compute(fg, ["a", "b"], 5000) is not None

        self.engine._feature_group_api._get_commit_details.assert_called_once_with(
            fg, 5000, self.engine.MAX_COMMITS + 1
        )
        fg.select.return_value.as_of.assert_called_once_with(
            exclude_until=500, wallclock_time=1000
        )
        statistics = self.statistics_engine._save_statistics.call_args.args[0]
        assert statistics.window_end_commit_time == 1000

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

    def _merge_with_counter(self, mocker, merges, every):
        self.variables["statistics_incremental_full_profile_every"] = every
        self._previous(mocker, [_snapshot(500, merges=merges)])
        self._commits(mocker, [_commit(1000), _commit(500)])
        spark_engine = mocker.patch("hsfs.engine._get_instance").return_value
        spark_engine._profile.return_value = '{"columns": []}'
        self.statistics_engine._parse_deequ_statistics.return_value = []
        return self.engine._merge(_fg(mocker, "HUDI"), ["a", "b"], 1000, False, None)

    def test_merges_below_the_full_profile_threshold(self, mocker):
        assert self._merge_with_counter(mocker, 49, "50") is not None

    def test_profiles_in_full_at_the_threshold(self, mocker):
        assert self._merge_with_counter(mocker, 50, "50") is None

    def test_the_counter_of_any_feature_triggers_the_full_profile(self, mocker):
        snapshot = _snapshot(500, merges=1)
        snapshot.feature_descriptive_statistics[1].extended_statistics["mergeable"] = {
            **MERGEABLE,
            "merges": 7,
        }
        self.variables["statistics_incremental_full_profile_every"] = "5"
        self._previous(mocker, [snapshot])
        self._commits(mocker, [_commit(1000), _commit(500)])

        assert (
            self.engine._merge(_fg(mocker, "HUDI"), ["a", "b"], 1000, False, None)
            is None
        )

    def test_zero_never_forces_a_full_profile(self, mocker):
        assert self._merge_with_counter(mocker, 1000, "0") is not None

    def test_a_missing_counter_counts_as_zero(self, mocker):
        assert self._merge_with_counter(mocker, None, "1") is not None

    def test_an_unreadable_threshold_defaults_to_fifty(self, mocker):
        assert self._merge_with_counter(mocker, 49, Exception("missing")) is not None
        assert self._merge_with_counter(mocker, 50, Exception("missing")) is None

    def test_falls_back_without_a_previous_snapshot(self, mocker):
        self._previous(mocker, [_snapshot(500, start=100)])
        self._commits(mocker, [_commit(1000), _commit(500)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_the_previous_snapshot_has_no_mergeable_state(self, mocker):
        self._previous(mocker, [_snapshot(500, mergeable=False)])
        self._commits(mocker, [_commit(1000), _commit(500)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_a_commit_in_between_updated_or_deleted_rows(self, mocker):
        self._previous(mocker, [_snapshot(500)])
        self._commits(mocker, [_commit(1000), _commit(800, updated=3), _commit(500)])

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None

    def test_falls_back_when_the_previous_snapshot_is_beyond_the_look_back(
        self, mocker
    ):
        # the API returns at most the commits looked back over, the snapshot is older
        self._previous(mocker, [_snapshot(1)])
        many = [_commit(1000 - i) for i in range(self.engine.MAX_COMMITS + 1)]
        self._commits(mocker, many)

        assert self.engine._compute(_fg(mocker), ["a"], 1000) is None
        self.statistics_engine._statistics_api._get.assert_not_called()

    # endregion
