#
#   Copyright 2024 Hopsworks AB
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
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call

import pytest
from hsfs import feature_group, feature_view, util
from hsfs.constructor import filter as filter_module
from hsfs.constructor import query
from hsfs.core import monitoring_window_config as mwc
from hsfs.core import monitoring_window_config_engine as mwce
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


DEFAULT_FEATURE_NAME = "amount"

ENGINE_GET_TYPE = "hsfs.engine._get_type"
CLIENT_GET_INSTANCE = "hopsworks_common.client._get_instance"


class TestMonitoringWindowConfigEngine:
    def test_time_range_str_to_time_delta(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        all_one_expression = "1w1d1h"
        negative_expression = "-1w-1d-1h"
        disordered_expression = "2h1d1w"
        double_expression = "2h3d"
        just_days_expression = "3d"
        just_hours_expression = "4h"
        just_weeks_expression = "5w"
        just_minutes_expression = "6m"
        just_seconds_expression = "7s"
        just_months_expression = "8M"
        just_years_expression = "9y"

        # Act
        all_one = monitoring_window_config_engine._time_range_str_to_time_delta(
            all_one_expression
        )
        just_days = monitoring_window_config_engine._time_range_str_to_time_delta(
            just_days_expression
        )
        just_hours = monitoring_window_config_engine._time_range_str_to_time_delta(
            just_hours_expression
        )
        just_weeks = monitoring_window_config_engine._time_range_str_to_time_delta(
            just_weeks_expression
        )
        disordered = monitoring_window_config_engine._time_range_str_to_time_delta(
            disordered_expression
        )
        double = monitoring_window_config_engine._time_range_str_to_time_delta(
            double_expression
        )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine._time_range_str_to_time_delta(
                negative_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine._time_range_str_to_time_delta(
                just_minutes_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine._time_range_str_to_time_delta(
                just_seconds_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine._time_range_str_to_time_delta(
                just_months_expression, "window_length"
            )
        with pytest.raises(ValueError, match=r"window_length"):
            monitoring_window_config_engine._time_range_str_to_time_delta(
                just_years_expression, "window_length"
            )

        # Assert
        assert isinstance(all_one, timedelta)
        assert all_one == timedelta(days=1, hours=1, weeks=1)
        assert isinstance(disordered, timedelta)
        assert disordered == timedelta(days=1, hours=2, weeks=1)
        assert isinstance(double, timedelta)
        assert double == timedelta(days=3, hours=2)
        assert isinstance(just_days, timedelta)
        assert just_days == timedelta(days=3)
        assert isinstance(just_hours, timedelta)
        assert just_hours == timedelta(hours=4)
        assert isinstance(just_weeks, timedelta)
        assert just_weeks == timedelta(weeks=5)

    def test_get_window_start_end_times_all_time(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(datetime.now())
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine._get_window_start_end_times(config)
        after_time = util._convert_event_time_to_timestamp(datetime.now())

        # Assert
        assert start_time is None
        assert before_time <= end_time <= after_time

    def test_get_window_start_end_times_rolling_time_no_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1w1d1h",
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(datetime.now())
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine._get_window_start_end_times(
            config,
        )
        after_time = util._convert_event_time_to_timestamp(datetime.now())

        # Assert
        assert (
            before_time
            <= (
                start_time
                + (timedelta(weeks=1, days=1, hours=1).total_seconds() * 1000)
            )
            <= after_time
        )
        assert before_time <= end_time <= after_time

    def test_get_window_start_end_times_rolling_time_short_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="2w1d",
            window_length="1d",
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine._get_window_start_end_times(
            config,
        )
        after_time = util._convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert (
            before_time
            <= start_time + (timedelta(weeks=2, days=1).total_seconds() * 1000)
            <= after_time
        )
        assert (
            before_time
            <= end_time + (timedelta(weeks=2).total_seconds() * 1000)
            <= after_time
        )

    def test_get_window_start_end_times_rolling_time_long_window_length(self):
        # Arrange
        monitoring_window_config_engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1d",
            window_length="48h",
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        (
            start_time,
            end_time,
        ) = monitoring_window_config_engine._get_window_start_end_times(
            config,
        )
        after_time = util._convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert (
            before_time
            <= start_time + (timedelta(days=1).total_seconds() * 1000)
            <= after_time
        )
        assert before_time <= end_time <= after_time

    def test_fetch_feature_group_data(self, mocker, backend_fixtures):
        # Arrange
        unit_test_fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        mocker.patch(ENGINE_GET_TYPE, return_value="spark")
        select_mock = mocker.patch(
            "hsfs.feature_group.FeatureGroup.select",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        as_of_mock = mocker.patch(
            "hsfs.constructor.query.Query.as_of",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        read_mock = mocker.patch("hsfs.constructor.query.Query.read")
        config_engine = mwce.MonitoringWindowConfigEngine()
        start_time = (datetime.now() - timedelta(days=1)).timestamp()
        end_time = datetime.now().timestamp()

        # Act
        _ = config_engine._fetch_feature_group_data(
            entity=unit_test_fg,
            feature_names=None,
            start_time=None,
            end_time=None,
        )
        _ = config_engine._fetch_feature_group_data(
            entity=unit_test_fg,
            feature_names=[DEFAULT_FEATURE_NAME],
            start_time=start_time,
            end_time=end_time,
        )
        _ = config_engine._fetch_feature_group_data(
            entity=unit_test_fg,
            feature_names=None,
            start_time=start_time,
            end_time=None,
        )
        _ = config_engine._fetch_feature_group_data(
            entity=unit_test_fg,
            feature_names=[DEFAULT_FEATURE_NAME],
            start_time=None,
            end_time=end_time,
        )

        # Assert
        assert select_mock.call_count == 2
        as_of_mock.assert_has_calls(
            [
                call(exclude_until=None, wallclock_time=None),
                call(exclude_until=start_time, wallclock_time=end_time),
                call(exclude_until=start_time, wallclock_time=None),
                call(exclude_until=None, wallclock_time=end_time),
            ],
            any_order=False,
        )
        assert read_mock.call_count == 4

    def test_fetch_feature_group_data_with_model_filter(self, mocker, backend_fixtures):
        # Arrange
        unit_test_fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        mocker.patch(ENGINE_GET_TYPE, return_value="spark")
        select_query = query.Query.from_response_json(
            backend_fixtures["query"]["get"]["response"]
        )
        mocker.patch(
            "hsfs.feature_group.FeatureGroup.select",
            return_value=select_query,
        )
        filter_mock = mocker.patch(
            "hsfs.constructor.query.Query.filter",
            return_value=select_query,
        )
        as_of_mock = mocker.patch(
            "hsfs.constructor.query.Query.as_of",
            return_value=select_query,
        )
        mocker.patch("hsfs.constructor.query.Query.read")
        config_engine = mwce.MonitoringWindowConfigEngine()
        # Use millis-since-epoch (the format produced by get_window_start_end_times).
        start_time = 1_700_000_000_000
        end_time = 1_700_086_400_000

        # Act
        _ = config_engine._fetch_feature_group_data(
            entity=unit_test_fg,
            feature_names=[DEFAULT_FEATURE_NAME],
            start_time=start_time,
            end_time=end_time,
            model_filter=("my_model", 3),
        )

        # Assert: model_name + model_version + log_time>= + log_time<= = 4 filters.
        # The previous bug constructed Filter("name") with one arg and crashed at runtime;
        # the second bug used as_of(exclude_until=...) which triggered Delta CDF on a
        # logging FG without delta.enableChangeDataFeed. Both must stay fixed.
        assert filter_mock.call_count == 4
        applied = [c.args[0] for c in filter_mock.call_args_list]
        assert all(isinstance(f, filter_module.Filter) for f in applied)
        names = [f._feature.name for f in applied]
        conditions = [f._condition for f in applied]
        assert names == ["model_name", "model_version", "log_time", "log_time"]
        assert conditions == [
            filter_module.Filter.EQ,
            filter_module.Filter.EQ,
            filter_module.Filter.GE,
            filter_module.Filter.LE,
        ]
        assert applied[0]._value == "my_model"
        assert applied[1]._value == "3"
        # log_time filter values are formatted UTC strings for the timestamp column
        # (see Feature._get_filter_value when type=="timestamp").
        assert isinstance(applied[2]._value, str)
        assert isinstance(applied[3]._value, str)
        # And as_of() must NOT be called on the model-monitoring path — that's the CDF
        # trigger we are explicitly avoiding.
        as_of_mock.assert_not_called()

    def test_fetch_feature_view_data(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch(ENGINE_GET_TYPE, return_value="spark")
        mocker.patch("hsfs.engine._get_instance")
        mocker.patch(CLIENT_GET_INSTANCE)
        unit_test_fv = feature_view.FeatureView.from_response_json(
            backend_fixtures["feature_view"]["get"]["response"]
        )
        mock_vector_server = mocker.patch(
            "hsfs.core.vector_server.VectorServer",
        )
        as_of_mock = mocker.patch(
            "hsfs.constructor.query.Query.as_of",
            return_value=query.Query.from_response_json(
                backend_fixtures["query"]["get"]["response"]
            ),
        )
        read_mock = mocker.patch("hsfs.constructor.query.Query.read")

        config_engine = mwce.MonitoringWindowConfigEngine()
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        # Act

        # use as_of
        _ = config_engine._fetch_feature_view_data(
            entity=unit_test_fv,
            feature_names=None,
            start_time=None,
            end_time=None,
        )
        _ = config_engine._fetch_feature_view_data(
            entity=unit_test_fv,
            feature_names=[DEFAULT_FEATURE_NAME],
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert read_mock.call_count == 2
        as_of_mock.assert_has_calls(
            [
                call(exclude_until=None, wallclock_time=None),
                call(exclude_until=start_time, wallclock_time=end_time),
            ],
            any_order=False,
        )
        assert mock_vector_server.call_count == 0

    def test_fetch_entity_data_in_monitoring_window(self, backend_fixtures, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        mocker.patch("hopsworks_common.client._get_instance")

        fetch_feature_group_data_mocker = mocker.patch(
            "hsfs.core.monitoring_window_config_engine.MonitoringWindowConfigEngine._fetch_feature_group_data",
        )
        fetch_feature_view_data_mocker = mocker.patch(
            "hsfs.core.monitoring_window_config_engine.MonitoringWindowConfigEngine._fetch_feature_view_data",
        )
        unit_test_fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        unit_test_fv = feature_view.FeatureView.from_response_json(
            backend_fixtures["feature_view"]["get"]["response"],
        )

        config_engine_fg = mwce.MonitoringWindowConfigEngine()
        config_engine_fv = mwce.MonitoringWindowConfigEngine()

        # Act
        config_engine_fg._fetch_entity_data_in_monitoring_window(
            entity=unit_test_fg,
            feature_names=None,
            start_time=None,
            end_time=None,
            row_percentage=0.5,
        )
        config_engine_fv._fetch_entity_data_in_monitoring_window(
            entity=unit_test_fv,
            feature_names=None,
            start_time=None,
            end_time=None,
            row_percentage=0.25,
        )

        # Assert
        fetch_feature_group_data_mocker.assert_has_calls(
            [
                call(
                    entity=unit_test_fg,
                    feature_names=None,
                    start_time=None,
                    end_time=None,
                    model_filter=None,
                ),
                call().sample(fraction=0.5),
            ]
        )
        fetch_feature_view_data_mocker.assert_has_calls(
            [
                call(
                    entity=unit_test_fv,
                    feature_names=None,
                    start_time=None,
                    end_time=None,
                    model_filter=None,
                ),
                call().sample(fraction=0.25),
            ]
        )


# ---------------------------------------------------------------------------
# anchor_end_ms tests — commit-anchored window start/end
# ---------------------------------------------------------------------------


class TestGetWindowStartEndTimesAnchor:
    """Tests for anchor_end_ms behaviour in get_window_start_end_times."""

    def test_rolling_time_anchor_pins_both_start_and_end(self):
        """ROLLING_TIME with anchor_end_ms produces [anchor-offset, anchor], not now-based times.

        This is the core regression guard: if the time_offset is shorter than the
        elapsed time since the anchor commit, a now()-anchored start would lie AFTER
        the anchor end, producing an empty/inverted window.
        The scenario: anchor commit was 2 hours ago; time_offset is 1 h (the minimum
        valid granularity — the parser only accepts w/d/h units).
        Before the fix: start = now() - 1 h ≈ 1 h ago > anchor (2 h ago) → inverted.
        After the fix:  start = anchor - 1 h → start < anchor = end → valid window.
        """
        engine = mwce.MonitoringWindowConfigEngine()

        # Anchor commit happened 2 hours ago; 1h offset is shorter than that lag.
        anchor_dt = datetime.now() - timedelta(hours=2)
        anchor_ms = util._convert_event_time_to_timestamp(anchor_dt)

        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1h",
        )
        offset_ms = timedelta(hours=1).total_seconds() * 1000

        start_time, end_time = engine._get_window_start_end_times(
            config, anchor_end_ms=anchor_ms
        )

        # No inversion: start must be strictly before end.
        assert start_time < end_time, (
            f"Window is inverted: start={start_time} >= end={end_time}"
        )
        # End must equal the anchor exactly (ms are passed through without a datetime round-trip).
        assert end_time == anchor_ms, (
            f"end_time {end_time} does not match anchor_ms {anchor_ms}"
        )
        # Start must equal anchor − offset exactly.
        assert start_time == anchor_ms - offset_ms, (
            f"start_time {start_time} != anchor_ms - offset {anchor_ms - offset_ms}"
        )

    def test_all_time_with_anchor_still_returns_none_start(self):
        """ALL_TIME always has start_time=None even when anchor_end_ms is set."""
        engine = mwce.MonitoringWindowConfigEngine()
        anchor_ms = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(hours=3)
        )
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
        )

        start_time, end_time = engine._get_window_start_end_times(
            config, anchor_end_ms=anchor_ms
        )

        assert start_time is None
        assert end_time == anchor_ms

    def test_rolling_time_no_anchor_still_uses_now(self):
        """Without anchor_end_ms, existing now()-anchored behaviour is unchanged."""
        engine = mwce.MonitoringWindowConfigEngine()
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1h",
        )
        offset_ms = timedelta(hours=1).total_seconds() * 1000

        before_ms = util._convert_event_time_to_timestamp(datetime.now())
        start_time, end_time = engine._get_window_start_end_times(config)
        after_ms = util._convert_event_time_to_timestamp(datetime.now())

        assert before_ms <= end_time <= after_ms
        # start ≈ end − offset (within 2 s of scheduling noise).
        assert abs(start_time - (end_time - offset_ms)) <= 2_000

    def test_run_single_window_monitoring_passes_anchor_to_get_times(self, mocker):
        """_run_single_window_monitoring with end_commit_time_override delegates anchor correctly.

        The override must be forwarded to get_window_start_end_times as anchor_end_ms
        so the start time is also anchored to the commit rather than now().
        """
        engine = mwce.MonitoringWindowConfigEngine()
        anchor_ms = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(hours=2)
        )
        config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ROLLING_TIME,
            time_offset="1h",
            row_percentage=1.0,
        )
        fg = MagicMock(spec=feature_group.FeatureGroup)
        fg._feature_store_id = 1
        fg.ENTITY_TYPE = "featuregroups"
        fg.time_travel_format = "DELTA"

        get_times_spy = mocker.patch.object(
            engine,
            "_get_window_start_end_times",
            wraps=engine._get_window_start_end_times,
        )
        mocker.patch.object(engine, "_init_statistics_engine")
        stats_engine_mock = MagicMock()
        engine._statistics_engine = stats_engine_mock
        stats_engine_mock._get_by_time_window.return_value = None
        mock_fds = FeatureDescriptiveStatistics(
            feature_name="amount", feature_type="Fractional"
        )
        computed_stats = MagicMock()
        computed_stats.feature_descriptive_statistics = [mock_fds]
        stats_engine_mock.compute_and_save_monitoring_statistics.return_value = (
            computed_stats
        )
        mocker.patch.object(
            engine,
            "_fetch_entity_data_in_monitoring_window",
            return_value=MagicMock(),
        )

        engine._run_single_window_monitoring(
            entity=fg,
            monitoring_window_config=config,
            feature_names=["amount"],
            end_commit_time_override=anchor_ms,
            model_filter=("iris", 1),
        )

        get_times_spy.assert_called_once_with(
            monitoring_window_config=config,
            anchor_end_ms=anchor_ms,
        )


# ---------------------------------------------------------------------------
# KLL-merge dispatch tests
# ---------------------------------------------------------------------------


def _make_hudi_fg(time_travel_format: str | None = "HUDI"):
    """Create a minimal FeatureGroup mock with the given time travel format."""
    fg = MagicMock(spec=feature_group.FeatureGroup)
    fg._time_travel_format = time_travel_format
    fg.time_travel_format = time_travel_format
    fg._feature_store_id = 1
    fg.ENTITY_TYPE = "featuregroups"
    return fg


def _make_rolling_window_config():
    return mwc.MonitoringWindowConfig(
        window_config_type=mwc.WindowConfigType.ROLLING_TIME,
        time_offset="7d",
        row_percentage=1.0,
    )


def _make_fv_entity():
    fv = MagicMock(spec=feature_view.FeatureView)
    fv._feature_store_id = 1
    fv.ENTITY_TYPE = "featureview"
    return fv


class TestMergeDispatch:
    """Tests for _should_use_merge_path and the full dispatch in _run_single_window_monitoring."""

    def test_rolling_hudi_fg_with_pdf_flags_uses_merge_path(self, mocker):
        """Rolling-time + HUDI FG + kll=True profile_flags triggers merge path."""
        fg = _make_hudi_fg("HUDI")
        window_config = _make_rolling_window_config()
        pdf_profile_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            "for_distribution_comparison": True,
        }

        synthetic_fds = FeatureDescriptiveStatistics(
            feature_name="amount",
            feature_type="Fractional",
            percentiles=[float(i) for i in range(1, 100)],
        )

        engine = mwce.MonitoringWindowConfigEngine()

        # Statistics don't exist yet — triggers the merge/re-profile path.
        mocker.patch.object(
            engine,
            "_init_statistics_engine",
        )
        stats_engine_mock = MagicMock()
        stats_engine_mock._get_by_time_window.return_value = None
        engine._statistics_engine = stats_engine_mock

        resolve_mock = mocker.patch.object(
            engine,
            "_resolve_rolling_reference_via_merge",
            return_value=[synthetic_fds],
        )

        result = engine._run_single_window_monitoring(
            entity=fg,
            monitoring_window_config=window_config,
            feature_names=["amount"],
            profile_flags=pdf_profile_flags,
        )

        assert resolve_mock.called
        assert result == [synthetic_fds]
        # compute_and_save must NOT have been called when merge succeeds.
        stats_engine_mock.compute_and_save_monitoring_statistics.assert_not_called()

    def test_rolling_iceberg_fg_with_pdf_flags_uses_merge_path(self):
        """Rolling-time + ICEBERG FG + kll=True profile_flags triggers merge path.

        ICEBERG is a commit/time-travel format like HUDI/DELTA, so it is eligible
        for the per-commit KLL-merge reference path.
        """
        fg = _make_hudi_fg("ICEBERG")
        window_config = _make_rolling_window_config()
        pdf_profile_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            "for_distribution_comparison": True,
        }

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fg, window_config, pdf_profile_flags)

        assert result

    def test_none_format_fg_never_reaches_merge_path(self):
        """NONE time-travel format FG must not trigger merge path."""
        fg = _make_hudi_fg(time_travel_format=None)
        window_config = _make_rolling_window_config()
        pdf_profile_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            "for_distribution_comparison": True,
        }

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fg, window_config, pdf_profile_flags)

        assert not result

    def test_detection_window_never_uses_merge_path(self):
        """ALL_TIME window (detection window typical type) never triggers merge path."""
        fg = _make_hudi_fg("HUDI")
        all_time_config = mwc.MonitoringWindowConfig(
            window_config_type=mwc.WindowConfigType.ALL_TIME,
            row_percentage=1.0,
        )
        pdf_profile_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            "for_distribution_comparison": True,
        }

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fg, all_time_config, pdf_profile_flags)

        assert not result

    def test_feature_view_entity_never_uses_merge_path(self):
        """FeatureView entities always bypass the merge path (scoped to FG-only)."""
        fv = _make_fv_entity()
        window_config = _make_rolling_window_config()
        pdf_profile_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            "for_distribution_comparison": True,
        }

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fv, window_config, pdf_profile_flags)

        assert not result

    def test_no_kll_profile_flags_never_uses_merge_path(self):
        """Scalar-only monitoring (no kll flag) never triggers merge path."""
        fg = _make_hudi_fg("HUDI")
        window_config = _make_rolling_window_config()
        scalar_profile_flags = None  # scalar monitoring passes None

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fg, window_config, scalar_profile_flags)

        assert not result

    def test_kll_without_distribution_comparison_flag_never_uses_merge_path(self):
        """KLL profile flags without for_distribution_comparison must not trigger merge path.

        kll=True alone is insufficient — PDF dispatcher must also set
        for_distribution_comparison=True so that unrelated kll profile runs
        (e.g. training-dataset statistics) cannot accidentally take the merge path.
        """
        fg = _make_hudi_fg("HUDI")
        window_config = _make_rolling_window_config()
        kll_only_flags = {
            "histograms": True,
            "kll": True,
            "histogram_bins": 20,
            # for_distribution_comparison deliberately absent
        }

        engine = mwce.MonitoringWindowConfigEngine()
        result = engine._should_use_merge_path(fg, window_config, kll_only_flags)

        assert not result

    def test_per_feature_fallback_returns_none_for_whole_window(self, mocker):
        """Per-feature fallback should trigger whole-window fallback if any feature fails to merge.

        Resolve_rolling_reference_via_merge body: if merge succeeds for feature A but
        fails for feature B, the whole window must fall back to re-profile (return None)
        rather than returning partial merged results. Tests the per-feature fallback rule
        at monitoring_window_config_engine.py:~555.
        """
        fg = _make_hudi_fg("HUDI")
        feature_names = ["feat_a", "feat_b"]

        # Two stats rows in the window, each carrying one FDS per feature.
        fds_a_row1 = FeatureDescriptiveStatistics(
            feature_name="feat_a", feature_type="Fractional", count=100
        )
        fds_b_row1 = FeatureDescriptiveStatistics(
            feature_name="feat_b", feature_type="Fractional", count=100
        )
        fds_a_row2 = FeatureDescriptiveStatistics(
            feature_name="feat_a", feature_type="Fractional", count=100
        )
        fds_b_row2 = FeatureDescriptiveStatistics(
            feature_name="feat_b", feature_type="Fractional", count=100
        )
        stats_row_1 = MagicMock()
        stats_row_1.feature_descriptive_statistics = [fds_a_row1, fds_b_row1]
        stats_row_2 = MagicMock()
        stats_row_2.feature_descriptive_statistics = [fds_a_row2, fds_b_row2]

        engine = mwce.MonitoringWindowConfigEngine()
        mocker.patch.object(engine, "_init_statistics_engine")
        stats_engine_mock = MagicMock()
        stats_engine_mock._get_all_in_time_window.return_value = [
            stats_row_1,
            stats_row_2,
        ]
        engine._statistics_engine = stats_engine_mock

        # feat_a merges successfully; feat_b falls back (returns None from merger).
        synthetic_a = FeatureDescriptiveStatistics(
            feature_name="feat_a",
            feature_type="Fractional",
            percentiles=[float(i) for i in range(1, 100)],
        )

        def fake_resolve_merged(fds_list, histogram_bins):
            # First call (feat_a): succeed. Second call (feat_b): fall back.
            if fds_list and fds_list[0].feature_name == "feat_a":
                return synthetic_a
            return None

        mocker.patch(
            "hsfs.core.distribution_engine.DistributionEngine.resolve_merged_reference",
            side_effect=fake_resolve_merged,
        )

        result = engine._resolve_rolling_reference_via_merge(
            entity=fg,
            feature_names=feature_names,
            start_time=1000,
            end_time=2000,
            histogram_bins=20,
        )

        assert result is None, (
            "partial merge (feat_a ok, feat_b fails) must trigger whole-window fallback"
        )

    def test_merge_succeeds_when_all_features_merge(self, mocker):
        """Sanity test: when every feature merges successfully, _resolve_rolling_reference_via_merge returns the full list of synthetic FDS."""
        fg = _make_hudi_fg("HUDI")
        feature_names = ["feat_a", "feat_b"]

        fds_a = FeatureDescriptiveStatistics(
            feature_name="feat_a", feature_type="Fractional", count=100
        )
        fds_b = FeatureDescriptiveStatistics(
            feature_name="feat_b", feature_type="Fractional", count=100
        )
        stats_row = MagicMock()
        stats_row.feature_descriptive_statistics = [fds_a, fds_b]

        engine = mwce.MonitoringWindowConfigEngine()
        mocker.patch.object(engine, "_init_statistics_engine")
        stats_engine_mock = MagicMock()
        stats_engine_mock._get_all_in_time_window.return_value = [stats_row]
        engine._statistics_engine = stats_engine_mock

        synthetic_a = FeatureDescriptiveStatistics(
            feature_name="feat_a",
            feature_type="Fractional",
            percentiles=[1.0] * 99,
        )
        synthetic_b = FeatureDescriptiveStatistics(
            feature_name="feat_b",
            feature_type="Fractional",
            percentiles=[2.0] * 99,
        )

        def fake_resolve_merged(fds_list, histogram_bins):
            return synthetic_a if fds_list[0].feature_name == "feat_a" else synthetic_b

        mocker.patch(
            "hsfs.core.distribution_engine.DistributionEngine.resolve_merged_reference",
            side_effect=fake_resolve_merged,
        )

        result = engine._resolve_rolling_reference_via_merge(
            entity=fg,
            feature_names=feature_names,
            start_time=1000,
            end_time=2000,
            histogram_bins=20,
        )

        assert result == [synthetic_a, synthetic_b]

    def test_merge_falls_back_when_commit_cap_reached(self, mocker):
        """When the fetched stats-row count hits _MAX_COMMITS_FOR_MERGE, the merge sample would be truncated. Must return None (fall back to re-profile)."""
        fg = _make_hudi_fg("HUDI")

        # Fabricate exactly _MAX_COMMITS_FOR_MERGE rows — hits the cap.
        stats_rows = []
        for _ in range(mwce._MAX_COMMITS_FOR_MERGE):
            fds = FeatureDescriptiveStatistics(
                feature_name="feat_a", feature_type="Fractional", count=100
            )
            row = MagicMock()
            row.feature_descriptive_statistics = [fds]
            stats_rows.append(row)

        engine = mwce.MonitoringWindowConfigEngine()
        mocker.patch.object(engine, "_init_statistics_engine")
        stats_engine_mock = MagicMock()
        stats_engine_mock._get_all_in_time_window.return_value = stats_rows
        engine._statistics_engine = stats_engine_mock

        # If the fallback fires early, resolve_merged_reference should NOT be called.
        resolve_mock = mocker.patch(
            "hsfs.core.distribution_engine.DistributionEngine.resolve_merged_reference",
        )

        result = engine._resolve_rolling_reference_via_merge(
            entity=fg,
            feature_names=["feat_a"],
            start_time=1000,
            end_time=2000,
            histogram_bins=20,
        )

        assert result is None
        assert not resolve_mock.called, (
            "cap reached — must fall back before even invoking the merger"
        )
