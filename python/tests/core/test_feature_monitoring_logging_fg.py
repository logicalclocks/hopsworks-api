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
"""Tests for logging feature group monitoring: sub-hour cron warnings and commit anchoring.

Covers Idea A (UserWarning when cron fires more than once per hour on a stream/logging FG)
and Idea D (anchor detection window end to the latest FG commit; reuse prior statistics
when no new commit has arrived since the last result).
"""

from __future__ import annotations

import contextlib
import warnings
from unittest.mock import MagicMock, patch

from hsfs import feature_group as fg_mod
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_config_engine
from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_statistics_result import FeatureStatisticsResult
from hsfs.feature_group_commit import FeatureGroupCommit
from hsfs.util import _is_sub_hour_cron


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEFAULT_FS_ID = 11
DEFAULT_FG_ID = 22
DEFAULT_CONFIG_ID = 33

_HOURLY_COMMIT_TIME = 1_700_000_000_000  # ms – an arbitrary "latest commit"
_OLDER_COMMIT_TIME = 1_699_996_400_000  # ms – a different (older) commit


def _make_fg(stream: bool = True) -> fg_mod.FeatureGroup:
    """Build a minimal FeatureGroup that satisfies isinstance checks."""
    return fg_mod.FeatureGroup(
        name="my_log_fg",
        version=1,
        featurestore_id=DEFAULT_FS_ID,
        id=DEFAULT_FG_ID,
        stream=stream,
        time_travel_format="DELTA",
    )


def _make_fm_config(
    model_name: str | None = "iris",
    model_version: int | None = 1,
    config_id: int = DEFAULT_CONFIG_ID,
) -> MagicMock:
    cfg = MagicMock(spec=fmc.FeatureMonitoringConfig)
    cfg.id = config_id
    cfg.model_name = model_name
    cfg.model_version = model_version
    cfg.trigger_type = fmc.TriggerType.CRON
    cfg.feature_statistics_configs = []
    cfg.get_feature_names.return_value = ["petal_length"]
    detection_wc = MagicMock(spec=mwc.MonitoringWindowConfig)
    detection_wc.window_config_type = mwc.WindowConfigType.ROLLING_TIME
    detection_wc.time_offset = "1d"
    cfg.detection_window_config = detection_wc
    cfg.reference_window_config = None
    return cfg


def _make_fds(
    feature_name: str = "petal_length", count: int = 100
) -> FeatureDescriptiveStatistics:
    return FeatureDescriptiveStatistics(feature_name=feature_name, count=count)


def _make_fm_result(
    config_id: int = DEFAULT_CONFIG_ID,
    detection_window_commit_time: int | None = _HOURLY_COMMIT_TIME,
    fsr_list: list | None = None,
) -> FeatureMonitoringResult:
    if fsr_list is None:
        fsr_list = [
            FeatureStatisticsResult(
                feature_name="petal_length",
                detection_statistics_id=999,
            )
        ]
    return FeatureMonitoringResult(
        feature_store_id=DEFAULT_FS_ID,
        execution_id=0,
        monitoring_time=_HOURLY_COMMIT_TIME,
        feature_monitoring_config_id=config_id,
        feature_statistics_results=fsr_list,
        detection_window_commit_time=detection_window_commit_time,
    )


# ---------------------------------------------------------------------------
# Idea A — _is_sub_hour_cron utility
# ---------------------------------------------------------------------------


class TestIsSubHourCron:
    def test_step_expression_sub_hour(self):
        assert _is_sub_hour_cron("0 */15 * * * ? *") is True

    def test_step_expression_sub_hour_30min(self):
        assert _is_sub_hour_cron("0 */30 * * * ? *") is True

    def test_step_expression_exactly_one_hour(self):
        # */60 is unusual but should be False (fires once per 60 min)
        assert _is_sub_hour_cron("0 */60 * * * ? *") is False

    def test_step_expression_multi_hour(self):
        assert _is_sub_hour_cron("0 */120 * * * ? *") is False

    def test_comma_list_is_sub_hour(self):
        assert _is_sub_hour_cron("0 0,15,30,45 * * * ? *") is True

    def test_range_with_step(self):
        assert _is_sub_hour_cron("0 0-59/15 * * * ? *") is True

    def test_single_minute_not_sub_hour(self):
        # Fires once per hour at minute 0
        assert _is_sub_hour_cron("0 0 * * * ? *") is False

    def test_daily_cron_not_sub_hour(self):
        assert _is_sub_hour_cron("0 0 12 ? * * *") is False

    def test_none_returns_false(self):
        assert _is_sub_hour_cron(None) is False

    def test_empty_returns_false(self):
        assert _is_sub_hour_cron("") is False

    def test_malformed_step_returns_false(self):
        assert _is_sub_hour_cron("0 */abc * * * ? *") is False


# ---------------------------------------------------------------------------
# Idea A — SDK UserWarning on create_model_monitoring in FeatureView
# ---------------------------------------------------------------------------


class TestSubHourCronWarningOnModelMonitoring:
    def test_create_model_monitoring_sub_hour_warns(self):
        """create_model_monitoring with a sub-hour cron emits a UserWarning."""
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with (
                patch(
                    "hsfs.feature_view.FeatureView.feature_logging",
                    new_callable=lambda: property(lambda self: MagicMock()),
                ),
                patch("hsml.core.model_api.ModelApi._get") as mock_model_get,
                patch("hsfs.feature_view.client._get_instance") as mock_client,
            ):
                mock_client.return_value._project_id = 1
                mock_model = MagicMock()
                mock_model.training_dataset_version = 2
                mock_model_get.return_value = mock_model

                # We only want to test the warning, so accept an AttributeError
                # when the mock feature_logging doesn't support get_feature_group().
                with contextlib.suppress(Exception):
                    FeatureView.create_model_monitoring(
                        fv,
                        name="test",
                        model_name="iris",
                        model_version=1,
                        cron_expression="0 */15 * * * ? *",
                    )

        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert any("more than once per hour" in str(w.message) for w in user_warnings)

    def test_create_model_monitoring_hourly_cron_no_warn(self):
        """create_model_monitoring with an hourly cron emits no UserWarning."""
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with (
                patch("hsml.core.model_api.ModelApi._get") as mock_model_get,
                patch("hsfs.feature_view.client._get_instance") as mock_client,
            ):
                mock_client.return_value._project_id = 1
                mock_model = MagicMock()
                mock_model.training_dataset_version = 2
                mock_model_get.return_value = mock_model

                with contextlib.suppress(Exception):
                    FeatureView.create_model_monitoring(
                        fv,
                        name="test",
                        model_name="iris",
                        model_version=1,
                        cron_expression="0 0 12 ? * * *",
                    )

        user_warnings = [
            w
            for w in caught
            if issubclass(w.category, UserWarning)
            and "more than once per hour" in str(w.message)
        ]
        assert len(user_warnings) == 0

    def test_create_model_monitoring_sub_hour_warns_only_once(self):
        """Delegating through the stream logging FG must not double-emit the warning.

        FeatureView.create_model_monitoring is the single source of the sub-hour-cron
        warning. It delegates to the logging FeatureGroup's create_feature_monitoring,
        which must not re-emit it, so the user sees exactly one warning.
        """
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        # A real stream FG: exercises the real create_feature_monitoring delegate so a
        # warning re-introduced there would surface as a duplicate.
        logging_fg = _make_fg(stream=True)
        logging_fg._features = []
        logging_fg._feature_monitoring_config_engine = MagicMock()
        logging_fg._feature_monitoring_config_engine._build_default_feature_monitoring_config.return_value = MagicMock()

        # Assign feature_logging directly on the mock so the delegation reaches the real
        # FG above — a class-level property patch is bypassed by MagicMock attribute lookup.
        fv.feature_logging.get_feature_group.return_value = logging_fg

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with (
                patch("hsml.core.model_api.ModelApi._get") as mock_model_get,
                patch("hsfs.feature_view.client._get_instance") as mock_client,
            ):
                mock_client.return_value._project_id = 1
                mock_model = MagicMock()
                mock_model.training_dataset_version = 2
                mock_model_get.return_value = mock_model

                FeatureView.create_model_monitoring(
                    fv,
                    name="test",
                    model_name="iris",
                    model_version=1,
                    cron_expression="0 */15 * * * ? *",
                )

        sub_hour_warnings = [
            w
            for w in caught
            if issubclass(w.category, UserWarning)
            and "more than once per hour" in str(w.message)
        ]
        assert len(sub_hour_warnings) == 1


# ---------------------------------------------------------------------------
# Idea D — _get_latest_fg_commit_time helper
# ---------------------------------------------------------------------------


class TestGetLatestFgCommitTime:
    def test_returns_commit_time_from_latest_commit(self, mocker):
        """_get_latest_fg_commit_time returns the commit_time of the first commit row."""
        engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FS_ID,
            feature_group_id=DEFAULT_FG_ID,
        )
        fg = _make_fg()

        commit = MagicMock(spec=FeatureGroupCommit)
        commit.commit_time = _HOURLY_COMMIT_TIME

        mocker.patch(
            "hsfs.core.feature_monitoring_config_engine.FeatureGroupApi._get_commit_details",
            return_value=[commit],
        )

        result = engine._get_latest_fg_commit_time(fg)
        assert result == _HOURLY_COMMIT_TIME

    def test_returns_none_when_no_commits(self, mocker):
        """_get_latest_fg_commit_time returns None when the FG has no commits yet."""
        engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FS_ID,
            feature_group_id=DEFAULT_FG_ID,
        )
        fg = _make_fg()

        mocker.patch(
            "hsfs.core.feature_monitoring_config_engine.FeatureGroupApi._get_commit_details",
            return_value=[],
        )

        result = engine._get_latest_fg_commit_time(fg)
        assert result is None

    def test_returns_none_on_exception(self, mocker):
        """_get_latest_fg_commit_time returns None and logs a warning on API failure."""
        engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FS_ID,
            feature_group_id=DEFAULT_FG_ID,
        )
        fg = _make_fg()

        mocker.patch(
            "hsfs.core.feature_monitoring_config_engine.FeatureGroupApi._get_commit_details",
            side_effect=Exception("network error"),
        )

        result = engine._get_latest_fg_commit_time(fg)
        assert result is None


# ---------------------------------------------------------------------------
# Idea D — run_feature_monitoring: window anchoring and reuse-without-recompute
# ---------------------------------------------------------------------------


class TestRunFeatureMonitoringIdeaD:
    def _make_engine(
        self,
    ) -> feature_monitoring_config_engine.FeatureMonitoringConfigEngine:
        return feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FS_ID,
            feature_group_id=DEFAULT_FG_ID,
        )

    def test_new_commit_triggers_recompute_and_anchors_end_time(self, mocker):
        """When a new commit exists (no prior result), _run_single_window_monitoring is called.

        The call must use end_commit_time_override equal to the latest commit time.
        """
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=_HOURLY_COMMIT_TIME,
        )
        mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
            return_value=None,  # no prior result
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        # _run_single_window_monitoring must have been called with the commit time as override
        run_single.assert_called_once()
        call_kwargs = run_single.call_args.kwargs
        assert call_kwargs["end_commit_time_override"] == _HOURLY_COMMIT_TIME
        assert call_kwargs["model_filter"] == ("iris", 1)

    def test_unmaterialized_offline_log_fg_yields_empty_detection_window(self, mocker):
        """A logging FG with no offline commits yields an empty detection window.

        When offline materialization has not run yet, the detection read is skipped and
        empty (count=0) statistics are emitted so the result is flagged as an empty
        detection window — instead of the offline read failing with DELTA_TABLE_NOT_FOUND
        and failing the job.
        """
        engine = self._make_engine()
        fg = _make_fg()
        # model_name/model_version set -> model_filter active
        config = _make_fm_config()

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=None,  # no offline commits -> offline not materialized
        )
        get_latest_result = mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
        )
        run_and_save = mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        # No offline data -> no read and no reuse lookup.
        run_single.assert_not_called()
        get_latest_result.assert_not_called()
        # The detection window is reported empty via count=0 statistics.
        run_and_save.assert_called_once()
        det_stats = run_and_save.call_args.kwargs["detection_statistics"]
        assert len(det_stats) == 1
        assert det_stats[0].feature_name == "petal_length"
        assert det_stats[0].count == 0
        # Nothing is materialized, so there is no commit to anchor the result to.
        assert run_and_save.call_args.kwargs["detection_window_commit_time"] is None

    def test_no_new_commit_reuses_previous_result_without_calling_compute(self, mocker):
        """When the latest commit matches the previous result's commit time, no recompute occurs.

        The statistics engine must NOT be called and a new result is saved reusing the
        previous statistics.
        """
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()

        prior_result = _make_fm_result(
            detection_window_commit_time=_HOURLY_COMMIT_TIME,
        )

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=_HOURLY_COMMIT_TIME,  # same commit as prior result
        )
        mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
            return_value=prior_result,
        )
        # _run_single_window_monitoring must NOT be called
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
        )
        save_mock = mocker.patch.object(
            engine._result_engine,
            "_save",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        run_single.assert_not_called()
        save_mock.assert_called_once()
        saved_result = save_mock.call_args.args[0]
        assert saved_result.detection_window_commit_time == _HOURLY_COMMIT_TIME
        # Reused FSRs reference the same FDS rows (by id) but are rebuilt with the
        # nested detection_statistics/reference_statistics stripped so the
        # create-endpoint accepts them.
        assert len(saved_result.feature_statistics_results) == len(
            prior_result.feature_statistics_results
        )
        for new_fsr, prior_fsr in zip(
            saved_result.feature_statistics_results,
            prior_result.feature_statistics_results,
            strict=True,
        ):
            assert new_fsr.feature_name == prior_fsr.feature_name
            assert new_fsr.detection_statistics_id == prior_fsr.detection_statistics_id
            assert new_fsr.detection_statistics is None
            assert new_fsr.reference_statistics is None

    def test_different_commit_since_last_result_triggers_recompute(self, mocker):
        """When the latest commit differs from the prior result's commit time, recompute runs.

        A normal compute pass is triggered with end anchored to the newer commit time.
        """
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()

        prior_result = _make_fm_result(
            detection_window_commit_time=_OLDER_COMMIT_TIME,  # older commit
        )

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=_HOURLY_COMMIT_TIME,  # new, newer commit
        )
        mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
            return_value=prior_result,
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        run_single.assert_called_once()
        call_kwargs = run_single.call_args.kwargs
        # End time should be anchored to the new commit
        assert call_kwargs["end_commit_time_override"] == _HOURLY_COMMIT_TIME

    def test_non_model_filter_path_unchanged(self, mocker):
        """For configs without model_name/model_version the Idea D path is not activated."""
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config(model_name=None, model_version=None)

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        get_latest_commit = mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        # Commit lookup must NOT happen for the non-model path
        get_latest_commit.assert_not_called()
        # Normal compute path must be taken
        run_single.assert_called_once()
        call_kwargs = run_single.call_args.kwargs
        assert call_kwargs["end_commit_time_override"] is None

    def test_prior_result_without_commit_time_triggers_recompute(self, mocker):
        """When a prior result exists but has no detection_window_commit_time, recompute runs.

        Old result format (no commit time stored) skips the reuse guard and falls through
        to a normal compute pass.
        """
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()

        prior_result = _make_fm_result(
            detection_window_commit_time=None,  # no commit time stored
        )

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=_HOURLY_COMMIT_TIME,
        )
        mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
            return_value=prior_result,
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        run_single.assert_called_once()

    def test_detection_window_commit_time_persisted_in_result(self, mocker):
        """The detection_window_commit_time is forwarded to _run_and_save_statistics_comparison."""
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
            return_value=_HOURLY_COMMIT_TIME,
        )
        mocker.patch.object(
            engine._result_engine,
            "_get_latest_by_config_id",
            return_value=None,
        )
        mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        save_comparison = mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        engine._run_feature_monitoring(entity=fg, config_name="cfg")

        save_comparison.assert_called_once()
        kwargs = save_comparison.call_args.kwargs
        assert kwargs["detection_window_commit_time"] == _HOURLY_COMMIT_TIME

    def test_ingestion_triggered_path_skips_idea_d(self, mocker):
        """When end_commit_time is provided by the caller (ingestion-triggered), Idea D skips.

        The commit fetch and reuse logic must not be activated when end_commit_time was
        already supplied by the ingestion trigger.
        """
        engine = self._make_engine()
        fg = _make_fg()
        config = _make_fm_config()
        config.trigger_type = fmc.TriggerType.INGESTION

        mocker.patch.object(
            engine._feature_monitoring_config_api,
            "_get_by_name",
            return_value=config,
        )
        get_latest_commit = mocker.patch.object(
            engine,
            "_get_latest_fg_commit_time",
        )
        run_single = mocker.patch.object(
            engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=[_make_fds()],
        )
        mocker.patch.object(
            engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=MagicMock(spec=FeatureMonitoringResult),
        )

        ingestion_commit_time = 1_700_000_050_000
        engine._run_feature_monitoring(
            entity=fg,
            config_name="cfg",
            end_commit_time=ingestion_commit_time,
        )

        # Idea D commit fetch must NOT be called when end_commit_time was supplied
        get_latest_commit.assert_not_called()
        run_single.assert_called_once()
        call_kwargs = run_single.call_args.kwargs
        assert call_kwargs["end_commit_time_override"] == ingestion_commit_time
