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
from types import SimpleNamespace

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.core import feature_view_engine
from hsfs.core.feature_logging import (
    LOG_MATERIALIZATION_INTERVALS,
    FeatureLogging,
    _materialization_cron,
)


class TestMaterializationInterval:
    def test_names_map_to_hourly_and_daily_quartz_expressions(self):
        assert _materialization_cron("hour") == "0 0 * * * ? *"
        assert _materialization_cron("Day") == "0 0 0 * * ? *"
        assert set(LOG_MATERIALIZATION_INTERVALS) == {"hour", "day"}

    def test_rejects_anything_else(self):
        with pytest.raises(ValueError, match="week"):
            _materialization_cron("week")
        with pytest.raises(ValueError, match="hour, day"):
            FeatureLogging(materialization_interval="0 0 * * * ? *")

    def test_rides_the_logging_dto_and_round_trips(self):
        logging = FeatureLogging(materialization_interval="DAY")
        wire = json.loads(logging.json())
        assert wire["materializationInterval"] == "day"
        restored = FeatureLogging.from_response_json(wire)
        assert restored.materialization_interval == "day"
        # Absent on the wire means the platform default, on both directions.
        assert FeatureLogging().materialization_interval is None
        assert (
            FeatureLogging.from_response_json({"id": 1}).materialization_interval
            is None
        )

    def test_the_transport_rides_the_dto_or_follows_the_group_layout(self, mocker):
        assert json.loads(FeatureLogging(transport="Job").json())["transport"] == "job"
        with pytest.raises(ValueError, match="realtime, job"):
            FeatureLogging(transport="kafka")
        # Without an explicit value the logging group's layout decides: a
        # stream group is the realtime path, an offline-only group the job path.
        assert (
            FeatureLogging(untransformed_features=mocker.Mock(stream=True)).transport
            == "realtime"
        )
        assert (
            FeatureLogging(untransformed_features=mocker.Mock(stream=False)).transport
            == "job"
        )
        assert FeatureLogging().transport is None

    def test_a_job_transport_view_reads_its_interval_from_the_commit_job(self, mocker):
        from hsfs.core import feature_logging as fl

        group = SimpleNamespace(
            name="fv_1_log",
            stream=False,
            online_enabled=False,
            materialization_job=None,
        )
        logging = fl.FeatureLogging(untransformed_features=group)
        assert logging.transport == "job"
        job_api = mocker.patch("hopsworks_common.core.job_api.JobApi")
        job_api.return_value.get_job.return_value = SimpleNamespace(
            job_schedule=SimpleNamespace(
                cron_expression=fl.LOG_MATERIALIZATION_INTERVALS["day"]
            )
        )
        assert logging.materialization_interval == "day"
        job_api.return_value.get_job.assert_called_once_with(
            "fv_1_log_feature_log_commit"
        )

    def test_an_unset_interval_is_read_back_from_the_job_schedule(self, mocker):
        # The backend keeps the cadence only on the job, so a view fetched fresh
        # reports the interval its schedule matches and None for a custom cron.
        fg = mocker.Mock()
        fg.materialization_job.job_schedule.cron_expression = "0 0 * * * ? *"
        assert (
            FeatureLogging(untransformed_features=fg).materialization_interval == "hour"
        )
        fg.materialization_job.job_schedule.cron_expression = "0 30 * * * ? *"
        assert (
            FeatureLogging(untransformed_features=fg).materialization_interval is None
        )
        fg.materialization_job.job_schedule = None
        assert (
            FeatureLogging(untransformed_features=fg).materialization_interval is None
        )


class TestEngine:
    def _engine(self, mocker):
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        return feature_view_engine.FeatureViewEngine(feature_store_id=99)

    def test_enable_logging_sends_the_interval_and_reschedules(self, mocker):
        engine = self._engine(mocker)
        schedule = mocker.patch.object(engine, "_schedule_log_materialization")
        fv = mocker.Mock(name="fv", version=1)

        engine._enable_feature_logging(fv, None, "day")

        sent = engine._feature_view_api._enable_feature_logging.call_args.args[2]
        assert sent.materialization_interval == "day"
        schedule.assert_called_once_with(fv, "day")
        assert fv.logging_enabled is True

    def test_enable_logging_without_an_interval_leaves_the_schedule_alone(self, mocker):
        engine = self._engine(mocker)
        schedule = mocker.patch.object(engine, "_schedule_log_materialization")

        engine._enable_feature_logging(mocker.Mock(), None)

        schedule.assert_not_called()

    def test_reschedule_puts_the_logging_job_on_the_chosen_cron(self, mocker):
        engine = self._engine(mocker)
        logging_fg = mocker.Mock()
        feature_logging = mocker.Mock(transport="realtime")
        feature_logging.get_feature_group.return_value = logging_fg
        mocker.patch.object(
            engine, "_get_feature_logging", return_value=feature_logging
        )

        engine._schedule_log_materialization(mocker.Mock(), "hour")

        logging_fg.materialization_job.schedule.assert_called_once_with("0 0 * * * ? *")

    def test_reschedule_on_the_job_transport_moves_the_commit_job(self, mocker):
        engine = self._engine(mocker)
        feature_logging = mocker.Mock(transport="job")
        mocker.patch.object(
            engine, "_get_feature_logging", return_value=feature_logging
        )
        commit_job = mocker.patch.object(engine, "_commit_job")
        fv = mocker.Mock(name="fv", version=1)

        engine._schedule_log_materialization(fv, "day")

        commit_job.assert_called_once_with(fv, feature_logging, "day")
        feature_logging.get_feature_group.return_value.materialization_job.schedule.assert_not_called()

    def test_enable_logging_refuses_the_other_transport(self, mocker):
        # One logging group per view, and its layout fixes the transport.
        engine = self._engine(mocker)
        mocker.patch.object(
            engine,
            "_get_feature_logging",
            return_value=mocker.Mock(transport="realtime"),
        )
        fv = mocker.Mock(name="fv", version=1, logging_enabled=True)

        with pytest.raises(FeatureStoreException, match="one transport"):
            engine._enable_feature_logging(fv, None, None, "job")
        engine._feature_view_api._enable_feature_logging.assert_not_called()

        # The same transport again is a no-op on the backend and not an error.
        engine._enable_feature_logging(fv, None, None, "realtime")
        engine._feature_view_api._enable_feature_logging.assert_called_once()

    def test_enable_logging_with_the_job_transport_creates_the_commit_job(self, mocker):
        engine = self._engine(mocker)
        created = mocker.Mock(transport="job")
        mocker.patch.object(engine, "_get_feature_logging", return_value=created)
        commit_job = mocker.patch.object(engine, "_commit_job")
        schedule = mocker.patch.object(engine, "_schedule_log_materialization")
        fv = mocker.Mock(name="fv", version=1, logging_enabled=False)

        engine._enable_feature_logging(fv, None, "hour", "JOB")

        sent = engine._feature_view_api._enable_feature_logging.call_args.args[2]
        assert sent.transport == "job"
        commit_job.assert_called_once_with(fv, created, "hour")
        schedule.assert_not_called()

    def test_materialize_on_the_job_transport_runs_the_commit_job(self, mocker):
        engine = self._engine(mocker)
        feature_logging = mocker.Mock(transport="job")
        mocker.patch.object(
            engine, "_get_feature_logging", return_value=feature_logging
        )
        job = mocker.Mock()
        mocker.patch.object(engine, "_commit_job", return_value=job)

        assert engine._materialize_feature_logs(mocker.Mock(), True, None) == [job]
        job.run.assert_called_once_with(await_termination=False)
        job._wait_for_job.assert_called_once_with(True)

    def test_online_read_is_refused_on_the_job_transport(self, mocker):
        engine = self._engine(mocker)
        feature_logging = mocker.Mock(transport="job")
        mocker.patch.object(
            engine, "_get_feature_logging", return_value=feature_logging
        )

        with pytest.raises(FeatureStoreException, match="no online copy"):
            engine._read_feature_logs(mocker.Mock(name="fv", version=1), online=True)

    def test_reschedule_refuses_a_view_without_logging(self, mocker):
        engine = self._engine(mocker)
        mocker.patch.object(engine, "_get_feature_logging", return_value=None)

        with pytest.raises(FeatureStoreException, match="enable logging"):
            engine._schedule_log_materialization(
                mocker.Mock(name="fv", version=3), "day"
            )

    def test_reschedule_validates_before_touching_the_backend(self, mocker):
        engine = self._engine(mocker)
        get_fg = mocker.patch.object(engine, "_get_logging_fg")

        with pytest.raises(ValueError):
            engine._schedule_log_materialization(mocker.Mock(), "minute")

        get_fg.assert_not_called()
