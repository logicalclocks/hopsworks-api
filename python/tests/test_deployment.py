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

import pytest
from hsml import deployment, predictor
from hsml.client.exceptions import ModelServingException
from hsml.constants import PREDICTOR_STATE
from hsml.core import serving_api
from hsml.engine import serving_engine


class TestDeployment:
    # from response json

    def test_from_response_json_list(self, mocker, backend_fixtures):
        # Arrange
        preds = [{"name": "pred_name"}]
        mock_pred_from_response_json = mocker.patch(
            "hsml.predictor.Predictor.from_response_json",
            return_value=preds,
        )
        mock_from_predictor = mocker.patch(
            "hsml.deployment.Deployment.from_predictor", return_value=preds[0]
        )

        # Act
        depl = deployment.Deployment.from_response_json(preds)

        # Assert
        assert isinstance(depl, list)
        assert depl[0] == preds[0]
        mock_pred_from_response_json.assert_called_once_with(preds)
        mock_from_predictor.assert_called_once_with(preds[0])

    def test_from_response_json_single(self, mocker, backend_fixtures):
        # Arrange
        pred = {"name": "pred_name"}
        mock_pred_from_response_json = mocker.patch(
            "hsml.predictor.Predictor.from_response_json",
            return_value=pred,
        )
        mock_from_predictor = mocker.patch(
            "hsml.deployment.Deployment.from_predictor", return_value=pred
        )

        # Act
        depl = deployment.Deployment.from_response_json(pred)

        # Assert
        assert depl == pred
        mock_pred_from_response_json.assert_called_once_with(pred)
        mock_from_predictor.assert_called_once_with(pred)

    # constructor

    def test_constructor_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)

        # Act
        d = deployment.Deployment(predictor=p)

        # Assert
        assert d.name == p.name
        assert d.description == p.description
        assert d.predictor == p
        assert isinstance(d._serving_api, serving_api.ServingApi)
        assert isinstance(d._serving_engine, serving_engine.ServingEngine)

    def test_constructor(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)

        # Act
        d = deployment.Deployment(predictor=p, name=p.name, description=p.description)

        # Assert
        assert d.name == p.name
        assert d.description == p.description
        assert d.predictor == p
        assert isinstance(d._serving_api, serving_api.ServingApi)
        assert isinstance(d._serving_engine, serving_engine.ServingEngine)

    def test_constructor_no_predictor(self):
        # Act
        with pytest.raises(ModelServingException) as e_info:
            _ = deployment.Deployment(predictor=None)

        # Assert
        assert "A predictor is required" in str(e_info.value)

    def test_constructor_wrong_predictor(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            _ = deployment.Deployment(predictor={"wrong": "type"})

        # Assert
        assert "not an instance of the Predictor class" in str(e_info.value)

    # from predictor

    def test_from_predictor(self, mocker):
        # Arrange
        class MockPredictor:
            _name = "name"
            _description = "description"

        p = MockPredictor()
        mock_deployment_init = mocker.patch(
            "hsml.deployment.Deployment.__init__", return_value=None
        )

        # Act
        deployment.Deployment.from_predictor(p)

        # Assert
        mock_deployment_init.assert_called_once_with(
            predictor=p, name=p._name, description=p._description
        )

    # save

    def test_save_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_save = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.save"
        )

        # Act
        d.save()

        # Assert
        mock_serving_engine_save.assert_called_once_with(d, 600)

    def test_save(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_save = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.save"
        )

        # Act
        await_update = 600
        d.save(await_update=await_update)

        # Assert
        mock_serving_engine_save.assert_called_once_with(d, await_update)

    # start

    def test_start_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_start = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.start"
        )

        # Act
        d.start()

        # Assert
        mock_serving_engine_start.assert_called_once_with(d, await_status=600)

    def test_start(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_start = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.start"
        )

        # Act
        await_running = 600
        d.start(await_running=await_running)

        # Assert
        mock_serving_engine_start.assert_called_once_with(d, await_status=await_running)

    # stop

    def test_stop_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_stop = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.stop"
        )

        # Act
        d.stop()

        # Assert
        mock_serving_engine_stop.assert_called_once_with(d, await_status=600)

    def test_stop(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_start = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.stop"
        )

        # Act
        await_stopped = 600
        d.stop(await_stopped=await_stopped)

        # Assert
        mock_serving_engine_start.assert_called_once_with(d, await_status=await_stopped)

    # restart

    def test_restart_when_running(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hsml.deployment.Deployment.is_stopped", return_value=False)
        mock_stop = mocker.patch("hsml.deployment.Deployment.stop")
        mock_start = mocker.patch("hsml.deployment.Deployment.start")

        # Act
        d.restart()

        # Assert
        mock_stop.assert_called_once_with(await_stopped=600)
        mock_start.assert_called_once_with(await_running=600)

    def test_restart_when_stopped_starts_in_place(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hsml.deployment.Deployment.is_stopped", return_value=True)
        mock_stop = mocker.patch("hsml.deployment.Deployment.stop")
        mock_start = mocker.patch("hsml.deployment.Deployment.start")

        # Act
        d.restart()

        # Assert
        mock_stop.assert_not_called()
        mock_start.assert_called_once_with(await_running=600)

    def test_restart_passes_custom_awaits(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hsml.deployment.Deployment.is_stopped", return_value=False)
        mock_stop = mocker.patch("hsml.deployment.Deployment.stop")
        mock_start = mocker.patch("hsml.deployment.Deployment.start")

        # Act
        d.restart(await_stopped=42, await_running=137)

        # Assert
        mock_stop.assert_called_once_with(await_stopped=42)
        mock_start.assert_called_once_with(await_running=137)

    # delete

    def test_delete_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_delete = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.delete"
        )

        # Act
        d.delete()

        # Assert
        mock_serving_engine_delete.assert_called_once_with(d, False)

    def test_delete(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_delete = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.delete"
        )

        # Act
        force = True
        d.delete(force=force)

        # Assert
        mock_serving_engine_delete.assert_called_once_with(d, force)

    # get state

    def test_get_state(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_get_state = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_state"
        )

        # Act
        d.get_state()

        # Assert
        mock_serving_engine_get_state.assert_called_once_with(d)

    # status

    # - is created

    def test_is_created_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_state",
            return_value=MockPredictorState(PREDICTOR_STATE.STATUS_CREATING),
        )

        # Act
        is_created = d.is_created()

        # Assert
        assert not is_created

    def test_is_created_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_FAILED,
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_RUNNING,
            PREDICTOR_STATE.STATUS_STARTING,
            PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert d.is_created()

    # is running

    def test_is_running_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_state",
            return_value=MockPredictorState(PREDICTOR_STATE.STATUS_RUNNING),
        )

        # Act and Assert
        assert d.is_running()

    def test_is_running_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_CREATING,
            PREDICTOR_STATE.STATUS_FAILED,
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_STARTING,
            # PREDICTOR_STATE.STATUS_RUNNING,
            PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert not d.is_running(or_idle=False, or_updating=False)

    def test_is_running_or_idle_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_RUNNING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert d.is_running(or_idle=True)

    def test_is_running_or_idle_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_CREATING,
            PREDICTOR_STATE.STATUS_FAILED,
            # PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_STARTING,
            # PREDICTOR_STATE.STATUS_RUNNING,
            PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert not d.is_running(or_idle=True, or_updating=False)

    def test_is_running_or_updating_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            # PREDICTOR_STATE.STATUS_CREATED,
            # PREDICTOR_STATE.STATUS_CREATING,
            # PREDICTOR_STATE.STATUS_FAILED,
            # PREDICTOR_STATE.STATUS_IDLE,
            # PREDICTOR_STATE.STATUS_STARTING,
            PREDICTOR_STATE.STATUS_RUNNING,
            # PREDICTOR_STATE.STATUS_STOPPED,
            # PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert d.is_running(or_updating=True)

    def test_is_running_or_updating_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_CREATING,
            PREDICTOR_STATE.STATUS_FAILED,
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_STARTING,
            # PREDICTOR_STATE.STATUS_RUNNING,
            PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            # PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert not d.is_running(or_idle=False, or_updating=True)

    # - is stopped

    def test_is_stopped_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_state",
            return_value=MockPredictorState(PREDICTOR_STATE.STATUS_STOPPED),
        )

        # Act and Assert
        assert d.is_stopped()

    def test_is_stopped_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_CREATING,
            PREDICTOR_STATE.STATUS_FAILED,
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_STARTING,
            PREDICTOR_STATE.STATUS_RUNNING,
            # PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert not d.is_stopped(or_created=False)

    def test_is_stopped_or_created_true(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            PREDICTOR_STATE.STATUS_CREATED,
            PREDICTOR_STATE.STATUS_CREATING,
            # PREDICTOR_STATE.STATUS_FAILED,
            # PREDICTOR_STATE.STATUS_IDLE,
            # PREDICTOR_STATE.STATUS_STARTING,
            # PREDICTOR_STATE.STATUS_RUNNING,
            PREDICTOR_STATE.STATUS_STOPPED,
            # PREDICTOR_STATE.STATUS_STOPPING,
            # PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert d.is_stopped(or_created=True)

    def test_is_stopped_or_created_false(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockPredictorState:
            def __init__(self, status):
                self.status = status

        valid_statuses = [
            # PREDICTOR_STATE.STATUS_CREATED,
            # PREDICTOR_STATE.STATUS_CREATING,
            PREDICTOR_STATE.STATUS_FAILED,
            PREDICTOR_STATE.STATUS_IDLE,
            PREDICTOR_STATE.STATUS_STARTING,
            PREDICTOR_STATE.STATUS_RUNNING,
            # PREDICTOR_STATE.STATUS_STOPPED,
            PREDICTOR_STATE.STATUS_STOPPING,
            PREDICTOR_STATE.STATUS_UPDATING,
        ]

        for valid_status in valid_statuses:
            mocker.patch(
                "hsml.engine.serving_engine.ServingEngine.get_state",
                return_value=MockPredictorState(valid_status),
            )

            # Act and Assert
            assert not d.is_stopped(or_created=True)

    # predict

    def test_predict(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_predict = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.predict"
        )

        # Act
        d.predict("data", "inputs")

        # Assert
        mock_serving_engine_predict.assert_called_once_with(d, "data", "inputs")

    # download artifact

    def test_download_artifact(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_serving_engine_download_artifact_files = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.download_artifact_files"
        )

        # Act
        d.download_artifact_files()

        # Assert
        mock_serving_engine_download_artifact_files.assert_called_once_with(
            d, local_path=None
        )

    # get logs

    def test_get_logs_default(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_util_get_members = mocker.patch(
            "hopsworks_common.util.get_members", return_value=["predictor"]
        )
        mock_print = mocker.patch("builtins.print")

        class MockLogs:
            instance_name = "instance_name"
            content = "content"

        mock_logs = [MockLogs()]
        mock_serving_get_logs = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_logs",
            return_value=mock_logs,
        )

        # Act
        d.get_logs()

        # Assert
        mock_util_get_members.assert_called_once()
        mock_serving_get_logs.assert_called_once_with(d, "predictor", 10)
        assert mock_print.call_count == len(mock_logs)

    def test_get_logs_component_valid(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_util_get_members = mocker.patch(
            "hopsworks_common.util.get_members", return_value=["valid"]
        )
        mock_print = mocker.patch("builtins.print")

        class MockLogs:
            instance_name = "instance_name"
            content = "content"

        mock_logs = [MockLogs()]
        mock_serving_get_logs = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_logs",
            return_value=mock_logs,
        )

        # Act
        d.get_logs(component="valid")

        # Assert
        mock_util_get_members.assert_called_once()
        mock_serving_get_logs.assert_called_once_with(d, "valid", 10)
        assert mock_print.call_count == len(mock_logs)

    def test_get_logs_component_invalid(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        # Act
        with pytest.raises(ValueError) as e_info:
            d.get_logs(component="invalid")

        # Assert
        assert "is not valid" in str(e_info.value)

    def test_get_logs_tail(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_util_get_members = mocker.patch(
            "hopsworks_common.util.get_members", return_value=["predictor"]
        )
        mock_print = mocker.patch("builtins.print")

        class MockLogs:
            instance_name = "instance_name"
            content = "content"

        mock_logs = [MockLogs()]
        mock_serving_get_logs = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_logs",
            return_value=mock_logs,
        )

        # Act
        d.get_logs(tail=40)

        # Assert
        mock_util_get_members.assert_called_once()
        mock_serving_get_logs.assert_called_once_with(d, "predictor", 40)
        assert mock_print.call_count == len(mock_logs)

    def test_get_logs_no_logs(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_util_get_members = mocker.patch(
            "hopsworks_common.util.get_members", return_value=["predictor"]
        )
        mock_print = mocker.patch("builtins.print")

        mock_serving_get_logs = mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_logs",
            return_value=None,
        )

        # Act
        d.get_logs()

        # Assert
        mock_util_get_members.assert_called_once()
        mock_serving_get_logs.assert_called_once_with(d, "predictor", 10)
        assert mock_print.call_count == 0

    # read_logs / tail_logs (programmatic, never print)

    def _make_chunk(
        self, instance_name="i-0", content="line\n", timestamp=None, doc_id=None
    ):
        # Pure Python stand-in for DeployableComponentLogs — only the
        # attributes the engine touches are needed.
        from types import SimpleNamespace

        return SimpleNamespace(
            instance_name=instance_name,
            content=content,
            timestamp=timestamp,
            doc_id=doc_id,
        )

    def test_read_logs_returns_string_no_capsys_output(
        self, mocker, backend_fixtures, capsys
    ):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        chunks = [
            self._make_chunk(content="hello\n"),
            self._make_chunk(content="world"),
        ]
        mock_api = mocker.patch(
            "hsml.core.serving_api.ServingApi.get_logs", return_value=chunks
        )

        # Act
        out = d.read_logs(tail=50)
        captured = capsys.readouterr()

        # Assert
        assert isinstance(out, str)
        assert "hello\n" in out and "world\n" in out  # trailing \n auto-added
        assert captured.out == "" and captured.err == ""
        mock_api.assert_called_once()

    def test_read_logs_forwards_source_and_time_window_to_api(
        self, mocker, backend_fixtures
    ):
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        mock_api = mocker.patch(
            "hsml.core.serving_api.ServingApi.get_logs", return_value=[]
        )

        d.read_logs(
            tail=200,
            source="opensearch",
            since="2026-05-08T00:00:00Z",
            until="2026-05-08T01:00:00Z",
            pod="my-pod-0",
        )

        # All optional params land in the API call as kwargs.
        kwargs = mock_api.call_args.kwargs
        assert kwargs["source"] == "opensearch"
        assert kwargs["since"] == "2026-05-08T00:00:00Z"
        assert kwargs["until"] == "2026-05-08T01:00:00Z"
        assert kwargs["pod"] == "my-pod-0"
        # tail goes through positionally per ServingApi.get_logs signature.
        assert mock_api.call_args.args[2] == 200

    def test_read_logs_multiple_instances_get_block_headers(
        self, mocker, backend_fixtures
    ):
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        mocker.patch(
            "hsml.core.serving_api.ServingApi.get_logs",
            return_value=[
                self._make_chunk(instance_name="pod-A", content="a1\n"),
                self._make_chunk(instance_name="pod-B", content="b1\n"),
            ],
        )

        out = d.read_logs()

        # Block headers separate the two instances; single-instance reads
        # would not include any header.
        assert "==> pod-A <==" in out and "==> pod-B <==" in out

    def test_tail_logs_yields_only_new_chunks_dedup_doc_id(
        self, mocker, backend_fixtures
    ):
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        # First poll: two new entries. Second poll: doc_id "x1" already
        # seen, doc_id "x3" is new → only "x3" should be yielded.
        first = [
            self._make_chunk(
                content="a\n", timestamp="2026-05-08T00:00:01Z", doc_id="x1"
            ),
            self._make_chunk(
                content="b\n", timestamp="2026-05-08T00:00:02Z", doc_id="x2"
            ),
        ]
        second = [
            self._make_chunk(
                content="a\n", timestamp="2026-05-08T00:00:01Z", doc_id="x1"
            ),
            self._make_chunk(
                content="c\n", timestamp="2026-05-08T00:00:03Z", doc_id="x3"
            ),
        ]
        mocker.patch(
            "hsml.core.serving_api.ServingApi.get_logs",
            side_effect=[first, second, []],
        )
        # No real sleeps in the test.
        mocker.patch("time.sleep")
        # Stop after a short wall clock so the test is deterministic.
        monot = mocker.patch("time.monotonic")
        monot.side_effect = [0.0, 0.5, 1.0, 99.0, 99.0]

        gen = d.tail_logs(timeout=10.0, since=None)
        first_chunk = next(gen)
        second_chunk = next(gen)
        # third call to get_logs returns [] → generator should exit on
        # timeout (monot side_effect drives it past the deadline).
        with pytest.raises(StopIteration):
            next(gen)

        assert "a\n" in first_chunk and "b\n" in first_chunk
        # On the second poll only the new entry (x3) appears.
        assert second_chunk.strip() == "c"

    def test_tail_logs_dedup_hash_for_kubernetes_source(self, mocker, backend_fixtures):
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        # No timestamp / doc_id → engine falls back to (instance, content) hash.
        first = [self._make_chunk(content="boot\n")]
        second = [
            self._make_chunk(content="boot\n"),
            self._make_chunk(content="ready\n"),
        ]
        mocker.patch(
            "hsml.core.serving_api.ServingApi.get_logs",
            side_effect=[first, second],
        )
        mocker.patch("time.sleep")
        monot = mocker.patch("time.monotonic")
        monot.side_effect = [0.0, 0.5, 99.0, 99.0]

        gen = d.tail_logs(source="kubernetes", timeout=10.0, since=None)
        first_chunk = next(gen)
        second_chunk = next(gen)

        assert first_chunk == "boot\n"
        # First poll already cached "boot"; second poll only yields "ready".
        assert second_chunk == "ready\n"

    def test_tail_logs_stops_on_status(self, mocker, backend_fixtures):
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])
        mocker.patch("hsml.core.serving_api.ServingApi.get_logs", return_value=[])
        # First state probe returns Running, second returns Stopped → loop exits.
        states = [
            mocker.MagicMock(status="Running"),
            mocker.MagicMock(status="Stopped"),
        ]
        mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_state",
            side_effect=states,
        )
        mocker.patch("time.sleep")
        # No timeout: the only exit path is stop_on_status.
        chunks = list(d.tail_logs(stop_on_status="Stopped", since=None))
        assert chunks == []

    def test_get_logs_legacy_still_prints(self, mocker, backend_fixtures, capsys):
        # Belt-and-braces: confirm the legacy method is still side-effect-y so
        # users who depend on its print behaviour are not silently broken.
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mocker.patch("hopsworks_common.util.get_members", return_value=["predictor"])

        class MockLog:
            def __repr__(self):
                return "[mock log line]"

        mocker.patch(
            "hsml.engine.serving_engine.ServingEngine.get_logs",
            return_value=[MockLog()],
        )

        ret = d.get_logs()
        captured = capsys.readouterr()

        assert ret is None
        assert "[mock log line]" in captured.out

    # get url

    def test_get_url(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        class MockClient:
            _project_id = "project_id"

        mock_client = MockClient()
        path = "/p/" + str(mock_client._project_id) + "/deployments/" + str(d.id)

        mock_util_get_hostname_replaced_url = mocker.patch(
            "hopsworks_common.util.get_hostname_replaced_url", return_value="url"
        )
        mock_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance", return_value=mock_client
        )

        # Act
        url = d.get_url()

        # Assert
        assert url == "url"
        mock_util_get_hostname_replaced_url.assert_called_once_with(path)
        mock_client_get_instance.assert_called_once()

    # get endpoint url

    def test_get_endpoint_url_delegates_to_predictor(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_predictor_get_endpoint_url = mocker.patch.object(
            p, "get_endpoint_url", return_value="https://example.com/v1/project/name"
        )

        # Act
        url = d.get_endpoint_url()

        # Assert
        assert url == "https://example.com/v1/project/name"
        mock_predictor_get_endpoint_url.assert_called_once()

    # get openai url

    def test_get_openai_url_delegates_to_predictor(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_predictor_get_openai_url = mocker.patch.object(
            p,
            "get_openai_url",
            return_value="https://example.com/v1/project/name/v1",
        )

        # Act
        url = d.get_openai_url()

        # Assert
        assert url == "https://example.com/v1/project/name/v1"
        mock_predictor_get_openai_url.assert_called_once()

    # get inference url

    def test_get_inference_url_delegates_to_predictor(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        mock_predictor_get_inference_url = mocker.patch.object(
            p,
            "get_inference_url",
            return_value="https://example.com/v1/project/name/v1/models/name:predict",
        )

        # Act
        url = d.get_inference_url()

        # Assert
        assert url == "https://example.com/v1/project/name/v1/models/name:predict"
        mock_predictor_get_inference_url.assert_called_once()

    # env vars

    def test_env_vars_proxies_to_predictor(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)
        assert d.env_vars is None

        # Act
        d.env_vars = {"FOO": "bar"}

        # Assert
        assert d.env_vars == {"FOO": "bar"}
        assert p.env_vars == {"FOO": "bar"}

    def test_env_vars_reflects_predictor_value(self, mocker, backend_fixtures):
        # Arrange
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        p.env_vars = {"A": "1"}

        # Act
        d = deployment.Deployment(predictor=p)

        # Assert
        assert d.env_vars == {"A": "1"}

    def test_env_vars_lifecycle_add_change_remove(self, mocker, backend_fixtures):
        # Lifecycle on the Deployment proxy: every assignment must land on the
        # underlying predictor (so save() sends it) and the read-back must
        # mirror it. Mirrors the loadtest's add → override → clear (None) →
        # re-set → clear ({}) sequence.
        p = self._get_dummy_predictor(mocker, backend_fixtures)
        d = deployment.Deployment(predictor=p)

        d.env_vars = {"FOO": "bar"}
        assert d.env_vars == {"FOO": "bar"}
        assert p.env_vars == {"FOO": "bar"}

        d.env_vars = {"NEW": "1"}
        assert d.env_vars == {"NEW": "1"}
        assert p.env_vars == {"NEW": "1"}

        d.env_vars = None
        assert d.env_vars is None
        assert p.env_vars is None

        d.env_vars = {"AGAIN": "2"}
        assert d.env_vars == {"AGAIN": "2"}
        d.env_vars = {}
        assert d.env_vars == {}
        assert p.env_vars == {}

    # auxiliary methods

    def _get_dummy_predictor(self, mocker, backend_fixtures):
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mocker.patch("hsml.predictor.Predictor._validate_serving_tool")
        mocker.patch("hsml.predictor.Predictor._validate_resources")
        mocker.patch("hsml.predictor.Predictor._validate_script_file")
        mocker.patch("hopsworks_common.util.get_obj_from_json")
        return predictor.Predictor(
            id=p_json["id"],
            name=p_json["name"],
            description=p_json["description"],
            version=p_json["version"],
            model_name=p_json["model_name"],
            model_path=p_json["model_path"],
            model_version=p_json["model_version"],
            model_framework=p_json["model_framework"],
            model_server=p_json["model_server"],
        )
