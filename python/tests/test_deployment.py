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
            model_name=p_json["model_name"],
            model_path=p_json["model_path"],
            model_version=p_json["model_version"],
            model_framework=p_json["model_framework"],
            model_server=p_json["model_server"],
            artifact_version=p_json["artifact_version"],
        )
