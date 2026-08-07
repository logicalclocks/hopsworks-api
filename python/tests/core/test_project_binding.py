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

"""What a handle for another project actually puts on the wire.

Three reviews found the same defect in whichever handle was looked at, and each fix was
verified by asserting that the handle carried the right project id. That is not the same
question: a bound handle can still reach the connection's project one layer down, through
the engine that polls a command, or through an object it hands back. So these tests read
the recorded request paths rather than the objects' attributes, and they drive the nested
paths a review has to guess at otherwise.

The login project is 5 throughout, and 42 is the project every assertion is about.
"""

import pytest
from hopsworks_common.client.exceptions import KafkaException


LOGIN_ID = 5
LOGIN_NAME = "login_project"
OTHER_ID = 42
OTHER_NAME = "project_b"


class Recorder:
    """A client that answers requests from a canned body and remembers what was asked."""

    def __init__(self, response=None):
        self._project_id = LOGIN_ID
        self._project_name = LOGIN_NAME
        self._base_url = "https://hopsworks"
        self._host = "hopsworks.example.com"
        self.response = response if response is not None else {}
        self.paths = []
        self.payloads = []

    def _send_request(self, method, path_params, **kwargs):
        path = [str(p) for p in path_params]
        self.paths.append(path)
        self.payloads.append(kwargs.get("data"))
        return self.response(path) if callable(self.response) else self.response

    def _is_external(self):
        return False

    def _get_ca_chain_path(self):
        return "/tmp/ca_chain.pem"

    def _get_client_cert_path(self):
        return "/tmp/client_cert.pem"

    def _get_client_key_path(self):
        return "/tmp/client_key.pem"


@pytest.fixture
def recorder(mocker):
    """Install a recording client under every name the code under test looks it up by."""
    from hopsworks_common import client

    # Deployment payloads are parsed through the serving limits, which a live connection reads
    # from the cluster.
    client._set_serving_num_instances_limits([1, -1])

    def _install(response=None):
        rec = Recorder(response)
        for module in ("hopsworks_common.client", "hsml.client"):
            mocker.patch(f"{module}._get_instance", return_value=rec)
        return rec

    yield _install
    client._set_serving_num_instances_limits(None)


def project_ids(paths):
    """The project each recorded path addressed, in order."""
    return [p[1] for p in paths if len(p) > 1 and p[0] == "project"]


def _deployment_body(backend_fixtures):
    """One deployment, as the single-deployment endpoints return it."""
    return backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
        "items"
    ][0]


class TestEnvironmentBinding:
    def test_creation_polls_the_project_it_created_in(self, recorder, mocker):
        """The engine used to poll the connection's project for a command created in another.

        The environment appears in project 42 and the command that builds it is 42's, so an
        engine on the login project either waits for something that never arrives or reads a
        command belonging to a different environment of the same name.
        """
        from hopsworks_common.core import environment_api

        rec = recorder({"name": "env", "commands": {"items": [{"status": "SUCCESS"}]}})
        mocker.patch("time.sleep")

        environment_api.EnvironmentApi(OTHER_ID, OTHER_NAME).create_environment("env")

        assert project_ids(rec.paths) == [str(OTHER_ID), str(OTHER_ID)]

    def test_a_returned_environment_installs_where_it_lives(self, recorder):
        """An Environment builds its own handles, and used to build them unbound."""
        from hopsworks_common.core import environment_api

        environment_body = {
            "name": "env",
            "commands": {"items": [{"status": "SUCCESS"}]},
        }
        library_body = {
            "channel": "requirements_txt",
            "package_source": "REQUIREMENTS_TXT",
            "library": "requirements.txt",
            "version": "1.0",
        }
        rec = recorder(
            lambda path: library_body if "libraries" in path else environment_body
        )
        env = environment_api.EnvironmentApi(OTHER_ID, OTHER_NAME).get_environment(
            "env"
        )

        env.install_requirements("Resources/requirements.txt", await_installation=False)

        assert project_ids(rec.paths) == [str(OTHER_ID), str(OTHER_ID), str(OTHER_ID)]
        # And the file is resolved against that project's Resources, not the login project's.
        assert f"/Projects/{OTHER_NAME}/Resources/requirements.txt" in rec.payloads[-1]

    def test_a_returned_environment_deletes_itself_where_it_lives(self, recorder):
        from hopsworks_common.core import environment_api

        rec = recorder({"name": "env"})
        env = environment_api.EnvironmentApi(OTHER_ID, OTHER_NAME).get_environment(
            "env"
        )

        env.delete()

        assert project_ids(rec.paths)[-1] == str(OTHER_ID)

    def test_an_unbound_environment_still_uses_the_connection(self, recorder):
        from hopsworks_common.core import environment_api

        rec = recorder({"name": "env"})
        env = environment_api.EnvironmentApi().get_environment("env")

        env.delete()

        assert project_ids(rec.paths) == [str(LOGIN_ID), str(LOGIN_ID)]


class TestGitBinding:
    def test_execution_polling_addresses_the_bound_project(self, recorder):
        """GitApi started the operation in 42 and its engine polled 5 for the result."""
        from hopsworks_common.core import git_api

        rec = recorder(
            {
                "id": 3,
                "submission_time": "",
                "execution_start": 0,
                "execution_stop": 0,
                "user": None,
                "git_command_configuration": {},
                "state": "SUCCESS",
                "config_secret": "",
                "repository": {"id": 9, "name": "repo", "creator": None},
            }
        )
        api = git_api.GitApi(OTHER_ID, OTHER_NAME)

        api._git_engine._git_op_execution_api._get_execution(9, 3)

        assert project_ids(rec.paths) == [str(OTHER_ID)]

    def test_a_returned_repository_stays_in_its_project(self, recorder):
        from hopsworks_common.core import git_api

        recorder({"count": 1, "items": [{"id": 9, "name": "repo", "creator": None}]})
        repos = git_api.GitApi(OTHER_ID, OTHER_NAME).get_repos()

        # The git handle, the remote handle, and the dataset handle its file operations use.
        assert repos[0]._git_api._project_id == OTHER_ID
        assert repos[0]._git_remote_api._project_id == OTHER_ID
        assert repos[0]._dataset_api._project_id == OTHER_ID
        assert repos[0]._dataset_api._project_name == OTHER_NAME
        # Including the engine the remote handle waits on.
        assert (
            repos[0]._git_remote_api._git_engine._git_op_execution_api._project_id
            == OTHER_ID
        )


class TestKafkaBinding:
    def test_a_returned_topic_is_deleted_where_it_lives(self, recorder):
        from hopsworks_common.core import kafka_api

        rec = recorder({"count": 1, "items": [{"name": "topic"}]})
        topic = kafka_api.KafkaApi(OTHER_ID, OTHER_NAME).get_topics()[0]

        topic._kafka_api._delete_topic("topic")

        assert project_ids(rec.paths) == [str(OTHER_ID), str(OTHER_ID)]

    def test_a_returned_topic_keeps_its_project_across_a_reread(self, recorder):
        from hopsworks_common.core import kafka_api

        recorder({"count": 1, "items": [{"name": "topic"}]})
        topic = kafka_api.KafkaApi(OTHER_ID, OTHER_NAME).get_topics()[0]

        topic.update_from_response_json({"name": "topic"})

        assert topic._kafka_api._project_id == OTHER_ID

    def test_default_config_refuses_a_project_the_certificate_is_not_for(
        self, recorder
    ):
        """The certificate is the Kafka identity, and it belongs to the login project.

        Returning this configuration for another project would produce and consume as the
        wrong identity and look like it had worked.
        """
        from hopsworks_common.core import kafka_api

        recorder()

        with pytest.raises(KafkaException) as e:
            kafka_api.KafkaApi(OTHER_ID, OTHER_NAME).get_default_config()

        assert OTHER_NAME in str(e.value)
        assert LOGIN_NAME in str(e.value)

    def test_default_config_still_serves_the_login_project(self, recorder):
        from hopsworks_common.core import kafka_api

        recorder({"brokers": ["INTERNAL://broker:9091"]})

        config = kafka_api.KafkaApi(LOGIN_ID, LOGIN_NAME).get_default_config()

        assert config["ssl.certificate.location"] == "/tmp/client_cert.pem"


class TestModelServingBinding:
    def test_the_named_projects_models_dataset_is_the_one_validated(self, recorder):
        """The check answered for the login project, so serving-disabled in 42 passed."""
        from hsml.core import model_serving_api

        rec = recorder({"attributes": {}})

        model_serving_api.ModelServingApi()._get(OTHER_NAME, OTHER_ID)

        assert project_ids(rec.paths) == [str(OTHER_ID)]

    def test_deployments_are_read_and_operated_in_their_project(
        self, recorder, backend_fixtures
    ):
        from hsml.model_serving import ModelServing

        rec = recorder(_deployment_body(backend_fixtures))
        deployment = ModelServing(OTHER_NAME, OTHER_ID).get_deployment_by_id(1)

        assert project_ids(rec.paths) == [str(OTHER_ID)]
        # The deployment carries the project it came from into everything it then does.
        assert deployment.model_registry_id == OTHER_ID
        assert deployment.project_name == OTHER_NAME
        assert deployment._serving_api._project_id == OTHER_ID
        assert deployment._serving_engine._serving_api._project_id == OTHER_ID
        assert deployment._serving_engine._dataset_api._project_id == OTHER_ID
        assert deployment._serving_engine._dataset_api._project_name == OTHER_NAME

    def test_a_deployments_tags_are_written_in_its_project(
        self, recorder, backend_fixtures
    ):
        from hsml.model_serving import ModelServing

        rec = recorder(_deployment_body(backend_fixtures))
        deployment = ModelServing(OTHER_NAME, OTHER_ID).get_deployment_by_id(1)

        deployment.add_tag("owner", "team")

        assert project_ids(rec.paths)[-1] == str(OTHER_ID)
        assert rec.paths[-1][-2:] == ["tags", "owner"]

    def test_a_deployments_logs_are_read_in_its_project(
        self, recorder, backend_fixtures
    ):
        from hsml.model_serving import ModelServing

        rec = recorder(_deployment_body(backend_fixtures))
        deployment = ModelServing(OTHER_NAME, OTHER_ID).get_deployment_by_id(1)

        deployment._serving_api._get_logs(deployment, "predictor", 10)

        assert project_ids(rec.paths)[-1] == str(OTHER_ID)
        assert rec.paths[-1][-1] == "logs"

    def test_the_inference_url_names_the_deployments_project(
        self, recorder, backend_fixtures
    ):
        """The Hopsworks inference path carried the login project id for a foreign deployment."""
        from hsml.model_serving import ModelServing

        recorder(_deployment_body(backend_fixtures))
        deployment = ModelServing(OTHER_NAME, OTHER_ID).get_deployment_by_id(1)

        url = deployment.predictor.get_inference_url()

        assert f"/project/{OTHER_ID}/inference/" in url
