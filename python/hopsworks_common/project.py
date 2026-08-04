#
#   Copyright 2022 Logical Clocks AB
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
from typing import TYPE_CHECKING, Literal

import humps
from hopsworks_apigen import deprecated, public
from hopsworks_common import alert, client, project_member, util
from hopsworks_common.core import (
    alerts_api,
    app_api,
    dataset_api,
    environment_api,
    git_api,
    job_api,
    kafka_api,
    opensearch_api,
    project_members_api,
    search_api,
    superset_api,
    trino_api,
)


if TYPE_CHECKING:
    from hsfs.feature_store import FeatureStore
    from hsml.model_registry import ModelRegistry
    from hsml.model_serving import ModelServing


@public("hopsworks.project.Project")
class Project:
    """Class representing a Hopsworks project.

    Use [`hopsworks.login`][hopsworks.login] to get the current project after logging in.

    Use [`hopsworks.create_project`][hopsworks.create_project] to create a new project and get the project object.
    """

    def __init__(
        self,
        archived=None,
        created=None,
        description=None,
        docker_image=None,
        hops_examples=None,
        inodeid=None,
        is_old_docker_image=None,
        is_preinstalled_docker_image=None,
        owner=None,
        project_id=None,
        project_name=None,
        project_team=None,
        quotas=None,
        retention_period=None,
        services=None,
        datasets=None,
        creation_status=None,
        project_namespace=None,
        **kwargs,
    ):
        self._id = project_id
        self._name = project_name
        self._owner = owner
        self._description = description
        self._created = created

        self._app_api = app_api.AppApi()
        self._opensearch_api = opensearch_api.OpenSearchApi()
        self._kafka_api = kafka_api.KafkaApi()
        self._job_api = job_api.JobApi()
        self._jobs_api = self._job_api  # deprecated
        self._git_api = git_api.GitApi()
        self._dataset_api = dataset_api.DatasetApi()
        self._environment_api = environment_api.EnvironmentApi()
        self._alerts_api = alerts_api.AlertsApi()
        self._project_members_api = project_members_api.ProjectMembersApi()
        self._search_api = search_api.SearchApi()
        self._project_namespace = project_namespace
        self._trino_api = None
        self._superset_api = None

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        return None

    @public
    @property
    def id(self):
        """Id of the project."""
        return self._id

    @public
    @property
    def name(self):
        """Name of the project."""
        return self._name

    @public
    @property
    def owner(self):
        """Owner of the project."""
        return self._owner

    @public
    @property
    def description(self):
        """Description of the project."""
        return self._description

    @public
    @property
    def created(self):
        """Timestamp when the project was created."""
        return self._created

    @public
    @property
    def project_namespace(self):
        """Kubernetes namespace used by project."""
        return self._project_namespace

    @public
    @property
    def home_path(self) -> str:
        """Path to the current user's home directory within this project.

        The home directory is located at `/Projects/<project_name>/Users/<username>`
        and is created automatically when a user joins a project.
        """
        _client = client._get_instance()
        if hasattr(_client, "_username") and _client._username:
            # External client stores the username directly
            username = _client._username
        else:
            # Internal client: HDFS user is formatted as <project_name>__<username>
            username = _client._project_user().split("__", 1)[1]
        return f"/Projects/{self._name}/Users/{username}"

    @public
    def get_feature_store(self, name: str | None = None) -> FeatureStore:
        """Connect to Project's Feature Store.

        Defaulting to the project name of default feature store. To get a
        shared feature store, the project name of the feature store is required.

        Example: Example for getting the Feature Store API of a project
            ```python
            import hopsworks

            project = hopsworks.login()

            fs = project.get_feature_store()
            ```

        Parameters:
            name: Project name of the feature store.

        Returns:
            The Feature Store API.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return client._get_connection()._get_feature_store(name)

    @public
    def get_model_registry(self) -> ModelRegistry:
        """Connect to Project's Model Registry API.

        Example: Example for getting the Model Registry API of a project
            ```python
            import hopsworks

            project = hopsworks.login()

            mr = project.get_model_registry()
            ```

        Returns:
            The Model Registry API.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return client._get_connection()._get_model_registry()

    @public
    def get_model_serving(self) -> ModelServing:
        """Connect to Project's Model Serving API.

        Example: Example for getting the Model Serving API of a project
            ```python
            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            ```

        Returns:
            The Model Serving API.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return client._get_connection()._get_model_serving()

    @public
    def get_kafka_api(self) -> kafka_api.KafkaApi:
        """Get the kafka api for the project.

        Returns:
            The Kafka Api handle.
        """
        _client = client._get_instance()
        if _client._is_external():
            _client._download_certs()
        return self._kafka_api

    @public
    def get_opensearch_api(self) -> opensearch_api.OpenSearchApi:
        """Get the opensearch api for the project.

        Returns:
            The OpenSearch Api handle.
        """
        _client = client._get_instance()
        if _client._is_external():
            _client._download_certs()
        return self._opensearch_api

    @public
    def get_job_api(self) -> job_api.JobApi:
        """Get the job API for the project.

        Returns:
            The Job Api handle.
        """
        return self._job_api

    @public
    def get_app_api(self) -> app_api.AppApi:
        """Get the app API for the project.

        Use this to manage Streamlit apps.

        Example:
            ```python
            apps = project.get_app_api()
            for app in apps.get_apps():
                print(f"{app.name}: {app.state}")
            ```

        Returns:
            The App Api handle.
        """
        return self._app_api

    @deprecated("hopsworks.project.Project.get_job_api")
    def get_jobs_api(self):
        """**Deprecated**, use get_job_api instead. Excluded from docs to prevent API breakage."""
        return self.get_job_api()

    @public
    def get_git_api(self) -> git_api.GitApi:
        """Get the git repository api for the project.

        Returns:
            The Git Api handle.
        """
        return self._git_api

    @public
    def get_dataset_api(self) -> dataset_api.DatasetApi:
        """Get the dataset api for the project.

        Returns:
            The Datasets Api handle.
        """
        return self._dataset_api

    @public
    def get_environment_api(self) -> environment_api.EnvironmentApi:
        """Get the Python environment API for the project.

        Returns:
            The Python Environment Api handle.
        """
        return self._environment_api

    @public
    def get_alerts_api(self) -> alerts_api.AlertsApi:
        """Get the alerts api for the project.

        Returns:
            The Alerts Api handle.
        """
        return self._alerts_api

    @public
    def get_members_api(self) -> project_members_api.ProjectMembersApi:
        """Get the project members API for the project.

        Use this to manage who has access to the project and at which role.

        Returns:
            The Project Members Api handle.
        """
        return self._project_members_api

    @public
    def get_members(self) -> list[project_member.ProjectMember]:
        """Get all members of the project.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            for member in project.get_members():
                print(member.email, member.role)
            ```

        Returns:
            List of `ProjectMember` objects, one per user who has access to the project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._project_members_api.get_members()

    @public
    def add_member(self, email: str, role: str) -> project_member.ProjectMember:
        """Add a user to the project.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.add_member("alice@example.com", "Data scientist")
            ```

        Parameters:
            email: Email address of the user to add.
            role: The project role to grant, one of `Data owner`, `Data scientist`,
                `Observer`, `Feature store restricted`.

        Returns:
            The newly added `ProjectMember`.

        Raises:
            ValueError: If `role` is not a settable project role.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if the caller is not a project owner.
        """
        return self._project_members_api.add_member(email, role)

    @public
    def remove_member(self, email: str, delete_home_dir: bool = False) -> None:
        """Remove a user from the project.

        Danger: Deletes the member's project files when `delete_home_dir=True`
            All files under this member's home directory in the project are
            permanently deleted and cannot be recovered.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            project.remove_member("alice@example.com")
            ```

        Parameters:
            email: Email address of the member to remove.
            delete_home_dir: Whether to also delete the member's home directory in the project.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request, for example if a data scientist tries to remove someone other than themselves.
        """
        self._project_members_api.remove_member(email, delete_home_dir=delete_home_dir)

    @public
    def get_search_api(self) -> search_api.SearchApi:
        """Get the search api for the project.

        Returns:
            The Search Api handle.
        """
        return self._search_api

    @public
    def get_trino_api(self) -> trino_api.TrinoApi:
        """Get the Trino API for the project.

        Returns:
            The Trino API handle.
        """
        if self._trino_api is None:
            self._trino_api = trino_api.TrinoApi(project=self)
        return self._trino_api

    @public
    def get_superset_api(self) -> superset_api.SupersetApi:
        """Get the Superset API for the project.

        Returns:
            The Superset API handle.
        """
        if self._superset_api is None:
            self._superset_api = superset_api.SupersetApi(project=self)
        return self._superset_api

    @public
    def get_alerts(self) -> list[alert.ProjectAlert]:
        """Get all alerts for the project.

        Returns:
            List of `ProjectAlert` objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._alerts_api.get_alerts()

    @public
    def get_alert(self, alert_id: int) -> alert.ProjectAlert | None:
        """Get an alert for the project by ID.

        Parameters:
            alert_id: The ID of the alert.

        Returns:
            The ProjectAlert object.
        """
        return self._alerts_api.get_alert(alert_id)

    @public
    def create_job_alert(
        self,
        receiver: str,
        status: Literal["job_finished", "job_failed", "job_killed", "job_long_running"],
        severity: Literal["critical", "warning", "info"],
    ) -> alert.ProjectAlert:
        """Create an alert for jobs in this project.

        Example: Example for creating a job alert
            ```python
            import hopsworks
            project = hopsworks.login()
            project.create_job_alert("my_receiver", "long_running", "info")
            ```

        Parameters:
            receiver: The receiver of the alert.
            status: The status of the alert.
            severity: The severity of the alert.

        Returns:
            The created `ProjectAlert` object.

        Raises:
            ValueError: If `status` or `severity` is invalid, also if `receiver` is `None`.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._alerts_api.create_project_alert(receiver, status, severity, "Jobs")

    @public
    def create_featurestore_alert(
        self,
        receiver: str,
        status: Literal[
            "feature_validation_success",
            "feature_validation_warning",
            "feature_validation_failure",
            "monitoring_shift_undetected",
            "monitoring_shift_detected",
            "monitoring_empty_detection_window",
            # deprecated since ~=3.8.1; kept for one release
            "feature_monitor_shift_undetected",
            "feature_monitor_shift_detected",
        ],
        severity: Literal["critical", "warning", "info"],
    ) -> alert.ProjectAlert:
        """Create an alert for feature validation and monitoring in this project.

        Example: Example for creating a featurestore alert
            ```python
            import hopsworks
            project = hopsworks.login()
            project.create_featurestore_alert("my_receiver", "feature_validation_success", "info")
            ```

        Parameters:
            receiver: The receiver of the alert.
            status: The status of the alert.
                The names feature_monitor_shift_undetected and
                feature_monitor_shift_detected are deprecated since ~=3.8.1 and will
                be removed in a future release.
            severity: The severity of the alert.

        Returns:
            The created `ProjectAlert` object.

        Raises:
            ValueError: If `status` or `severity` is invalid, also if `receiver` is `None`.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._alerts_api.create_project_alert(
            receiver, status, severity, "Featurestore"
        )

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        if self._description is not None:
            return f"Project({self._name!r}, {self._owner!r}, {self._description!r})"
        return f"Project({self._name!r}, {self._owner!r})"

    @public
    def get_url(self):
        """Get url to the project in Hopsworks."""
        path = "/p/" + str(self.id)
        return util._get_hostname_replaced_url(path)
