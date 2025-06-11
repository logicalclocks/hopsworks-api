#
#   Copyright 2025 Hopsworks AB
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
import logging
from datetime import datetime, timedelta
from typing import List, Literal, Optional, Union, get_args

import humps
from hopsworks_common import (
    alert,
    alert_receiver,
    client,
    decorators,
    triggered_alert,
    usage,
)
from hopsworks_common.engine import alerts_engine


_GLOBAL_RECEIVER_PREFIX = "global-receiver"
_API_MATCH = {"key": "api", "value": "alerts-api"}

_PROJECT_JOB_STATUS_ARG = Literal[
    "job_finished",
    "job_failed",
    "job_killed",
    "job_long_running",
]

_PROJECT_JOB_STATUS = get_args(_PROJECT_JOB_STATUS_ARG)

_PROJECT_FS_STATUS_ARG = Literal[
    "validation_success",
    "validation_warning",
    "validation_failure",
    "feature_validation_success",
    "feature_validation_warning",
    "feature_validation_failure",
    "feature_monitor_shift_undetected",
    "feature_monitor_shift_detected",
]
_PROJECT_FS_STATUS = get_args(_PROJECT_FS_STATUS_ARG)

_SEVERITY_ARG = Literal["warning", "critical", "info"]
_SEVERITY = get_args(_SEVERITY_ARG)

_SERVICES_ARG = Literal["Featurestore", "Jobs"]
_SERVICES = get_args(_SERVICES_ARG)

_JOB_STATUS_ARG = Literal[
    "finished",
    "failed",
    "killed",
    "long_running",
    "job_finished",
    "job_failed",
    "job_killed",
    "job_long_running",
]
_JOB_STATUS = get_args(_JOB_STATUS_ARG)

_VALIDATION_STATUS_ARG = Literal[
    "success",
    "warning",
    "failure",
    "validation_success",
    "validation_warning",
    "validation_failure",
    "feature_validation_success",
    "feature_validation_warning",
    "feature_validation_failure",
]
_VALIDATION_STATUS = get_args(_VALIDATION_STATUS_ARG)

_MONITORING_STATUS_ARG = Literal[
    "feature_monitor_shift_undetected",
    "feature_monitor_shift_detected",
]
_MONITORING_STATUS = get_args(_MONITORING_STATUS_ARG)


class AlertsApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    @usage.method_logger
    def get_alerts(self) -> List[alert.ProjectAlert]:
        """
        Get all project alerts.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alert = alerts_api.get_alerts(alert_id=1)

        ```
        # Returns
            `List[ProjectAlert]`: List of ProjectAlert objects.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "service", "alerts"]
        headers = {"content-type": "application/json"}
        return alert.ProjectAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    @decorators.catch_not_found(
        "hopsworks_common.alert.ProjectAlert", fallback_return=None
    )
    def get_alert(self, alert_id: int) -> Optional[alert.ProjectAlert]:
        """
        Get a specific project alert by ID.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alert = alerts_api.get_alert(alert_id=1)

        ```
        # Arguments
            alert_id: The ID of the alert to retrieve.
        # Returns
            `ProjectAlert`: The ProjectAlert object.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "service", "alerts", alert_id]
        headers = {"content-type": "application/json"}
        return alert.ProjectAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    def get_job_alerts(self, job_name: str) -> List[alert.JobAlert]:
        """
        Get all job alerts.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        job_alerts = alerts_api.get_job_alerts(job_name="my_job")

        ```
        # Arguments
            job_name: The name of the job.
        # Returns
            `List[JobAlert]`: List of JobAlert objects.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", job_name, "alerts"]
        headers = {"content-type": "application/json"}
        return alert.JobAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    @decorators.catch_not_found("hopsworks_common.alert.JobAlert", fallback_return=None)
    def get_job_alert(self, job_name: str, alert_id: int) -> Optional[alert.JobAlert]:
        """
        Get a specific job alert by ID.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        job_alerts = alerts_api.get_job_alert(job_name="my_job", alert_id=1)

        ```
        # Arguments
            job_name: The name of the job.
            alert_id: The ID of the alert to retrieve.
        # Returns
            `JobAlert`: The JobAlert object.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job_name,
            "alerts",
            alert_id,
        ]
        headers = {"content-type": "application/json"}
        return alert.JobAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    def get_feature_group_alerts(
        self,
        feature_store_id: int,
        feature_group_id: int,
    ) -> List[alert.FeatureGroupAlert]:
        """
        Get all feature group alerts.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        feature_group_alerts = alerts_api.get_feature_group_alerts(feature_store_id=1, feature_group_id=1)

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_group_id: The ID of the feature group.
        # Returns
            `List[FeatureGroupAlert]`: List of FeatureGroupAlert objects.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            feature_group_id,
            "alerts",
        ]
        headers = {"content-type": "application/json"}
        return alert.FeatureGroupAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    @decorators.catch_not_found(
        "hopsworks_common.alert.FeatureGroupAlert", fallback_return=None
    )
    def get_feature_group_alert(
        self,
        feature_store_id: int,
        feature_group_id: int,
        alert_id: int,
    ) -> Optional[alert.FeatureGroupAlert]:
        """
        Get a specific feature group alert by ID.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        feature_group_alerts = alerts_api.get_feature_group_alert(feature_store_id=1, feature_group_id=1, alert_id=1)

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_group_id: The ID of the feature group.
            alert_id: The ID of the alert to retrieve.
        # Returns
            `FeatureGroupAlert`: The FeatureGroupAlert object.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            feature_group_id,
            "alerts",
            alert_id,
        ]
        headers = {"content-type": "application/json"}
        return alert.FeatureGroupAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    def get_feature_view_alerts(
        self,
        feature_store_id: int,
        feature_view_name: str,
        feature_view_version: int,
    ) -> List[alert.FeatureViewAlert]:
        """
        Get all feature view alerts.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        feature_view_alerts = alerts_api.get_feature_view_alerts(feature_store_id=1, feature_view_name="my_feature_view", feature_view_version=1, alert_id=1)

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_view_name: The name of the feature view.
            feature_view_version: The version of the feature view.
        # Returns
            `List[FeatureViewAlert]`: List of FeatureViewAlert objects.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featureview",
            feature_view_name,
            "version",
            feature_view_version,
            "alerts",
        ]
        headers = {"content-type": "application/json"}
        return alert.FeatureViewAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    @decorators.catch_not_found(
        "hopsworks_common.alert.FeatureViewAlert", fallback_return=None
    )
    def get_feature_view_alert(
        self,
        feature_store_id: int,
        feature_view_name: str,
        feature_view_version: int,
        alert_id: int,
    ) -> Optional[alert.FeatureViewAlert]:
        """
        Get a specific feature view alert by ID.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        feature_view_alerts = alerts_api.get_feature_view_alert(feature_store_id=1, feature_view_name="my_feature_view", feature_view_version=1, alert_id=1)

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_view_name: The name of the feature view.
            feature_view_version: The version of the feature view.
            alert_id: The ID of the alert to retrieve.
        # Returns
            `FeatureViewAlert`: The FeatureViewAlert object.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featureview",
            feature_view_name,
            "version",
            feature_view_version,
            "alerts",
            alert_id,
        ]
        headers = {"content-type": "application/json"}
        return alert.FeatureViewAlert.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    def create_project_alert(
        self,
        receiver: str,
        status: Union[_PROJECT_FS_STATUS_ARG, _PROJECT_JOB_STATUS_ARG],
        severity: _SEVERITY_ARG,
        service: _SERVICES_ARG,
        threshold=0,
    ) -> alert.ProjectAlert:
        """
        Create a new alert.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        new_alert = alerts_api.create_project_alert(receiver="email", status="job_finished", severity="warning", service="Jobs")

        ```
        # Arguments
            status: The status that will trigger the alert (job_finished, job_failed, job_killed, job_long_running, feature_validation_success, feature_validation_warning, feature_validation_failure, feature_monitor_shift_undetected, feature_monitor_shift_detected).
            severity: The severity of the alert (warning, critical, info).
            receiver: The receiver of the alert (e.g., email, webhook).
            service: The service associated with the alert (Featurestore, Jobs).
            threshold: The threshold for the alert (default is 0).
        # Returns
            `ProjectAlert`: The created ProjectAlert object.
        # Raises
            `ValueError`: If the service is not Featurestore or Jobs, or if the status is not valid for the specified service.
            `ValueError`: If the severity is not valid.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if service not in _SERVICES:
            raise ValueError(f"Service must be one of the following {_SERVICES}.")
        if status not in _PROJECT_FS_STATUS + _PROJECT_JOB_STATUS:
            raise ValueError(
                f"Status must be one of the following: {_PROJECT_FS_STATUS + _PROJECT_JOB_STATUS}."
            )
        if severity not in _SEVERITY:
            raise ValueError(f"Severity must be one of the following: {_SEVERITY}.")
        if service == "Featurestore" and status in _PROJECT_JOB_STATUS:
            raise ValueError(
                f"Featurestore service does not support job alerts. Supported values are {_PROJECT_FS_STATUS}."
            )
        if service == "Jobs" and status in _PROJECT_FS_STATUS:
            raise ValueError(
                f"Jobs service does not support featurestore alerts. Supported values are {_PROJECT_JOB_STATUS}."
            )

        # feature_validation_ prefix is added for readablity in the API
        if status.startswith("feature_validation_"):
            status = status.replace("feature_validation_", "validation_")

        _client = client.get_instance()
        receiver = self._fix_receiver_name(receiver, _client._project_name)
        path_params = ["project", _client._project_id, "service", "alerts"]
        alert_data = {
            "status": status.upper(),
            "severity": severity.upper(),
            "receiver": receiver,
            "service": service.upper(),
            "threshold": threshold,
        }
        headers = {"content-type": "application/json"}
        return alert.ProjectAlert.from_response_json(
            _client._send_request(
                "POST", path_params, data=json.dumps(alert_data), headers=headers
            )
        )

    @usage.method_logger
    def create_feature_group_alert(
        self,
        feature_store_id: int,
        feature_group_id: int,
        receiver: str,
        status: Union[_VALIDATION_STATUS_ARG, _MONITORING_STATUS_ARG],
        severity: _SEVERITY_ARG,
    ) -> alert.FeatureGroupAlert:
        """
        Create a new feature group alert.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        new_alert = alerts_api.create_feature_group_alert(67, 1, receiver="email", status="feature_validation_warning", severity="warning")

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_group_id: The ID of the feature group.
            receiver: The receiver of the alert (e.g., email, webhook).
            status: The status that will trigger the alert (feature_validation_success, feature_validation_warning, feature_validation_failure, feature_monitor_shift_undetected, feature_monitor_shift_detected).
            severity: The severity of the alert (warning, critical, info).
        # Returns
            `FeatureGroupAlert`: The created FeatureGroupAlert object.
        # Raises
            `ValueError`: If the status is not valid.
            `ValueError`: If the severity is not valid.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        if status not in _VALIDATION_STATUS + _MONITORING_STATUS:
            raise ValueError(
                f"Status must be one of the following: {_VALIDATION_STATUS + _MONITORING_STATUS}."
            )

        # feature_validation_ prefix is added for readablity in the API
        if status.startswith("feature_validation_"):
            status = status.replace("feature_validation_", "")

        # validation_ prefix is added to match the project created alert API
        if status.startswith("validation_"):
            status = status.replace("validation_", "")

        if severity not in _SEVERITY:
            raise ValueError(f"Severity must be one of the following: {_SEVERITY}.")
        _client = client.get_instance()
        receiver = self._fix_receiver_name(receiver, _client._project_name)
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            feature_group_id,
            "alerts",
        ]

        alert_data = {
            "status": status.upper(),
            "severity": severity.upper(),
            "receiver": receiver,
        }
        headers = {"content-type": "application/json"}
        return alert.FeatureGroupAlert.from_response_json(
            _client._send_request(
                "POST", path_params, data=json.dumps(alert_data), headers=headers
            )
        )

    @usage.method_logger
    def create_feature_view_alert(
        self,
        feature_store_id: int,
        feature_view_name: str,
        feature_view_version: int,
        receiver: str,
        status: _MONITORING_STATUS_ARG,
        severity: _SEVERITY_ARG,
    ) -> alert.FeatureViewAlert:
        """
        Create a new feature view alert.
        ```python
        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        new_alert = alerts_api.create_feature_view_alert(67, "fv", 1, receiver="email", status="feature_monitor_shift_undetected", severity="warning")

        ```
        # Arguments
            feature_store_id: The ID of the feature store.
            feature_view_name: The name of the feature view.
            feature_view_version: The version of the feature view.
            receiver: The receiver of the alert (e.g., email, webhook).
            status: The status that will trigger the alert (feature_monitor_shift_undetected, feature_monitor_shift_detected).
            severity: The severity of the alert (warning, critical, info).
        # Returns
            `FeatureViewAlert`: The created FeatureViewAlert object.
        # Raises
            `ValueError`: if status is not valid.
            `ValueError`: if severity is not valid.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if status not in _MONITORING_STATUS:
            raise ValueError(
                f"Status must be one of the following: {_MONITORING_STATUS}."
            )
        if severity not in _SEVERITY:
            raise ValueError(f"Severity must be one of the following: {_SEVERITY}.")
        _client = client.get_instance()
        receiver = self._fix_receiver_name(receiver, _client._project_name)
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featureview",
            feature_view_name,
            "version",
            feature_view_version,
            "alerts",
        ]
        alert_data = {
            "status": status.upper(),
            "severity": severity.upper(),
            "receiver": receiver,
        }
        headers = {"content-type": "application/json"}
        return alert.FeatureViewAlert.from_response_json(
            _client._send_request(
                "POST", path_params, data=json.dumps(alert_data), headers=headers
            )
        )

    @usage.method_logger
    def create_job_alert(
        self,
        job_name: str,
        receiver: str,
        status: _JOB_STATUS_ARG,
        severity: _SEVERITY_ARG,
    ) -> alert.JobAlert:
        """
        Create a new job alert.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        new_alert = alerts_api.create_job_alert(job_name="my_job", receiver="email", status="finished", severity="warning")

        ```
        # Arguments
            job_name: The name of the job.
            receiver: The receiver of the alert (e.g., email, webhook).
            status: The status of the alert (finished, failed, killed, long_running).
            severity: The severity of the alert (warning, critical, info).
        # Returns
            `JobAlert`: The created JobAlert object.
        # Raises
            `ValueError`: If the job name is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if status not in _JOB_STATUS:
            raise ValueError(f"Status must be one of the following: {_JOB_STATUS}.")
        if severity not in _SEVERITY:
            raise ValueError(f"Severity must be one of the following: {_SEVERITY}.")

        # job_ prefix is added to match the project created alert API
        if status.startswith("job_"):
            status = status.replace("job_", "")

        _client = client.get_instance()
        receiver = self._fix_receiver_name(receiver, _client._project_name)
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job_name,
            "alerts",
        ]
        alert_data = {
            "status": status.upper(),
            "severity": severity.upper(),
            "receiver": receiver,
        }
        headers = {"content-type": "application/json"}
        return alert.JobAlert.from_response_json(
            _client._send_request(
                "POST", path_params, data=json.dumps(alert_data), headers=headers
            )
        )

    @usage.method_logger
    def get_alert_receivers(self) -> List[alert_receiver.AlertReceiver]:
        """
        Get all alert receivers.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alert_receivers = alerts_api.get_alert_receivers()

        ```
        # Returns
            `List[AlertReceiver]`: List of alert receivers.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "alerts", "receivers"]
        query_params = {"expand": True, "global": True}
        headers = {"content-type": "application/json"}
        return alert_receiver.AlertReceiver.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    @usage.method_logger
    @decorators.catch_not_found(
        "hopsworks_common.alert_receiver.AlertReceiver", fallback_return=None
    )
    def get_alert_receiver(self, name: str) -> Optional[alert_receiver.AlertReceiver]:
        """
        Get a specific alert receivers by name.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alert_receiver = alerts_api.get_alert_receiver("email")

        ```
        # Arguments
            name: The name of the alert receiver to retrieve.
        # Returns
            `AlertReceiver`: The alert receiver object.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        name = self._fix_receiver_name(name, _client._project_name)
        path_params = ["project", _client._project_id, "alerts", "receivers", name]
        headers = {"content-type": "application/json"}
        return alert_receiver.AlertReceiver.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    @usage.method_logger
    def create_alert_receiver(
        self,
        name: str,
        email_configs: List[alert_receiver.EmailConfig] = None,
        slack_configs: List[alert_receiver.SlackConfig] = None,
        pagerduty_configs: List[alert_receiver.PagerDutyConfig] = None,
        webhook_configs: List[alert_receiver.WebhookConfig] = None,
    ) -> alert_receiver.AlertReceiver:
        """
        Create a new alert receiver.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        new_alert_receiver = alerts_api.create_alert_receiver(name="email", email_configs=[{"to": "email@mail.com"}])

        ```
        # Arguments
            name: The name of the alert receiver (e.g., email, webhook).
            email_configs: List of email configurations (optional).
            slack_configs: List of Slack configurations (optional).
            pagerduty_configs: List of PagerDuty configurations (optional).
            webhook_configs: List of webhook configurations (optional).
        # Returns
            `AlertReceiver`: The created alert receiver object.
        # Raises
            `ValueError`: If multiple configurations are provided.
            `ValueError`: If the global channel for the configuration is not configured.
            'TimeoutError': If the alert receiver creation times out.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        configured_receivers = self._get_configured_receivers()
        if (
            email_configs is not None
            and configured_receivers["email_configured"] is False
        ):
            raise ValueError(
                "Email channel is not configured. Please contact an administrator."
            )
        if (
            slack_configs is not None
            and configured_receivers["slack_configured"] is False
        ):
            raise ValueError(
                "Slack channel is not configured. Please contact an administrator."
            )
        if (
            pagerduty_configs is not None
            and configured_receivers["pager_duty_configured"] is False
        ):
            raise ValueError(
                "PagerDuty channel is not configured. Please contact an administrator."
            )

        non_none_args = [
            arg
            for arg in (
                email_configs,
                slack_configs,
                pagerduty_configs,
                webhook_configs,
            )
            if arg is not None
        ]
        if len(non_none_args) > 1:
            raise ValueError(
                "Only one of email_configs, slack_configs, pagerduty_configs, or webhook_configs can be provided."
            )

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "alerts", "receivers"]
        headers = {"content-type": "application/json"}
        data = {
            "name": name,
            "emailConfigs": self._validate_receiver_config(
                email_configs, alert_receiver.EmailConfig
            ),
            "slackConfigs": self._validate_receiver_config(
                slack_configs, alert_receiver.SlackConfig
            ),
            "pagerDutyConfigs": self._validate_receiver_config(
                pagerduty_configs, alert_receiver.PagerDutyConfig
            ),
            "webhookConfigs": self._validate_receiver_config(
                webhook_configs, alert_receiver.WebhookConfig
            ),
        }
        query_params = {"defaultTemplate": True}

        _client._send_request(
            "POST",
            path_params,
            data=json.dumps(data),
            query_params=query_params,
            headers=headers,
        )
        return alerts_engine.AlertsEngine().await_receiver(name)

    @usage.method_logger
    def delete_alert(self, alert_id: int):
        """
        Delete an alert by ID.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alerts_api.delete_alert(alert_id=1)

        ```
        # Arguments
            alert_id: The ID of the alert to delete.
        # Returns
            `None`
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "service", "alerts", alert_id]
        headers = {"content-type": "application/json"}
        _client._send_request("DELETE", path_params, headers=headers)
        self._log.info(f"Alert with ID {alert_id} deleted successfully.")

    @usage.method_logger
    def trigger_alert(
        self,
        receiver_name: str,
        title: str,
        summary: str,
        description: str,
        severity: _SEVERITY_ARG,
        status: str,
        name: str,
        generator_url: str = None,
        expire_after_sec: int = None,
    ):
        """
        Trigger an alert.
        ```python

        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        alerts_api.trigger_alert(receiver_name="email", title="Title", summary="Alert summary", description="Alert description", severity="info", status="script_finished", name="my_alert")

        ```
        # Arguments
            receiver_name: The receiver of the alert (e.g., email, webhook).
            summary: The summary of the alert.
            description: The description of the alert.
            severity: The severity of the alert (warning, critical, info).
            status: The status of the alert.
            name: The name of the alert.
            generator_url: The URL of the alert generator (optional).
            expire_after_sec: The time in seconds after which the alert should expire (optional).
        # Returns
            `None`
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if severity not in _SEVERITY:
            raise ValueError(f"Severity must be one of the following: {_SEVERITY}.")
        _client = client.get_instance()
        self._create_route_if_not_exist(receiver_name, status, severity)
        path_params = ["project", _client._project_id, "alerts"]
        headers = {"content-type": "application/json"}
        data = {
            "alerts": [
                {
                    "labels": [
                        {"key": "alertname", "value": name},
                        {"key": "severity", "value": severity},
                        {"key": "status", "value": status},
                        _API_MATCH,
                    ],
                    "annotations": [
                        {"key": "title", "value": title},
                        {"key": "summary", "value": summary},
                        {"key": "description", "value": description},
                    ],
                    "startsAt": datetime.now().isoformat()
                    if expire_after_sec
                    else None,
                    "endsAt": (
                        datetime.now() + timedelta(seconds=expire_after_sec)
                    ).isoformat()
                    if expire_after_sec
                    else None,
                    "generatorURL": generator_url,
                }
            ]
        }
        _client._send_request(
            "POST", path_params, data=json.dumps(data), headers=headers
        )

    @usage.method_logger
    def get_triggered_alerts(
        self, active: bool = True, silenced: bool = False, inhibited: bool = False
    ) -> List[triggered_alert.TriggeredAlert]:
        """
        Get triggered alerts.
        ```python
        import hopsworks

        project = hopsworks.login()

        alerts_api = project.get_alerts_api()

        triggered_alerts = alerts_api.get_triggered_alerts()

        ```
        # Arguments
            active: Whether to include active alerts (default is True).
            silenced: Whether to include silenced alerts (default is False).
            inhibited: Whether to include inhibited alerts (default is False).
        # Returns
            `List[TriggeredAlert]`: The triggered alert objects.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "alerts"]
        query_params = {"active": active, "silenced": silenced, "inhibited": inhibited}
        headers = {"content-type": "application/json"}
        return triggered_alert.TriggeredAlert.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    def _get_configured_receivers(self):
        """
        Get configured alert receivers.
        :return: A list of configured alert receivers.
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "alerts", "receivers", "default"]
        headers = {"content-type": "application/json"}
        return humps.decamelize(
            _client._send_request("GET", path_params, headers=headers)
        )

    def _create_route_if_not_exist(
        self, receiver_name: str, status: str, severity: _SEVERITY_ARG
    ):
        """
        Create a route for the alert receiver.
        :return: None
        """
        _client = client.get_instance()
        if receiver_name is None:
            raise ValueError("Receiver name cannot be None.")

        receiver_name = self._fix_receiver_name(receiver_name, _client._project_name)
        # Check if the receiver exists
        receiver = self.get_alert_receiver(receiver_name)
        if receiver is None:
            raise ValueError(f"Receiver {receiver_name} does not exist.")

        # Only create a route if the receiver is a project receiver
        if receiver_name.startswith(f"{_client._project_name}__"):
            path_params = ["project", _client._project_id, "alerts", "routes"]
            headers = {"content-type": "application/json"}
            match = [
                {"key": "status", "value": status},
                {"key": "severity", "value": severity},
                _API_MATCH,
            ]
            data = {
                "receiver": receiver_name,
                "status": status,
                "groupBy": ["..."],  # aggregate by all possible labels
                "match": match,
                "continue": True,
            }

            try:
                _client._send_request(
                    "POST", path_params, data=json.dumps(data), headers=headers
                )
                alerts_engine.AlertsEngine().await_route(
                    receiver_name,
                    match,
                )
            except client.exceptions.RestAPIError as e:
                if (
                    e.response.status_code == 400
                    and e.response.json().get("errorCode", "") == 390002
                    and "already exists" in e.response.json().get("errorMsg", "")
                ):
                    self._log.info(
                        f"Route for receiver {receiver_name} and match {status} already exists."
                    )
                else:
                    raise e

    def _fix_receiver_name(self, name, project_name):
        """
        Fix the receiver name by adding the project name prefix if necessary.
        :param name: The name of the receiver.
        :param project_name: The name of the project.
        :return: The fixed receiver name.
        """
        if not name.startswith(f"{_GLOBAL_RECEIVER_PREFIX}__") and not name.startswith(
            f"{project_name}__"
        ):
            return f"{project_name}__{name}"
        return name

    def _validate_receiver_config(self, items, cls):
        if items is None or len(items) == 0:
            return None
        if not isinstance(items, list):
            raise ValueError(f"Expected list, got {type(items)}")
        normalized = []
        for item in items:
            if isinstance(item, cls):
                normalized.append(item.to_dict())
            elif isinstance(item, dict):
                json_decamelized = humps.decamelize(item)
                normalized.append(cls(**json_decamelized).to_dict())
            else:
                raise ValueError(f"Invalid item type: {type(item)} for {cls}")
        return normalized
