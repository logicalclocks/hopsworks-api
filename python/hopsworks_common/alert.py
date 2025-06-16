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
from typing import Optional

import humps
from hopsworks_common import util


class Alert:
    def __init__(
        self,
        id=None,
        status=None,
        alert_type=None,
        severity=None,
        receiver=None,
        created=None,
        **kwargs,
    ):
        self._id = id
        self._status = status
        self._alert_type = alert_type
        self._severity = severity
        self._receiver = receiver
        self._created = created

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if "items" in json_decamelized:
                return [cls(**receiver) for receiver in json_decamelized["items"]]
            else:
                return []
        else:
            return cls(**json_decamelized)

    @property
    def id(self) -> Optional[int]:
        """return the id of the alert"""
        return self._id

    @property
    def status(self) -> Optional[str]:
        """return the status of the alert"""
        return self._status

    @property
    def alert_type(self) -> Optional[str]:
        """return the type of the alert"""
        return self._alert_type

    @property
    def severity(self) -> Optional[str]:
        """return the severity of the alert"""
        return self._severity

    @property
    def receiver(self) -> Optional[str]:
        """return the receiver of the alert"""
        return self._receiver

    @property
    def created(self) -> Optional[str]:
        """return the creation time of the alert"""
        return self._created

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the alert as a dictionary"""
        return {
            "id": self._id,
            "status": self._status,
            "severity": self._severity,
            "receiver": self._receiver,
            "created": self._created,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Alert({self._id!r}, {self._status!r}, {self._severity!r}, {self._receiver!r}, {self._created!r})"


class ProjectAlert(Alert):
    NOT_FOUND_ERROR_CODE = 150078

    def __init__(
        self,
        id=None,
        project_name=None,
        status=None,
        alert_type=None,
        severity=None,
        receiver=None,
        created=None,
        service=None,
        threshold=None,
        **kwargs,
    ):
        super().__init__(
            id=id,
            status=status,
            alert_type=alert_type,
            severity=severity,
            receiver=receiver,
            created=created,
            **kwargs,
        )
        self._project_name = project_name
        self._threshold = threshold
        self._service = service

    @property
    def project_name(self) -> Optional[str]:
        """return the name of the project"""
        return self._project_name

    @property
    def service(self) -> Optional[str]:
        """return the name of the service"""
        return self._service

    @property
    def threshold(self) -> Optional[str]:
        """return the threshold of the alert"""
        return self._threshold

    def to_dict(self):
        """return the alert as a dictionary"""
        return {
            "id": self._id,
            "project_name": self._project_name,
            "status": self._status,
            "severity": self._severity,
            "receiver": self._receiver,
            "created": self._created,
            "service": self._service,
            "threshold": self._threshold,
        }

    def __repr__(self):
        return f"ProjectAlert({self._id!r}, {self._project_name!r}, {self._status!r}, {self._severity!r}, {self._receiver!r}, {self._created!r}, {self._service!r}, {self._threshold!r})"


class JobAlert(Alert):
    NOT_FOUND_ERROR_CODE = 130034

    def __init__(
        self,
        id=None,
        job_name=None,
        status=None,
        alert_type=None,
        severity=None,
        receiver=None,
        created=None,
        threshold=None,
        **kwargs,
    ):
        super().__init__(
            id=id,
            status=status,
            alert_type=alert_type,
            severity=severity,
            receiver=receiver,
            created=created,
            **kwargs,
        )
        self._job_name = job_name
        self._threshold = threshold

    @property
    def job_name(self) -> Optional[str]:
        """return the name of the job"""
        return self._job_name

    @property
    def threshold(self) -> Optional[str]:
        """return the threshold of the alert"""
        return self._threshold

    def to_dict(self):
        """return the alert as a dictionary"""
        return {
            "id": self._id,
            "job_name": self._job_name,
            "status": self._status,
            "severity": self._severity,
            "receiver": self._receiver,
            "created": self._created,
            "threshold": self._threshold,
        }

    def __repr__(self):
        return f"JobAlert({self._id!r}, {self._job_name!r}, {self._status!r}, {self._severity!r}, {self._receiver!r}, {self._created!r}, {self._threshold!r})"


class FeatureGroupAlert(Alert):
    NOT_FOUND_ERROR_CODE = 270155

    def __init__(
        self,
        id=None,
        feature_store_name=None,
        feature_group_id=None,
        feature_group_name=None,
        status=None,
        alert_type=None,
        severity=None,
        receiver=None,
        created=None,
        **kwargs,
    ):
        super().__init__(
            id=id,
            status=status,
            alert_type=alert_type,
            severity=severity,
            receiver=receiver,
            created=created,
            **kwargs,
        )
        self._feature_store_name = feature_store_name
        self._feature_group_id = feature_group_id
        self._feature_group_name = feature_group_name

    @property
    def feature_store_name(self) -> Optional[str]:
        """return the name of the feature store"""
        return self._feature_store_name

    @property
    def feature_group_id(self) -> Optional[str]:
        """return the id of the feature group"""
        return self._feature_group_id

    @property
    def feature_group_name(self) -> Optional[str]:
        """return the name of the feature group"""
        return self._feature_group_name

    def to_dict(self):
        """return the alert as a dictionary"""
        return {
            "id": self._id,
            "feature_store_name": self._feature_store_name,
            "feature_group_id": self._feature_group_id,
            "feature_group_name": self._feature_group_name,
            "status": self._status,
            "severity": self._severity,
            "receiver": self._receiver,
            "created": self._created,
        }


    def __repr__(self):
        return f"FeatureGroupAlert({self._id!r}, {self._feature_store_name!r}, {self._feature_group_id!r}, {self._feature_group_name!r}, {self._status!r}, {self._severity!r}, {self._receiver!r}, {self._created!r})"


class FeatureViewAlert(Alert):
    NOT_FOUND_ERROR_CODE = 270155

    def __init__(
        self,
        id=None,
        feature_store_name=None,
        feature_view_id=None,
        feature_view_name=None,
        feature_view_version=None,
        status=None,
        alert_type=None,
        severity=None,
        receiver=None,
        created=None,
        **kwargs,
    ):
        super().__init__(
            id=id,
            status=status,
            alert_type=alert_type,
            severity=severity,
            receiver=receiver,
            created=created,
            **kwargs,
        )
        self._feature_store_name = feature_store_name
        self._feature_view_id = feature_view_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

    @property
    def feature_store_name(self) -> Optional[str]:
        """return the name of the feature store"""
        return self._feature_store_name

    @property
    def feature_view_id(self) -> Optional[str]:
        """return the id of the feature view"""
        return self._feature_view_id

    @property
    def feature_view_name(self) -> Optional[str]:
        """return the name of the feature view"""
        return self._feature_view_name

    @property
    def feature_view_version(self) -> Optional[str]:
        """return the version of the feature view"""
        return self._feature_view_version

    def to_dict(self):
        """return the alert as a dictionary"""
        return {
            "id": self._id,
            "feature_store_name": self._feature_store_name,
            "feature_view_id": self._feature_view_id,
            "feature_view_name": self._feature_view_name,
            "feature_view_version": self._feature_view_version,
            "status": self._status,
            "severity": self._severity,
            "receiver": self._receiver,
            "created": self._created,
        }

    def __repr__(self):
        return f"FeatureViewAlert({self._id!r}, {self._feature_store_name!r}, {self._feature_view_id!r}, {self._feature_view_name!r}, {self._feature_view_version!r}, {self._status!r}, {self._severity!r}, {self._receiver!r}, {self._created!r})"
