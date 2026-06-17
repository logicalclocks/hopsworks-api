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
from __future__ import annotations

import json
import warnings

import humps
from hopsworks_apigen import public
from hopsworks_common import util


# Mapping from old uppercase wire values (emitted by backends before ~=3.8.1)
# to their replacements in the new naming scheme.
_DEPRECATED_ALERT_STATUS_WIRE_MAP = {
    "SUCCESS": "VALIDATION_SUCCESS",
    "WARNING": "VALIDATION_WARNING",
    "FAILURE": "VALIDATION_FAILURE",
    "FEATURE_MONITOR_SHIFT_UNDETECTED": "MONITORING_SHIFT_UNDETECTED",
    "FEATURE_MONITOR_SHIFT_DETECTED": "MONITORING_SHIFT_DETECTED",
}


def _normalize_alert_status(status: str | None, stacklevel: int = 2) -> str | None:
    """Normalize a deprecated alert status wire value to its replacement.

    Called during deserialization via `Alert.from_response_json` whenever an
    `Alert` object is constructed from a backend response.
    If *status* is one of the old enum names that were renamed in version
    ~=3.8.1, the function emits a `DeprecationWarning` and returns the new
    name.
    Otherwise it returns *status* unchanged.

    Args:
        status: The raw status string from the backend response, or `None`.
        stacklevel: Passed directly to `warnings.warn` to attribute the warning
            to the correct call frame.
            The default of 2 attributes the warning to the caller of
            `_normalize_alert_status` (i.e. `from_response_json`), which is
            the correct frame when the normalization is invoked from
            `from_response_json`.

    Returns:
        The normalized status string, or `None` if *status* is `None`.
    """
    if status is None:
        return None
    new = _DEPRECATED_ALERT_STATUS_WIRE_MAP.get(status)
    if new is not None:
        warnings.warn(
            f"Alert status {status!r} is deprecated and will be removed in a future release. "
            f"Use {new!r} instead. "
            "The connected backend may need to be upgraded.",
            DeprecationWarning,
            stacklevel=stacklevel,
        )
        return new
    return status


@public("hopsworks.alert.Alert")
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
                return [
                    cls(
                        **{
                            **item,
                            "status": _normalize_alert_status(
                                item.get("status"), stacklevel=4
                            ),
                        }
                    )
                    for item in json_decamelized["items"]
                ]
            return []
        return cls(
            **{
                **json_decamelized,
                "status": _normalize_alert_status(
                    json_decamelized.get("status"), stacklevel=4
                ),
            }
        )

    @public
    @property
    def id(self) -> int | None:
        """Return the id of the alert."""
        return self._id

    @public
    @property
    def status(self) -> str | None:
        """Return the status of the alert."""
        return self._status

    @public
    @property
    def alert_type(self) -> str | None:
        """Return the type of the alert."""
        return self._alert_type

    @public
    @property
    def severity(self) -> str | None:
        """Return the severity of the alert."""
        return self._severity

    @public
    @property
    def receiver(self) -> str | None:
        """Return the receiver of the alert."""
        return self._receiver

    @public
    @property
    def created(self) -> str | None:
        """Return the creation time of the alert."""
        return self._created

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        """Return the alert as a dictionary.

        Returns:
            A dictionary representation of the alert.
        """
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


@public("hopsworks.alert.ProjectAlert")
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

    @public
    @property
    def project_name(self) -> str | None:
        """Return the name of the project."""
        return self._project_name

    @public
    @property
    def service(self) -> str | None:
        """Return the name of the service."""
        return self._service

    @public
    @property
    def threshold(self) -> str | None:
        """Return the threshold of the alert."""
        return self._threshold

    def to_dict(self):
        """Return the alert as a dictionary."""
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


@public("hopsworks.alert.JobAlert")
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

    @public
    @property
    def job_name(self) -> str | None:
        """Return the name of the job."""
        return self._job_name

    @public
    @property
    def threshold(self) -> str | None:
        """Return the threshold of the alert."""
        return self._threshold

    def to_dict(self):
        """Return the alert as a dictionary."""
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


@public("hopsworks.alert.FeatureGroupAlert")
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

    @public
    @property
    def feature_store_name(self) -> str | None:
        """Return the name of the feature store."""
        return self._feature_store_name

    @public
    @property
    def feature_group_id(self) -> str | None:
        """Return the id of the feature group."""
        return self._feature_group_id

    @public
    @property
    def feature_group_name(self) -> str | None:
        """Return the name of the feature group."""
        return self._feature_group_name

    def to_dict(self):
        """Return the alert as a dictionary."""
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


@public("hopsworks.alert.FeatureViewAlert")
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

    @public
    @property
    def feature_store_name(self) -> str | None:
        """Return the name of the feature store."""
        return self._feature_store_name

    @public
    @property
    def feature_view_id(self) -> str | None:
        """Return the id of the feature view."""
        return self._feature_view_id

    @public
    @property
    def feature_view_name(self) -> str | None:
        """Return the name of the feature view."""
        return self._feature_view_name

    @public
    @property
    def feature_view_version(self) -> str | None:
        """Return the version of the feature view."""
        return self._feature_view_version

    def to_dict(self):
        """Return the alert as a dictionary."""
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
