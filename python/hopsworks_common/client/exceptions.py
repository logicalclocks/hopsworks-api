#
#   Copyright 2020 Logical Clocks AB
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

from enum import Enum
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import also_available_as
from requests.exceptions import SSLError


if TYPE_CHECKING:
    import requests


@also_available_as(
    "hopsworks.client.exceptions.RestAPIError", "hsml.client.exceptions.RestAPIError"
)
class RestAPIError(Exception):
    """REST Exception encapsulating the response object and url."""

    STATUS_CODE_BAD_REQUEST = 400
    STATUS_CODE_UNAUTHORIZED = 401
    STATUS_CODE_FORBIDDEN = 403
    STATUS_CODE_NOT_FOUND = 404
    STATUS_CODE_INTERNAL_SERVER_ERROR = 500

    class FeatureStoreErrorCode(int, Enum):
        FEATURE_GROUP_COMMIT_NOT_FOUND = 270227
        STATISTICS_NOT_FOUND = 270228

        def __eq__(self, other: int | Any) -> bool:
            if isinstance(other, int):
                return self.value == other
            if isinstance(other, self.__class__):
                return self is other
            return False

    def __init__(self, url: str, response: requests.Response) -> None:
        try:
            error_object = response.json()
            if isinstance(error_object, str):
                error_object = {"errorMsg": error_object}
        except Exception:
            error_object = {}
            self.error_code = None
        message = (
            "Metadata operation error: (url: {}). Server response: \n"
            "HTTP code: {}, HTTP reason: {}, body: {}, error code: {}, error msg: {}, user "
            "msg: {}".format(
                url,
                response.status_code,
                response.reason,
                response.content,
                error_object.get("errorCode", ""),
                error_object.get("errorMsg", ""),
                error_object.get("usrMsg", ""),
            )
        )
        if len(error_object) != 0:
            self.error_code = error_object.get("errorCode", "")
        super().__init__(message)
        self.url = url
        self.response = response


@also_available_as(
    "hopsworks.client.exceptions.UnknownSecretStorageError",
    "hsml.client.exceptions.UnknownSecretStorageError",
)
class UnknownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter."""


@also_available_as(
    "hopsworks.client.exceptions.FeatureStoreException",
    "hsml.client.exceptions.FeatureStoreException",
)
class FeatureStoreException(Exception):
    """Generic feature store exception."""

    DUPLICATE_RECORD_ERROR_MESSAGE = (
        "Duplicate records detected: The dataset contains multiple rows that share identical values "
        "across all available columns from primary_key, and if defined: event_time and partition_key. "
        "Please remove or deduplicate these records before inserting."
    )


@also_available_as(
    "hopsworks.client.exceptions.VectorDatabaseException",
    "hsml.client.exceptions.VectorDatabaseException",
)
class VectorDatabaseException(Exception):
    # reason
    REQUESTED_K_TOO_LARGE = "REQUESTED_K_TOO_LARGE"
    REQUESTED_NUM_RESULT_TOO_LARGE = "REQUESTED_NUM_RESULT_TOO_LARGE"
    OTHERS = "OTHERS"

    # info
    REQUESTED_K_TOO_LARGE_INFO_K = "k"
    REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N = "n"

    def __init__(self, reason: str, message: str, info: str) -> None:
        super().__init__(message)
        self._info = info
        self._reason = reason

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def info(self) -> str:
        return self._info


@also_available_as(
    "hopsworks.client.exceptions.DataValidationException",
    "hsml.client.exceptions.DataValidationException",
)
class DataValidationException(FeatureStoreException):
    """Raised when data validation fails only when using "STRICT" validation ingestion policy."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


@also_available_as(
    "hopsworks.client.exceptions.ExternalClientError",
    "hsml.client.exceptions.ExternalClientError",
)
class ExternalClientError(TypeError):
    """Raised when external client cannot be initialized due to missing arguments."""

    def __init__(self, missing_argument: str) -> None:
        message = (
            f"{missing_argument} cannot be of type NoneType, {missing_argument} is a non-optional "
            "argument to connect to hopsworks from an external environment."
        )
        super().__init__(message)


@also_available_as("hsml.client.exceptions.InternalClientError")
class InternalClientError(TypeError):
    """Raised when hopsworks internal client is missing some necessary configuration."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


@also_available_as(
    "hopsworks.client.exceptions.HopsworksSSLClientError",
    "hsml.client.exceptions.HopsworksSSLClientError",
)
class HopsworksSSLClientError(SSLError):
    """Raised when the client connection fails with SSL related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


@also_available_as(
    "hopsworks.client.exceptions.GitException", "hsml.client.exceptions.GitException"
)
class GitException(Exception):
    """Generic git exception."""


@also_available_as(
    "hopsworks.client.exceptions.JobException", "hsml.client.exceptions.JobException"
)
class JobException(Exception):
    """Generic job exception."""


@also_available_as(
    "hopsworks.client.exceptions.EnvironmentException",
    "hsml.client.exceptions.EnvironmentException",
)
class EnvironmentException(Exception):
    """Generic python environment exception."""


@also_available_as(
    "hopsworks.client.exceptions.KafkaException",
    "hsml.client.exceptions.KafkaException",
)
class KafkaException(Exception):
    """Generic kafka exception."""


@also_available_as(
    "hopsworks.client.exceptions.DatasetException",
    "hsml.client.exceptions.DatasetException",
)
class DatasetException(Exception):
    """Generic dataset exception."""


@also_available_as(
    "hopsworks.client.exceptions.ProjectException",
    "hsml.client.exceptions.ProjectException",
)
class ProjectException(Exception):
    """Generic project exception."""


@also_available_as(
    "hopsworks.client.exceptions.OpenSearchException",
    "hsml.client.exceptions.OpenSearchException",
)
class OpenSearchException(Exception):
    """Generic opensearch exception."""


@also_available_as(
    "hopsworks.client.exceptions.JobExecutionException",
    "hsml.client.exceptions.JobExecutionException",
)
class JobExecutionException(Exception):
    """Generic job executions exception."""


@also_available_as("hsml.client.exceptions.ModelRegistryException")
class ModelRegistryException(Exception):
    """Generic model registry exception."""


@also_available_as("hsml.client.exceptions.ModelServingException")
class ModelServingException(Exception):
    """Generic model serving exception."""

    ERROR_CODE_SERVING_NOT_FOUND = 240000
    ERROR_CODE_ILLEGAL_ARGUMENT = 240001
    ERROR_CODE_DUPLICATED_ENTRY = 240011

    ERROR_CODE_DEPLOYMENT_NOT_RUNNING = 250001
