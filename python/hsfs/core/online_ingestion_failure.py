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
from __future__ import annotations

import json
from typing import Any

from hopsworks_apigen import public
from hopsworks_common import util


@public
class OnlineIngestionFailure:
    """A single record that never made it into the online feature store.

    Online ingestion reports how many rows failed, but a count alone cannot tell you which rows
    were lost or what was wrong with them.
    This object identifies the Kafka record that was dropped and the reason it was rejected, so the
    offending rows can be corrected and inserted again.

    Danger: The record itself is not retained
        Only the record's coordinates and the reason are kept, not its contents.
        The record stays readable from its Kafka topic until the topic's retention period expires,
        after which it is gone for good.
    """

    def __init__(
        self,
        failure_type: str | None = None,
        failure_reason: str | None = None,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
        record_key: str | None = None,
        feature_group_id: int | None = None,
        online_ingestion_id: int | None = None,
        **kwargs,
    ):
        """Initialize an OnlineIngestionFailure object.

        Parameters:
            failure_type: The category of the failure, such as "USER_DATA" or "DESERIALIZATION".
            failure_reason: The error reported for this record.
            topic: The Kafka topic the record was read from.
            partition: The Kafka partition the record was read from.
            offset: The record's offset within its partition.
            record_key: The record's Kafka key, which is the concatenated primary key of the row.
            feature_group_id: The feature group the record was destined for.
            online_ingestion_id: The ingestion the record belonged to.
        """
        self._failure_type = failure_type
        self._failure_reason = failure_reason
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._record_key = record_key
        self._feature_group_id = feature_group_id
        self._online_ingestion_id = online_ingestion_id

    @classmethod
    def _from_log_arguments(
        cls, log_arguments: dict[str, Any]
    ) -> OnlineIngestionFailure:
        """Create an OnlineIngestionFailure from the structured arguments of an onlinefs log entry.

        Every value logged by onlinefs is a string, so the numeric fields are converted here and
        left as `None` when they are absent or unparsable.

        Parameters:
            log_arguments: The `log_arguments` object of a single onlinefs log entry.

        Returns:
            The failure described by the log entry.
        """

        def as_int(value: Any) -> int | None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        return cls(
            failure_type=log_arguments.get("failure_type") or None,
            failure_reason=log_arguments.get("failure_reason") or None,
            topic=log_arguments.get("topic") or None,
            partition=as_int(log_arguments.get("partition")),
            offset=as_int(log_arguments.get("offset")),
            record_key=log_arguments.get("record_key") or None,
            feature_group_id=as_int(log_arguments.get("feature_group_id")),
            online_ingestion_id=as_int(log_arguments.get("online_ingestion_id")),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the OnlineIngestionFailure object to a dictionary.

        Returns:
            The failure's fields, keyed as the backend names them.
        """
        return {
            "failureType": self._failure_type,
            "failureReason": self._failure_reason,
            "topic": self._topic,
            "partition": self._partition,
            "offset": self._offset,
            "recordKey": self._record_key,
            "featureGroupId": self._feature_group_id,
            "onlineIngestionId": self._online_ingestion_id,
        }

    def json(self) -> str:
        """Serialize the OnlineIngestionFailure object to a JSON string.

        Returns:
            The failure as JSON.
        """
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self) -> str:
        return (
            f"OnlineIngestionFailure({self._topic}[{self._partition}]@{self._offset}: "
            f"{self._failure_type} - {self._failure_reason})"
        )

    @public
    @property
    def failure_type(self) -> str | None:
        """Get the category of the failure.

        One of `"ROW_CREATION"`, `"METADATA"`, `"FEATURE_GROUP_DELETED"`, `"DESERIALIZATION"`,
        `"USER_DATA"` or `"COMMIT"`.
        `"USER_DATA"` means the database refused the row's values, most often because a string or
        binary value is longer than the online column allows.
        """
        return self._failure_type

    @public
    @property
    def failure_reason(self) -> str | None:
        """Get the error reported for this record."""
        return self._failure_reason

    @public
    @property
    def topic(self) -> str | None:
        """Get the Kafka topic the record was read from."""
        return self._topic

    @public
    @property
    def partition(self) -> int | None:
        """Get the Kafka partition the record was read from."""
        return self._partition

    @public
    @property
    def offset(self) -> int | None:
        """Get the record's offset within its partition.

        Together with [`topic`][hsfs.core.online_ingestion_failure.OnlineIngestionFailure.topic]
        and
        [`partition`][hsfs.core.online_ingestion_failure.OnlineIngestionFailure.partition]
        this locates the record in Kafka for as long as the topic retains it.
        """
        return self._offset

    @public
    @property
    def record_key(self) -> str | None:
        """Get the record's Kafka key, which is the concatenated primary key of the row."""
        return self._record_key

    @public
    @property
    def feature_group_id(self) -> int | None:
        """Get the id of the feature group the record was destined for."""
        return self._feature_group_id

    @public
    @property
    def online_ingestion_id(self) -> int | None:
        """Get the id of the ingestion the record belonged to."""
        return self._online_ingestion_id
