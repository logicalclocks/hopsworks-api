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
import time
import warnings
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
)

import humps
from hopsworks_apigen import public
from hopsworks_common import client, util
from hsfs.core import online_ingestion_failure, online_ingestion_result
from hsfs.core.opensearch import OpenSearchClientSingleton
from tqdm.auto import tqdm


if TYPE_CHECKING:
    from hsfs import feature_group as fg_mod


@public
class OnlineIngestion:
    """Metadata object used to provide Online Ingestion information for a feature group.

    This class encapsulates the state and results of an online ingestion operation,
    including progress tracking and log retrieval.
    """

    def __init__(
        self,
        id: int | None = None,
        num_entries: int | None = None,
        results: list[online_ingestion_result.OnlineIngestionResult]
        | list[dict[str, Any]] = None,
        feature_group: fg_mod.FeatureGroup = None,
        **kwargs,
    ):
        """Initialize an OnlineIngestion object.

        Parameters:
            id: The unique identifier for the ingestion operation.
            num_entries: The total number of entries to ingest.
            results:
                List of ingestion results or their JSON representations.
            feature_group: The feature group associated with this ingestion.
        """
        self._id = id
        self._num_entries = num_entries  # specified when inserting (optional since might not be specified when using streaming)
        self._results = (
            [
                (
                    online_ingestion_result.OnlineIngestionResult.from_response_json(
                        result
                    )
                    if isinstance(result, dict)
                    else result
                )
                for result in results
            ]
            if results
            else []
        )  # batch inserts performed by onlinefs
        self._feature_group = feature_group

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any], feature_group: fg_mod.FeatureGroup = None
    ) -> OnlineIngestion:
        """Create an OnlineIngestion object from a JSON response.

        Parameters:
            json_dict: The JSON dictionary from the API response.
            feature_group: The feature group associated with this ingestion.

        Returns:
            OnlineIngestion: The created OnlineIngestion object, or a list of them if multiple items are present.
        """
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        if "count" not in json_decamelized:
            return cls(**json_decamelized, feature_group=feature_group)
        if json_decamelized["count"] == 1:
            return cls(**json_decamelized["items"][0], feature_group=feature_group)
        if json_decamelized["count"] > 1:
            return [
                cls(**item, feature_group=feature_group)
                for item in json_decamelized["items"]
            ]
        return None

    @public
    def refresh(self):
        """Refresh the state of this OnlineIngestion object from the backend."""
        online_ingestion = self.feature_group.get_online_ingestion(self._id)
        self.__dict__.update(online_ingestion.__dict__)

    def to_dict(self):
        """Convert the OnlineIngestion object to a dictionary.

        Returns:
            dict: Dictionary representation of the object.
        """
        return {"id": self._id, "numEntries": self._num_entries}

    def json(self):
        """Serialize the OnlineIngestion object to a JSON string.

        Returns:
            str: JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @public
    @property
    def id(self) -> int | None:
        """Get the unique identifier for the ingestion operation."""
        return self._id

    @public
    @property
    def num_entries(self) -> int | None:
        """Get the total number of entries to ingest."""
        return self._num_entries

    @num_entries.setter
    def num_entries(self, num_entries: int) -> None:
        """Set the total number of entries to ingest.

        Parameters:
            num_entries: The number of entries.
        """
        self._num_entries = num_entries

    @public
    @property
    def results(
        self,
    ) -> list[online_ingestion_result.OnlineIngestionResult]:
        """Get the list of ingestion results."""
        return self._results

    @public
    @property
    def feature_group(self) -> fg_mod.FeatureGroup:
        """Get the feature group associated with this ingestion."""
        return self._feature_group

    @public
    def wait_for_completion(self, options: dict[str, Any] = None):
        """Wait for the online ingestion operation to complete, displaying a progress bar.

        Parameters:
            options: Options for waiting.
                - "timeout" (int): Maximum time to wait in seconds (default: 60).
                - "period" (int): Polling period in seconds (default: 1).

        Raises:
            Warning: If the timeout is exceeded before completion.
        """
        if options is None:
            options = {}

        # Set timeout time
        timeout_delta = timedelta(seconds=options.get("timeout", 60))
        timeout_time = datetime.now() + timeout_delta

        with tqdm(
            total=self.num_entries,
            bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt}",
            desc="Online data ingestion progress",
            mininterval=1,
        ) as progress_bar:
            while True:
                # Get total number of rows processed
                rows_processed = sum(result.rows for result in self.results)

                # Update progress bar colour based on the worst status seen so far
                if any(result.status == "FAILED" for result in self.results):
                    progress_bar.colour = "RED"
                elif any(result.status == "IGNORED" for result in self.results):
                    progress_bar.colour = "YELLOW"
                progress_bar.n = rows_processed
                progress_bar.refresh()

                # Check if the online ingestion is complete
                if self.num_entries and rows_processed >= self.num_entries:
                    break

                # Check if the timeout has been reached (if timeout is 0 we will wait indefinitely)
                if timeout_delta != timedelta(0) and datetime.now() >= timeout_time:
                    warnings.warn(
                        f"Timeout of {timeout_delta} was exceeded while waiting for online ingestion completion.",
                        stacklevel=1,
                    )
                    break

                # Sleep for the specified period in seconds
                time.sleep(options.get("period", 1))

                self.refresh()

    def _search_logs(
        self, must: list[dict[str, Any]], size: int
    ) -> list[dict[str, Any]]:
        """Search the onlinefs logs of this ingestion in OpenSearch.

        Parameters:
            must: Additional query clauses narrowing the search.
            size: Maximum number of log entries to retrieve.

        Returns:
            The matching log entries, most relevant first.
        """
        open_search_client = OpenSearchClientSingleton()

        response = open_search_client._search(
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "log_arguments.feature_group_id": f"{self.feature_group.id}"
                                }
                            },
                            {
                                "match": {
                                    "log_arguments.online_ingestion_id": f"{self.id}"
                                }
                            },
                            *must,
                        ]
                    }
                },
                "size": size,
            },
            index=f"onlinefs_{client._get_instance()._project_id}-*",
        )

        return response["hits"]["hits"]

    @public
    def print_logs(self, priority: str = "error", size: int = 20):
        """Print logs related to the online ingestion operation from OpenSearch.

        Parameters:
            priority: Log priority to filter by (default: "error").
            size: Number of log entries to retrieve (default: 20).
        """
        for hit in self._search_logs([{"match": {"priority": priority}}], size):
            print(hit["_source"]["error"]["data"])

    @public
    def get_failures(
        self, size: int = 100
    ) -> list[online_ingestion_failure.OnlineIngestionFailure]:
        """Get the records of this ingestion that never reached the online feature store.

        [`results`][hsfs.core.online_ingestion.OnlineIngestion.results] reports how many rows
        failed; this reports which ones and why.
        Each failure identifies a Kafka record by topic, partition and offset, and carries the
        reason it was rejected - a value too long for its online column, a payload that does not
        match the schema, or a row the database refused.

        Use it after an insert to find the rows that need correcting:

        ```python
        ingestion = feature_group.get_latest_online_ingestion()
        ingestion.wait_for_completion()
        for failure in ingestion.get_failures():
            print(failure.record_key, failure.failure_type, failure.failure_reason)
        ```

        Info: Failures are read from logs, not from a durable store
            Failures are recovered from the online ingestion service's logs, so they are subject to
            the log retention of the cluster and are not guaranteed to be available indefinitely.
            A record is reported here as long as its feature group and ingestion headers could be
            read, even when the rest of the record could not be parsed.
            A record whose headers are themselves unreadable cannot be attributed to this
            ingestion at all, and is only visible in the raw online ingestion service logs.

        Parameters:
            size: Maximum number of failures to retrieve.

        Returns:
            The failed records, or an empty list if none of this ingestion's records failed.
        """
        return [
            online_ingestion_failure.OnlineIngestionFailure._from_log_arguments(
                hit["_source"]["log_arguments"]
            )
            for hit in self._search_logs(
                [{"exists": {"field": "log_arguments.failure_type"}}], size
            )
            if "log_arguments" in hit.get("_source", {})
        ]
