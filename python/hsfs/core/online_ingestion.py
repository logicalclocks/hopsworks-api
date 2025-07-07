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
    Any,
    Dict,
    List,
    Optional,
    Union,
)

import humps
from hopsworks_common import client, util
from hsfs import feature_group as fg_mod
from hsfs.core import online_ingestion_result
from hsfs.core.opensearch import OpenSearchClientSingleton
from tqdm.auto import tqdm


class OnlineIngestion:
    """
    Metadata object used to provide Online Ingestion information for a feature group.

    This class encapsulates the state and results of an online ingestion operation,
    including progress tracking and log retrieval.
    """

    def __init__(
        self,
        id: Optional[int] = None,
        num_entries: Optional[int] = None,
        results: Union[
            List[online_ingestion_result.OnlineIngestionResult],
            List[Dict[str, Any]],
        ] = None,
        feature_group: fg_mod.FeatureGroup = None,
        **kwargs,
    ):
        """
        Initialize an OnlineIngestion object.

        # Arguments
            id (Optional[int]): The unique identifier for the ingestion operation.
            num_entries (Optional[int]): The total number of entries to ingest.
            results (Union[List[OnlineIngestionResult], List[Dict[str, Any]]], optional):
                List of ingestion results or their JSON representations.
            feature_group (FeatureGroup, optional): The feature group associated with this ingestion.
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
        cls, json_dict: Dict[str, Any], feature_group: fg_mod.FeatureGroup = None
    ) -> OnlineIngestion:
        """
        Create an OnlineIngestion object from a JSON response.

        # Arguments
            json_dict (Dict[str, Any]): The JSON dictionary from the API response.
            feature_group (FeatureGroup, optional): The feature group associated with this ingestion.

        # Returns
            OnlineIngestion: The created OnlineIngestion object, or a list of them if multiple items are present.
        """
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        if "count" not in json_decamelized:
            return cls(**json_decamelized, feature_group=feature_group)
        elif json_decamelized["count"] == 1:
            return cls(**json_decamelized["items"][0], feature_group=feature_group)
        elif json_decamelized["count"] > 1:
            return [
                cls(**item, feature_group=feature_group)
                for item in json_decamelized["items"]
            ]
        else:
            return None

    def refresh(self):
        """
        Refresh the state of this OnlineIngestion object from the backend.
        """
        online_ingestion = self.feature_group.get_online_ingestion(self._id)
        self.__dict__.update(online_ingestion.__dict__)

    def to_dict(self):
        """
        Convert the OnlineIngestion object to a dictionary.

        # Returns
            dict: Dictionary representation of the object.
        """
        return {"id": self._id, "numEntries": self._num_entries}

    def json(self):
        """
        Serialize the OnlineIngestion object to a JSON string.

        # Returns
            str: JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> Optional[int]:
        """
        Get the unique identifier for the ingestion operation.

        # Returns
            Optional[int]: The ingestion ID.
        """
        return self._id

    @property
    def num_entries(self) -> Optional[int]:
        """
        Get the total number of entries to ingest.

        # Returns
            Optional[int]: The number of entries.
        """
        return self._num_entries

    @num_entries.setter
    def num_entries(self, num_entries: int) -> None:
        """
        Set the total number of entries to ingest.

        # Arguments
            num_entries (int): The number of entries.
        """
        self._num_entries = num_entries

    @property
    def results(
        self,
    ) -> List[online_ingestion_result.OnlineIngestionResult]:
        """
        Get the list of ingestion results.

        # Returns
            List[OnlineIngestionResult]: List of ingestion result objects.
        """
        return self._results

    @property
    def feature_group(self) -> fg_mod.FeatureGroup:
        """
        Get the feature group associated with this ingestion.

        # Returns
            FeatureGroup: The associated feature group.
        """
        return self._feature_group

    def wait_for_completion(self, options: Dict[str, Any] = None):
        """
        Wait for the online ingestion operation to complete, displaying a progress bar.

        # Arguments
            options (Dict[str, Any], optional): Options for waiting.
                - "timeout" (int): Maximum time to wait in seconds (default: 60).
                - "period" (int): Polling period in seconds (default: 1).

        # Raises
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

                # Update progress bar
                if any(result.status != "UPSERTED" for result in self.results):
                    progress_bar.colour = "RED"
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

    def print_logs(self, priority: str = "error", size: int = 20):
        """
        Print logs related to the online ingestion operation from OpenSearch.

        # Arguments
            priority (str, optional): Log priority to filter by (default: "error").
            size (int, optional): Number of log entries to retrieve (default: 20).
        """
        open_search_client = OpenSearchClientSingleton()

        response = open_search_client.search(
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
                            {"match": {"priority": priority}},
                        ]
                    }
                },
                "size": size,
            },
            index=f"onlinefs_{client.get_instance()._project_id}-*",
        )

        for hit in response["hits"]["hits"]:
            print(hit["_source"]["error"]["data"])
