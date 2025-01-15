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
from hsfs.core import online_ingestion_batch_result
from hsfs.core.opensearch import OpenSearchClientSingleton
from tqdm.auto import tqdm


class OnlineIngestion:
    """
    Metadata object used to provide Online Ingestion information for a feature group.
    """

    def __init__(
        self,
        id: Optional[int] = None,
        num_entries: Optional[int] = None,
        rows_upserted: Optional[int] = None,
        rows_failed: Optional[int] = None,
        rows_ignored: Optional[int] = None,
        batch_results: Union[
            List[online_ingestion_batch_result.OnlineIngestionBatchResult],
            List[Dict[str, Any]],
        ] = None,
        feature_group: fg_mod.FeatureGroup = None,
        **kwargs,
    ):
        self._id = id
        self._num_entries = num_entries  # specified when inserting (optional since might not be specified when using streaming)
        self._rows_upserted = rows_upserted
        self._rows_failed = rows_failed
        self._rows_ignored = rows_ignored
        self._batch_results = (
            [
                (
                    online_ingestion_batch_result.OnlineIngestionBatchResult.from_response_json(
                        batch_result
                    )
                    if isinstance(batch_result, dict)
                    else batch_result
                )
                for batch_result in batch_results
            ]
            if batch_results
            else []
        )  # batch inserts performed by onlinefs
        self._feature_group = feature_group

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any], feature_group: fg_mod.FeatureGroup = None
    ) -> OnlineIngestion:
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
        from hsfs.core.online_ingestion_api import OnlineIngestionApi

        online_ingestion = OnlineIngestionApi().get_online_ingestion(
            self.feature_group, query_params={"filter_by": f"ID:{self.id}"}
        )
        self.__dict__.update(online_ingestion.__dict__)

    def to_dict(self):
        return {"id": self._id, "numEntries": self._num_entries}

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def num_entries(self) -> Optional[int]:
        return self._num_entries

    @num_entries.setter
    def num_entries(self, num_entries: int) -> None:
        self._num_entries = num_entries

    @property
    def rows_upserted(self) -> int:
        return 0 if self._rows_upserted is None else self._rows_upserted

    @property
    def rows_failed(self) -> int:
        return 0 if self._rows_failed is None else self._rows_failed

    @property
    def rows_ignored(self) -> int:
        return 0 if self._rows_ignored is None else self._rows_ignored

    @property
    def batch_results(
        self,
    ) -> List[online_ingestion_batch_result.OnlineIngestionBatchResult]:
        return self._batch_results

    @property
    def feature_group(self) -> fg_mod.FeatureGroup:
        return self._feature_group

    def wait_for_completion(self, options: Dict[str, Any] = None):
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
                rows_processed = self.rows_upserted + self.rows_failed + self.rows_ignored

                # Update progress bar
                if self.rows_failed or self.rows_ignored:
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
