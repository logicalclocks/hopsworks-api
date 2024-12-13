#
#   Copyright 2024 Hopsworks AB
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
from typing import Any, Dict, Optional

import humps
from hopsworks_common import util
from tqdm.auto import tqdm


class OnlineIngestion:
    """
    Metadata object used to provide Online Ingestion information for a feature group.
    """

    def __init__(
        self,
        id: Optional[int] = None,
        num_entries: int = None,
        current_offsets: int = None,
        processed_entries: int = None,
        inserted_entries: int = None,
        aborted_entries: int = None,
        batch_results = None,
        **kwargs,
    ):
        self._id = id
        self._num_entries = num_entries # specified when inserting
        self._current_offsets = current_offsets
        self._processed_entries = processed_entries
        self._inserted_entries = inserted_entries
        self._aborted_entries = aborted_entries
        self._batch_results = batch_results # batch inserts performed by onlinefs

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "OnlineIngestion":
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        if "count" not in json_decamelized:
            return cls(**json_decamelized)
        elif json_decamelized["count"] == 1:
            return cls(**json_decamelized["items"][0])
        elif json_decamelized["count"] > 1:
            return [cls(**item) for item in json_decamelized["items"]]
        else:
            return None

    def refresh(self):
        from hsfs.core.online_ingestion_api import OnlineIngestionApi

        online_ingestion = OnlineIngestionApi().get_online_ingestion(
            self.feature_group,
            query_params={"filter_by": f"ID:{self.id}"}
        )
        self.__dict__.update(online_ingestion.__dict__)

    def to_dict(self):
        return {
            "id": self._id,
            "numEntries": self._num_entries
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def id(self) -> int:
        return self._id

    @property
    def num_entries(self) -> int:
        return self._num_entries

    @num_entries.setter
    def num_entries(self, num_entries: str) -> None:
        self._num_entries = num_entries

    @property
    def feature_group(self):
        return self._feature_group

    @feature_group.setter
    def feature_group(self, feature_group) -> None:
        self._feature_group = feature_group

    @property
    def current_offsets(self) -> str:
        return self._current_offsets

    @property
    def processed_entries(self) -> int:
        return 0 if self._processed_entries is None else self._processed_entries

    @property
    def inserted_entries(self) -> int:
        return 0 if self._inserted_entries is None else self._inserted_entries

    @property
    def aborted_entries(self) -> int:
        return 0 if self._aborted_entries is None else self._aborted_entries

    @property
    def batch_results(self):
        return self._batch_results

    def wait_for_completion(self):
        with tqdm(total=self.num_entries,
                  bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt}",
                  desc="Online data ingestion progress",
                  mininterval=1) as progress_bar:
            while True:
                if self.aborted_entries:
                    progress_bar.colour = "RED"

                progress_bar.n = self.processed_entries
                progress_bar.refresh()

                if self.processed_entries >= self.num_entries:
                    break

                time.sleep(1)

                self.refresh()
