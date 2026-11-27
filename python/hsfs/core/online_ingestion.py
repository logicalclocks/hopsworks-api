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
import math
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


# How often a wait whose counts have stopped moving reads the logs for the rows the counts leave out
_LOGS_CHECK_INTERVAL = timedelta(seconds=10)
# Log entries, or records, fetched per search; search_after and composite pages go past the result window of OpenSearch, 10,000 by default
_PAGE_SIZE = 1000
# The fields that identify a Kafka record.
# Every value OnlineFS logs is a string, which OpenSearch maps with a keyword subfield to sort and group on.
_RECORD_FIELDS = {
    name: f"log_arguments.{name}.keyword" for name in ("topic", "partition", "offset")
}
# A daily index whose logs carry none of these fields has no mapping for them, hence unmapped_type.
_FAILURES_SORT = [
    {field: {"order": "asc", "unmapped_type": "keyword"}}
    for field in _RECORD_FIELDS.values()
]
_RECORDS = [
    {name: {"terms": {"field": field}}} for name, field in _RECORD_FIELDS.items()
]
_FAILED = {"exists": {"field": "log_arguments.failure_type"}}
# A row OnlineFS applied in a batch the database then refused, logged instead of counted
_UNCOUNTED = {"exists": {"field": "log_arguments.uncounted_status"}}


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
                  Set to `0` to wait until every entry is ingested or refused.
                  An ingestion without an entry count cannot tell when that is, so with `0` its wait does not end on its own; it warns when it starts.
                - "period" (int): Polling period in seconds (default: 1).

        Raises:
            Warning: If the timeout is exceeded before completion, if the ingestion completed with failed rows, or if the online ingestion service's logs could not be read.
                [`get_failures`][hsfs.core.online_ingestion.OnlineIngestion.get_failures] lists which records failed and why.
                The rows of a batch refused because the project's online storage memory quota is used up are not counted in the results.
                Once the counts stop moving, the wait reads the online ingestion service's logs, which report each of those rows, and completes when every row is counted or reported there.
                If the logs cannot be read, the wait goes on with the counts alone.
        """
        if options is None:
            options = {}

        timeout = options.get("timeout", 60)
        if timeout == 0 and self.num_entries is None:
            warnings.warn(
                f"Online ingestion {self.id} has no entry count, so its completion cannot be detected; "
                "with a timeout of 0 this wait does not end on its own.",
                stacklevel=1,
            )

        # Set timeout time
        timeout_delta = timedelta(seconds=timeout)
        timeout_time = datetime.now() + timeout_delta
        last_rows_processed = None
        next_logs_check = None
        logs_unreadable = False

        with tqdm(
            total=self.num_entries,
            bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt}",
            desc="Online data ingestion progress",
            mininterval=1,
        ) as progress_bar:
            while True:
                # Get total number of rows processed
                rows_processed = sum(result.rows for result in self.results)
                failed_rows = sum(
                    result.rows for result in self.results if result.status == "FAILED"
                )

                # Update progress bar colour based on the worst status seen so far
                if any(result.status == "FAILED" for result in self.results):
                    progress_bar.colour = "RED"
                elif any(result.status == "IGNORED" for result in self.results):
                    progress_bar.colour = "YELLOW"
                progress_bar.n = rows_processed
                progress_bar.refresh()

                # Check if the online ingestion is complete
                if self.num_entries is not None and rows_processed >= self.num_entries:
                    self._warn_failed_rows(failed_rows)
                    break

                # RonDB refuses the counts of a batch it refused over the memory quota as well, so the counts of such an ingestion stop short; OnlineFS logs each row of that batch instead.
                if (
                    self.num_entries is not None
                    and rows_processed == last_rows_processed
                    and (next_logs_check is None or datetime.now() >= next_logs_check)
                ):
                    next_logs_check = datetime.now() + _LOGS_CHECK_INTERVAL
                    deadline = None if timeout_delta == timedelta(0) else timeout_time
                    try:
                        logged = self._count_logged_records(
                            self.num_entries - rows_processed, deadline
                        )
                    # The check only spares the wait its timeout, so a failed read leaves it to the counts.
                    # A search the timeout cut short is reported as the timeout, below.
                    except Exception as e:
                        if not logs_unreadable and (
                            deadline is None or datetime.now() < deadline
                        ):
                            logs_unreadable = True
                            warnings.warn(
                                f"The online ingestion service logs could not be read, so the wait for online ingestion {self.id} goes on with its counts alone: {e}",
                                stacklevel=1,
                            )
                        logged = None
                    logged_failed, logged_applied = logged or (0, 0)
                    # A failed row is counted as FAILED or IGNORED, logged as a record, or both, so max, not sum.
                    # Where some rows are only counted and others only logged, this falls short and the wait goes on.
                    ignored_rows = sum(
                        result.rows
                        for result in self.results
                        if result.status == "IGNORED"
                    )
                    dropped_rows = max(failed_rows + ignored_rows, logged_failed)
                    applied_rows = (
                        rows_processed - failed_rows - ignored_rows + logged_applied
                    )
                    if applied_rows + dropped_rows >= self.num_entries:
                        self._warn_failed_rows(dropped_rows - ignored_rows)
                        break
                last_rows_processed = rows_processed

                # Check if the timeout has been reached (if timeout is 0 we will wait indefinitely)
                if timeout_delta != timedelta(0) and datetime.now() >= timeout_time:
                    warnings.warn(
                        f"Timeout of {timeout_delta} was exceeded while waiting for online ingestion completion. "
                        "Records the online feature store refused, for example over the project's storage quota, "
                        "are listed by get_failures().",
                        stacklevel=1,
                    )
                    break

                # Sleep for the specified period in seconds
                time.sleep(options.get("period", 1))

                self.refresh()

    def _warn_failed_rows(self, failed_rows: int) -> None:
        """Warn that the ingestion completed with failed rows, if it did.

        Parameters:
            failed_rows: The number of rows that failed.
        """
        if failed_rows:
            warnings.warn(
                f"Online ingestion {self.id} completed with {failed_rows} failed rows. "
                "get_failures() lists each failed record that the online ingestion service logged, and why; "
                "a batch that failed as a whole, such as one over a vector index quota, is only reported in the project's online ingestion service logs.",
                stacklevel=1,
            )

    def _count_logged_records(
        self, at_least: int, deadline: datetime | None
    ) -> tuple[int, int] | None:
        """Count the distinct records the logs of this ingestion report as failed, and as applied but not counted.

        A record logged more than once, as when the online ingestion service processes it again, is counted once.
        A record logged both ways counts as failed.

        Parameters:
            at_least: The fewest log entries that could account for the rows the wait is missing; with fewer, the records are not counted.
            deadline: When the wait times out; no search starts after it, and each is bounded by the time left.

        Returns:
            The number of failed records and the number of applied records, `(0, 0)` if fewer than `at_least` log entries report either, or `None` if the deadline passed before every record was counted.
        """
        failed = applied = 0
        after = None
        while True:
            request_timeout = None
            if deadline is not None:
                remaining = (deadline - datetime.now()).total_seconds()
                if remaining <= 0:
                    return None
                request_timeout = math.ceil(remaining)
            response = self._search_logs(
                [{"bool": {"should": [_FAILED, _UNCOUNTED]}}],
                0,
                request_timeout=request_timeout,
                # beyond 10,000 hits OpenSearch reports a lower bound unless asked for the exact total
                track_total_hits=True,
                aggs={
                    "records": {
                        "composite": {
                            "size": _PAGE_SIZE,
                            "sources": _RECORDS,
                            **({} if after is None else {"after": after}),
                        },
                        "aggs": {"failed": {"filter": _FAILED}},
                    }
                },
            )
            if response["hits"]["total"]["value"] < at_least:
                return 0, 0
            records = response["aggregations"]["records"]
            for record in records["buckets"]:
                if record["failed"]["doc_count"]:
                    failed += 1
                else:
                    applied += 1
            if len(records["buckets"]) < _PAGE_SIZE:
                return failed, applied
            after = records["after_key"]

    def _search_logs(
        self,
        must: list[dict[str, Any]],
        size: int,
        request_timeout: int | None = None,
        **body,
    ) -> dict[str, Any]:
        """Search the onlinefs logs of this ingestion in OpenSearch.

        Parameters:
            must: Additional query clauses narrowing the search.
            size: Maximum number of log entries to retrieve.
            request_timeout: Seconds the request may take, or `None` for the client's default.
            **body: Further fields of the search request, such as `sort` or `aggs`.

        Returns:
            The search response: the matching log entries under `hits.hits`, how many matched under `hits.total`, and any aggregations under `aggregations`.
        """
        open_search_client = OpenSearchClientSingleton()

        return open_search_client._search(
            options=None if request_timeout is None else {"timeout": request_timeout},
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
                **body,
            },
            index=f"onlinefs_{client._get_instance()._project_id}-*",
        )

    @public
    def print_logs(self, priority: str = "error", size: int = 20) -> None:
        """Print logs related to the online ingestion operation from OpenSearch.

        Parameters:
            priority: Log priority to filter by.
            size: Maximum number of log entries to retrieve.
        """
        response = self._search_logs([{"match": {"priority": priority}}], size)
        for hit in response["hits"]["hits"]:
            print(hit["_source"]["error"]["data"])

    @public
    def get_failures(
        self, size: int | None = 100
    ) -> list[online_ingestion_failure.OnlineIngestionFailure]:
        """Get the records of this ingestion that never reached the online feature store.

        [`results`][hsfs.core.online_ingestion.OnlineIngestion.results] reports how many rows failed; this reports which ones and why.
        Each failure identifies a Kafka record by topic, partition and offset, and carries the reason it was rejected: a value too long for its online column, a payload that does not match the schema, a row the database refused, or a write refused because the project's online storage quota is used up.
        Each record is listed once, even when the online ingestion service logged it more than once.
        Rows refused over the project's online storage memory quota are listed here but not counted in `results`, since the quota also refuses the count.
        A batch that failed as a whole, such as one over a vector index quota, is counted in `results` but has no record listed here.

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
            size: Maximum number of failures to retrieve, or `None` to retrieve every one of them.

        Returns:
            The failed records, or an empty list if none of this ingestion's records failed.
        """
        failures = []
        seen = set()
        search_after = None
        while size is None or len(failures) < size:
            page_size = (
                _PAGE_SIZE if size is None else min(_PAGE_SIZE, size - len(failures))
            )
            hits = self._search_logs(
                [_FAILED],
                page_size,
                sort=_FAILURES_SORT,
                **({} if search_after is None else {"search_after": search_after}),
            )["hits"]["hits"]
            for hit in hits:
                if "log_arguments" not in hit.get("_source", {}):
                    continue
                failure = (
                    online_ingestion_failure.OnlineIngestionFailure._from_log_arguments(
                        hit["_source"]["log_arguments"]
                    )
                )
                record = (failure.topic, failure.partition, failure.offset)
                if failure.offset is None or record not in seen:
                    seen.add(record)
                    failures.append(failure)
            if len(hits) < page_size:
                break
            search_after = hits[-1]["sort"]
        return failures
