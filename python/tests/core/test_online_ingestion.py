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
import itertools
import warnings
from datetime import datetime, timedelta

from hsfs.core import online_ingestion
from hsfs.core.online_ingestion_failure import OnlineIngestionFailure
from hsfs.core.online_ingestion_result import OnlineIngestionResult
from opensearchpy.exceptions import AuthorizationException


class _StillWaiting(Exception):
    pass


def _log_arguments(**overrides):
    # onlinefs logs every argument as a string
    arguments = {
        "failure_type": "USER_DATA",
        "failure_reason": "ClusterJUserException: Data length 300 too long for column 'name'",
        "topic": "119_onlinefs",
        "partition": "2",
        "offset": "41",
        "record_key": "id_7",
        "feature_group_id": "119",
        "online_ingestion_id": "5",
    }
    arguments.update(overrides)
    return arguments


class TestOnlineIngestionFailure:
    def test_from_log_arguments(self):
        # Act
        failure = OnlineIngestionFailure._from_log_arguments(_log_arguments())

        # Assert
        assert failure.failure_type == "USER_DATA"
        assert "too long for column" in failure.failure_reason
        assert failure.topic == "119_onlinefs"
        assert failure.partition == 2
        assert failure.offset == 41
        assert failure.record_key == "id_7"
        assert failure.feature_group_id == 119
        assert failure.online_ingestion_id == 5

    def test_from_log_arguments_missing_fields(self):
        # Arrange
        # a record that failed before it could be attributed to a feature group carries only its
        # Kafka coordinates
        arguments = {
            "failure_type": "ROW_CREATION",
            "topic": "119_onlinefs",
            "partition": "0",
            "offset": "3",
        }

        # Act
        failure = OnlineIngestionFailure._from_log_arguments(arguments)

        # Assert
        assert failure.failure_type == "ROW_CREATION"
        assert failure.failure_reason is None
        assert failure.feature_group_id is None
        assert failure.online_ingestion_id is None
        assert failure.offset == 3

    def test_from_log_arguments_unparsable_numbers(self):
        # Act
        # LogArgument renders a null value as an empty string
        failure = OnlineIngestionFailure._from_log_arguments(
            _log_arguments(offset="", partition="not-a-number")
        )

        # Assert
        assert failure.offset is None
        assert failure.partition is None

    def test_to_dict(self):
        # Act
        failure = OnlineIngestionFailure._from_log_arguments(_log_arguments())

        # Assert
        assert failure.to_dict()["recordKey"] == "id_7"
        assert failure.to_dict()["failureType"] == "USER_DATA"


class TestOnlineIngestion:
    def _ingestion(self, mocker, hits):
        mocker.patch("hopsworks_common.client._get_instance")
        singleton = mocker.patch("hsfs.core.online_ingestion.OpenSearchClientSingleton")
        singleton.return_value._search.return_value = {"hits": {"hits": hits}}

        feature_group = mocker.Mock()
        feature_group.id = 119
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=10, feature_group=feature_group
        )
        return ingestion, singleton.return_value._search

    def test_get_failures(self, mocker):
        # Arrange
        ingestion, _ = self._ingestion(
            mocker,
            [
                {"_source": {"log_arguments": _log_arguments()}},
                {
                    "_source": {
                        "log_arguments": _log_arguments(
                            failure_type="DESERIALIZATION", offset="42"
                        )
                    }
                },
            ],
        )

        # Act
        failures = ingestion.get_failures()

        # Assert
        assert [failure.failure_type for failure in failures] == [
            "USER_DATA",
            "DESERIALIZATION",
        ]
        assert [failure.offset for failure in failures] == [41, 42]

    def test_get_failures_none(self, mocker):
        # Arrange
        ingestion, _ = self._ingestion(mocker, [])

        # Act & Assert
        assert ingestion.get_failures() == []

    def test_get_failures_skips_entries_without_arguments(self, mocker):
        # Arrange
        ingestion, _ = self._ingestion(
            mocker,
            [{"_source": {}}, {"_source": {"log_arguments": _log_arguments()}}],
        )

        # Act & Assert
        assert len(ingestion.get_failures()) == 1

    def test_get_failures_query(self, mocker):
        # Arrange
        ingestion, search = self._ingestion(mocker, [])

        # Act
        ingestion.get_failures(size=7)

        # Assert
        body = search.call_args.kwargs["body"]
        must = body["query"]["bool"]["must"]
        assert body["size"] == 7
        # failures are found by the presence of the structured field, not by log wording
        assert {"exists": {"field": "log_arguments.failure_type"}} in must
        assert {"match": {"log_arguments.feature_group_id": "119"}} in must
        assert {"match": {"log_arguments.online_ingestion_id": "5"}} in must
        assert body["sort"] == online_ingestion._FAILURES_SORT
        assert "search_after" not in body

    @staticmethod
    def _page(*offsets):
        return {
            "hits": {
                "hits": [
                    {
                        "_source": {"log_arguments": _log_arguments(offset=offset)},
                        "sort": ["119_onlinefs", "2", offset],
                    }
                    for offset in offsets
                ]
            }
        }

    def test_get_failures_pages_through_every_failure(self, mocker):
        # Arrange - pages of two, and offset 2 logged twice, as a retried batch does
        ingestion, search = self._ingestion(mocker, [])
        mocker.patch.object(online_ingestion, "_PAGE_SIZE", 2)
        search.side_effect = [
            self._page("1", "2"),
            self._page("2", "3"),
            self._page("4"),
        ]

        # Act
        failures = ingestion.get_failures(size=None)

        # Assert
        assert [failure.offset for failure in failures] == [1, 2, 3, 4]
        bodies = [call.kwargs["body"] for call in search.call_args_list]
        assert [body["size"] for body in bodies] == [2, 2, 2]
        assert [body.get("search_after") for body in bodies] == [
            None,
            ["119_onlinefs", "2", "2"],
            ["119_onlinefs", "2", "3"],
        ]

    def test_get_failures_stops_at_size(self, mocker):
        # Arrange
        ingestion, search = self._ingestion(mocker, [])
        mocker.patch.object(online_ingestion, "_PAGE_SIZE", 2)
        search.side_effect = [self._page("1", "2"), self._page("3")]

        # Act
        failures = ingestion.get_failures(size=3)

        # Assert
        assert [failure.offset for failure in failures] == [1, 2, 3]
        assert [call.kwargs["body"]["size"] for call in search.call_args_list] == [
            2,
            1,
        ]

    @staticmethod
    def _records(entries, *records):
        """A page of the logged-records aggregation, over records given as (offset, failure entries)."""
        buckets = [
            {
                "key": {"topic": "119_onlinefs", "partition": "2", "offset": offset},
                "failed": {"doc_count": failures},
            }
            for offset, failures in records
        ]
        return {
            "hits": {"hits": [], "total": {"value": entries}},
            "aggregations": {
                "records": {
                    "buckets": buckets,
                    **({"after_key": buckets[-1]["key"]} if buckets else {}),
                }
            },
        }

    def test_count_logged_records_counts_each_record_once(self, mocker):
        # Arrange - pages of two records: offset 1 failed and was logged again when it was processed again,
        # offset 2 was applied in a batch the database refused, offset 3 failed
        ingestion, search = self._ingestion(mocker, [])
        mocker.patch.object(online_ingestion, "_PAGE_SIZE", 2)
        search.side_effect = [
            self._records(4, ("1", 2), ("2", 0)),
            self._records(4, ("3", 1)),
        ]

        # Act & Assert
        assert ingestion._count_logged_records(4, None) == (2, 1)
        bodies = [call.kwargs["body"] for call in search.call_args_list]
        assert {
            "bool": {"should": [online_ingestion._FAILED, online_ingestion._UNCOUNTED]}
        } in bodies[0]["query"]["bool"]["must"]
        assert bodies[0]["size"] == 0
        # beyond 10,000 hits OpenSearch reports a lower bound unless asked for the exact total
        assert bodies[0]["track_total_hits"] is True
        records = bodies[0]["aggs"]["records"]
        # one bucket per record, however many entries the logs hold for it
        assert records["composite"]["sources"] == [
            {"topic": {"terms": {"field": "log_arguments.topic.keyword"}}},
            {"partition": {"terms": {"field": "log_arguments.partition.keyword"}}},
            {"offset": {"terms": {"field": "log_arguments.offset.keyword"}}},
        ]
        assert records["aggs"] == {"failed": {"filter": online_ingestion._FAILED}}
        assert "after" not in records["composite"]
        assert bodies[1]["aggs"]["records"]["composite"]["after"] == {
            "topic": "119_onlinefs",
            "partition": "2",
            "offset": "2",
        }

    def test_count_logged_records_does_not_count_too_few_entries(self, mocker):
        # Arrange
        ingestion, search = self._ingestion(mocker, [])
        search.return_value = self._records(3, ("1", 1), ("2", 1), ("3", 1))

        # Act & Assert
        assert ingestion._count_logged_records(4, None) == (0, 0)
        assert search.call_count == 1

    def _slow_searches(self, mocker, search, seconds):
        """Run the module's clock from 0 by hand, each search taking the given seconds; return the start times."""
        start = datetime(2026, 1, 1)
        clock = [start]
        mock_datetime = mocker.patch("hsfs.core.online_ingestion.datetime")
        mock_datetime.now.side_effect = lambda: clock[0]
        started = []

        def slow_search(**kwargs):
            started.append((clock[0] - start).total_seconds())
            clock[0] += timedelta(seconds=seconds)
            # a full page of records, so the scan would go on
            return self._records(
                100, *((str(i), 1) for i in range(online_ingestion._PAGE_SIZE))
            )

        search.side_effect = slow_search
        return start, clock, started

    def test_count_logged_records_starts_no_search_after_the_deadline(self, mocker):
        # Arrange - pages of two, each search taking 6 seconds, 2 seconds before the deadline
        ingestion, search = self._ingestion(mocker, [])
        mocker.patch.object(online_ingestion, "_PAGE_SIZE", 2)
        start, _, started = self._slow_searches(mocker, search, 6)

        # Act & Assert - the scan is dropped rather than counted in part
        assert ingestion._count_logged_records(4, start + timedelta(seconds=2)) is None
        assert started == [0]
        # bounded by the time left
        assert search.call_args.kwargs["options"] == {"timeout": 2}

    def test_print_logs(self, mocker):
        # Arrange
        # the OpenSearch wrapper exposes _search; calling search raised AttributeError
        ingestion, search = self._ingestion(
            mocker, [{"_source": {"error": {"data": "some failure"}}}]
        )
        mock_print = mocker.patch("builtins.print")

        # Act
        ingestion.print_logs()

        # Assert
        assert search.call_count == 1
        mock_print.assert_called_once_with("some failure")

    def _patch_wait(self, mocker, results_after_refresh=None):
        start = datetime(2026, 1, 1)
        ticks = itertools.count()
        mock_datetime = mocker.patch("hsfs.core.online_ingestion.datetime")
        # every read of the clock is 30 seconds after the previous one
        mock_datetime.now.side_effect = lambda: (
            start + timedelta(seconds=30 * next(ticks))
        )
        mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "refresh",
            autospec=True,
            side_effect=lambda self: setattr(
                self, "_results", results_after_refresh or []
            ),
        )
        sleeps = itertools.count(1)

        def sleep(period):
            # stands in for the user interrupting a wait that is still polling after 10 rounds
            if next(sleeps) > 10:
                raise _StillWaiting

        # Patch the module's own name for time: patching time.sleep would replace it for every thread in
        # the process, and a thread that sleeps meanwhile (Spark's, in the full suite) uses up these calls.
        mock_time = mocker.patch("hsfs.core.online_ingestion.time")
        mock_time.sleep.side_effect = sleep
        return mock_time.sleep

    def _wait(self, ingestion, options):
        """Return the warnings the wait emitted and whether it ended on its own."""
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            try:
                ingestion.wait_for_completion(options=options)
                ended = True
            except _StillWaiting:
                ended = False
        return [str(warning.message) for warning in record], ended

    def test_wait_for_completion_without_entry_count_warns_it_does_not_end(
        self, mocker
    ):
        # Arrange
        self._patch_wait(mocker)
        ingestion = online_ingestion.OnlineIngestion(id=5)

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert any("5 has no entry count" in message for message in messages)
        assert not ended

    def test_wait_for_completion_without_entry_count_keeps_explicit_timeout(
        self, mocker
    ):
        # Arrange
        sleep = self._patch_wait(mocker)
        ingestion = online_ingestion.OnlineIngestion(id=5)

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 30})

        # Assert
        assert not any("no entry count" in message for message in messages)
        assert any("Timeout of 0:00:30 was exceeded" in message for message in messages)
        assert ended
        sleep.assert_not_called()

    def test_wait_for_completion_zero_entries(self, mocker):
        # Arrange
        sleep = self._patch_wait(mocker)
        ingestion = online_ingestion.OnlineIngestion(id=5, num_entries=0)

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == []
        assert ended
        sleep.assert_not_called()

    def test_wait_for_completion_with_entry_count_waits_until_ingested(self, mocker):
        # Arrange
        sleep = self._patch_wait(
            mocker,
            results_after_refresh=[
                OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=10)
            ],
        )
        ingestion = online_ingestion.OnlineIngestion(id=5, num_entries=10)

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == []
        assert ended
        assert sleep.call_count == 1

    def test_wait_for_completion_with_failed_rows_warns(self, mocker):
        # Arrange
        self._patch_wait(
            mocker,
            results_after_refresh=[
                OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=7),
                OnlineIngestionResult(online_ingestion_id=5, status="FAILED", rows=3),
            ],
        )
        ingestion = online_ingestion.OnlineIngestion(id=5, num_entries=10)

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == [
            "Online ingestion 5 completed with 3 failed rows. "
            "get_failures() lists each failed record that the online ingestion service logged, and why; "
            "a batch that failed as a whole, such as one over a vector index quota, is only reported in the project's online ingestion service logs."
        ]
        assert ended

    def test_wait_for_completion_ends_once_the_logs_account_for_the_missing_rows(
        self, mocker
    ):
        # Arrange - 4 rows counted, and the 6 refused over the memory quota only in the logs
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
        ]
        self._patch_wait(mocker, results_after_refresh=counted)
        count_logged = mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            return_value=(6, 0),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=10, results=counted
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith(
            "Online ingestion 5 completed with 6 failed rows."
        )
        # fewer log entries than the 6 missing rows could not complete the wait, and a timeout of 0 has no deadline
        count_logged.assert_called_once_with(6, None)

    def test_wait_for_completion_keeps_waiting_while_rows_are_missing(self, mocker):
        # Arrange - the logs account for only 5 of the 6 missing rows
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
        ]
        self._patch_wait(mocker, results_after_refresh=counted)
        mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            return_value=(5, 0),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=10, results=counted
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == []
        assert not ended

    def test_wait_for_completion_does_not_read_the_logs_while_rows_arrive(self, mocker):
        # Arrange - one more row is counted on every refresh, while the logs would account for the rest at any time
        self._patch_wait(mocker)
        progress = itertools.count(5)
        online_ingestion.OnlineIngestion.refresh.side_effect = lambda self: setattr(
            self,
            "_results",
            [
                OnlineIngestionResult(
                    online_ingestion_id=5, status="UPSERTED", rows=next(progress)
                )
            ],
        )
        count_logged = mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            return_value=(6, 0),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5,
            num_entries=10,
            results=[
                OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
            ],
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == []
        assert ended
        count_logged.assert_not_called()

    def _wait_with_logs(self, mocker, counted, num_entries, logged):
        """Wait with the counts stuck at counted and the logs reporting logged as (failed, applied) records."""
        self._patch_wait(mocker, results_after_refresh=counted)
        mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            return_value=logged,
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=num_entries, results=counted
        )
        return self._wait(ingestion, {"timeout": 0})

    def test_wait_for_completion_accounts_for_an_applied_row_the_counts_leave_out(
        self, mocker
    ):
        # Arrange - two upsert_if_newer rows in a batch refused over the memory quota, with neither counted:
        # one was refused and one was not newer than the stored row
        # Act
        messages, ended = self._wait_with_logs(mocker, [], 2, (1, 1))

        # Assert
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith(
            "Online ingestion 5 completed with 1 failed rows."
        )

    def test_wait_for_completion_accounts_for_failed_records_of_every_kind(
        self, mocker
    ):
        # Arrange - one row applied and one failed with bad data, both counted, then the third refused over the
        # memory quota with its count: the logs hold two failed records, one of them counted
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=1),
            OnlineIngestionResult(online_ingestion_id=5, status="FAILED", rows=1),
        ]

        # Act
        messages, ended = self._wait_with_logs(mocker, counted, 3, (2, 0))

        # Assert
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith(
            "Online ingestion 5 completed with 2 failed rows."
        )

    def test_wait_for_completion_does_not_count_a_failed_row_twice(self, mocker):
        # Arrange - two rows refused over the disk quota are both counted and logged, and the third is still to come
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="FAILED", rows=2)
        ]

        # Act
        messages, ended = self._wait_with_logs(mocker, counted, 3, (2, 0))

        # Assert
        assert messages == []
        assert not ended

    def test_wait_for_completion_does_not_count_an_ignored_row_twice(self, mocker):
        # Arrange - two records of a deleted feature group are counted as IGNORED and logged, two more rows are
        # still to come
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="IGNORED", rows=2)
        ]

        # Act
        messages, ended = self._wait_with_logs(mocker, counted, 4, (2, 0))

        # Assert
        assert messages == []
        assert not ended

    def test_wait_for_completion_counts_failures_besides_ignored_rows(self, mocker):
        # Arrange - as above, and the logs also report the last 3 rows as refused over the memory quota
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="IGNORED", rows=2)
        ]

        # Act
        messages, ended = self._wait_with_logs(mocker, counted, 5, (5, 0))

        # Assert
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith(
            "Online ingestion 5 completed with 3 failed rows."
        )

    def test_wait_for_completion_counts_a_record_logged_twice_once(self, mocker):
        # Arrange - the first of two records was refused and logged twice, the second is still to come
        mocker.patch("hopsworks_common.client._get_instance")
        singleton = mocker.patch("hsfs.core.online_ingestion.OpenSearchClientSingleton")
        singleton.return_value._search.return_value = self._records(2, ("1", 2))
        self._patch_wait(mocker)
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=2, feature_group=mocker.Mock(id=119)
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert
        assert messages == []
        assert not ended

    def test_wait_for_completion_goes_on_when_the_logs_cannot_be_read(self, mocker):
        # Arrange - the log search is refused, and the next refresh counts every row
        self._patch_wait(mocker)
        counts = iter([4, 10])
        online_ingestion.OnlineIngestion.refresh.side_effect = lambda self: setattr(
            self,
            "_results",
            [
                OnlineIngestionResult(
                    online_ingestion_id=5, status="UPSERTED", rows=next(counts)
                )
            ],
        )
        mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            side_effect=AuthorizationException(403, "security_exception", {}),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5,
            num_entries=10,
            results=[
                OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
            ],
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 600})

        # Assert - within its timeout, which the clock reaches only after the second refresh
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith(
            "The online ingestion service logs could not be read, so the wait for online ingestion 5 goes on with its counts alone:"
        )

    def test_wait_for_completion_starts_no_log_search_after_its_timeout(self, mocker):
        # Arrange - 4 of 10 rows counted, a 2 second timeout, and log searches of 6 seconds over many pages
        mocker.patch("hopsworks_common.client._get_instance")
        singleton = mocker.patch("hsfs.core.online_ingestion.OpenSearchClientSingleton")
        mocker.patch.object(online_ingestion, "_PAGE_SIZE", 2)
        _, clock, started = self._slow_searches(
            mocker, singleton.return_value._search, 6
        )
        mock_time = mocker.patch("hsfs.core.online_ingestion.time")
        mock_time.sleep.side_effect = lambda period: clock.__setitem__(
            0, clock[0] + timedelta(seconds=period)
        )
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
        ]
        mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "refresh",
            autospec=True,
            side_effect=lambda self: setattr(self, "_results", counted),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=10, results=counted, feature_group=mocker.Mock(id=119)
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 2})

        # Assert - one page, started at 1 second, then the timeout with nothing counted from the partial scan
        assert started == [1]
        assert ended
        assert len(messages) == 1
        assert messages[0].startswith("Timeout of 0:00:02 was exceeded")

    def test_wait_for_completion_warns_once_while_the_logs_cannot_be_read(self, mocker):
        # Arrange
        counted = [
            OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
        ]
        self._patch_wait(mocker, results_after_refresh=counted)
        count_logged = mocker.patch.object(
            online_ingestion.OnlineIngestion,
            "_count_logged_records",
            side_effect=AuthorizationException(403, "security_exception", {}),
        )
        ingestion = online_ingestion.OnlineIngestion(
            id=5, num_entries=10, results=counted
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 0})

        # Assert - it keeps trying, and says so once
        assert not ended
        assert len(messages) == 1
        assert count_logged.call_count > 1

    def test_wait_for_completion_timeout_with_uncounted_rows_points_at_get_failures(
        self, mocker
    ):
        # Arrange - the 6 rows refused over the memory quota were never counted
        self._patch_wait(mocker)
        ingestion = online_ingestion.OnlineIngestion(
            id=5,
            num_entries=10,
            results=[
                OnlineIngestionResult(online_ingestion_id=5, status="UPSERTED", rows=4)
            ],
        )

        # Act
        messages, ended = self._wait(ingestion, {"timeout": 30})

        # Assert
        assert len(messages) == 1
        assert messages[0].startswith("Timeout of 0:00:30 was exceeded")
        assert "get_failures()" in messages[0]
        assert ended
