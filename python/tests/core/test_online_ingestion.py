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
