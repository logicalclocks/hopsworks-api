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
from hsfs.core import online_ingestion
from hsfs.core.online_ingestion_failure import OnlineIngestionFailure


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
