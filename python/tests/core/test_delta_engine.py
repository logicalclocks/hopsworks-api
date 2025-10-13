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

from hsfs.core.delta_engine import DeltaEngine
from hsfs.feature_group_commit import FeatureGroupCommit


class TestDeltaEngine:

    def test_get_last_commit_metadata_spark(self, mocker):
        # Arrange
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
            {"version": 2, "operation": "MERGE", "timestamp": "2024-01-02T00:00:00Z"},
            {"version": 3, "operation": "OPTIMIZE", "timestamp": "2024-01-03T00:00:00Z"},
        ]

        # Create fake Rows with asDict()
        mock_rows = [mocker.MagicMock(asDict=lambda row=row: row) for row in mock_history_data]

        # Mock Spark DataFrame
        mock_spark_df = mocker.MagicMock()
        mock_spark_df.collect.return_value = mock_rows

        # Mock DeltaTable
        mock_delta_table = mocker.MagicMock()
        mock_delta_table.history.return_value = mock_spark_df

        mocker_get_delta_feature_group_commit = mocker.patch("hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit", return_value="result")

        # Patch DeltaTable
        mocker.patch("delta.tables.DeltaTable.forPath", return_value=mock_delta_table)

        # Act
        result = DeltaEngine._get_last_commit_metadata(mocker.MagicMock(), "s3://some/path")

        # Assert
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once()
        mocker_get_delta_feature_group_commit.assert_called_once_with(mock_history_data[1], mock_history_data[0])

    def test_get_last_commit_metadata_empty_history(self, mocker):
        # Arrange
        mock_history_data = []

        # Create fake Rows with asDict()
        mock_rows = [mocker.MagicMock(asDict=lambda row=row: row) for row in mock_history_data]

        # Mock Spark DataFrame
        mock_spark_df = mocker.MagicMock()
        mock_spark_df.collect.return_value = mock_rows

        # Mock DeltaTable
        mock_delta_table = mocker.MagicMock()
        mock_delta_table.history.return_value = mock_spark_df

        mocker_get_delta_feature_group_commit = mocker.patch("hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit", return_value="result")

        # Patch DeltaTable
        mocker.patch("delta.tables.DeltaTable.forPath", return_value=mock_delta_table)

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result is None
        mocker_get_delta_feature_group_commit.assert_not_called()

    def test_get_last_commit_metadata_one_history_entry(self, mocker):
        # Arrange
        mock_history_data = [
            {"version": 1, "operation": "WRITE", "timestamp": "2024-01-01T00:00:00Z"},
        ]

        # Create fake Rows with asDict()
        mock_rows = [mocker.MagicMock(asDict=lambda row=row: row) for row in mock_history_data]

        # Mock Spark DataFrame
        mock_spark_df = mocker.MagicMock()
        mock_spark_df.collect.return_value = mock_rows

        # Mock DeltaTable
        mock_delta_table = mocker.MagicMock()
        mock_delta_table.history.return_value = mock_spark_df

        mocker_get_delta_feature_group_commit = mocker.patch("hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit", return_value="result")

        # Patch DeltaTable
        mocker.patch("delta.tables.DeltaTable.forPath", return_value=mock_delta_table)

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result == "result"
        mocker_get_delta_feature_group_commit.assert_called_once()
        mocker_get_delta_feature_group_commit.assert_called_once_with(mock_history_data[0], mock_history_data[0])

    def test_get_last_commit_metadata_one_history_entry_optimize(self, mocker):
        # Arrange
        mock_history_data = [
            {"version": 1, "operation": "OPTIMIZE", "timestamp": "2024-01-01T00:00:00Z"},
        ]

        # Create fake Rows with asDict()
        mock_rows = [mocker.MagicMock(asDict=lambda row=row: row) for row in mock_history_data]

        # Mock Spark DataFrame
        mock_spark_df = mocker.MagicMock()
        mock_spark_df.collect.return_value = mock_rows

        # Mock DeltaTable
        mock_delta_table = mocker.MagicMock()
        mock_delta_table.history.return_value = mock_spark_df

        mocker_get_delta_feature_group_commit = mocker.patch("hsfs.core.delta_engine.DeltaEngine._get_delta_feature_group_commit", return_value="result")

        # Patch DeltaTable
        mocker.patch("delta.tables.DeltaTable.forPath", return_value=mock_delta_table)

        # Act
        result = DeltaEngine._get_last_commit_metadata(None, "s3://some/path")

        # Assert
        assert result is None
        mocker_get_delta_feature_group_commit.assert_not_called()

    def test_get_delta_feature_group_commit_merge(self, mocker):
        # Arrange
        last_commit = {
            "operation": "MERGE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {
                "numTargetRowsInserted": 10,
                "numTargetRowsUpdated": 5,
                "numTargetRowsDeleted": 2,
            },
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch("hsfs.core.delta_engine.util.convert_event_time_to_timestamp", side_effect = lambda ts: ts)
        mocker.patch("hsfs.core.delta_engine.util.get_hudi_datestr_from_timestamp", side_effect = lambda ts: f"date-{ts}")

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(last_commit, oldest_commit)

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 5
        assert fg_commit.rows_deleted == 2
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_write(self, mocker):
        # Arrange
        last_commit = {
            "operation": "WRITE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {
                "numOutputRows": 10
            },
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch("hsfs.core.delta_engine.util.convert_event_time_to_timestamp", side_effect = lambda ts: ts)
        mocker.patch("hsfs.core.delta_engine.util.get_hudi_datestr_from_timestamp", side_effect = lambda ts: f"date-{ts}")

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(last_commit, oldest_commit)

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"

    def test_get_delta_feature_group_commit_other(self, mocker):
        # Arrange
        last_commit = {
            "operation": "OPTIMIZE",
            "timestamp": "2024-01-02T12:00:00Z",
            "operationMetrics": {
            },
        }
        oldest_commit = {
            "timestamp": "2024-01-01T08:00:00Z",
        }

        mocker.patch("hsfs.core.delta_engine.util.convert_event_time_to_timestamp", side_effect = lambda ts: ts)
        mocker.patch("hsfs.core.delta_engine.util.get_hudi_datestr_from_timestamp", side_effect = lambda ts: f"date-{ts}")

        # Act
        fg_commit = DeltaEngine._get_delta_feature_group_commit(last_commit, oldest_commit)

        # Assert
        assert isinstance(fg_commit, FeatureGroupCommit)
        assert fg_commit.commit_time == "2024-01-02T12:00:00Z"
        assert fg_commit.commit_date_string == "date-2024-01-02T12:00:00Z"
        assert fg_commit.rows_inserted == 0
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0
        assert fg_commit.last_active_commit_time == "2024-01-01T08:00:00Z"
