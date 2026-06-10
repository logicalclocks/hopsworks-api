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
from unittest import mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group
from hsfs.core import feature_group_engine
from hsfs.core.iceberg_engine import IcebergEngine


def _make_fg(
    location: str = "hopsfs://nn/apps/hive/warehouse/fs.db/fg_1",
    primary_key=None,
    event_time=None,
    partition_key=None,
):
    fg = mock.MagicMock()
    fg.name = "fg"
    fg.version = 1
    fg.location = location
    fg.prepare_spark_location.return_value = location
    fg.primary_key = primary_key if primary_key is not None else ["pk"]
    fg.event_time = event_time
    fg.partition_key = partition_key if partition_key is not None else []
    return fg


def _make_engine(mocker, fg=None, spark_session=None, spark_context=None):
    mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
    return IcebergEngine(
        feature_store_id=99,
        feature_store_name="fs",
        feature_group=fg if fg is not None else _make_fg(),
        spark_session=spark_session if spark_session is not None else mocker.Mock(),
        spark_context=spark_context,
    )


def _make_alias(start_timestamp=None, end_timestamp=None, alias="fg_1"):
    fg_alias = mock.MagicMock()
    fg_alias.alias = alias
    fg_alias.left_feature_group_start_timestamp = start_timestamp
    fg_alias.left_feature_group_end_timestamp = end_timestamp
    return fg_alias


class TestIcebergEngine:
    def test_init_requires_spark_session(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            IcebergEngine(
                feature_store_id=99,
                feature_store_name="fs",
                feature_group=_make_fg(),
                spark_session=None,
                spark_context=None,
            )

        # Assert
        assert "only supported with the Spark engine" in str(e_info.value)

    def test_get_merge_keys_primary_key_only(self, mocker):
        # Arrange
        fg = _make_fg(primary_key=["pk1", "pk2"])
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        merge_keys = iceberg_engine._get_merge_keys()

        # Assert
        assert merge_keys == ["pk1", "pk2"]

    def test_get_merge_keys_all_key_types(self, mocker):
        # Arrange
        fg = _make_fg(primary_key=["pk"], event_time="et", partition_key=["pt"])
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        merge_keys = iceberg_engine._get_merge_keys()

        # Assert
        assert merge_keys == ["pk", "et", "pt"]

    def test_setup_iceberg_read_opts_snapshot_query(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias()

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(fg_alias, "location")

        # Assert
        assert options == {}

    def test_setup_iceberg_read_opts_time_travel_query(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias(end_timestamp=1234567890000)

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(fg_alias, "location")

        # Assert
        assert options == {"as-of-timestamp": "1234567890000"}

    def test_setup_iceberg_read_opts_incremental_query(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias(start_timestamp=1000, end_timestamp=2000)
        mocker.patch.object(
            iceberg_engine, "_resolve_snapshot_id_at", side_effect=[11, 22]
        )

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(fg_alias, "location")

        # Assert
        assert options == {"start-snapshot-id": "11", "end-snapshot-id": "22"}

    def test_setup_iceberg_read_opts_incremental_query_no_start_snapshot(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias(start_timestamp=1000)
        mocker.patch.object(
            iceberg_engine, "_resolve_snapshot_id_at", return_value=None
        )

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._setup_iceberg_read_opts(fg_alias, "location")

        # Assert
        assert "no Iceberg snapshot exists" in str(e_info.value)

    def test_setup_iceberg_read_opts_merges_options(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias()

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(
            fg_alias,
            "location",
            read_options={"iceberg.split-size": "128", "other": "value"},
        )

        # Assert
        assert options == {"split-size": "128", "other": "value"}

    def test_resolve_snapshot_id_at(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(
            iceberg_engine,
            "_read_snapshots",
            return_value=[
                {"committed_at": 1700000001000, "snapshot_id": 1},
                {"committed_at": 1700000002000, "snapshot_id": 2},
                {"committed_at": 1700000003000, "snapshot_id": 3},
            ],
        )

        # Act & Assert
        assert iceberg_engine._resolve_snapshot_id_at("location", 1700000002500) == 2
        assert iceberg_engine._resolve_snapshot_id_at("location", 1700000003000) == 3
        assert iceberg_engine._resolve_snapshot_id_at("location", 1700000000500) is None

    def test_register_temporary_table_calls_spark_read(self, mocker):
        # Arrange
        spark_session = mocker.Mock()
        iceberg_engine = _make_engine(mocker, spark_session=spark_session)
        fg_alias = _make_alias(alias="fg_1_alias")

        # Act
        iceberg_engine._register_temporary_table(fg_alias, read_options=None)

        # Assert
        spark_session.read.format.assert_called_once_with("iceberg")
        load_mock = spark_session.read.format.return_value.options.return_value.load
        load_mock.assert_called_once_with("hopsfs://nn/apps/hive/warehouse/fs.db/fg_1")
        load_mock.return_value.createOrReplaceTempView.assert_called_once_with(
            "fg_1_alias"
        )

    def test_save_iceberg_fg_calls_write_and_commit(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_commit = mocker.Mock()
        mocker.patch.object(
            iceberg_engine, "_write_iceberg_dataset", return_value=fg_commit
        )

        # Act
        iceberg_engine._save_iceberg_fg(
            mocker.Mock(), write_options=None, validation_id=42
        )

        # Assert
        assert fg_commit.validation_id == 42
        iceberg_engine._feature_group_api._commit.assert_called_once_with(
            iceberg_engine._feature_group, fg_commit
        )

    def test_write_iceberg_dataset_creates_table_when_missing(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_is_iceberg_table_at", return_value=False)
        create_mock = mocker.patch.object(iceberg_engine, "_create_iceberg_table")
        append_mock = mocker.patch.object(iceberg_engine, "_append_iceberg_dataset")
        merge_mock = mocker.patch.object(iceberg_engine, "_merge_iceberg_dataset")
        mocker.patch.object(iceberg_engine, "_get_last_commit_metadata")

        # Act
        iceberg_engine._write_iceberg_dataset(mocker.Mock(), None, operation="upsert")

        # Assert
        assert create_mock.call_count == 1
        assert append_mock.call_count == 1
        assert merge_mock.call_count == 0

    def test_write_iceberg_dataset_insert_appends(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_is_iceberg_table_at", return_value=True)
        create_mock = mocker.patch.object(iceberg_engine, "_create_iceberg_table")
        append_mock = mocker.patch.object(iceberg_engine, "_append_iceberg_dataset")
        merge_mock = mocker.patch.object(iceberg_engine, "_merge_iceberg_dataset")
        mocker.patch.object(iceberg_engine, "_get_last_commit_metadata")

        # Act
        iceberg_engine._write_iceberg_dataset(mocker.Mock(), None, operation="insert")

        # Assert
        assert create_mock.call_count == 0
        assert append_mock.call_count == 1
        assert merge_mock.call_count == 0

    def test_write_iceberg_dataset_upsert_merges(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_is_iceberg_table_at", return_value=True)
        create_mock = mocker.patch.object(iceberg_engine, "_create_iceberg_table")
        append_mock = mocker.patch.object(iceberg_engine, "_append_iceberg_dataset")
        merge_mock = mocker.patch.object(
            iceberg_engine, "_merge_iceberg_dataset", return_value=(1, 2, 0)
        )
        commit_mock = mocker.patch.object(iceberg_engine, "_get_last_commit_metadata")

        # Act
        iceberg_engine._write_iceberg_dataset(mocker.Mock(), None, operation="upsert")

        # Assert
        assert create_mock.call_count == 0
        assert append_mock.call_count == 0
        assert merge_mock.call_count == 1
        commit_mock.assert_called_once_with(
            "hopsfs://nn/apps/hive/warehouse/fs.db/fg_1",
            rows_inserted=1,
            rows_updated=2,
            rows_deleted=0,
        )

    def test_create_iceberg_table_blocked_in_connect_mode(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=None)

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._create_iceberg_table(mocker.Mock(), "location")

        # Assert
        assert "Spark Connect" in str(e_info.value)

    def test_delete_record_not_iceberg_table_raises(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_is_iceberg_table_at", return_value=False)

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._delete_record(mocker.Mock())

        # Assert
        assert "is not ICEBERG enabled" in str(e_info.value)

    def test_get_last_commit_metadata_append(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(
            iceberg_engine,
            "_read_snapshots",
            return_value=[
                {
                    "committed_at": 1700000001000,
                    "snapshot_id": 1,
                    "operation": "append",
                    "summary": {"added-records": "5", "total-records": "5"},
                },
                {
                    "committed_at": 1700000002000,
                    "snapshot_id": 2,
                    "operation": "append",
                    "summary": {"added-records": "10", "total-records": "15"},
                },
            ],
        )

        # Act
        fg_commit = iceberg_engine._get_last_commit_metadata("location")

        # Assert
        assert fg_commit.commit_time == 1700000002000
        assert fg_commit.last_active_commit_time == 1700000001000
        assert fg_commit.rows_inserted == 10
        assert fg_commit.rows_updated == 0
        assert fg_commit.rows_deleted == 0

    def test_get_last_commit_metadata_with_overrides(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(
            iceberg_engine,
            "_read_snapshots",
            return_value=[
                {
                    "committed_at": 2000,
                    "snapshot_id": 2,
                    "operation": "overwrite",
                    "summary": {"added-records": "100", "total-records": "100"},
                },
            ],
        )

        # Act
        fg_commit = iceberg_engine._get_last_commit_metadata(
            "location", rows_inserted=3, rows_updated=4, rows_deleted=5
        )

        # Assert
        assert fg_commit.rows_inserted == 3
        assert fg_commit.rows_updated == 4
        assert fg_commit.rows_deleted == 5

    def test_get_last_commit_metadata_no_snapshots_returns_none(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_read_snapshots", return_value=[])

        # Act
        fg_commit = iceberg_engine._get_last_commit_metadata("location")

        # Assert
        assert fg_commit is None

    def test_get_total_records(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(
            iceberg_engine,
            "_read_snapshots",
            return_value=[
                {"committed_at": 1000, "summary": {"total-records": "5"}},
                {"committed_at": 2000, "summary": {"total-records": "15"}},
            ],
        )

        # Act & Assert
        assert iceberg_engine._get_total_records("location") == 15

    def test_get_total_records_empty_table(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_read_snapshots", return_value=[])

        # Act & Assert
        assert iceberg_engine._get_total_records("location") == 0


class TestFeatureGroupEngineIceberg:
    def test_commit_details_time_travel_format_iceberg(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine._get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util._get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
            id=10,
        )

        # Act
        fg_engine._commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value._get_commit_details.call_count == 1

    def test_commit_delete_iceberg(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._get_spark_session_and_context",
            return_value=(mocker.Mock(), mocker.Mock()),
        )
        mock_iceberg_engine = mocker.patch(
            "hsfs.core.feature_group_engine.iceberg_engine.IcebergEngine"
        )
        mock_hudi_engine = mocker.patch(
            "hsfs.core.feature_group_engine.hudi_engine.HudiEngine"
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
            id=10,
        )

        # Act
        feature_group_engine.FeatureGroupEngine._commit_delete(
            feature_group=fg, delete_df=None, write_options={}
        )

        # Assert
        assert mock_iceberg_engine.return_value._delete_record.call_count == 1
        assert mock_hudi_engine.return_value._delete_record.call_count == 0

    def test_save_empty_table_creates_iceberg_table_for_iceberg_format(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type")
        fg_engine = feature_group_engine.FeatureGroupEngine(feature_store_id=1)
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
        )
        mocker.patch.object(
            feature_group_engine.FeatureGroupEngine,
            "_get_spark_session_and_context",
            return_value=("spark", "context"),
        )
        iceberg_engine_mock = mocker.Mock()
        iceberg_engine_cls = mocker.patch(
            "hsfs.core.feature_group_engine.iceberg_engine.IcebergEngine",
            return_value=iceberg_engine_mock,
        )

        # Act
        fg_engine._save_empty_table(fg)

        # Assert
        iceberg_engine_cls.assert_called_once_with(
            fg.feature_store_id,
            fg.feature_store_name,
            fg,
            "spark",
            "context",
        )
        iceberg_engine_mock._save_empty_table.assert_called_once_with(
            write_options=None
        )

    def test_save_empty_table_noop_for_iceberg_without_spark(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type")
        fg_engine = feature_group_engine.FeatureGroupEngine(feature_store_id=1)
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
        )
        mocker.patch.object(
            feature_group_engine.FeatureGroupEngine,
            "_get_spark_session_and_context",
            return_value=(None, None),
        )
        iceberg_engine_cls = mocker.patch(
            "hsfs.core.feature_group_engine.iceberg_engine.IcebergEngine"
        )

        # Act
        result = fg_engine._save_empty_table(fg)

        # Assert
        assert result is None
        iceberg_engine_cls.assert_not_called()
