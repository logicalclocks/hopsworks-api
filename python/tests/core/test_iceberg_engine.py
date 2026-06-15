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
import sys
from unittest import mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import HAS_PYICEBERG
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


_NO_SPARK = object()


def _make_engine(mocker, fg=None, spark_session=None, spark_context=None):
    mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
    mocker.patch("hsfs.core.variable_api.VariableApi")
    mocker.patch("hopsworks_common.core.project_api.ProjectApi")
    if spark_session is _NO_SPARK:
        spark_session = None
    elif spark_session is None:
        spark_session = mocker.Mock()
    return IcebergEngine(
        feature_store_id=99,
        feature_store_name="fs",
        feature_group=fg if fg is not None else _make_fg(),
        spark_session=spark_session,
        spark_context=spark_context,
    )


def _make_alias(start_timestamp=None, end_timestamp=None, alias="fg_1"):
    fg_alias = mock.MagicMock()
    fg_alias.alias = alias
    fg_alias.left_feature_group_start_timestamp = start_timestamp
    fg_alias.left_feature_group_end_timestamp = end_timestamp
    return fg_alias


class TestIcebergEngine:
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

        # Assert: vectorization is disabled to avoid the executor JVM crash
        assert options == {"vectorization-enabled": "false"}

    def test_setup_iceberg_read_opts_time_travel_query(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias(end_timestamp=1234567890000)

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(fg_alias, "location")

        # Assert
        assert options == {
            "vectorization-enabled": "false",
            "as-of-timestamp": "1234567890000",
        }

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
        assert options == {
            "vectorization-enabled": "false",
            "start-snapshot-id": "11",
            "end-snapshot-id": "22",
        }

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
        assert options == {
            "vectorization-enabled": "false",
            "split-size": "128",
            "other": "value",
        }

    def test_setup_iceberg_read_opts_user_can_override_vectorization(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        fg_alias = _make_alias()

        # Act
        options = iceberg_engine._setup_iceberg_read_opts(
            fg_alias,
            "location",
            read_options={"vectorization-enabled": "true"},
        )

        # Assert: a user-provided read option still takes precedence
        assert options == {"vectorization-enabled": "true"}

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
        assert "is not ICEBERG enabled." in str(e_info.value)

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
        mocker.patch("hsfs.core.feature_group_engine.HAS_PYICEBERG", False)
        iceberg_engine_cls = mocker.patch(
            "hsfs.core.feature_group_engine.iceberg_engine.IcebergEngine"
        )

        # Act
        result = fg_engine._save_empty_table(fg)

        # Assert
        assert result is None
        iceberg_engine_cls.assert_not_called()

    def test_save_empty_table_uses_pyiceberg_without_spark(self, mocker):
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
        mocker.patch("hsfs.core.feature_group_engine.HAS_PYICEBERG", True)
        iceberg_engine_mock = mocker.Mock()
        mocker.patch(
            "hsfs.core.feature_group_engine.iceberg_engine.IcebergEngine",
            return_value=iceberg_engine_mock,
        )

        # Act
        fg_engine._save_empty_table(fg)

        # Assert
        iceberg_engine_mock._save_empty_table.assert_called_once_with(
            write_options=None
        )


class TestIcebergCatalogWrites:
    def test_get_catalog_write_config_disabled(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)

        # Act & Assert
        assert iceberg_engine._get_catalog_write_config(None) == (None, {}, None)
        assert iceberg_engine._get_catalog_write_config({"other": "x"}) == (
            None,
            {},
            None,
        )

    def test_get_catalog_write_config(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)

        # Act
        name, properties, identifier = iceberg_engine._get_catalog_write_config(
            {
                "iceberg.catalog": "hadoop_prod",
                "iceberg.catalog.type": "hadoop",
                "iceberg.catalog.warehouse": "hdfs://nn/warehouse",
                "other": "ignored",
            }
        )

        # Assert
        assert name == "hadoop_prod"
        assert properties == {"type": "hadoop", "warehouse": "hdfs://nn/warehouse"}
        assert identifier == "fs.fg_1"

    def test_get_catalog_write_config_identifier_override(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)

        # Act
        name, properties, identifier = iceberg_engine._get_catalog_write_config(
            {
                "iceberg.catalog": "prod",
                "iceberg.catalog.table-identifier": "db.custom_table",
            }
        )

        # Assert
        assert name == "prod"
        assert properties == {}
        assert identifier == "db.custom_table"

    def test_write_iceberg_dataset_catalog_spark_upsert_merges(self, mocker):
        # Arrange
        spark = mocker.MagicMock()
        spark.catalog.tableExists.return_value = True
        fg = _make_fg(primary_key=["pk"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark)
        build_mock = mocker.patch.object(iceberg_engine, "_build_fg_commit")
        dataset = mocker.MagicMock()

        # Act
        iceberg_engine._write_iceberg_dataset(
            dataset,
            {"iceberg.catalog": "hadoop_prod", "iceberg.catalog.type": "hadoop"},
            operation="upsert",
        )

        # Assert
        spark.conf.set.assert_any_call("spark.sql.catalog.hadoop_prod.type", "hadoop")
        merge_sql = spark.sql.call_args_list[0].args[0]
        assert merge_sql.startswith("MERGE INTO hadoop_prod.fs.fg_1 t USING")
        assert "t.pk = u.pk" in merge_sql
        dataset.createOrReplaceTempView.assert_called_once_with("fg_1_updates")
        assert build_mock.call_count == 1
        # path-based machinery must not be touched in catalog mode
        fg.prepare_spark_location.assert_not_called()

    def test_write_iceberg_dataset_catalog_spark_create(self, mocker):
        # Arrange
        spark = mocker.MagicMock()
        spark.catalog.tableExists.return_value = False
        fg = _make_fg(primary_key=["pk"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark)
        mocker.patch.object(iceberg_engine, "_build_fg_commit")
        dataset = mocker.MagicMock()

        # Act
        iceberg_engine._write_iceberg_dataset(
            dataset, {"iceberg.catalog": "prod"}, operation="upsert"
        )

        # Assert
        dataset.writeTo.assert_called_once_with("prod.fs.fg_1")
        dataset.writeTo.return_value.using.assert_called_once_with("iceberg")
        dataset.writeTo.return_value.using.return_value.create.assert_called_once()

    @pytest.mark.skipif(not HAS_PYICEBERG, reason="pyiceberg not installed")
    def test_write_pyiceberg_dataset_catalog_upsert(self, mocker):
        # Arrange
        fg = _make_fg(primary_key=["pk"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        catalog = mocker.MagicMock()
        catalog.table_exists.return_value = True
        table = catalog.load_table.return_value
        table.upsert.return_value = mocker.Mock(rows_inserted=1, rows_updated=2)
        load_catalog_mock = mocker.patch(
            "pyiceberg.catalog.load_catalog", return_value=catalog
        )
        arrow_table = mocker.Mock()
        mocker.patch.object(
            iceberg_engine, "_prepare_arrow_table", return_value=arrow_table
        )
        setup_mock = mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        publish_mock = mocker.patch.object(iceberg_engine, "_publish_hadoop_metadata")
        mocker.patch.object(iceberg_engine, "_pyiceberg_snapshots", return_value=[])
        build_mock = mocker.patch.object(iceberg_engine, "_build_fg_commit")

        # Act
        iceberg_engine._write_pyiceberg_dataset(
            mocker.Mock(),
            write_options={
                "iceberg.catalog": "prod",
                "iceberg.catalog.uri": "http://rest:8181",
            },
            operation="upsert",
        )

        # Assert
        load_catalog_mock.assert_called_once_with("prod", uri="http://rest:8181")
        table.upsert.assert_called_once_with(arrow_table, join_cols=["pk"])
        assert build_mock.call_args.kwargs == {
            "rows_inserted": 1,
            "rows_updated": 2,
            "rows_deleted": 0,
        }
        # the catalog owns the pointer: no version-hint publishing,
        # no HopsFS-specific setup
        publish_mock.assert_not_called()
        setup_mock.assert_not_called()


class TestIcebergArrowFlight:
    def test_iceberg_query_supported_by_query_service(self, mocker):
        # Arrange
        from hsfs.core import arrow_flight_client

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            time_travel_format="ICEBERG",
        )
        query = mocker.Mock()
        query._left_feature_group = fg
        query._left_feature_group_start_time = None
        query._left_feature_group_end_time = None
        query._joins = []

        # Act & Assert
        assert arrow_flight_client._is_query_supported_rec(query)


class TestIcebergMaterialization:
    def test_python_engine_iceberg_fg_requires_pyiceberg(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.feature_group.HAS_PYICEBERG", False)

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=99,
                primary_key=[],
                foreign_key=[],
                partition_key=[],
                time_travel_format="ICEBERG",
            )

        # Assert
        # Mirrors the DELTA behavior: fail loudly instead of silently routing
        # writes through Kafka and the materialization job.
        assert "pyiceberg library is not installed" in str(e_info.value)

    def test_python_engine_iceberg_fg_direct_writes_with_pyiceberg(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.feature_group.HAS_PYICEBERG", True)

        # Act
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
        )

        # Assert
        # With pyiceberg installed, offline writes happen directly from the
        # Python engine, mirroring the delta-rs behavior.
        assert fg.stream is False

    def test_save_dataframe_iceberg_stream_runs_materialization_job(self, mocker):
        # Arrange
        from hsfs.engine import python

        mock_python_engine_run_materialization_job = mocker.patch(
            "hsfs.engine.python.Engine._run_materialization_job"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine._legacy_save_dataframe"
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            stream=True,
            time_travel_format="ICEBERG",
        )

        # Act
        python_engine._save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_run_materialization_job.call_count == 1
        assert mock_python_engine_legacy_save_dataframe.call_count == 0

    def test_python_engine_iceberg_fg_streams_for_external_hopsfs(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.feature_group.HAS_PYICEBERG", True)
        client_mock = mocker.patch("hsfs.feature_group.client._get_instance")
        client_mock.return_value._is_external.return_value = True

        # Act
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="ICEBERG",
        )

        # Assert
        # External clients cannot reach HopsFS with PyIceberg (no libhdfs), so
        # inserts route through Kafka and the materialization job instead.
        assert fg.stream is True

    def test_save_dataframe_iceberg_direct_write(self, mocker):
        # Arrange
        from hsfs.engine import python

        mock_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine._legacy_save_dataframe"
        )
        mock_iceberg_engine = mocker.patch("hsfs.core.iceberg_engine.IcebergEngine")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="ICEBERG",
        )

        # Act
        python_engine._save_dataframe(
            feature_group=fg,
            dataframe=mocker.Mock(),
            operation="upsert",
            online_enabled=False,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert
        assert mock_write_dataframe_kafka.call_count == 0
        assert mock_legacy_save_dataframe.call_count == 0
        assert mock_iceberg_engine.return_value._save_iceberg_fg.call_count == 1


class TestPyIcebergEngine:
    def _pyiceberg_engine(self, mocker, fg=None):
        # Pretend pyiceberg is installed so the decorator gate passes.
        mocker.patch("hopsworks_common.decorators.HAS_PYICEBERG", True)
        return _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)

    def test_get_pyiceberg_properties_gcs_sets_credentials_env(
        self, mocker, monkeypatch
    ):
        # Arrange
        from hsfs import storage_connector as sc

        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        fg = _make_fg()
        connector = mocker.Mock()
        connector.type = sc.StorageConnector.GCS
        connector.key_path = "/Projects/test/Resources/key.json"
        fg.storage_connector = connector
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        engine_instance = mocker.patch("hsfs.engine._get_instance")
        engine_instance.return_value._add_file.return_value = "/tmp/key.json"

        # Act
        props = iceberg_engine._get_pyiceberg_properties()

        # Assert
        # GCS auth goes through the application-default chain; without this
        # env var the client stalls probing the GCP metadata server
        import os

        assert props == {}
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/tmp/key.json"

    def test_get_pyiceberg_properties_adls(self, mocker):
        # Arrange
        from hsfs import storage_connector as sc

        fg = _make_fg()
        connector = mocker.Mock()
        connector.type = sc.StorageConnector.ADLS
        connector.account_name = "account"
        connector.application_id = "client-id"
        connector.service_credential = "secret"
        connector.directory_id = "tenant"
        fg.storage_connector = connector
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)

        # Act
        props = iceberg_engine._get_pyiceberg_properties()

        # Assert
        assert props == {
            "adls.account-name": "account",
            "adls.client-id": "client-id",
            "adls.client-secret": "secret",
            "adls.tenant-id": "tenant",
        }

    def test_pyiceberg_write_supported(self, mocker):
        # Arrange
        fg = _make_fg()
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        client_mock = mocker.patch("hsfs.core.iceberg_engine.client._get_instance")

        # Act & Assert: non-HopsFS storage is always writable
        fg._is_hopsfs_storage.return_value = False
        assert iceberg_engine._pyiceberg_write_supported() is True

        # Act & Assert: HopsFS is writable only from inside the cluster
        fg._is_hopsfs_storage.return_value = True
        client_mock.return_value._is_external.return_value = False
        assert iceberg_engine._pyiceberg_write_supported() is True
        client_mock.return_value._is_external.return_value = True
        assert iceberg_engine._pyiceberg_write_supported() is False

    def test_setup_pyiceberg_raises_for_external_hopsfs(self, mocker):
        # Arrange
        fg = _make_fg()
        fg._is_hopsfs_storage.return_value = True
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        client_mock = mocker.patch("hsfs.core.iceberg_engine.client._get_instance")
        client_mock.return_value._is_external.return_value = True

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._setup_pyiceberg()

        # Assert
        assert "not supported from external Python clients" in str(e_info.value)

    def test_write_pyiceberg_dataset_requires_pyiceberg(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.decorators.HAS_PYICEBERG", False)
        iceberg_engine = _make_engine(mocker, spark_session=_NO_SPARK)

        # Act
        with pytest.raises(ModuleNotFoundError) as e_info:
            iceberg_engine._write_pyiceberg_dataset(mocker.Mock())

        # Assert
        assert "pyiceberg" in str(e_info.value)

    def test_save_iceberg_fg_dispatches_to_pyiceberg_without_spark(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_session=_NO_SPARK)
        fg_commit = mocker.Mock()
        spark_write_mock = mocker.patch.object(iceberg_engine, "_write_iceberg_dataset")
        pyiceberg_write_mock = mocker.patch.object(
            iceberg_engine, "_write_pyiceberg_dataset", return_value=fg_commit
        )

        # Act
        iceberg_engine._save_iceberg_fg(mocker.Mock(), write_options=None)

        # Assert
        assert spark_write_mock.call_count == 0
        assert pyiceberg_write_mock.call_count == 1
        iceberg_engine._feature_group_api._commit.assert_called_once_with(
            iceberg_engine._feature_group, fg_commit
        )

    def test_read_table_version(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        io = mocker.MagicMock()
        io.new_input.return_value.exists.return_value = True
        io.new_input.return_value.open.return_value.__enter__.return_value.read.return_value = b"3\n"

        # Act & Assert
        assert iceberg_engine._read_table_version(io, "location") == 3
        io.new_input.assert_called_with("location/metadata/version-hint.text")

    def test_read_table_version_missing(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        io = mocker.MagicMock()
        io.new_input.return_value.exists.return_value = False

        # Act & Assert
        assert iceberg_engine._read_table_version(io, "location") is None

    def test_publish_hadoop_metadata(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        table = mocker.MagicMock()
        table.metadata_location = "location/metadata/00002-uuid.metadata.json"
        io = table.io
        io.new_input.return_value.open.return_value.__enter__.return_value.read.return_value = b"metadata"

        # Act
        iceberg_engine._publish_hadoop_metadata(table, "location", 2)

        # Assert
        io.new_input.assert_called_once_with(
            "location/metadata/00002-uuid.metadata.json"
        )
        output_paths = [call.args[0] for call in io.new_output.call_args_list]
        assert output_paths == [
            "location/metadata/v2.metadata.json",
            "location/metadata/version-hint.text",
        ]
        versioned_create = io.new_output.return_value.create
        assert versioned_create.call_args_list[0].kwargs == {"overwrite": False}
        assert versioned_create.call_args_list[1].kwargs == {"overwrite": True}

    def test_write_pyiceberg_dataset_creates_table_when_missing(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        table = mocker.MagicMock()
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        arrow_table = mocker.Mock()
        mocker.patch.object(
            iceberg_engine, "_prepare_arrow_table", return_value=arrow_table
        )
        mocker.patch.object(iceberg_engine, "_make_pyiceberg_catalog")
        mocker.patch.object(
            iceberg_engine, "_load_pyiceberg_table", return_value=(None, None)
        )
        create_mock = mocker.patch.object(
            iceberg_engine, "_create_pyiceberg_table", return_value=table
        )
        publish_mock = mocker.patch.object(iceberg_engine, "_publish_hadoop_metadata")
        mocker.patch.object(iceberg_engine, "_pyiceberg_snapshots", return_value=[])
        mocker.patch.object(iceberg_engine, "_build_fg_commit")

        # Act
        iceberg_engine._write_pyiceberg_dataset(mocker.Mock(), operation="upsert")

        # Assert
        assert create_mock.call_count == 1
        table.append.assert_called_once_with(arrow_table)
        table.upsert.assert_not_called()
        publish_mock.assert_called_once_with(table, "location", 1)

    def test_write_pyiceberg_dataset_insert_appends(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        table = mocker.MagicMock()
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        arrow_table = mocker.Mock()
        mocker.patch.object(
            iceberg_engine, "_prepare_arrow_table", return_value=arrow_table
        )
        mocker.patch.object(iceberg_engine, "_make_pyiceberg_catalog")
        mocker.patch.object(
            iceberg_engine, "_load_pyiceberg_table", return_value=(2, table)
        )
        publish_mock = mocker.patch.object(iceberg_engine, "_publish_hadoop_metadata")
        mocker.patch.object(iceberg_engine, "_pyiceberg_snapshots", return_value=[])
        mocker.patch.object(iceberg_engine, "_build_fg_commit")

        # Act
        iceberg_engine._write_pyiceberg_dataset(mocker.Mock(), operation="insert")

        # Assert
        table.append.assert_called_once_with(arrow_table)
        table.upsert.assert_not_called()
        publish_mock.assert_called_once_with(table, "location", 3)

    def test_write_pyiceberg_dataset_upsert_merges(self, mocker):
        # Arrange
        fg = _make_fg(primary_key=["pk"], event_time="et")
        iceberg_engine = self._pyiceberg_engine(mocker, fg=fg)
        table = mocker.MagicMock()
        table.upsert.return_value = mocker.Mock(rows_inserted=2, rows_updated=3)
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        arrow_table = mocker.Mock()
        mocker.patch.object(
            iceberg_engine, "_prepare_arrow_table", return_value=arrow_table
        )
        mocker.patch.object(iceberg_engine, "_make_pyiceberg_catalog")
        mocker.patch.object(
            iceberg_engine, "_load_pyiceberg_table", return_value=(5, table)
        )
        publish_mock = mocker.patch.object(iceberg_engine, "_publish_hadoop_metadata")
        mocker.patch.object(iceberg_engine, "_pyiceberg_snapshots", return_value=[])
        build_mock = mocker.patch.object(iceberg_engine, "_build_fg_commit")

        # Act
        iceberg_engine._write_pyiceberg_dataset(mocker.Mock(), operation="upsert")

        # Assert
        table.append.assert_not_called()
        table.upsert.assert_called_once_with(arrow_table, join_cols=["pk", "et"])
        publish_mock.assert_called_once_with(table, "location", 6)
        assert build_mock.call_args.kwargs == {
            "rows_inserted": 2,
            "rows_updated": 3,
            "rows_deleted": 0,
        }

    def test_delete_pyiceberg_record_not_iceberg_table_raises(self, mocker):
        # Arrange
        iceberg_engine = self._pyiceberg_engine(mocker)
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        mocker.patch.object(iceberg_engine, "_make_pyiceberg_catalog")
        mocker.patch.object(
            iceberg_engine, "_load_pyiceberg_table", return_value=(None, None)
        )

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._delete_pyiceberg_record(mocker.Mock())

        # Assert
        assert "is not ICEBERG enabled." in str(e_info.value)

    def test_delete_record_dispatches_to_pyiceberg_without_spark(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_session=_NO_SPARK)
        delete_mock = mocker.patch.object(iceberg_engine, "_delete_pyiceberg_record")
        delete_df = mocker.Mock()

        # Act
        iceberg_engine._delete_record(delete_df)

        # Assert
        delete_mock.assert_called_once_with(delete_df)

    @pytest.mark.skipif(not HAS_PYICEBERG, reason="pyiceberg not installed")
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="pyiceberg's PyArrowFileIO mishandles local Windows paths "
        "(file:///C:/... resolves to /C:/...); production locations are "
        "always remote URIs",
    )
    def test_pyiceberg_round_trip(self, mocker, tmp_path):
        # End-to-end against a real local Iceberg table: create, upsert,
        # HadoopTables publish, reload from version-hint, and delete.
        import pyarrow as pa

        fg = _make_fg(primary_key=["id"])
        # a file:// URI keeps PyIceberg's filesystem resolution working on
        # Windows too, where a plain path starts with a drive letter that
        # would be parsed as a URI scheme
        fg.location = (tmp_path / "fg_1").as_uri()
        fg._is_hopsfs_storage.return_value = False
        fg.storage_connector = None
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)

        # Act 1: first write creates the table
        commit1 = iceberg_engine._write_pyiceberg_dataset(
            pa.table({"id": [1, 2], "value": ["a", "b"]}), operation="insert"
        )

        # Act 2: upsert updates id=2 and inserts id=3
        commit2 = iceberg_engine._write_pyiceberg_dataset(
            pa.table({"id": [2, 3], "value": ["B", "c"]}), operation="upsert"
        )

        # Assert: commits and HadoopTables protocol files
        assert commit1.rows_inserted == 2
        assert commit2.rows_updated == 1
        assert commit2.rows_inserted == 1
        metadata_dir = tmp_path / "fg_1" / "metadata"
        assert (metadata_dir / "version-hint.text").read_text() == "2"
        assert (metadata_dir / "v1.metadata.json").exists()
        assert (metadata_dir / "v2.metadata.json").exists()

        # Assert: a fresh catalog resolves the published version and the data
        catalog = iceberg_engine._make_pyiceberg_catalog()
        version, table = iceberg_engine._load_pyiceberg_table(catalog, fg.location)
        assert version == 2
        rows = table.scan().to_arrow().sort_by("id").to_pydict()
        assert rows["id"] == [1, 2, 3]
        assert rows["value"] == ["a", "B", "c"]

        # Act 3: delete id=1
        iceberg_engine._delete_pyiceberg_record(pa.table({"id": [1]}))

        # Assert: row gone, version bumped
        catalog = iceberg_engine._make_pyiceberg_catalog()
        version, table = iceberg_engine._load_pyiceberg_table(catalog, fg.location)
        assert version == 3
        rows = table.scan().to_arrow().sort_by("id").to_pydict()
        assert rows["id"] == [2, 3]
