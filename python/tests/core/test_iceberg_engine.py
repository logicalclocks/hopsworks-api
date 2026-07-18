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
from hsfs.core import feature_group_engine, partition_transforms
from hsfs.core.iceberg_engine import IcebergEngine


def _make_fg(
    location: str = "hopsfs://nn/apps/hive/warehouse/fs.db/fg_1",
    primary_key=None,
    event_time=None,
    partition_key=None,
    partitioned_by=None,
    zorder_by=None,
    sort_order=None,
):
    fg = mock.MagicMock()
    fg.name = "fg"
    fg.version = 1
    fg.location = location
    fg.prepare_spark_location.return_value = location
    fg.primary_key = primary_key if primary_key is not None else ["pk"]
    fg.event_time = event_time
    fg.partition_key = partition_key if partition_key is not None else []
    fg.partitioned_by = partitioned_by
    fg.zorder_by = zorder_by
    fg.sort_order = sort_order
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

    def test_register_temporary_table_routes_glue_through_catalog(self, mocker):
        # Arrange
        from hsfs import storage_connector

        connector = storage_connector.GlueConnector(
            id=2,
            name="glue",
            featurestore_id=99,
            access_key="ak",
            secret_key="sk",
            region="eu-north-1",
            # Hopsworks-internal database; the data source holds the real Glue one.
            database="hopsworks_featurestore",
        )
        fg = _make_fg(location="s3://ralfsbucket/iceberg-warehouse/ralfsglue.db/fg_1")
        fg.storage_connector = connector
        fg.data_source.database = "ralfsglue"
        fg.data_source.table = "fg_1"
        spark_session = mocker.Mock()
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark_session)
        fg_alias = _make_alias(alias="fg_1_alias")

        # Act
        iceberg_engine._register_temporary_table(fg_alias, read_options=None)

        # Assert
        # Reads the catalog-resolved table via .table() (the data source
        # database wins over the connector's internal one), not the S3 path.
        table_mock = spark_session.read.format.return_value.options.return_value.table
        table_mock.assert_called_once_with("glue_catalog.ralfsglue.fg_1")
        table_mock.return_value.createOrReplaceTempView.assert_called_once_with(
            "fg_1_alias"
        )
        # Configures the Glue catalog on the session.
        conf_calls = {call.args[0] for call in spark_session.conf.set.call_args_list}
        assert "spark.sql.catalog.glue_catalog.catalog-impl" in conf_calls
        assert "spark.sql.catalog.glue_catalog.warehouse" in conf_calls

    def test_glue_catalog_write_options_injects_catalog(self, mocker):
        # Arrange
        from hsfs import storage_connector

        connector = storage_connector.GlueConnector(
            id=2,
            name="glue",
            featurestore_id=99,
            access_key="ak",
            secret_key="sk",
            region="eu-north-1",
            database="ralfsglue",
        )
        fg = _make_fg(location="s3://ralfsbucket/iceberg-warehouse/ralfsglue.db/fg_1")
        fg.storage_connector = connector
        fg.data_source.database = "ralfsglue"
        fg.data_source.table = "fg_1"
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        options = iceberg_engine._glue_catalog_write_options({})

        # Assert
        assert options["iceberg.catalog"] == "glue_catalog"
        assert options["iceberg.catalog.table-identifier"] == "ralfsglue.fg_1"
        assert options["iceberg.catalog.catalog-impl"] == connector.GLUE_CATALOG_IMPL
        assert options["iceberg.catalog.s3.access-key-id"] == "ak"
        # Warehouse is the root, not the per-table location.
        assert (
            options["iceberg.catalog.warehouse"] == "s3://ralfsbucket/iceberg-warehouse"
        )

    def test_glue_warehouse_trims_database_and_table_suffix(self, mocker):
        # Arrange
        from hsfs import storage_connector
        from hsfs.core.glue_catalog import GlueCatalog

        connector = storage_connector.GlueConnector(
            id=2, name="glue", featurestore_id=99, database="hopsworks_featurestore"
        )
        fg = _make_fg(location="s3://ralfsbucket/iceberg-warehouse/ralfsglue.db/fg_1")
        fg.storage_connector = connector
        fg.data_source.database = "ralfsglue"
        fg.data_source.table = "fg_1"
        glue = GlueCatalog._for_feature_group(fg)

        # Act & Assert — warehouse is the root, identifier uses the data source.
        assert glue.warehouse == "s3://ralfsbucket/iceberg-warehouse"
        assert glue.identifier == "ralfsglue.fg_1"
        assert glue.qualified_name == "glue_catalog.ralfsglue.fg_1"

    def test_glue_catalog_write_options_respects_explicit_catalog(self, mocker):
        # Arrange
        from hsfs import storage_connector

        connector = storage_connector.GlueConnector(
            id=2, name="glue", featurestore_id=99, database="ralfsglue"
        )
        fg = _make_fg()
        fg.storage_connector = connector
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act: a user-provided catalog must not be overridden.
        options = iceberg_engine._glue_catalog_write_options(
            {"iceberg.catalog": "my_catalog"}
        )

        # Assert
        assert options["iceberg.catalog"] == "my_catalog"
        assert "iceberg.catalog.table-identifier" not in options

    def test_glue_catalog_write_options_noop_without_glue(self, mocker):
        # Arrange: non-Glue feature group leaves write options untouched.
        fg = _make_fg()
        fg.storage_connector = None
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        options = iceberg_engine._glue_catalog_write_options({"mode": "append"})

        # Assert
        assert options == {"mode": "append"}

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

    def test_partition_spec_transforms_prefers_partitioned_by(self, mocker):
        # Arrange
        fg = _make_fg(partitioned_by=["day(ts)", "bucket(16, pk)"])
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        transforms = iceberg_engine._partition_spec_transforms()

        # Assert
        assert [(t.name, t.source, t.param) for t in transforms] == [
            ("day", "ts", None),
            ("bucket", "pk", 16),
        ]

    def test_partition_spec_transforms_identity_fallback_from_partition_key(
        self, mocker
    ):
        # Arrange
        fg = _make_fg(partition_key=["a", "b"])
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act
        transforms = iceberg_engine._partition_spec_transforms()

        # Assert
        assert [(t.name, t.source) for t in transforms] == [
            ("identity", "a"),
            ("identity", "b"),
        ]

    def test_partition_spec_transforms_old_grain_spec_is_opaque(self, mocker):
        # Arrange: pre-transform stored specs do not parse and derive nothing
        fg = _make_fg(partitioned_by=["year", "month"])
        iceberg_engine = _make_engine(mocker, fg=fg)

        # Act & Assert
        assert iceberg_engine._partition_spec_transforms() == []

    @staticmethod
    def _self_returning_spec_builder(mocker):
        spec_builder = mocker.MagicMock()
        for method in (
            "identity",
            "bucket",
            "truncate",
            "alwaysNull",
            "year",
            "month",
            "day",
            "hour",
        ):
            getattr(spec_builder, method).return_value = spec_builder
        return spec_builder

    def test_create_iceberg_table_builds_spec_from_transforms(self, mocker):
        # Arrange
        fg = _make_fg(
            partitioned_by=[
                "day(ts)",
                "bucket(16, pk)",
                "truncate(4, zip)",
                "void(x)",
                "cat",
            ]
        )
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        spec_builder = self._self_returning_spec_builder(mocker)
        jvm.org.apache.iceberg.PartitionSpec.builderFor.return_value = spec_builder
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)

        # Act
        iceberg_engine._create_iceberg_table(mocker.MagicMock(), "hopsfs://nn/p")

        # Assert: each transform lands on its PartitionSpec builder method
        spec_builder.day.assert_called_once_with("ts")
        spec_builder.bucket.assert_called_once_with("pk", 16)
        spec_builder.truncate.assert_called_once_with("zip", 4)
        spec_builder.alwaysNull.assert_called_once_with("x")
        spec_builder.identity.assert_called_once_with("cat")
        # a partitioned table shuffles rows to their partitions on write
        properties = jvm.java.util.HashMap.return_value
        properties.put.assert_called_once_with("write.distribution-mode", "hash")
        # HadoopTables.create(schema, spec, properties, location)
        tables = jvm.org.apache.iceberg.hadoop.HadoopTables.return_value
        create_args = tables.create.call_args.args
        assert len(create_args) == 4
        assert create_args[1] is spec_builder.build.return_value
        assert create_args[2] is properties
        assert create_args[3] == "hopsfs://nn/p"

    def test_create_iceberg_table_unpartitioned_sets_no_distribution_mode(self, mocker):
        # Arrange
        fg = _make_fg()
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)

        # Act
        iceberg_engine._create_iceberg_table(mocker.MagicMock(), "hopsfs://nn/p")

        # Assert
        jvm.java.util.HashMap.return_value.put.assert_not_called()

    def test_optimize_requires_classic_spark(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=None)

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="classic"):
            iceberg_engine._optimize()

    def test_optimize_rejects_glue_catalog(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=mocker.Mock())

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="Glue"):
            iceberg_engine._optimize()

    def test_optimize_zorder_rewrites_data_files(self, mocker):
        # Arrange
        fg = _make_fg(zorder_by=["merchant_id", "amount"])
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)

        # Act
        iceberg_engine._optimize()

        # Assert: the table is loaded path-based and rewritten with z-order
        tables = jvm.org.apache.iceberg.hadoop.HadoopTables.return_value
        tables.load.assert_called_once_with(
            "hopsfs://nn/apps/hive/warehouse/fs.db/fg_1"
        )
        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get.return_value.rewriteDataFiles.return_value
        spark_context._gateway.new_array.assert_called_once_with(
            jvm.java.lang.String, 2
        )
        cols = spark_context._gateway.new_array.return_value
        action.zOrder.assert_called_once_with(cols)
        zordered = action.zOrder.return_value
        # rewrite_all defaults to False, so no full-table rewrite by default
        zordered.option.assert_not_called()
        zordered.execute.assert_called_once_with()

    def test_optimize_zorder_rewrite_all_sets_option(self, mocker):
        # Arrange
        fg = _make_fg(zorder_by=["merchant_id"])
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)

        # Act: an operator asks for the initial full z-order explicitly
        iceberg_engine._optimize(rewrite_all=True)

        # Assert: rewrite-all is set only when requested
        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get.return_value.rewriteDataFiles.return_value
        zordered = action.zOrder.return_value
        zordered.option.assert_called_once_with("rewrite-all", "true")
        zordered.option.return_value.execute.assert_called_once_with()

    def test_optimize_where_applies_iceberg_filter(self, mocker):
        # Arrange
        fg = _make_fg()
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        # In Iceberg 1.7 the converter is at
        # org.apache.spark.sql.execution.datasources with convertToIcebergExpression
        converter = (
            jvm.org.apache.spark.sql.execution.datasources.SparkExpressionConverter
        )

        # Act
        iceberg_engine._optimize(where="amount > 100")

        # Assert: the predicate is converted to an Iceberg expression and the
        # rewrite is restricted with .filter(); the temp view is cleaned up
        converter.collectResolvedSparkExpression.assert_called_once()
        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get.return_value.rewriteDataFiles.return_value
        action.filter.assert_called_once_with(
            converter.convertToIcebergExpression.return_value
        )
        iceberg_engine._spark_session.catalog.dropTempView.assert_called_once()

    def test_optimize_compacts_without_zorder_by(self, mocker):
        # Arrange
        fg = _make_fg()
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, fg=fg, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)

        # Act
        iceberg_engine._optimize()

        # Assert: plain bin-packing compaction, no z-order strategy
        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get.return_value.rewriteDataFiles.return_value
        action.zOrder.assert_not_called()
        action.execute.assert_called_once_with()

    def test_update_partition_spec_requires_classic_spark(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=None)
        add = partition_transforms._parse(["hour(ts)"])

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="classic Spark session"):
            iceberg_engine._update_partition_spec(add, [])

    def test_update_partition_spec_rejects_glue_catalog(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=mocker.Mock())
        add = partition_transforms._parse(["hour(ts)"])

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="Glue"):
            iceberg_engine._update_partition_spec(add, [])

    def test_update_partition_spec_rejects_void_add(self, mocker):
        # Arrange: removing a field already voids it in the evolved spec
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        add = partition_transforms._parse(["void(x)"])

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="void transforms"):
            iceberg_engine._update_partition_spec(add, [])

    def test_update_partition_spec_commits_through_jvm(self, mocker):
        # Arrange
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        add = partition_transforms._parse(["hour(ts)"])
        remove = partition_transforms._parse(["day(ts)"])

        # Act
        iceberg_engine._update_partition_spec(add, remove)

        # Assert: the table is loaded path-based and the spec update chains
        # removeField before addField, then commits
        tables = jvm.org.apache.iceberg.hadoop.HadoopTables.return_value
        tables.load.assert_called_once_with(
            "hopsfs://nn/apps/hive/warehouse/fs.db/fg_1"
        )
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        expressions.day.assert_called_once_with("ts")
        expressions.hour.assert_called_once_with("ts")
        update = tables.load.return_value.updateSpec.return_value
        update.removeField.assert_called_once_with(expressions.day.return_value)
        after_remove = update.removeField.return_value
        after_remove.addField.assert_called_once_with(expressions.hour.return_value)
        after_remove.addField.return_value.commit.assert_called_once_with()
        tables.load.return_value.refresh.assert_called_once_with()

    def test_update_partition_spec_add_with_alias_names_field(self, mocker):
        # Arrange
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        add = partition_transforms._parse(["bucket(16,id) as shard"])

        # Act
        iceberg_engine._update_partition_spec(add, [])

        # Assert: the two-argument addField overload names the partition field
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        tables = jvm.org.apache.iceberg.hadoop.HadoopTables.return_value
        update = tables.load.return_value.updateSpec.return_value
        update.addField.assert_called_once_with(
            "shard", expressions.bucket.return_value
        )

    def test_update_partition_spec_empty_reconciles_without_commit(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        table = mocker.MagicMock()
        mocker.patch.object(iceberg_engine, "_load_jvm_table", return_value=table)
        spec_mock = mocker.patch.object(
            IcebergEngine, "_spec_expressions", return_value=["day(ts)"]
        )

        # Act
        result = iceberg_engine._update_partition_spec([], [])

        # Assert: nothing is committed; the current spec is only read back
        table.updateSpec.assert_not_called()
        spec_mock.assert_called_once_with(table)
        assert result == ["day(ts)"]

    def test_update_partition_spec_returns_committed_spec(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        table = mocker.MagicMock()
        mocker.patch.object(iceberg_engine, "_load_jvm_table", return_value=table)
        mocker.patch.object(
            IcebergEngine,
            "_spec_expressions",
            return_value=["bucket(4,id)", "hour(ts)"],
        )
        add = partition_transforms._parse(["hour(ts)"])

        # Act
        result = iceberg_engine._update_partition_spec(add, [])

        # Assert: the returned spec is read back from the refreshed table
        table.updateSpec.return_value.addField.return_value.commit.assert_called_once_with()
        table.refresh.assert_called_once_with()
        assert result == ["bucket(4,id)", "hour(ts)"]

    @staticmethod
    def _jvm_partition_field(mocker, name, transform, source_id):
        field = mocker.Mock()
        field.name.return_value = name
        field.transform.return_value.toString.return_value = transform
        field.sourceId.return_value = source_id
        return field

    def test_spec_expressions_round_trips_transform_grammar(self, mocker):
        # Arrange: identity, parameterized, temporal, aliased, and void
        # (V1 removed-field placeholder) fields
        table = mocker.Mock()
        schema = table.schema.return_value
        schema.findColumnName.side_effect = {1: "region", 2: "id", 3: "ts"}.get
        table.spec.return_value.fields.return_value = [
            self._jvm_partition_field(mocker, "region", "identity", 1),
            self._jvm_partition_field(mocker, "id_bucket", "bucket[16]", 2),
            self._jvm_partition_field(mocker, "shard", "day", 3),
            self._jvm_partition_field(mocker, "old_field", "void", 3),
        ]

        # Act
        expressions = IcebergEngine._spec_expressions(table)

        # Assert: an alias suffix appears whenever the actual field name
        # differs from Iceberg's generated default
        assert expressions == [
            "region",
            "bucket(16,id)",
            "day(ts) as shard",
            "void(ts) as old_field",
        ]

    def test_spec_expressions_rejects_inexpressible_transform(self, mocker):
        # Arrange
        table = mocker.Mock()
        table.schema.return_value.findColumnName.return_value = "ts"
        table.spec.return_value.fields.return_value = [
            self._jvm_partition_field(mocker, "weird", "unknown<42>", 1),
        ]

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="cannot be expressed"):
            IcebergEngine._spec_expressions(table)

    def test_load_jvm_table_glue_uses_catalog(self, mocker):
        # Arrange
        spark = mocker.MagicMock()
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(
            mocker, spark_session=spark, spark_context=spark_context
        )
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=mocker.Mock())
        mocker.patch.object(
            iceberg_engine, "_prepare_glue_session", return_value="glue.db.tbl"
        )

        # Act
        table = iceberg_engine._load_jvm_table()

        # Assert: the Glue catalog owns the pointer; no path-based load
        load_mock = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable
        load_mock.assert_called_once_with(spark._jsparkSession, "glue.db.tbl")
        assert table is load_mock.return_value
        jvm.org.apache.iceberg.hadoop.HadoopTables.assert_not_called()

    def test_load_jvm_table_path_failure_points_at_user_catalog(self, mocker):
        # Arrange: a failing path-based load usually means the table is
        # owned by an ad-hoc user catalog (the iceberg.catalog write option)
        spark_context = mocker.MagicMock()
        jvm = spark_context._jvm
        iceberg_engine = _make_engine(mocker, spark_context=spark_context)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        tables = jvm.org.apache.iceberg.hadoop.HadoopTables.return_value
        tables.load.side_effect = RuntimeError("no version-hint")

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="user-provided catalog"):
            iceberg_engine._load_jvm_table()

    @pytest.mark.parametrize(
        ("expr", "method", "args"),
        [
            ("identity(c)", "ref", ("c",)),
            ("bucket(16, c)", "bucket", ("c", 16)),
            ("truncate(4, c)", "truncate", ("c", 4)),
            ("year(c)", "year", ("c",)),
            ("month(c)", "month", ("c",)),
            ("day(c)", "day", ("c",)),
            ("hour(c)", "hour", ("c",)),
        ],
    )
    def test_iceberg_term_mapping(self, mocker, expr, method, args):
        # Arrange
        jvm = mocker.MagicMock()
        transform = partition_transforms._parse_expression(expr)

        # Act
        term = IcebergEngine._iceberg_term(jvm, transform)

        # Assert
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        getattr(expressions, method).assert_called_once_with(*args)
        assert term is getattr(expressions, method).return_value

    def test_iceberg_term_rejects_week(self, mocker):
        # Arrange: week is not an Iceberg transform
        jvm = mocker.MagicMock()
        transform = partition_transforms._parse_expression("week(ts)")

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="Iceberg term"):
            IcebergEngine._iceberg_term(jvm, transform)

    def test_delete_record_not_iceberg_table_raises(self, mocker):
        # Arrange
        iceberg_engine = _make_engine(mocker)
        mocker.patch.object(iceberg_engine, "_is_iceberg_table_at", return_value=False)

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            iceberg_engine._delete_record(mocker.Mock())

        # Assert
        assert "is not ICEBERG enabled." in str(e_info.value)

    def test_reconcile_timestamp_tz_localizes_naive_to_zoned_target(self, mocker):
        # Arrange: incoming naive timestamp, existing table column is timestamptz
        import pyarrow as pa

        arrow_table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "time": pa.array([0], type=pa.timestamp("us")),
            }
        )
        table = mocker.Mock()
        table.schema.return_value.as_arrow.return_value = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("time", pa.timestamp("us", tz="UTC")),
            ]
        )

        # Act
        result = IcebergEngine._reconcile_timestamp_tz(arrow_table, table)

        # Assert: time column is now zoned, id untouched
        assert result.schema.field("time").type == pa.timestamp("us", tz="UTC")
        assert result.schema.field("id").type == pa.int64()

    def test_reconcile_timestamp_tz_leaves_matching_columns(self, mocker):
        # Arrange: both incoming and target are naive -> no change
        import pyarrow as pa

        arrow_table = pa.table({"time": pa.array([0], type=pa.timestamp("us"))})
        table = mocker.Mock()
        table.schema.return_value.as_arrow.return_value = pa.schema(
            [pa.field("time", pa.timestamp("us"))]
        )

        # Act
        result = IcebergEngine._reconcile_timestamp_tz(arrow_table, table)

        # Assert
        assert result.schema.field("time").type == pa.timestamp("us")

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
        dataset.schema.fields = []

        # Act
        iceberg_engine._write_iceberg_dataset(
            dataset, {"iceberg.catalog": "prod"}, operation="upsert"
        )

        # Assert: the table is created through DDL, then the data is appended
        create_ddl = spark.sql.call_args_list[0].args[0]
        assert create_ddl.startswith("CREATE TABLE prod.fs.fg_1 ")
        assert "USING iceberg" in create_ddl
        assert "PARTITIONED BY" not in create_ddl
        dataset.writeTo.assert_called_once_with("prod.fs.fg_1")
        dataset.writeTo.return_value.append.assert_called_once_with()

    def test_create_iceberg_table_catalog_partitioned_ddl(self, mocker):
        # Arrange
        spark = mocker.MagicMock()
        fg = _make_fg(
            partitioned_by=[
                "day(ts)",
                "bucket(16, pk)",
                "truncate(4, zip)",
                "cat",
            ]
        )
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark)
        pk_field = mock.Mock()
        pk_field.name = "pk"
        pk_field.dataType.simpleString.return_value = "bigint"
        ts_field = mock.Mock()
        ts_field.name = "ts"
        ts_field.dataType.simpleString.return_value = "timestamp"
        dataset = mocker.MagicMock()
        dataset.schema.fields = [pk_field, ts_field]

        # Act
        iceberg_engine._create_iceberg_table_catalog(dataset, "prod.fs.fg_1")

        # Assert: Spark DDL spells temporal transforms in plural
        statement = spark.sql.call_args.args[0]
        assert statement.startswith(
            "CREATE TABLE prod.fs.fg_1 (pk bigint, ts timestamp) USING iceberg"
        )
        assert (
            "PARTITIONED BY (days(ts), bucket(16, pk), truncate(4, zip), cat)"
            in statement
        )
        assert "TBLPROPERTIES ('write.distribution-mode'='hash')" in statement

    @pytest.mark.parametrize("expr", ["void(x)", "bucket(16, pk) as shard"])
    def test_create_iceberg_table_catalog_rejects_inexpressible_transforms(
        self, mocker, expr
    ):
        # Arrange: Spark's CREATE TABLE DDL grammar cannot express void
        # transforms or partition-field aliases; they are rejected rather
        # than silently dropped
        spark = mocker.MagicMock()
        fg = _make_fg(partitioned_by=[expr])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark)

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="catalog CREATE TABLE DDL"):
            iceberg_engine._create_iceberg_table_catalog(
                mocker.MagicMock(), "prod.fs.fg_1"
            )
        spark.sql.assert_not_called()

    def test_create_iceberg_table_catalog_rejects_sort_order(self, mocker):
        # Arrange: the catalog creation path cannot apply a persistent sort
        # order; erasing an accepted argument is not an option
        spark = mocker.MagicMock()
        fg = _make_fg(sort_order=["pk asc"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=spark)

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="catalog creation path"):
            iceberg_engine._create_iceberg_table_catalog(
                mocker.MagicMock(), "prod.fs.fg_1"
            )
        spark.sql.assert_not_called()

    @pytest.mark.skipif(not HAS_PYICEBERG, reason="pyiceberg not installed")
    def test_write_pyiceberg_dataset_catalog_upsert(self, mocker):
        # Arrange
        import pyarrow as pa

        fg = _make_fg(primary_key=["pk"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        catalog = mocker.MagicMock()
        catalog.table_exists.return_value = True
        table = catalog.load_table.return_value
        table.schema.return_value.as_arrow.return_value = pa.schema([])
        table.upsert.return_value = mocker.Mock(rows_inserted=1, rows_updated=2)
        load_catalog_mock = mocker.patch(
            "pyiceberg.catalog.load_catalog", return_value=catalog
        )
        arrow_table = pa.table({"pk": pa.array([1], type=pa.int64())})
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

    @pytest.mark.skipif(not HAS_PYICEBERG, reason="pyiceberg not installed")
    def test_write_pyiceberg_dataset_catalog_create_rejects_sort_order(self, mocker):
        # Arrange: create_table would silently drop the sort order, so the
        # creation path rejects it before creating anything
        import pyarrow as pa

        fg = _make_fg(primary_key=["pk"], sort_order=["pk asc"])
        iceberg_engine = _make_engine(mocker, fg=fg, spark_session=_NO_SPARK)
        catalog = mocker.MagicMock()
        catalog.table_exists.return_value = False
        mocker.patch("pyiceberg.catalog.load_catalog", return_value=catalog)
        arrow_table = pa.table({"pk": pa.array([1], type=pa.int64())})
        mocker.patch.object(
            iceberg_engine, "_prepare_arrow_table", return_value=arrow_table
        )

        # Act & Assert
        with pytest.raises(
            FeatureStoreException, match="PyIceberg catalog creation path"
        ):
            iceberg_engine._write_pyiceberg_dataset(
                mocker.Mock(),
                write_options={"iceberg.catalog": "prod"},
                operation="upsert",
            )
        catalog.create_namespace.assert_not_called()
        catalog.create_table.assert_not_called()


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
        import pyarrow as pa

        iceberg_engine = self._pyiceberg_engine(mocker)
        table = mocker.MagicMock()
        table.schema.return_value.as_arrow.return_value = pa.schema([])
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        arrow_table = pa.table({"id": pa.array([1], type=pa.int64())})
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
        import pyarrow as pa

        fg = _make_fg(primary_key=["pk"], event_time="et")
        iceberg_engine = self._pyiceberg_engine(mocker, fg=fg)
        table = mocker.MagicMock()
        table.schema.return_value.as_arrow.return_value = pa.schema([])
        table.upsert.return_value = mocker.Mock(rows_inserted=2, rows_updated=3)
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="location"
        )
        arrow_table = pa.table({"pk": pa.array([1], type=pa.int64())})
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
    def test_describe_layout_python_glue_loads_through_catalog(self, mocker):
        # Arrange: the Glue catalog owns the current-metadata pointer, so
        # layout introspection loads through pyiceberg's Glue catalog rather
        # than the table path
        iceberg_engine = self._pyiceberg_engine(mocker)
        glue = mocker.Mock()
        glue.catalog_name = "glue"
        glue.identifier = "db.tbl"
        glue._pyiceberg_catalog_properties.return_value = {"type": "glue"}
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=glue)
        catalog = mocker.MagicMock()
        table = catalog.load_table.return_value
        table.specs.return_value = {}
        table.sort_order.return_value.fields = []
        load_catalog_mock = mocker.patch(
            "pyiceberg.catalog.load_catalog", return_value=catalog
        )

        # Act
        layout = iceberg_engine._describe_layout()

        # Assert
        load_catalog_mock.assert_called_once_with("glue", type="glue")
        catalog.load_table.assert_called_once_with("db.tbl")
        assert layout == {
            "partition_spec": None,
            "partition_specs": None,
            "sort_order": None,
        }

    def test_describe_layout_jvm_current_spec_and_transformed_sort(self, mocker):
        # The current spec is table.spec(), not the highest spec id (external
        # engines can add non-default specs), and a transformed sort field
        # renders its transform instead of masquerading as an identity sort.
        iceberg_engine = _make_engine(mocker, spark_context=mocker.MagicMock())

        def jvm_field(name, transform, source_id):
            f = mocker.MagicMock()
            f.name.return_value = name
            f.transform.return_value.toString.return_value = transform
            f.sourceId.return_value = source_id
            return f

        schema = mocker.MagicMock()
        schema.findColumnName.side_effect = lambda i: {1: "ts", 2: "id"}[i]
        current_spec = mocker.MagicMock()
        current_spec.fields.return_value = [jvm_field("ts_day", "day", 1)]
        other_spec = mocker.MagicMock()
        other_spec.fields.return_value = [jvm_field("id_bucket", "bucket[4]", 2)]
        sort_field = mocker.MagicMock()
        sort_field.transform.return_value.toString.return_value = "bucket[16]"
        sort_field.sourceId.return_value = 2
        sort_field.direction.return_value.toString.return_value = "DESC"
        sort_field.nullOrder.return_value.toString.return_value = "NULLS_LAST"
        table = mocker.MagicMock()
        table.schema.return_value = schema
        table.spec.return_value = current_spec
        table.specs.return_value = {0: current_spec, 1: other_spec}
        table.sortOrder.return_value.fields.return_value = [sort_field]
        mocker.patch.object(iceberg_engine, "_load_jvm_table", return_value=table)

        layout = iceberg_engine._describe_layout()

        assert layout["partition_spec"] == ["ts_day: day(ts)"]
        assert layout["partition_specs"] == [
            ["ts_day: day(ts)"],
            ["id_bucket: bucket[4](id)"],
        ]
        assert layout["sort_order"] == ["bucket[16](id) desc nulls last"]

    def test_describe_layout_python_no_metadata_points_at_user_catalog(self, mocker):
        # Arrange: no version-hint.text at the location; either nothing was
        # written yet or a user-provided catalog owns the pointer
        iceberg_engine = self._pyiceberg_engine(mocker)
        mocker.patch.object(iceberg_engine, "_glue_catalog", return_value=None)
        mocker.patch.object(iceberg_engine, "_setup_pyiceberg")
        mocker.patch.object(iceberg_engine, "_make_pyiceberg_catalog")
        mocker.patch.object(
            iceberg_engine, "_get_pyiceberg_location", return_value="loc"
        )
        mocker.patch.object(
            iceberg_engine, "_load_pyiceberg_table", return_value=(None, None)
        )

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="version-hint.text") as e:
            iceberg_engine._describe_layout()
        assert "user-provided catalog" in str(e.value)

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
