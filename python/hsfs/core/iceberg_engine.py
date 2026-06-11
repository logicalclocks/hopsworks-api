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
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import project_api
from hopsworks_common.core.constants import HAS_POLARS
from hopsworks_common.core.type_systems import _convert_offline_type_to_pyarrow_type
from hopsworks_common.decorators import _uses_pyiceberg
from hsfs import feature_group, feature_group_commit, util
from hsfs.core import feature_group_api, variable_api


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from hsfs.constructor import hudi_feature_group_alias

# Note: Avoid importing optional PyIceberg dependencies at module import time.
# They are imported on-demand inside methods to provide friendly errors only
# when the functionality is used.
_logger = logging.getLogger(__name__)


def _make_ephemeral_catalog(name: str, properties: dict[str, str]):
    """Build a dict-backed PyIceberg catalog living for one operation in-process.

    PyIceberg can only commit through a catalog, but a catalog is nothing more
    than a pointer store from table identifier to current metadata location.
    For Hopsworks path-based tables that pointer is owned by the
    `HadoopTables` `version-hint.text` protocol, so the catalog only has to
    host the commit mechanics for a single operation.
    A plain dict suffices and avoids the SQLAlchemy dependency of PyIceberg's
    `SqlCatalog`.
    """
    from pyiceberg.catalog import Catalog, MetastoreCatalog
    from pyiceberg.exceptions import (
        CommitFailedException,
        NoSuchTableError,
        TableAlreadyExistsError,
    )
    from pyiceberg.io import load_file_io
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.serializers import FromInputFile
    from pyiceberg.table import CommitTableResponse, Table
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.typedef import EMPTY_DICT

    class _EphemeralCatalog(MetastoreCatalog):
        """In-memory pointer store hosting PyIceberg commit mechanics."""

        def __init__(self, name, **props):
            super().__init__(name, **props)
            self._tables = {}

        @staticmethod
        def _key(identifier):
            return Catalog.identifier_to_tuple(identifier)

        def create_table(
            self,
            identifier,
            schema,
            location=None,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
            sort_order=UNSORTED_SORT_ORDER,
            properties=EMPTY_DICT,
        ):
            key = self._key(identifier)
            if key in self._tables:
                raise TableAlreadyExistsError(f"Table {key} already exists")
            staged_table = self._create_staged_table(
                identifier=identifier,
                schema=schema,
                location=location,
                partition_spec=partition_spec,
                sort_order=sort_order,
                properties=properties,
            )
            self._write_metadata(
                staged_table.metadata, staged_table.io, staged_table.metadata_location
            )
            self._tables[key] = staged_table.metadata_location
            return self.load_table(identifier)

        def register_table(self, identifier, metadata_location):
            key = self._key(identifier)
            if key in self._tables:
                raise TableAlreadyExistsError(f"Table {key} already exists")
            self._tables[key] = metadata_location
            return self.load_table(identifier)

        def load_table(self, identifier):
            key = self._key(identifier)
            metadata_location = self._tables.get(key)
            if metadata_location is None:
                raise NoSuchTableError(f"Table does not exist: {key}")
            io = load_file_io(properties=self.properties, location=metadata_location)
            metadata = FromInputFile.table_metadata(io.new_input(metadata_location))
            return Table(
                identifier=key,
                metadata=metadata,
                metadata_location=metadata_location,
                io=self._load_file_io(metadata.properties, metadata_location),
                catalog=self,
            )

        def commit_table(self, table, requirements, updates):
            key = self._key(table.name())
            try:
                current_table = self.load_table(key)
            except NoSuchTableError:
                current_table = None
            updated_staged_table = self._update_and_stage_table(
                current_table, table.name(), requirements, updates
            )
            if (
                current_table
                and updated_staged_table.metadata == current_table.metadata
            ):
                # no changes, do nothing
                return CommitTableResponse(
                    metadata=current_table.metadata,
                    metadata_location=current_table.metadata_location,
                )
            self._write_metadata(
                metadata=updated_staged_table.metadata,
                io=updated_staged_table.io,
                metadata_path=updated_staged_table.metadata_location,
            )
            if (
                current_table
                and self._tables.get(key) != current_table.metadata_location
            ):
                raise CommitFailedException(
                    f"Table {key} has been updated concurrently"
                )
            self._tables[key] = updated_staged_table.metadata_location
            return CommitTableResponse(
                metadata=updated_staged_table.metadata,
                metadata_location=updated_staged_table.metadata_location,
            )

        def table_exists(self, identifier):
            return self._key(identifier) in self._tables

        def create_namespace(self, namespace, properties=EMPTY_DICT):
            # namespaces are irrelevant for a single-table pointer store
            pass

        def _unsupported(self, *_args, **_kwargs):
            raise NotImplementedError(
                "Not supported by the ephemeral Hopsworks catalog."
            )

        # the remaining catalog surface is not needed for single-table commits
        create_table_transaction = _unsupported
        drop_table = _unsupported
        purge_table = _unsupported
        rename_table = _unsupported
        drop_namespace = _unsupported
        list_tables = _unsupported
        list_namespaces = _unsupported
        load_namespace_properties = _unsupported
        update_namespace_properties = _unsupported
        list_views = _unsupported
        drop_view = _unsupported
        view_exists = _unsupported

    return _EphemeralCatalog(name, **properties)


class IcebergEngine:
    """Engine for feature groups stored as Apache Iceberg tables.

    Tables are addressed by their filesystem location (Iceberg `HadoopTables`),
    mirroring how the Delta engine operates, so no Spark catalog configuration
    is required on the session.
    With the Spark engine, reads and writes go through the Iceberg Spark source.
    Without Spark, writes go through PyIceberg (the Iceberg analog of delta-rs):
    the write commits through an ephemeral local catalog and is then published
    using the `HadoopTables` protocol (`metadata/vN.metadata.json` plus
    `version-hint.text`), so PyIceberg and Spark writers stay interoperable.

    Alternatively, a write can commit through a user-provided catalog by
    passing the `iceberg.catalog` write option (see
    [`_get_catalog_write_config`][hsfs.core.iceberg_engine.IcebergEngine._get_catalog_write_config]).
    In catalog mode the catalog owns the current-metadata pointer, so all
    readers and writers of that table must use the same catalog.
    """

    ICEBERG_SPARK_FORMAT = "iceberg"
    ICEBERG_QUERY_TIME_TRAVEL_AS_OF_TIMESTAMP = "as-of-timestamp"
    ICEBERG_START_SNAPSHOT_ID = "start-snapshot-id"
    ICEBERG_END_SNAPSHOT_ID = "end-snapshot-id"
    ICEBERG_MERGE_SCHEMA = "merge-schema"
    ICEBERG_CHECK_ORDERING = "check-ordering"
    ICEBERG_DOT_PREFIX = "iceberg."
    ICEBERG_CATALOG_OPTION = "iceberg.catalog"
    ICEBERG_CATALOG_PROP_PREFIX = "iceberg.catalog."
    ICEBERG_CATALOG_TABLE_IDENTIFIER_OPTION = "iceberg.catalog.table-identifier"
    SNAPSHOTS_METADATA_SUFFIX = "#snapshots"
    PYICEBERG_NAMESPACE = "hopsworks"
    APPEND = "append"
    # Snapshot operations that change data; metadata-only operations
    # (e.g. "replace" from compaction) are ignored for commit accounting.
    DATA_OPERATIONS = ["append", "overwrite", "delete"]

    def __init__(
        self,
        feature_store_id: int,
        feature_store_name: str,
        feature_group: feature_group.FeatureGroup,
        spark_session,
        spark_context,
    ):
        _logger.debug(
            f"Initializing IcebergEngine {feature_group.name} v{feature_group.version}"
        )
        self._feature_group = feature_group
        self._spark_session = spark_session
        self._spark_context = spark_context
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi()
        self._variable_api = variable_api.VariableApi()
        self._project_api = project_api.ProjectApi()

    def _save_iceberg_fg(
        self,
        dataset,
        write_options: dict[str, Any] | None,
        validation_id: int | None = None,
        operation: str = "upsert",
    ) -> feature_group_commit.FeatureGroupCommit:
        operation = operation.lower() if operation else "upsert"
        if self._spark_session is not None:
            _logger.debug(
                f"Saving Iceberg dataset using spark to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_iceberg_dataset(dataset, write_options, operation)
        else:
            _logger.debug(
                f"Saving Iceberg dataset using pyiceberg to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_pyiceberg_dataset(
                dataset, write_options=write_options, operation=operation
            )
        if fg_commit is None:
            # nothing was committed (e.g. schema-only table creation)
            return None
        fg_commit.validation_id = validation_id
        return self._feature_group_api._commit(self._feature_group, fg_commit)

    def _is_iceberg_table_at(self, location: str) -> bool:
        """Return True iff *location* contains a readable Iceberg table.

        Probes by planning a read; planning only touches table metadata and
        fails when no Iceberg metadata exists at the location.
        """
        try:
            _ = (
                self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT)
                .load(location)
                .schema
            )
            return True
        except Exception:  # noqa: BLE001 - any failure means not an Iceberg table here
            return False

    def _create_iceberg_table(self, dataset, location: str) -> None:
        """Create an empty Iceberg table at *location* with the dataset's schema.

        Path-based (HadoopTables) tables cannot be created through the
        DataFrame writer, so the table is created through the JVM Iceberg API,
        which requires classic Spark.
        """
        if self._spark_context is None:
            raise FeatureStoreException(
                "Creating Iceberg tables is not supported in Spark Connect mode "
                "because it requires JVM bridge access. "
                "Create the feature group from a classic Spark session instead."
            )
        _logger.debug(f"Creating Iceberg table at {location}")
        jvm = self._spark_context._jvm
        schema = jvm.org.apache.iceberg.spark.SparkSchemaUtil.convert(
            dataset._jdf.schema()
        )
        spec_builder = jvm.org.apache.iceberg.PartitionSpec.builderFor(schema)
        for partition_col in self._feature_group.partition_key or []:
            spec_builder = spec_builder.identity(partition_col)
        jvm.org.apache.iceberg.hadoop.HadoopTables(
            self._spark_context._jsc.hadoopConfiguration()
        ).create(schema, spec_builder.build(), location)

    def _get_catalog_write_config(
        self, write_options: dict[str, Any] | None
    ) -> tuple[str | None, dict[str, str], str | None]:
        """Extract the user-provided catalog configuration from write options.

        Catalog mode is enabled by the `iceberg.catalog` write option naming
        the catalog to commit through.
        Connection properties can be passed as `iceberg.catalog.<prop>`
        options (e.g. `iceberg.catalog.type`, `iceberg.catalog.uri`,
        `iceberg.catalog.warehouse`); with Spark they can equally be
        configured on the session as `spark.sql.catalog.<name>.*`, and
        without Spark in a `.pyiceberg.yaml` configuration file.
        The table is addressed as `<feature store name>.<name>_<version>`
        unless `iceberg.catalog.table-identifier` overrides it.

        Warning: Single pointer authority
            In catalog mode the catalog owns the current-metadata pointer
            instead of the table location's `version-hint.text`, so all
            readers and writers of that table must use the same catalog.

        Parameters:
            write_options: User provided write options.

        Returns:
            Tuple of catalog name, catalog properties, and table identifier; the name is None when no catalog was requested.
        """
        if not write_options or self.ICEBERG_CATALOG_OPTION not in write_options:
            return None, {}, None
        catalog_name = write_options[self.ICEBERG_CATALOG_OPTION]
        properties = {}
        identifier = f"{self._feature_store_name}.{self._feature_group.name}_{self._feature_group.version}"
        for key, value in write_options.items():
            if key == self.ICEBERG_CATALOG_TABLE_IDENTIFIER_OPTION:
                identifier = value
            elif key != self.ICEBERG_CATALOG_OPTION and key.startswith(
                self.ICEBERG_CATALOG_PROP_PREFIX
            ):
                properties[key[len(self.ICEBERG_CATALOG_PROP_PREFIX) :]] = value
        return catalog_name, properties, identifier

    @staticmethod
    def _append_requested(operation: str, write_options: dict[str, Any] | None) -> bool:
        return operation == "insert" or (
            isinstance(write_options, dict)
            and str(write_options.get("mode", "")).lower() == IcebergEngine.APPEND
        )

    def _write_iceberg_dataset(
        self, dataset, write_options: dict[str, Any] | None, operation: str = "upsert"
    ) -> feature_group_commit.FeatureGroupCommit:
        if write_options is None:
            write_options = {}

        catalog_name, catalog_properties, identifier = self._get_catalog_write_config(
            write_options
        )
        if catalog_name:
            return self._write_iceberg_dataset_catalog(
                dataset, catalog_name, catalog_properties, identifier, operation
            )

        location = self._feature_group.prepare_spark_location()

        rows_inserted = rows_updated = rows_deleted = None
        if not self._is_iceberg_table_at(location):
            self._create_iceberg_table(dataset, location)
            self._append_iceberg_dataset(dataset, location, write_options)
        elif operation == "insert":
            _logger.debug(
                f"Insert operation requested for {location}. Using append mode, skipping merge."
            )
            self._append_iceberg_dataset(dataset, location, write_options)
        else:
            rows_inserted, rows_updated, rows_deleted = self._merge_iceberg_dataset(
                dataset, location, write_options
            )

        return self._get_last_commit_metadata(
            location,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
        )

    def _write_iceberg_dataset_catalog(
        self,
        dataset,
        catalog_name: str,
        catalog_properties: dict[str, str],
        identifier: str,
        operation: str,
    ) -> feature_group_commit.FeatureGroupCommit | None:
        """Write through a user-provided Spark catalog instead of the table path.

        The catalog is configured on the session from the
        `iceberg.catalog.<prop>` write options when given; otherwise the
        session's existing `spark.sql.catalog.<name>.*` configuration is used.
        Upserts run as a SQL `MERGE INTO`, which requires the Iceberg Spark
        SQL extensions on the session.
        """
        spark = self._spark_session
        base_key = f"spark.sql.catalog.{catalog_name}"
        impl = catalog_properties.pop("impl", None)
        if impl is not None:
            spark.conf.set(base_key, impl)
        elif spark.conf.get(base_key, None) is None:
            spark.conf.set(base_key, "org.apache.iceberg.spark.SparkCatalog")
        for prop, value in catalog_properties.items():
            spark.conf.set(f"{base_key}.{prop}", value)

        qualified = f"{catalog_name}.{identifier}"
        _logger.debug(f"Writing Iceberg dataset through catalog table {qualified}")
        if not spark.catalog.tableExists(qualified):
            writer = dataset.writeTo(qualified).using(self.ICEBERG_SPARK_FORMAT)
            if self._feature_group.partition_key:
                from pyspark.sql.functions import col

                writer = writer.partitionedBy(
                    *[col(c) for c in self._feature_group.partition_key]
                )
            writer.create()
        elif operation == "insert":
            dataset.writeTo(qualified).append()
        else:
            updates_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_updates"
            )
            dataset.createOrReplaceTempView(updates_alias)
            merge_condition = " AND ".join(
                f"t.{key} = u.{key}" for key in self._get_merge_keys()
            )
            spark.sql(
                f"MERGE INTO {qualified} t USING {updates_alias} u ON {merge_condition} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )

        snapshots = [
            row.asDict(recursive=True)
            for row in spark.sql(
                f"SELECT committed_at, snapshot_id, operation, summary FROM {qualified}.snapshots"
            ).collect()
        ]
        snapshots.sort(key=lambda s: s["committed_at"])
        return self._build_fg_commit(snapshots)

    def _append_iceberg_dataset(
        self, dataset, location: str, write_options: dict[str, Any]
    ) -> None:
        (
            dataset.write.format(self.ICEBERG_SPARK_FORMAT)
            .options(**write_options)
            .mode(self.APPEND)
            .save(location)
        )

    def _merge_iceberg_dataset(
        self, dataset, location: str, write_options: dict[str, Any]
    ) -> tuple[int, int, int]:
        """Upsert *dataset* into the Iceberg table at *location*.

        Spark SQL `MERGE INTO` requires a catalog-resolved table identifier,
        which path-based tables do not have, so the upsert is expressed as an
        anti-join of the existing data against the incoming keys followed by an
        atomic overwrite of the table with the union.

        Returns:
            Tuple of rows inserted, rows updated, and rows deleted by the merge.
        """
        existing = self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT).load(
            location
        )

        merge_keys = self._get_merge_keys()
        _logger.debug(f"Merging into Iceberg table at {location} on keys {merge_keys}")

        old_total = self._get_total_records(location)
        num_updates = dataset.count()

        remaining = existing.join(
            dataset.select(*merge_keys), on=merge_keys, how="left_anti"
        )
        merged = remaining.select(*existing.columns).unionByName(
            dataset.select(*existing.columns)
        )

        (
            merged.write.format(self.ICEBERG_SPARK_FORMAT)
            .options(**write_options)
            .mode("overwrite")
            .save(location)
        )

        # rows that matched an existing key were replaced rather than added,
        # so the table grew by num_updates minus the matched rows
        rows_matched = old_total + num_updates - self._get_total_records(location)
        rows_updated = max(rows_matched, 0)
        rows_inserted = max(num_updates - rows_updated, 0)
        return rows_inserted, rows_updated, 0

    def _get_merge_keys(self) -> list[str]:
        """Return the columns identifying a row for upserts and deletes.

        Matches the Delta merge condition: primary key plus event time plus
        partition key.
        """
        merge_keys = self._feature_group.primary_key.copy()
        if self._feature_group.event_time is not None:
            merge_keys.append(self._feature_group.event_time)
        if self._feature_group.partition_key:
            merge_keys = merge_keys + self._feature_group.partition_key
        return merge_keys

    def _delete_record(self, delete_df) -> feature_group_commit.FeatureGroupCommit:
        if self._spark_session is None:
            return self._delete_pyiceberg_record(delete_df)
        location = self._feature_group.prepare_spark_location()
        if not self._is_iceberg_table_at(location):
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not ICEBERG enabled "
            )

        existing = self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT).load(
            location
        )
        merge_keys = self._get_merge_keys()
        _logger.debug(
            f"Deleting records from Iceberg table at {location} on keys {merge_keys}"
        )

        old_total = self._get_total_records(location)
        remaining = existing.join(
            delete_df.select(*merge_keys), on=merge_keys, how="left_anti"
        )
        (
            remaining.select(*existing.columns)
            .write.format(self.ICEBERG_SPARK_FORMAT)
            .mode("overwrite")
            .save(location)
        )

        rows_deleted = max(old_total - self._get_total_records(location), 0)
        fg_commit = self._get_last_commit_metadata(
            location,
            rows_inserted=0,
            rows_updated=0,
            rows_deleted=rows_deleted,
        )
        return self._feature_group_api._commit(self._feature_group, fg_commit)

    def _register_temporary_table(
        self,
        iceberg_fg_alias: hudi_feature_group_alias.HudiFeatureGroupAlias,
        read_options: dict[str, Any] | None = None,
    ) -> None:
        location = self._feature_group.prepare_spark_location()
        _logger.debug(
            f"Registering temporary table for Iceberg feature group {self._feature_group.name} v{self._feature_group.version} at location {location}"
        )

        iceberg_options = self._setup_iceberg_read_opts(
            iceberg_fg_alias, location, read_options=read_options
        )
        self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT).options(
            **iceberg_options
        ).load(location).createOrReplaceTempView(iceberg_fg_alias.alias)

    def _setup_iceberg_read_opts(
        self,
        iceberg_fg_alias: hudi_feature_group_alias.HudiFeatureGroupAlias,
        location: str,
        read_options: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        iceberg_options = {}
        if iceberg_fg_alias.left_feature_group_end_timestamp is None and (
            iceberg_fg_alias.left_feature_group_start_timestamp is None
            or iceberg_fg_alias.left_feature_group_start_timestamp == 0
        ):
            # snapshot query latest state
            iceberg_options = {}
        elif (
            iceberg_fg_alias.left_feature_group_end_timestamp is not None
            and iceberg_fg_alias.left_feature_group_start_timestamp is None
        ):
            # snapshot query with end time; Iceberg expects epoch milliseconds
            iceberg_options = {
                self.ICEBERG_QUERY_TIME_TRAVEL_AS_OF_TIMESTAMP: str(
                    iceberg_fg_alias.left_feature_group_end_timestamp
                ),
            }
        elif iceberg_fg_alias.left_feature_group_start_timestamp is not None:
            # incremental query; Iceberg only supports snapshot-id bounds,
            # so the wallclock bounds are resolved against the snapshot log
            start_snapshot_id = self._resolve_snapshot_id_at(
                location, iceberg_fg_alias.left_feature_group_start_timestamp
            )
            if start_snapshot_id is None:
                raise FeatureStoreException(
                    "Cannot run the incremental query: no Iceberg snapshot exists at or before "
                    f"the start time {iceberg_fg_alias.left_feature_group_start_timestamp}."
                )
            iceberg_options = {
                self.ICEBERG_START_SNAPSHOT_ID: str(start_snapshot_id),
            }
            if iceberg_fg_alias.left_feature_group_end_timestamp is not None:
                end_snapshot_id = self._resolve_snapshot_id_at(
                    location, iceberg_fg_alias.left_feature_group_end_timestamp
                )
                if end_snapshot_id is not None:
                    iceberg_options[self.ICEBERG_END_SNAPSHOT_ID] = str(end_snapshot_id)

        if read_options:
            for key in read_options:
                if isinstance(key, str) and key.startswith(self.ICEBERG_DOT_PREFIX):
                    # iceberg read options do not have the "iceberg." prefix
                    iceberg_options[key[len(self.ICEBERG_DOT_PREFIX) :]] = read_options[
                        key
                    ]
                else:
                    iceberg_options[key] = read_options[key]

        _logger.debug(
            f"Iceberg read options for feature group {self._feature_group.name} v{self._feature_group.version}: {iceberg_options}"
        )

        return iceberg_options

    def _read_snapshots(self, location: str) -> list[dict[str, Any]]:
        """Return the table's snapshot log, oldest first.

        Reads the `snapshots` metadata table through the path-based metadata
        table syntax, so it works without a configured catalog.
        """
        snapshots_df = self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT).load(
            location + self.SNAPSHOTS_METADATA_SUFFIX
        )
        snapshots = [row.asDict(recursive=True) for row in snapshots_df.collect()]
        snapshots.sort(key=lambda s: s["committed_at"])
        return snapshots

    def _resolve_snapshot_id_at(self, location: str, timestamp: int) -> int | None:
        """Return the id of the latest snapshot committed at or before *timestamp*."""
        snapshot_id = None
        for snapshot in self._read_snapshots(location):
            committed_at = util._convert_event_time_to_timestamp(
                snapshot["committed_at"]
            )
            if committed_at <= timestamp:
                snapshot_id = snapshot["snapshot_id"]
            else:
                break
        return snapshot_id

    def _get_total_records(self, location: str) -> int:
        """Return the row count of the current snapshot from its summary."""
        snapshots = self._read_snapshots(location)
        if not snapshots:
            return 0
        return int((snapshots[-1].get("summary") or {}).get("total-records", 0))

    def _save_empty_table(self, write_options: dict[str, Any] | None = None) -> None:
        """Create an empty Iceberg table with the schema from the feature group features.

        This makes the feature schema available at the table path so that the
        subsequent writes can refer to it.
        """
        if self._spark_session is not None:
            self._save_empty_iceberg_table_pyspark(write_options=write_options)
        else:
            self._save_empty_iceberg_table_python(write_options=write_options)

    def _save_empty_iceberg_table_pyspark(
        self, write_options: dict[str, Any] | None = None
    ) -> None:
        ddl_fields = []
        for _feature in self._feature_group.columns:
            if _feature.type:
                ddl_fields.append(f"{_feature.name} {_feature.type}")
            else:
                raise FeatureStoreException(
                    f"Feature '{_feature.name}' does not have a type defined. "
                    "Cannot create Iceberg table schema."
                )

        ddl_schema = ", ".join(ddl_fields)
        empty_df = self._spark_session.createDataFrame([], ddl_schema)

        self._write_iceberg_dataset(empty_df, write_options or {})

    def _save_empty_iceberg_table_python(
        self, write_options: dict[str, Any] | None = None
    ) -> None:
        """Create an empty Iceberg table from the feature group schema using PyIceberg.

        Converts feature types directly to PyArrow types without requiring Spark,
        mirroring the delta-rs empty-table creation.
        """
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "PyArrow is required to create empty Iceberg tables."
            ) from e

        pyarrow_fields = []
        for _feature in self._feature_group.columns:
            if not _feature.type:
                raise FeatureStoreException(
                    f"Feature '{_feature.name}' does not have a type defined. "
                    "Cannot create Iceberg table schema."
                )
            try:
                pyarrow_type = _convert_offline_type_to_pyarrow_type(_feature.type)
                pyarrow_fields.append(
                    pa.field(_feature.name, pyarrow_type, nullable=True)
                )
            except Exception as e:
                raise FeatureStoreException(
                    f"Failed to convert type '{_feature.type}' for feature '{_feature.name}': {str(e)}"
                ) from e

        empty_arrow_table = pa.schema(pyarrow_fields).empty_table()

        self._write_pyiceberg_dataset(empty_arrow_table, write_options=write_options)

    def _get_last_commit_metadata(
        self,
        base_path: str,
        rows_inserted: int | None = None,
        rows_updated: int | None = None,
        rows_deleted: int | None = None,
    ) -> feature_group_commit.FeatureGroupCommit | None:
        """Build a FeatureGroupCommit from the latest data-changing Iceberg snapshot.

        The merge and delete paths pass explicit row counts because an Iceberg
        overwrite snapshot summary only exposes file-level added/deleted record
        counts, which over-count for a full-table rewrite.
        """
        _logger.debug(
            f"Retrieving last commit metadata for Iceberg table at {base_path}"
        )
        return self._build_fg_commit(
            self._read_snapshots(base_path),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
        )

    def _build_fg_commit(
        self,
        snapshots: list[dict[str, Any]],
        rows_inserted: int | None = None,
        rows_updated: int | None = None,
        rows_deleted: int | None = None,
    ) -> feature_group_commit.FeatureGroupCommit | None:
        """Build a FeatureGroupCommit from a snapshot log sorted oldest first."""
        snapshots = [s for s in snapshots if s.get("operation") in self.DATA_OPERATIONS]
        if not snapshots:
            return None

        oldest_snapshot = snapshots[0]
        last_snapshot = snapshots[-1]
        summary = last_snapshot.get("summary") or {}

        commit_timestamp = util._convert_event_time_to_timestamp(
            last_snapshot["committed_at"]
        )
        oldest_commit_timestamp = util._convert_event_time_to_timestamp(
            oldest_snapshot["committed_at"]
        )

        if rows_inserted is None:
            # plain appends report exact counts in the snapshot summary
            rows_inserted = int(summary.get("added-records", 0))
        if rows_updated is None:
            rows_updated = 0
        if rows_deleted is None:
            rows_deleted = 0

        _logger.debug(
            f"Commit metrics {commit_timestamp} - inserted: {rows_inserted}, updated: {rows_updated}, deleted: {rows_deleted}"
        )

        return feature_group_commit.FeatureGroupCommit(
            commitid=None,
            commit_date_string=util._get_hudi_datestr_from_timestamp(commit_timestamp),
            commit_time=commit_timestamp,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            last_active_commit_time=oldest_commit_timestamp,
        )

    # region PyIceberg (non-Spark) operations

    def _pyiceberg_write_supported(self) -> bool:
        """Whether PyIceberg can reach this feature group's storage from this client.

        PyIceberg accesses HopsFS through PyArrow's Hadoop filesystem
        (libhdfs), which is only available inside the Hopsworks cluster;
        external Python clients must ingest through the materialization job
        instead.
        """
        if not self._feature_group._is_hopsfs_storage():
            return True
        return not client._get_instance()._is_external()

    def _setup_pyiceberg(self) -> None:
        """Validate that PyIceberg can access the feature group's storage."""
        if not self._pyiceberg_write_supported():
            raise FeatureStoreException(
                "Writing to a HopsFS-backed Iceberg feature group is not supported from external Python clients, "
                "because PyIceberg requires the Hadoop native library (libhdfs) to access HopsFS. "
                "Insert from a Spark application or a job/notebook running inside Hopsworks, "
                "or create the feature group with stream=True to ingest through the materialization job."
            )

    def _get_pyiceberg_location(self) -> str:
        if not self._feature_group._is_hopsfs_storage():
            location = self._feature_group.location
            _logger.debug(f"Non-HopsFS storage, using location as-is: {location}")
            return location

        _client = client._get_instance()
        location = self._feature_group.location.replace(
            "hopsfs:/", "hdfs:/"
        )  # pyiceberg requires hdfs scheme

        if _client._is_external():
            parsed_url = urlparse(location)
            try:
                pyiceberg_loc = f"hdfs://{self._variable_api._get_loadbalancer_external_domain('namenode')}:{parsed_url.port}{parsed_url.path}"
                _logger.debug(
                    f"External client, using namenode url + pyiceberg location: {pyiceberg_loc}"
                )
                return pyiceberg_loc
            except FeatureStoreException as e:
                raise FeatureStoreException(
                    "Failed to write to Iceberg table. Make sure namenode load balancer has been setup on the cluster."
                ) from e
        else:
            _logger.debug(f"Internal client, using pyiceberg location: {location}")
            return location

    def _get_pyiceberg_properties(self) -> dict[str, str]:
        """Build PyIceberg FileIO properties from the feature group's storage connector.

        Returns an empty dict for HopsFS (handled separately via env vars) and for
        feature groups without a connector.
        For S3 and ADLS connectors the relevant credential keys are returned.
        For GCS connectors the service account key is downloaded and exposed
        through `GOOGLE_APPLICATION_CREDENTIALS`, because without explicit
        credentials the GCS filesystem stalls probing the GCP metadata server
        for application-default credentials.
        """
        from hsfs import storage_connector as sc

        connector = self._feature_group.storage_connector
        if connector is None or connector.type == sc.StorageConnector.HOPSFS:
            return {}
        if connector.type == sc.StorageConnector.S3:
            props = {}
            if connector.access_key:
                props["s3.access-key-id"] = connector.access_key
            if connector.secret_key:
                props["s3.secret-access-key"] = connector.secret_key
            if connector.session_token:
                props["s3.session-token"] = connector.session_token
            if connector.region:
                props["s3.region"] = connector.region
            return props
        if connector.type == sc.StorageConnector.ADLS:
            props = {}
            if connector.account_name:
                props["adls.account-name"] = connector.account_name
            if connector.application_id:
                props["adls.client-id"] = connector.application_id
            if connector.service_credential:
                props["adls.client-secret"] = connector.service_credential
            if connector.directory_id:
                props["adls.tenant-id"] = connector.directory_id
            return props
        if connector.type == sc.StorageConnector.GCS:
            if connector.key_path:
                # key_path is a HopsFS path; download it locally
                from hsfs import engine

                local_key_path = engine._get_instance()._add_file(connector.key_path)
                _logger.debug(
                    f"Setting GOOGLE_APPLICATION_CREDENTIALS to {local_key_path}"
                )
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_key_path
            return {}
        return {}

    def _pyiceberg_identifier(self) -> str:
        return f"{self.PYICEBERG_NAMESPACE}.{self._feature_group.name}_{self._feature_group.version}"

    def _make_pyiceberg_catalog(self):
        """Create an ephemeral in-memory catalog for the duration of one operation.

        PyIceberg can only write through a catalog, while Hopsworks Iceberg
        tables are path-based (`HadoopTables`).
        The catalog therefore only hosts the commit mechanics in memory; the
        durable table state is published back to the table location by
        [`_publish_hadoop_metadata`][hsfs.core.iceberg_engine.IcebergEngine._publish_hadoop_metadata].
        """
        return _make_ephemeral_catalog(
            "hopsworks_local", self._get_pyiceberg_properties()
        )

    def _read_table_version(self, io, location: str) -> int | None:
        """Return the table version from `version-hint.text`, or None if absent."""
        input_file = io.new_input(f"{location}/metadata/version-hint.text")
        if not input_file.exists():
            return None
        with input_file.open() as f:
            return int(f.read().decode("utf-8").strip())

    def _load_pyiceberg_table(self, catalog, location: str):
        """Register the table at *location* into *catalog* and return (version, table).

        Returns (None, None) if no table exists at the location yet.
        """
        from pyiceberg.io import load_file_io

        io = load_file_io(catalog.properties, location)
        version = self._read_table_version(io, location)
        if version is None:
            return None, None
        metadata_location = f"{location}/metadata/v{version}.metadata.json"
        table = catalog.register_table(self._pyiceberg_identifier(), metadata_location)
        return version, table

    def _create_pyiceberg_table(self, catalog, location: str, arrow_schema):
        table = catalog.create_table(
            self._pyiceberg_identifier(), schema=arrow_schema, location=location
        )
        if self._feature_group.partition_key:
            with table.update_spec() as update_spec:
                for partition_col in self._feature_group.partition_key:
                    update_spec.add_identity(partition_col)
        return table

    def _publish_hadoop_metadata(self, table, location: str, next_version: int) -> None:
        """Publish the table's current metadata in the `HadoopTables` protocol.

        Copies the catalog-committed metadata file to `metadata/v<N>.metadata.json`
        and points `version-hint.text` at it, so Spark path-based readers and
        writers observe the commit.
        Creating the versioned file without overwrite doubles as conflict
        detection against concurrent writers.
        """
        io = table.io
        with io.new_input(table.metadata_location).open() as f:
            metadata_bytes = f.read()
        versioned_file = io.new_output(
            f"{location}/metadata/v{next_version}.metadata.json"
        )
        with versioned_file.create(overwrite=False) as f:
            f.write(metadata_bytes)
        version_hint = io.new_output(f"{location}/metadata/version-hint.text")
        with version_hint.create(overwrite=True) as f:
            f.write(str(next_version).encode("utf-8"))
        _logger.debug(
            f"Published Iceberg metadata v{next_version} for table at {location}"
        )

    def _prepare_arrow_table(
        self, dataset: pd.DataFrame | pa.Table | pl.DataFrame
    ) -> pa.Table:
        """Normalize *dataset* into a PyArrow Table for PyIceberg writes."""
        import pyarrow as pa

        if HAS_POLARS:
            import polars as pl

            if isinstance(dataset, pl.DataFrame):
                _logger.debug("Converting DataFrame to Arrow Table for Iceberg write")
                dataset = dataset.to_arrow()

        # The Delta normalization (microsecond timestamps, float16 to float32,
        # date64 to date32) is generic Arrow sanitation that applies to Iceberg
        # V2 tables equally.
        from hsfs.core.delta_engine import DeltaEngine

        dataset = DeltaEngine._prepare_df_for_delta(dataset)
        if not isinstance(dataset, pa.Table):
            raise FeatureStoreException(
                f"Cannot write dataset of type {type(dataset)} to an Iceberg table without Spark. "
                "Provide a pandas, polars, or PyArrow dataset."
            )
        return dataset

    def _pyiceberg_snapshots(self, table) -> list[dict[str, Any]]:
        """Return the table's snapshot log, oldest first, as plain dicts."""
        snapshots = []
        for snapshot in table.metadata.snapshots:
            summary = snapshot.summary
            operation = None
            properties = {}
            if summary is not None:
                operation = getattr(summary.operation, "value", None) or str(
                    summary.operation
                )
                properties = dict(getattr(summary, "additional_properties", {}) or {})
            snapshots.append(
                {
                    "committed_at": snapshot.timestamp_ms,
                    "snapshot_id": snapshot.snapshot_id,
                    "operation": operation,
                    "summary": properties,
                }
            )
        snapshots.sort(key=lambda s: s["committed_at"])
        return snapshots

    @_uses_pyiceberg
    def _write_pyiceberg_dataset(
        self,
        dataset: pd.DataFrame | pa.Table | pl.DataFrame,
        write_options: dict[str, Any] | None = None,
        operation: str = "upsert",
    ) -> feature_group_commit.FeatureGroupCommit | None:
        """Write *dataset* to the Iceberg table using PyIceberg.

        Parameters:
            dataset: Dataset to write to the Iceberg table.
        """
        arrow_table = self._prepare_arrow_table(dataset)

        catalog_name, catalog_properties, identifier = self._get_catalog_write_config(
            write_options
        )
        if catalog_name:
            return self._write_pyiceberg_dataset_catalog(
                arrow_table,
                catalog_name,
                catalog_properties,
                identifier,
                operation,
                write_options,
            )

        self._setup_pyiceberg()
        location = self._get_pyiceberg_location()

        catalog = self._make_pyiceberg_catalog()
        version, table = self._load_pyiceberg_table(catalog, location)

        append_requested = self._append_requested(operation, write_options)

        rows_inserted = rows_updated = rows_deleted = None
        if table is None:
            _logger.debug(
                f"Iceberg table not found at {location}. A new Iceberg table will be created."
            )
            table = self._create_pyiceberg_table(catalog, location, arrow_table.schema)
            table.append(arrow_table)
        elif append_requested:
            _logger.debug(f"Append requested for {location}. Skipping merge operation.")
            table.append(arrow_table)
        else:
            upsert_result = table.upsert(arrow_table, join_cols=self._get_merge_keys())
            rows_inserted = upsert_result.rows_inserted
            rows_updated = upsert_result.rows_updated
            rows_deleted = 0

        self._publish_hadoop_metadata(table, location, (version or 0) + 1)
        return self._build_fg_commit(
            self._pyiceberg_snapshots(table),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
        )

    def _write_pyiceberg_dataset_catalog(
        self,
        arrow_table: pa.Table,
        catalog_name: str,
        catalog_properties: dict[str, str],
        identifier: str,
        operation: str,
        write_options: dict[str, Any] | None,
    ) -> feature_group_commit.FeatureGroupCommit | None:
        """Write through a user-provided PyIceberg catalog instead of the table path.

        The catalog is loaded with PyIceberg's standard resolution: the
        `iceberg.catalog.<prop>` write options are passed as catalog
        properties and merged with any `.pyiceberg.yaml` configuration for the
        same catalog name.
        """
        import contextlib

        from pyiceberg.catalog import Catalog, load_catalog
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        catalog = load_catalog(catalog_name, **catalog_properties)
        _logger.debug(
            f"Writing Iceberg dataset through catalog {catalog_name} table {identifier}"
        )

        rows_inserted = rows_updated = rows_deleted = None
        if not catalog.table_exists(identifier):
            with contextlib.suppress(NamespaceAlreadyExistsError):
                catalog.create_namespace(Catalog.namespace_from(identifier))
            table = catalog.create_table(identifier, schema=arrow_table.schema)
            table.append(arrow_table)
        elif self._append_requested(operation, write_options):
            table = catalog.load_table(identifier)
            table.append(arrow_table)
        else:
            table = catalog.load_table(identifier)
            upsert_result = table.upsert(arrow_table, join_cols=self._get_merge_keys())
            rows_inserted = upsert_result.rows_inserted
            rows_updated = upsert_result.rows_updated
            rows_deleted = 0

        return self._build_fg_commit(
            self._pyiceberg_snapshots(table),
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
        )

    @_uses_pyiceberg
    def _delete_pyiceberg_record(
        self, delete_df
    ) -> feature_group_commit.FeatureGroupCommit:
        self._setup_pyiceberg()
        location = self._get_pyiceberg_location()

        catalog = self._make_pyiceberg_catalog()
        version, table = self._load_pyiceberg_table(catalog, location)
        if table is None:
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not ICEBERG enabled "
            )

        merge_keys = self._get_merge_keys()
        deletes = self._prepare_arrow_table(delete_df).select(merge_keys)
        existing = table.scan().to_arrow()
        remaining = existing.join(deletes, keys=merge_keys, join_type="left anti")

        table.overwrite(remaining.select(existing.column_names))
        self._publish_hadoop_metadata(table, location, (version or 0) + 1)

        fg_commit = self._build_fg_commit(
            self._pyiceberg_snapshots(table),
            rows_inserted=0,
            rows_updated=0,
            rows_deleted=max(existing.num_rows - remaining.num_rows, 0),
        )
        return self._feature_group_api._commit(self._feature_group, fg_commit)

    # endregion
