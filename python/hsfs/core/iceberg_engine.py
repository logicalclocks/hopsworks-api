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

import contextlib
import logging
import os
import re
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import project_api
from hopsworks_common.core.constants import HAS_POLARS
from hopsworks_common.core.type_systems import _convert_offline_type_to_pyarrow_type
from hopsworks_common.decorators import _uses_pyiceberg
from hsfs import feature_group, feature_group_commit, util
from hsfs.core import (
    feature_group_api,
    glue_catalog,
    partition_transforms,
    variable_api,
)


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
    ICEBERG_SPARK_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
    ICEBERG_QUERY_TIME_TRAVEL_AS_OF_TIMESTAMP = "as-of-timestamp"
    ICEBERG_START_SNAPSHOT_ID = "start-snapshot-id"
    ICEBERG_END_SNAPSHOT_ID = "end-snapshot-id"
    ICEBERG_DOT_PREFIX = "iceberg."
    # Iceberg's vectorized (Arrow) reader can crash the executor JVM with a
    # SIGSEGV in the shaded Arrow bounds check (FSTORE-2067), so tables are
    # created with Parquet vectorization disabled and every reader falls back
    # to the row-based path.
    # Remove together with the FSTORE-2067 fix.
    ICEBERG_TABLE_PROPERTIES = {"read.parquet.vectorization.enabled": "false"}
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
    ) -> feature_group_commit.FeatureGroupCommit | None:
        partition_transforms._require_writable(self._feature_group)
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

    def _partition_spec_transforms(
        self,
    ) -> list[partition_transforms.PartitionTransform]:
        """Return the transforms making up the table's partition spec.

        `partitioned_by` transforms when set; otherwise the `partition_key`
        columns as identity transforms. Unparseable stored specs (feature
        groups created with the removed grain grammar) yield no transforms.
        """
        transforms = partition_transforms._try_parse(self._feature_group.partitioned_by)
        if transforms is not None:
            return transforms
        return [
            partition_transforms.PartitionTransform(partition_transforms.IDENTITY, col)
            for col in self._feature_group.partition_key or []
        ]

    def _create_iceberg_table(self, dataset, location: str) -> None:
        """Create an empty Iceberg table at *location* with the dataset's schema.

        Path-based (HadoopTables) tables cannot be created through the
        DataFrame writer, so the table is created through the JVM Iceberg API,
        which requires classic Spark.
        The feature group's `partitioned_by` transforms (or `partition_key`
        identities) become the table's partition spec.
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
        transforms = self._partition_spec_transforms()
        for t in transforms:
            # Every builder method has a targetName overload for the
            # partition-field alias.
            args = (t.source, t.alias) if t.alias else (t.source,)
            if t.name == partition_transforms.IDENTITY:
                spec_builder = spec_builder.identity(*args)
            elif t.name == partition_transforms.BUCKET:
                spec_builder = (
                    spec_builder.bucket(t.source, t.param, t.alias)
                    if t.alias
                    else spec_builder.bucket(t.source, t.param)
                )
            elif t.name == partition_transforms.TRUNCATE:
                spec_builder = (
                    spec_builder.truncate(t.source, t.param, t.alias)
                    if t.alias
                    else spec_builder.truncate(t.source, t.param)
                )
            elif t.name == partition_transforms.VOID:
                spec_builder = spec_builder.alwaysNull(*args)
            elif t.name == "year":
                spec_builder = spec_builder.year(*args)
            elif t.name == "month":
                spec_builder = spec_builder.month(*args)
            elif t.name == "day":
                spec_builder = spec_builder.day(*args)
            elif t.name == "hour":
                spec_builder = spec_builder.hour(*args)
        sort_fields = partition_transforms._try_parse_sort_order(
            self._feature_group.sort_order
        )
        properties = jvm.java.util.HashMap()
        for prop, value in self.ICEBERG_TABLE_PROPERTIES.items():
            properties.put(prop, value)
        if sort_fields:
            # Range distribution organizes new writes by the sort order
            # without waiting for a rewrite.
            properties.put("write.distribution-mode", "range")
        elif transforms:
            # Shuffle rows to their partitions before the write; without a
            # distribution mode, unsorted appends to a multi-transform spec
            # fail with the clustered-write assertion.
            properties.put("write.distribution-mode", "hash")
        table = jvm.org.apache.iceberg.hadoop.HadoopTables(
            self._spark_context._jsc.hadoopConfiguration()
        ).create(schema, spec_builder.build(), properties, location)
        if sort_fields:
            null_order = jvm.org.apache.iceberg.NullOrder
            expressions = jvm.org.apache.iceberg.expressions.Expressions
            replace = table.replaceSortOrder()
            for f in sort_fields:
                nulls = (
                    null_order.NULLS_FIRST
                    if f.null_order == "first"
                    else null_order.NULLS_LAST
                )
                # Only the (Term, NullOrder) overloads are non-default
                # interface methods; the (String, NullOrder) variants are
                # missing from the deployed Iceberg version.
                term = expressions.ref(f.column)
                if f.direction == "asc":
                    replace = replace.asc(term, nulls)
                else:
                    replace = replace.desc(term, nulls)
            replace.commit()

    # Spark DDL spells the temporal transforms in plural.
    _SPARK_DDL_TRANSFORMS = {
        "year": "years",
        "month": "months",
        "day": "days",
        "hour": "hours",
    }

    def _create_iceberg_table_catalog(self, dataset, qualified: str) -> None:
        """Create the catalog table with the feature group's partition spec via DDL.

        Spark's `CREATE TABLE ... PARTITIONED BY` DDL grammar cannot express
        `void` transforms, partition-field aliases, or a sort order; those
        are rejected rather than silently dropped, so an accepted API
        argument never disappears depending on the creation path.
        """
        for t in self._partition_spec_transforms():
            if t.name == partition_transforms.VOID or t.alias:
                raise FeatureStoreException(
                    f"partitioned_by element '{t}' cannot be expressed in the "
                    "catalog CREATE TABLE DDL (void transforms and partition "
                    "field aliases are unsupported there). Create the feature "
                    "group without a catalog data source, or drop the "
                    "unsupported element."
                )
        if self._feature_group.sort_order:
            raise FeatureStoreException(
                "sort_order cannot be applied on the catalog creation path "
                "yet. Create the feature group without a catalog data "
                "source."
            )
        schema_ddl = ", ".join(
            f"{field.name} {field.dataType.simpleString()}"
            for field in dataset.schema.fields
        )
        clauses = []
        for t in self._partition_spec_transforms():
            if t.name == partition_transforms.IDENTITY:
                clauses.append(t.source)
            elif t.param is not None:
                clauses.append(f"{t.name}({t.param}, {t.source})")
            else:
                ddl_name = self._SPARK_DDL_TRANSFORMS.get(t.name, t.name)
                clauses.append(f"{ddl_name}({t.source})")
        statement = (
            f"CREATE TABLE {qualified} ({schema_ddl}) USING {self.ICEBERG_SPARK_FORMAT}"
        )
        properties = dict(self.ICEBERG_TABLE_PROPERTIES)
        if clauses:
            statement += f" PARTITIONED BY ({', '.join(clauses)})"
            properties["write.distribution-mode"] = "hash"
        props_ddl = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
        statement += f" TBLPROPERTIES ({props_ddl})"
        _logger.debug(f"Creating Iceberg catalog table: {statement}")
        self._spark_session.sql(statement)

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

    def _glue_catalog(self) -> glue_catalog.GlueCatalog | None:
        """Return the feature group's Glue catalog binding, or None if it is not Glue-backed."""
        return glue_catalog.GlueCatalog._for_feature_group(self._feature_group)

    def _require_absolute_location(self) -> str:
        """Return the feature group's location, requiring it to be an absolute URI.

        A Glue table is created at the feature group's own location; the
        location must carry a scheme (e.g. `s3://`) so the data lands in the
        bucket rather than at a relative local path.

        The location is generated by the backend (from the Glue database
        location or an explicit data source path), so it is normally already
        absolute; this check is a safety net that fails loudly rather than
        silently creating the table at a relative local path.

        Returns:
            The feature group's location.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the location is missing or not absolute.
        """
        location = self._feature_group.location
        if not location or "://" not in location:
            raise FeatureStoreException(
                f"Glue feature group {self._feature_group.name} has a non-absolute "
                f"location {location!r}; expected an absolute URI such as "
                "'s3://bucket/path'. Set the data source path or check the "
                "storage connector configuration."
            )
        return location

    def _prepare_glue_session(self, glue: glue_catalog.GlueCatalog) -> str:
        """Configure the session for the Glue catalog and return the qualified table name.

        Shared prologue for the catalog-mediated read, delete and schema
        evolution paths: it sets the Glue credentials and
        `spark.sql.catalog.<name>.*` on the session and returns the
        `<catalog>.<database>.<table>` name those operations address.

        Parameters:
            glue: The Glue catalog binding for this feature group.

        Returns:
            The catalog-qualified table name.
        """
        glue._configure_spark_session(
            self._spark_session, self._spark_context, self.ICEBERG_SPARK_CATALOG_IMPL
        )
        return glue.qualified_name

    def _glue_catalog_write_options(
        self, write_options: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Augment write options to commit through the Glue Data Catalog.

        Returns the options unchanged when the feature group has no Glue
        connector or when the caller already selected a catalog explicitly with
        `iceberg.catalog`.
        Otherwise the Glue catalog name, properties and table identifier are
        derived from the connector and data source, so that a plain
        `fg.insert()` registers the table in the Glue Data Catalog while the
        data stays on S3.
        """
        write_options = dict(write_options or {})
        if self.ICEBERG_CATALOG_OPTION in write_options:
            return write_options

        glue = self._glue_catalog()
        if glue is None:
            return write_options

        return glue._iceberg_write_options(
            write_options,
            catalog_option=self.ICEBERG_CATALOG_OPTION,
            identifier_option=self.ICEBERG_CATALOG_TABLE_IDENTIFIER_OPTION,
            property_prefix=self.ICEBERG_CATALOG_PROP_PREFIX,
        )

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

        write_options = self._glue_catalog_write_options(write_options)

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

    def _configure_spark_catalog(
        self, catalog_name: str, catalog_properties: dict[str, str]
    ) -> None:
        """Configure `spark.sql.catalog.<name>.*` on the session from catalog properties.

        Sets the catalog implementation (from the `impl` property, falling back
        to `SparkCatalog` when not already configured) and forwards the
        remaining properties.
        Used by both the catalog write path and the catalog read path so they
        configure the session identically.
        """
        spark = self._spark_session
        properties = dict(catalog_properties)
        base_key = f"spark.sql.catalog.{catalog_name}"
        impl = properties.pop("impl", None)
        if impl is not None:
            spark.conf.set(base_key, impl)
        elif spark.conf.get(base_key, None) is None:
            spark.conf.set(base_key, self.ICEBERG_SPARK_CATALOG_IMPL)
        for prop, value in properties.items():
            spark.conf.set(f"{base_key}.{prop}", value)

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
        glue = self._glue_catalog()
        if glue is not None:
            # The Glue metadata client authenticates via the JVM SDK chain.
            glue._set_jvm_credentials(self._spark_context)
        self._configure_spark_catalog(catalog_name, catalog_properties)

        qualified = f"{catalog_name}.{identifier}"
        _logger.debug(f"Writing Iceberg dataset through catalog table {qualified}")
        if not spark.catalog.tableExists(qualified):
            self._create_iceberg_table_catalog(dataset, qualified)
            dataset.writeTo(qualified).append()
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
        partition_transforms._require_writable(self._feature_group)
        if self._spark_session is None:
            return self._delete_pyiceberg_record(delete_df)

        glue = self._glue_catalog()
        if glue is not None:
            return self._delete_record_via_catalog(glue, delete_df)

        location = self._feature_group.prepare_spark_location()
        if not self._is_iceberg_table_at(location):
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not ICEBERG enabled."
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

    def _delete_record_via_catalog(
        self, glue: glue_catalog.GlueCatalog, delete_df
    ) -> feature_group_commit.FeatureGroupCommit:
        """Delete records from a Glue Data Catalog table through a SQL `MERGE INTO`.

        Catalog-resolved tables support `MERGE INTO ... WHEN MATCHED THEN
        DELETE`, which the path-based delete cannot use; this requires the
        Iceberg Spark SQL extensions on the session.
        """
        spark = self._spark_session
        qualified = self._prepare_glue_session(glue)
        if not spark.catalog.tableExists(qualified):
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not ICEBERG enabled."
            )

        merge_keys = self._get_merge_keys()
        _logger.debug(
            f"Deleting records from Glue catalog table {qualified} on keys {merge_keys}"
        )

        old_total = self._get_total_records(qualified)
        deletes_alias = (
            f"{self._feature_group.name}_{self._feature_group.version}_deletes"
        )
        delete_df.select(*merge_keys).createOrReplaceTempView(deletes_alias)
        merge_condition = " AND ".join(f"t.{key} = u.{key}" for key in merge_keys)
        spark.sql(
            f"MERGE INTO {qualified} t USING {deletes_alias} u ON {merge_condition} "
            "WHEN MATCHED THEN DELETE"
        )

        rows_deleted = max(old_total - self._get_total_records(qualified), 0)
        fg_commit = self._get_last_commit_metadata(
            qualified,
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
        glue = self._glue_catalog()
        if glue is not None:
            self._register_temporary_table_via_catalog(
                glue, iceberg_fg_alias, read_options=read_options
            )
            return

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

    def _register_temporary_table_via_catalog(
        self,
        glue: glue_catalog.GlueCatalog,
        iceberg_fg_alias: hudi_feature_group_alias.HudiFeatureGroupAlias,
        read_options: dict[str, Any] | None = None,
    ) -> None:
        """Register the feature group's Glue Data Catalog table as a temporary view.

        Configures the Glue catalog on the session and reads through the
        catalog identifier (`<catalog>.<database>.<table>`) rather than the S3
        path, because the table's current-metadata pointer lives in Glue, not
        in the table location's `version-hint.text`.
        """
        qualified = self._prepare_glue_session(glue)
        _logger.debug(
            f"Registering Glue feature group {self._feature_group.name} "
            f"v{self._feature_group.version} from catalog table {qualified}"
        )

        # Time-travel and other read options apply the same way as for a
        # path-based read; only the addressing differs: a catalog-resolved
        # table is read with `.table()`, not `.load()`.
        iceberg_options = self._setup_iceberg_read_opts(
            iceberg_fg_alias, qualified, read_options=read_options
        )
        self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT).options(
            **iceberg_options
        ).table(qualified).createOrReplaceTempView(iceberg_fg_alias.alias)

    def _add_columns_via_catalog(self, glue: glue_catalog.GlueCatalog) -> None:
        """Add the feature group's missing columns to its Glue Data Catalog table.

        Evolves the schema through the catalog (`ALTER TABLE ... ADD COLUMNS`)
        as a metadata-only commit rather than the path-based `HadoopTables`
        API, because the table's current-metadata pointer lives in Glue.
        Only columns missing from the table are added; existing columns are
        never altered.
        Requires the Iceberg Spark SQL extensions on the session.
        """
        spark = self._spark_session
        qualified = self._prepare_glue_session(glue)
        existing_columns = {field.name for field in spark.table(qualified).schema}
        new_features = [
            _feature
            for _feature in self._feature_group.columns
            if _feature.name not in existing_columns
        ]
        if not new_features:
            return

        column_ddl = ", ".join(
            f"{_feature.name} {self._iceberg_column_type(_feature.type)}"
            for _feature in new_features
        )
        _logger.debug(f"Adding columns to Glue catalog table {qualified}: {column_ddl}")
        spark.sql(f"ALTER TABLE {qualified} ADD COLUMNS ({column_ddl})")

    @staticmethod
    def _iceberg_column_type(feature_type: str) -> str:
        """Map a Hopsworks feature type to the type used for Iceberg schema evolution.

        Spark's `timestamp` converts to Iceberg `timestamptz`, while Hopsworks
        stores Hive-style timestamps without a zone, so timestamps are mapped
        through `timestamp_ntz` instead.
        The lookahead keeps struct field names (which precede ':') untouched.
        """
        return re.sub(r"(?<![\w])timestamp(?![\w:])", "timestamp_ntz", feature_type)

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

    @staticmethod
    def _is_catalog_identifier(address: str) -> bool:
        """Whether *address* is a catalog identifier rather than a filesystem path.

        Catalog identifiers are dotted names (`catalog.db.table`); paths carry a
        scheme such as `s3a://` or `hopsfs://`.
        """
        return "://" not in address

    def _read_snapshots(self, location: str) -> list[dict[str, Any]]:
        """Return the table's snapshot log, oldest first.

        Reads the `snapshots` metadata table, addressing it through the catalog
        (`<table>.snapshots`) for catalog identifiers and the path-based
        metadata-table syntax (`<location>#snapshots`) for filesystem paths.
        """
        if self._is_catalog_identifier(location):
            snapshots_df = self._spark_session.table(f"{location}.snapshots")
        else:
            snapshots_df = self._spark_session.read.format(
                self.ICEBERG_SPARK_FORMAT
            ).load(location + self.SNAPSHOTS_METADATA_SUFFIX)
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

    def _optimize(
        self,
        strategy: str | None = None,
        columns: list[str] | None = None,
        rewrite_all: bool | None = None,
        target_file_size_mb: int | None = None,
        where: str | None = None,
    ) -> dict[str, Any]:
        """Rewrite the table's data files with the requested strategy.

        Runs the Iceberg `rewriteDataFiles` Spark action through the JVM.
        Without an explicit strategy, the feature group's stored layout
        decides: z-order over `zorder_by`, else the persistent `sort_order`,
        else bin-packing compaction. `rewrite_all` defaults to False for
        every strategy so a routine maintenance call never rewrites the whole
        table by accident; pass `rewrite_all=True` for the initial full
        z-order (the planner otherwise skips file groups below its
        thresholds). `where` restricts the rewrite to the matching files
        through the Iceberg `RewriteDataFiles.filter` expression.
        """
        if self._spark_context is None:
            raise FeatureStoreException(
                "optimize() on ICEBERG feature groups requires a classic "
                "Spark session (the rewrite is a JVM Spark action)."
            )
        if self._glue_catalog() is not None:
            raise FeatureStoreException(
                "optimize() is not supported on Glue-backed Iceberg feature "
                "groups yet; run the rewrite_data_files procedure through "
                "your Glue catalog instead."
            )
        if strategy is None:
            if columns or self._feature_group.zorder_by:
                strategy = "zorder"
            elif self._feature_group.sort_order:
                strategy = "sort"
            else:
                strategy = "binpack"
        location = self._feature_group.prepare_spark_location()
        jvm = self._spark_context._jvm
        table = self._load_jvm_table()
        action = jvm.org.apache.iceberg.spark.actions.SparkActions.get(
            self._spark_session._jsparkSession
        ).rewriteDataFiles(table)
        if strategy == "zorder":
            zorder_columns = columns or self._feature_group.zorder_by
            if not zorder_columns:
                raise FeatureStoreException(
                    "optimize(strategy='zorder') needs columns: pass "
                    "columns=[...] or create the feature group with "
                    "zorder_by."
                )
            # Canonicalize to the sanitized feature names; a one-off
            # columns=["Merchant_ID"] must resolve to the stored column.
            zorder_columns = [util._autofix_feature_name(c) for c in zorder_columns]
            cols = self._spark_context._gateway.new_array(
                jvm.java.lang.String, len(zorder_columns)
            )
            for i, col in enumerate(zorder_columns):
                cols[i] = col
            action = action.zOrder(cols)
            _logger.debug(f"Rewriting {location} with z-order on {zorder_columns}")
        elif strategy == "sort":
            action = action.sort()
            _logger.debug(f"Rewriting {location} to the table sort order")
        else:
            _logger.debug(f"Rewriting {location} with bin-packing compaction")
        if where:
            action = action.filter(self._iceberg_filter(jvm, location, where))
            _logger.debug(f"Restricting rewrite of {location} to: {where}")
        if rewrite_all:
            action = action.option("rewrite-all", "true")
        if target_file_size_mb is not None:
            action = action.option(
                "target-file-size-bytes", str(target_file_size_mb * 1024 * 1024)
            )
        result = action.execute()
        metrics = {"strategy": strategy}
        for key, getter in (
            ("rewritten_data_files", "rewrittenDataFilesCount"),
            ("added_data_files", "addedDataFilesCount"),
            ("rewritten_bytes", "rewrittenBytesCount"),
        ):
            # noqa comment intentionally broad: the metric getters vary by
            # Iceberg version.
            with contextlib.suppress(Exception):
                metrics[key] = getattr(result, getter)()
        return metrics

    def _iceberg_filter(self, jvm, location: str, where: str):
        """Convert a SQL predicate to an Iceberg expression for a filtered rewrite.

        `RewriteDataFiles.filter` takes an Iceberg `Expression`, so the SQL
        `where` is resolved against the table's schema through Spark and
        converted with Iceberg's `SparkExpressionConverter` (the same path the
        `rewrite_data_files` stored procedure uses). In Iceberg 1.7 the object
        lives at `org.apache.spark.sql.execution.datasources` and the method is
        `convertToIcebergExpression`. The table is registered as a uniquely
        named temporary view so the predicate resolves for path-based tables
        (which have no catalog-resolvable name) without colliding with a
        caller's views or a concurrent optimize in the same session.
        """
        converter = (
            jvm.org.apache.spark.sql.execution.datasources.SparkExpressionConverter
        )
        view = f"_hopsworks_rewrite_{uuid.uuid4().hex}"
        (
            self._spark_session.read.format(self.ICEBERG_SPARK_FORMAT)
            .load(location)
            .createOrReplaceTempView(view)
        )
        try:
            resolved = converter.collectResolvedSparkExpression(
                self._spark_session._jsparkSession, view, where
            )
            return converter.convertToIcebergExpression(resolved)
        except Exception as e:  # noqa: BLE001 - surface an actionable error
            raise FeatureStoreException(
                f"Could not convert the optimize(where=...) predicate {where!r} "
                "to an Iceberg filter. Use a predicate over the feature "
                f"group's columns, e.g. \"event_ts >= '2026-01-01'\". ({e})"
            ) from e
        finally:
            self._spark_session.catalog.dropTempView(view)

    def _load_jvm_table(self):
        """Load the JVM Iceberg table for metadata operations.

        Glue-backed feature groups load through the Glue Data Catalog (the
        connector is a durable catalog binding); other feature groups load
        path-based. A failing path-based load usually means the table is
        owned by an ad-hoc user catalog (the `iceberg.catalog` write
        option), which has no durable binding to load through.
        """
        jvm = self._spark_context._jvm
        glue = self._glue_catalog()
        if glue is not None:
            qualified = self._prepare_glue_session(glue)
            return jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
                self._spark_session._jsparkSession, qualified
            )
        location = self._feature_group.prepare_spark_location()
        try:
            return jvm.org.apache.iceberg.hadoop.HadoopTables(
                self._spark_context._jsc.hadoopConfiguration()
            ).load(location)
        except Exception as e:
            raise FeatureStoreException(
                f"Cannot load the Iceberg table at {location}: {e} "
                "If the table was created through a user-provided catalog "
                "(the iceberg.catalog write option), its current-metadata "
                "pointer lives in that catalog; run the operation through "
                "the catalog instead."
            ) from e

    def _describe_layout(self) -> dict[str, Any]:
        """Read the table's actual partition specs and sort order.

        Uses the JVM table under Spark and PyIceberg otherwise, so drift
        against the stored Hopsworks metadata (external evolution, layouts
        loaded through createIfNotExists) is visible.
        `partition_spec` is the table's current default spec; the history is
        ordered by spec id and may include non-default specs, so its last
        element is not necessarily the current one. Transformed sort fields
        (possible through external engines) render as `transform(source)`.
        Glue-backed feature groups load through the Glue Data Catalog (the
        durable catalog binding on the storage connector); a table owned by
        an ad-hoc user catalog (the `iceberg.catalog` write option) has no
        durable binding and fails with a pointer to that catalog.
        """
        if self._spark_context is not None:
            table = self._load_jvm_table()
            schema = table.schema()

            def spec_fields(spec):
                return [
                    f"{field.name()}: "
                    f"{field.transform().toString()}"
                    f"({schema.findColumnName(field.sourceId())})"
                    for field in spec.fields()
                ]

            def sort_term(field):
                # An externally defined sort may use a transform; rendering
                # only the source column would misreport it as an identity
                # sort.
                transform = field.transform().toString()
                source = schema.findColumnName(field.sourceId())
                return source if transform == "identity" else f"{transform}({source})"

            specs = sorted(
                ((spec_id, table.specs().get(spec_id)) for spec_id in table.specs()),
                key=lambda pair: pair[0],
            )
            # table.spec() is the current default; the highest spec id is not
            # necessarily current (external engines can add non-default specs).
            current = spec_fields(table.spec())
            history = [spec_fields(spec) for _, spec in specs]
            sort_fields = [
                f"{sort_term(f)} "
                f"{'asc' if f.direction().toString().lower().startswith('asc') else 'desc'} "
                f"nulls {'first' if 'FIRST' in f.nullOrder().toString().upper() else 'last'}"
                for f in table.sortOrder().fields()
            ]
        else:
            glue = self._glue_catalog()
            if glue is not None:
                from pyiceberg.catalog import load_catalog

                catalog = load_catalog(
                    glue.catalog_name, **glue._pyiceberg_catalog_properties()
                )
                table = catalog.load_table(glue.identifier)
            else:
                self._setup_pyiceberg()
                catalog = self._make_pyiceberg_catalog()
                _, table = self._load_pyiceberg_table(
                    catalog, self._get_pyiceberg_location()
                )
            if table is None:
                raise FeatureStoreException(
                    f"Feature group {self._feature_group.name} has no "
                    "readable path-based Iceberg metadata "
                    "(version-hint.text). Either no data was written yet, or "
                    "the table is owned by a user-provided catalog (the "
                    "iceberg.catalog write option), whose current-metadata "
                    "pointer lives in that catalog; inspect the layout "
                    "through the catalog instead."
                )
            schema = table.schema()

            def spec_fields(spec):
                return [
                    f"{field.name}: {field.transform}"
                    f"({schema.find_column_name(field.source_id)})"
                    for field in spec.fields
                ]

            def sort_term(field):
                transform = str(field.transform)
                source = schema.find_column_name(field.source_id)
                return source if transform == "identity" else f"{transform}({source})"

            current = spec_fields(table.spec())
            history = [
                spec_fields(spec)
                for spec in sorted(table.specs().values(), key=lambda s: s.spec_id)
            ]
            sort_fields = [
                f"{sort_term(f)} "
                f"{'asc' if 'asc' in str(f.direction).lower() else 'desc'} "
                f"nulls {'first' if 'first' in str(f.null_order).lower() else 'last'}"
                for f in table.sort_order().fields
            ]
        return {
            "partition_spec": current or None,
            "partition_specs": history or None,
            "sort_order": sort_fields or None,
        }

    def _update_partition_spec(
        self,
        add_transforms: list[partition_transforms.PartitionTransform],
        remove_transforms: list[partition_transforms.PartitionTransform],
    ) -> list[str]:
        """Evolve the table's partition spec through the Iceberg Java API.

        Partition evolution is metadata-only: existing data keeps its layout
        and new writes use the evolved spec. With nothing to add or remove,
        no commit happens and the current spec is only read back. Returns
        the committed current spec serialized in the transform grammar, so
        the caller persists the actual table state rather than a
        reconstruction from possibly stale stored metadata.
        """
        if self._spark_context is None:
            raise FeatureStoreException(
                "update_partition_spec() requires a classic Spark session "
                "(the evolution commits through the Iceberg Java API)."
            )
        if self._glue_catalog() is not None:
            raise FeatureStoreException(
                "update_partition_spec() is not supported on Glue-backed "
                "Iceberg feature groups yet; evolve the spec through your "
                "Glue catalog instead."
            )
        for t in add_transforms:
            if t.name == partition_transforms.VOID:
                raise FeatureStoreException(
                    "void transforms cannot be added through "
                    "update_partition_spec(); removing a field already "
                    "voids it in the evolved spec."
                )
        location = self._feature_group.prepare_spark_location()
        jvm = self._spark_context._jvm
        table = self._load_jvm_table()
        if add_transforms or remove_transforms:
            update = table.updateSpec()
            for t in remove_transforms:
                update = update.removeField(self._iceberg_term(jvm, t))
            for t in add_transforms:
                term = self._iceberg_term(jvm, t)
                # The two-argument overload names the partition field.
                update = (
                    update.addField(t.alias, term) if t.alias else update.addField(term)
                )
            _logger.debug(
                f"Evolving partition spec at {location}: "
                f"add={[str(t) for t in add_transforms]} "
                f"remove={[str(t) for t in remove_transforms]}"
            )
            update.commit()
            table.refresh()
        return self._spec_expressions(table)

    @staticmethod
    def _spec_expressions(table) -> list[str]:
        """Serialize the table's current partition spec in the transform grammar.

        Inverse of the creation mapping: emits an alias suffix whenever the
        field name differs from Iceberg's generated default, so aliases and
        externally evolved fields round-trip into the stored metadata. V1
        tables keep removed fields as void transforms; those serialize as
        `void(col) as <original_name>` and parse back cleanly.
        """
        schema = table.schema()
        expressions = []
        for field in table.spec().fields():
            transform = field.transform().toString()
            source = schema.findColumnName(field.sourceId())
            match = re.fullmatch(r"(\w+)(?:\[(\d+)\])?", transform)
            if match is None:
                raise FeatureStoreException(
                    f"Partition transform {transform!r} on field "
                    f"{field.name()!r} cannot be expressed in the "
                    "partitioned_by grammar."
                )
            name, param = match.group(1), match.group(2)
            t = partition_transforms.PartitionTransform(
                name, source, int(param) if param else None
            )
            if field.name() != t.field_name:
                t = partition_transforms.PartitionTransform(
                    name, source, t.param, alias=field.name()
                )
            expressions.append(str(t))
        return expressions

    @staticmethod
    def _iceberg_term(jvm, t: partition_transforms.PartitionTransform):
        """Map a parsed transform to an Iceberg expression term."""
        expressions = jvm.org.apache.iceberg.expressions.Expressions
        if t.name == partition_transforms.IDENTITY:
            return expressions.ref(t.source)
        if t.name == partition_transforms.BUCKET:
            return expressions.bucket(t.source, t.param)
        if t.name == partition_transforms.TRUNCATE:
            return expressions.truncate(t.source, t.param)
        if t.name == "year":
            return expressions.year(t.source)
        if t.name == "month":
            return expressions.month(t.source)
        if t.name == "day":
            return expressions.day(t.source)
        if t.name == "hour":
            return expressions.hour(t.source)
        raise FeatureStoreException(
            f"Transform '{t}' cannot be expressed as an Iceberg term."
        )

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
        if connector.type in (sc.StorageConnector.S3, sc.StorageConnector.GLUE):
            # Glue tables are stored on S3, so the same S3 FileIO credentials apply.
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
        if self._feature_group.sort_order:
            raise FeatureStoreException(
                "sort_order requires the Spark engine (the persistent sort "
                "order commits through the Iceberg Java API). Create the "
                "feature group from a Spark environment."
            )
        transforms = self._partition_spec_transforms()
        properties = {"write.distribution-mode": "hash"} if transforms else {}
        properties.update(self.ICEBERG_TABLE_PROPERTIES)
        table = catalog.create_table(
            self._pyiceberg_identifier(),
            schema=arrow_schema,
            location=location,
            properties=properties,
        )
        self._apply_pyiceberg_spec(table, transforms)
        return table

    @staticmethod
    def _apply_pyiceberg_spec(table, transforms) -> None:
        """Apply the feature group's transforms to a freshly created table.

        The spec commit happens before any data commit and, on the
        path-based flow, before `version-hint.text` is published, so readers
        never observe the intermediate unpartitioned table.
        """
        if not transforms:
            return
        with table.update_spec() as update_spec:
            for t in transforms:
                update_spec.add_field(
                    t.source,
                    IcebergEngine._pyiceberg_transform(t),
                    partition_field_name=t.alias,
                )

    @staticmethod
    def _pyiceberg_transform(t: partition_transforms.PartitionTransform):
        """Map a parsed transform to its PyIceberg transform object."""
        from pyiceberg import transforms as pit

        if t.name == partition_transforms.IDENTITY:
            return pit.IdentityTransform()
        if t.name == partition_transforms.BUCKET:
            return pit.BucketTransform(num_buckets=t.param)
        if t.name == partition_transforms.TRUNCATE:
            return pit.TruncateTransform(width=t.param)
        if t.name == partition_transforms.VOID:
            return pit.VoidTransform()
        if t.name == "year":
            return pit.YearTransform()
        if t.name == "month":
            return pit.MonthTransform()
        if t.name == "day":
            return pit.DayTransform()
        if t.name == "hour":
            return pit.HourTransform()
        raise FeatureStoreException(f"Unsupported Iceberg transform '{t}'.")

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

    @staticmethod
    def _reconcile_timestamp_tz(arrow_table: pa.Table, table) -> pa.Table:
        """Align timestamp columns to an existing Iceberg table's zone.

        `_prepare_arrow_table` produces timezone-naive `timestamp` columns
        (the Hopsworks offline type maps to `pa.timestamp("us")`), but an
        Iceberg table created elsewhere — e.g. by Spark, which writes
        `timestamptz` — declares its timestamp columns with a zone.
        PyIceberg's append and upsert require the incoming schema to match the
        table schema exactly and reject naive against zoned, so localize each
        naive column whose target column carries a zone to UTC (the zone
        PyIceberg uses for all `timestamptz` columns).
        """
        import pyarrow as pa

        target_by_name = {field.name: field.type for field in table.schema().as_arrow()}
        new_cols = []
        changed = False
        for i, field in enumerate(arrow_table.schema):
            col = arrow_table.column(i)
            target_type = target_by_name.get(field.name)
            if (
                pa.types.is_timestamp(field.type)
                and field.type.tz is None
                and target_type is not None
                and pa.types.is_timestamp(target_type)
                and target_type.tz is not None
            ):
                col = col.cast(pa.timestamp(field.type.unit, tz="UTC"))
                changed = True
            new_cols.append(col)
        if not changed:
            return arrow_table
        return pa.Table.from_arrays(new_cols, names=arrow_table.column_names)

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

        write_options = self._glue_catalog_write_options(write_options)
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
            arrow_table = self._reconcile_timestamp_tz(arrow_table, table)
            table.append(arrow_table)
        else:
            arrow_table = self._reconcile_timestamp_tz(arrow_table, table)
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
        from pyiceberg.catalog import Catalog, load_catalog
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        # The properties extracted from the write options follow the Iceberg
        # Spark schema; PyIceberg needs its own schema, so for a Glue feature
        # group rebuild them from the connector's PyIceberg options.
        glue = self._glue_catalog()
        create_location = None
        if glue is not None:
            catalog_properties = glue._pyiceberg_catalog_properties()
            # Register the table at the feature group's own S3 location instead
            # of letting PyIceberg derive it from the catalog warehouse, which
            # would place a new table at a wrong (possibly local) path.
            create_location = self._require_absolute_location()

        catalog = load_catalog(catalog_name, **catalog_properties)
        _logger.debug(
            f"Writing Iceberg dataset through catalog {catalog_name} table {identifier}"
        )

        rows_inserted = rows_updated = rows_deleted = None
        if not catalog.table_exists(identifier):
            if self._feature_group.sort_order:
                # create_table would silently drop the sort order; an
                # accepted argument must never be erased by the write path.
                raise FeatureStoreException(
                    "sort_order requires the Spark engine (the persistent "
                    "sort order commits through the Iceberg Java API); the "
                    "PyIceberg catalog creation path cannot apply it. "
                    "Create the feature group from a Spark environment."
                )
            with contextlib.suppress(NamespaceAlreadyExistsError):
                catalog.create_namespace(Catalog.namespace_from(identifier))
            transforms = self._partition_spec_transforms()
            properties = dict(self.ICEBERG_TABLE_PROPERTIES)
            if transforms:
                # a partitioned table shuffles rows to their partitions on
                # (Spark) write, matching the other creation paths
                properties["write.distribution-mode"] = "hash"
            table = catalog.create_table(
                identifier,
                schema=arrow_table.schema,
                location=create_location,
                properties=properties,
            )
            # Apply the feature group's partition spec before the first data
            # commit; without this the catalog table would silently stay
            # unpartitioned.
            self._apply_pyiceberg_spec(table, transforms)
            table.append(arrow_table)
        elif self._append_requested(operation, write_options):
            table = catalog.load_table(identifier)
            arrow_table = self._reconcile_timestamp_tz(arrow_table, table)
            table.append(arrow_table)
        else:
            table = catalog.load_table(identifier)
            arrow_table = self._reconcile_timestamp_tz(arrow_table, table)
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
                f"Feature group {self._feature_group.name} is not ICEBERG enabled."
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
