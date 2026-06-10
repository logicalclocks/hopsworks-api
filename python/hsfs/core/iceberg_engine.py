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
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group, feature_group_commit, util
from hsfs.core import feature_group_api


if TYPE_CHECKING:
    from hsfs.constructor import hudi_feature_group_alias

_logger = logging.getLogger(__name__)


class IcebergEngine:
    """Spark engine for feature groups stored as Apache Iceberg tables.

    Tables are addressed by their filesystem location (Iceberg `HadoopTables`),
    mirroring how the Delta engine operates, so no Spark catalog configuration
    is required on the session.
    Iceberg operations are only available with the Spark engine; in the Python
    engine Iceberg feature groups are written through the materialization job
    running on the cluster.
    """

    ICEBERG_SPARK_FORMAT = "iceberg"
    ICEBERG_QUERY_TIME_TRAVEL_AS_OF_TIMESTAMP = "as-of-timestamp"
    ICEBERG_START_SNAPSHOT_ID = "start-snapshot-id"
    ICEBERG_END_SNAPSHOT_ID = "end-snapshot-id"
    ICEBERG_MERGE_SCHEMA = "merge-schema"
    ICEBERG_CHECK_ORDERING = "check-ordering"
    ICEBERG_DOT_PREFIX = "iceberg."
    SNAPSHOTS_METADATA_SUFFIX = "#snapshots"
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
        if spark_session is None:
            raise FeatureStoreException(
                "Iceberg time travel format operations are only supported with the Spark engine. "
                "Insert into the feature group from a Spark application or rely on the "
                "materialization job when using the Python engine."
            )
        self._feature_group = feature_group
        self._spark_session = spark_session
        self._spark_context = spark_context
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def _save_iceberg_fg(
        self,
        dataset,
        write_options: dict[str, Any] | None,
        validation_id: int | None = None,
        operation: str = "upsert",
    ) -> feature_group_commit.FeatureGroupCommit:
        operation = operation.lower() if operation else "upsert"
        _logger.debug(
            f"Saving Iceberg dataset using spark to feature group {self._feature_group.name} v{self._feature_group.version}"
        )
        fg_commit = self._write_iceberg_dataset(dataset, write_options, operation)
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

    def _write_iceberg_dataset(
        self, dataset, write_options: dict[str, Any] | None, operation: str = "upsert"
    ) -> feature_group_commit.FeatureGroupCommit:
        location = self._feature_group.prepare_spark_location()
        if write_options is None:
            write_options = {}

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
        snapshots = [
            s
            for s in self._read_snapshots(base_path)
            if s.get("operation") in self.DATA_OPERATIONS
        ]
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
