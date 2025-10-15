#
#   Copyright 2024 Hopsworks AB
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
import warnings
from urllib.parse import urlparse

import humps
from hopsworks.core import project_api
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group_commit, util
from hsfs.core import feature_group_api, variable_api


# Note: Avoid importing optional Delta dependencies at module import time.
# They are imported on-demand inside methods to provide friendly errors only
# when the functionality is used.
_logger = logging.getLogger(__name__)


class DeltaEngine:
    DELTA_SPARK_FORMAT = "delta"
    DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT = "timestampAsOf"

    def __init__(
        self,
        feature_store_id,
        feature_store_name,
        feature_group,
        spark_session,
        spark_context,
    ):
        _logger.debug(
            f"Initializing DeltaEngine {feature_group.name} v{feature_group.version}"
        )
        self._feature_group = feature_group
        self._spark_context = spark_context
        self._spark_session = spark_session
        if self._spark_session:
            self._spark_session.conf.set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi()
        self._variable_api = variable_api.VariableApi()
        self._project_api = project_api.ProjectApi()
        self._setup_delta_rs()

    def save_delta_fg(self, dataset, write_options, validation_id=None):
        if self._spark_session is not None:
            _logger.debug(
                f"Saving Delta dataset using spark to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_delta_dataset(dataset, write_options)
        else:
            _logger.debug(
                f"Saving Delta dataset using delta-rs to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_delta_rs_dataset(dataset)
        fg_commit.validation_id = validation_id
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def register_temporary_table(self, delta_fg_alias, read_options):
        location = self._feature_group.prepare_spark_location()
        _logger.debug(
            f"Registering temporary table for Delta feature group {self._feature_group.name} v{self._feature_group.version} at location {location}"
        )

        delta_options = self._setup_delta_read_opts(delta_fg_alias, read_options)
        self._spark_session.read.format(self.DELTA_SPARK_FORMAT).options(
            **delta_options
        ).load(location).createOrReplaceTempView(delta_fg_alias.alias)

    def _setup_delta_read_opts(self, delta_fg_alias, read_options):
        delta_options = {}
        if delta_fg_alias.left_feature_group_end_timestamp is None and (
            delta_fg_alias.left_feature_group_start_timestamp is None
            or delta_fg_alias.left_feature_group_start_timestamp == 0
        ):
            # snapshot query latest state
            delta_options = {}
        elif (
            delta_fg_alias.left_feature_group_end_timestamp is not None
            and delta_fg_alias.left_feature_group_start_timestamp is None
        ):
            # snapshot query with end time
            _delta_commit_end_time = util.get_delta_datestr_from_timestamp(
                delta_fg_alias.left_feature_group_end_timestamp
            )
            delta_options = {
                self.DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT: _delta_commit_end_time,
            }

        if read_options:
            delta_options.update(read_options)

        _logger.debug(
            f"Delta read options for feature group {self._feature_group.name} v{self._feature_group.version}: {delta_options}"
        )

        return delta_options

    def delete_record(self, delete_df):
        if self._spark_session is not None:
            try:
                from delta.tables import DeltaTable
            except ImportError as e:
                raise ImportError(
                    "Delta Lake (delta-spark) is required for Spark operations. "
                    "Install 'delta-spark' or include it in your environment."
                ) from e
            location = self._feature_group.prepare_spark_location()
            fg_source_table = DeltaTable.forPath(self._spark_session, location)
            is_delta_table = DeltaTable.isDeltaTable(self._spark_session, location)
        else:
            location = self._feature_group.location.replace("hopsfs", "hdfs")
            try:
                from deltalake import DeltaTable as DeltaRsTable
                from deltalake.exceptions import TableNotFoundError
            except ImportError as e:
                raise ImportError(
                    "Delta Lake (deltalake) is required for non-Spark operations. "
                    "Install 'hops-deltalake' to enable Delta RS features."
                ) from e
            try:
                fg_source_table = DeltaRsTable(location)
                is_delta_table = True
            except TableNotFoundError:
                is_delta_table = False

        if not is_delta_table:
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not DELTA enabled "
            )
        else:
            source_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_source"
            )
            updates_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_updates"
            )
            merge_query_str = self._generate_merge_query(source_alias, updates_alias)

            if self._spark_session is not None:
                fg_source_table.alias(source_alias).merge(
                    delete_df.alias(updates_alias), merge_query_str
                ).whenMatchedDelete().execute()
            else:
                fg_source_table.merge(
                    source=delete_df,
                    predicate=merge_query_str,
                    source_alias=updates_alias,
                    target_alias=source_alias,
                ).when_matched_delete().execute()
            fg_commit = self._get_last_commit_metadata(self._spark_session, location)
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def _write_delta_dataset(self, dataset, write_options):
        try:
            from delta.tables import DeltaTable
        except ImportError as e:
            raise ImportError(
                "Delta Lake (delta-spark) is required for Spark operations. "
                "Install 'delta-spark' or include it in your environment."
            ) from e
        location = self._feature_group.prepare_spark_location()
        if write_options is None:
            write_options = {}

        if not DeltaTable.isDeltaTable(self._spark_session, location):
            (
                dataset.write.format(DeltaEngine.DELTA_SPARK_FORMAT)
                .options(**write_options)
                .partitionBy(
                    self._feature_group.partition_key
                    if self._feature_group.partition_key
                    else []
                )
                .mode("append")
                .save(location)
            )
        else:
            fg_source_table = DeltaTable.forPath(self._spark_session, location)

            source_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_source"
            )
            updates_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_updates"
            )
            merge_query_str = self._generate_merge_query(source_alias, updates_alias)

            fg_source_table.alias(source_alias).merge(
                dataset.alias(updates_alias), merge_query_str
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        return self._get_last_commit_metadata(self._spark_session, location)

    def _setup_delta_rs(self):
        _logger.debug("Setting up delta-rs environment")
        _client = client.get_instance()
        if _client._is_external():
            _logger.debug("Setting up delta-rs for external client")
            os.environ["PEMS_DIR"] = _client.get_certs_folder()
            _logger.debug(f"PEMS_DIR set to {os.environ['PEMS_DIR']}")
            try:
                datanode_ip = self._variable_api.get_loadbalancer_external_domain(
                    "datanode"
                )
                _logger.debug(
                    f"Setting HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE to {datanode_ip}"
                )
                os.environ["HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"] = datanode_ip
            except FeatureStoreException as e:
                raise FeatureStoreException(
                    "Failed to write to delta table in external cluster. Make sure datanode load balancer has been setup on the cluster."
                ) from e

            user_name = self._project_api.get_user_info().get("username", None)

            if not user_name:
                raise FeatureStoreException(
                    "Failed to write to delta table in external cluster. Cannot get user name for project."
                )
            else:
                project_username = f"{_client.project_name}__{user_name}"
                _logger.debug(f"Setting LIBHDFS_DEFAULT_USER to {project_username}")
                os.environ["LIBHDFS_DEFAULT_USER"] = project_username

    def _get_delta_rs_location(self):
        _client = client.get_instance()
        location = self._feature_group.location.replace(
            "hopsfs:/", "hdfs:/"
        )  # deltars requires hdfs scheme

        if _client._is_external():
            parsed_url = urlparse(location)
            try:
                deltars_loc = f"hdfs://{self._variable_api.get_loadbalancer_external_domain('namenode')}:{parsed_url.port}{parsed_url.path}"
                _logger.debug(
                    f"External client, using namenode url + delta-rs location: {deltars_loc}"
                )
                return deltars_loc
            except FeatureStoreException as e:
                raise FeatureStoreException(
                    "Failed to write to delta table. Make sure namenode load balancer has been setup on the cluster."
                ) from e
        else:
            _logger.debug(f"Internal client, using delta-rs location: {location}")
            return location

    def _write_delta_rs_dataset(self, dataset):
        try:
            import polars as pl
            import pyarrow as pa  # noqa: F401  # used indirectly via _prepare_df_for_delta
            from deltalake import DeltaTable as DeltaRsTable
            from deltalake import write_deltalake as deltars_write
            from deltalake.exceptions import TableNotFoundError
        except ImportError as e:
            raise ImportError(
                "Delta Lake (deltalake) and its dependencies are required for non-Spark operations. "
                "Install 'hops-deltalake' to enable Delta RS features."
            ) from e
        location = self._get_delta_rs_location()

        _logger.debug("Converting DataFrame to Arrow Table for Delta write")
        if isinstance(dataset, pl.DataFrame):
            dataset = dataset.to_arrow()
        else:
            dataset = self._prepare_df_for_delta(dataset)

        try:
            fg_source_table = DeltaRsTable(location)
            is_delta_table = True
            _logger.debug(
                f"Delta table found at {location}. Proceeding with merge operation."
            )
        except TableNotFoundError:
            _logger.debug(
                f"Delta table not found at {location}. A new Delta table will be created."
            )
            is_delta_table = False

        if not is_delta_table:
            deltars_write(
                location, dataset, partition_by=self._feature_group.partition_key
            )
        else:
            source_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_source"
            )
            updates_alias = (
                f"{self._feature_group.name}_{self._feature_group.version}_updates"
            )
            merge_query_str = self._generate_merge_query(source_alias, updates_alias)

            (
                fg_source_table.merge(
                    source=dataset,
                    predicate=merge_query_str,
                    source_alias=updates_alias,
                    target_alias=source_alias,
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        _logger.debug(
            f"Executed delta-rs write. Retrieving commit metadata for Delta table at {location}"
        )
        return self._get_last_commit_metadata(self._spark_session, location)

    @staticmethod
    def _prepare_df_for_delta(df, timestamp_precision="us"):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "pandas and pyarrow are required to prepare data for Delta operations."
            ) from e
        """
        Prepares a pandas DataFrame for Delta Lake operations by fixing timestamp columns.

        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame to prepare
        timestamp_precision : str, default='us'
            Precision for timestamps (ns, us, ms, s)

        Returns:
        --------
        pyarrow.Table
            PyArrow table ready for Delta Lake
        """
        # Process timestamp columns
        df_copy = df.copy()
        for col in df_copy.select_dtypes(include=["datetime64"]).columns:
            # For timezone-aware timestamps, convert to UTC and remove timezone info
            if hasattr(df_copy[col].dtype, "tz") and df_copy[col].dtype.tz is not None:
                df_copy[col] = df_copy[col].dt.tz_convert("UTC").dt.tz_localize(None)

        # Convert to basic PyArrow table first
        _logger.debug("Converting DataFrame to basic PyArrow Table")
        table = pa.Table.from_pandas(df_copy, preserve_index=False)

        # Cast timestamp columns to the specified precision and float16 to float32
        _logger.debug(
            f"Casting timestamp and float16 columns if needed"
        )
        new_cols = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_timestamp(field.type):
                # Cast to specified precision
                new_cols.append(col.cast(pa.timestamp(timestamp_precision)))
            elif pa.types.is_float16(field.type):  # delta lake do not support float16
                # Convert float16 to float32
                warnings.warn(
                    f"Casting float16 column '{field.name}' to float32 for Delta Lake compatibility.",
                    UserWarning,
                    stacklevel=1,
                )
                new_cols.append(col.cast(pa.float32()))
            else:
                new_cols.append(col)

        # Create new table with modified columns
        _logger.debug("Creating new PyArrow Table with modified columns")
        return pa.Table.from_arrays(new_cols, names=table.column_names)

    def vacuum(self, retention_hours: int):
        location = self._feature_group.prepare_spark_location()
        _logger.debug(
            f"Vacuuming Delta table for feature group {self._feature_group.name} v{self._feature_group.version} at location {location} with retention {retention_hours} hours"
        )
        retention = (
            f"RETAIN {retention_hours} HOURS" if retention_hours is not None else ""
        )
        self._spark_session.sql(f"VACUUM '{location}' {retention}")

    def _generate_merge_query(self, source_alias, updates_alias):
        _logger.debug(
            f"Generating merge query for feature group {self._feature_group.name} v{self._feature_group.version} from source alias {source_alias} and updates alias {updates_alias}"
        )
        merge_query_list = []
        primary_key = self._feature_group.primary_key

        # add event time to primary key for upserts
        if self._feature_group.event_time is not None:
            primary_key.append(self._feature_group.event_time)

        # add partition key for upserts
        if self._feature_group.partition_key:
            primary_key = primary_key + self._feature_group.partition_key

        for pk in primary_key:
            merge_query_list.append(f"{source_alias}.{pk} == {updates_alias}.{pk}")
        merge_query_str = " AND ".join(merge_query_list)
        _logger.debug(f"Merge query: {merge_query_str}")
        return merge_query_str

    @staticmethod
    def _get_last_commit_metadata(spark_context, base_path):
        """
        Retrieve oldest and last data-changing commits (MERGE/WRITE) from a Delta table.
        Uses shared filtering logic for both Spark and delta-rs.
        """
        data_ops = ["MERGE", "WRITE"]
        _logger.debug(f"Retrieving last commit metadata for Delta table at {base_path}")

        # --- Get commit history ---
        if spark_context is not None:
            try:
                from delta.tables import DeltaTable
            except ImportError as e:
                raise ImportError(
                    "Delta Lake (delta-spark) is required to read commit metadata. "
                    "Install 'delta-spark' or include it in your environment."
                ) from e
            # Spark DeltaTable (returns Spark DataFrame)
            fg_source_table = DeltaTable.forPath(spark_context, base_path)
            history = fg_source_table.history()
            history_records = [r.asDict() for r in history.collect()]
            _logger.debug(f"history_records for {base_path}: {history_records}")
        else:
            try:
                from deltalake import DeltaTable as DeltaRsTable
            except ImportError as e:
                raise ImportError(
                    "Delta Lake (deltalake) is required to read commit metadata. "
                    "Install 'hops-deltalake' to enable Delta RS features."
                ) from e
            # delta-rs DeltaTable (returns list[dict])
            fg_source_table = DeltaRsTable(base_path)
            history_records = fg_source_table.history()
            _logger.debug(f"history_records for {base_path}: {history_records}")

        if not history_records:
            return None

        # --- Shared logic below ---
        filtered = [c for c in history_records if c.get("operation") in data_ops]
        if not filtered:
            return None

        # oldest = smallest version, latest = largest version
        oldest_commit = min(filtered, key=lambda c: c["version"])
        last_commit = max(filtered, key=lambda c: c["version"])

        _logger.debug(
            f"Oldest commit: {oldest_commit['version']} at {oldest_commit['timestamp']}"
        )
        _logger.debug(
            f"Last commit: {last_commit['version']} at {last_commit['timestamp']}"
        )

        return DeltaEngine._get_delta_feature_group_commit(last_commit, oldest_commit)

    @staticmethod
    def _get_delta_feature_group_commit(last_commit, oldest_commit):
        _logger.debug(f"Extract info about the latest commit {last_commit}")
        operation = last_commit["operation"]
        commit_timestamp = util.convert_event_time_to_timestamp(
            last_commit["timestamp"]
        )
        commit_date_string = util.get_hudi_datestr_from_timestamp(commit_timestamp)
        operation_metrics = last_commit["operationMetrics"]

        # Extract info about the oldest remaining commit
        oldest_commit_timestamp = util.convert_event_time_to_timestamp(
            oldest_commit["timestamp"]
        )

        # Default all to zero
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0

        humps.camelize(operation_metrics)

        # Depending on operation, set the relevant metrics
        if operation == "WRITE":
            rows_inserted = operation_metrics.get("numOutputRows", 0)
        elif operation == "MERGE":
            rows_inserted = operation_metrics.get("numTargetRowsInserted", 0)
            rows_updated = operation_metrics.get("numTargetRowsUpdated", 0)
            rows_deleted = operation_metrics.get("numTargetRowsDeleted", 0)

        _logger.debug(
            f"Commit metrics {commit_timestamp} - inserted: {rows_inserted}, updated: {rows_updated}, deleted: {rows_deleted}"
        )

        fg_commit = feature_group_commit.FeatureGroupCommit(
            commitid=None,
            commit_date_string=commit_date_string,
            commit_time=commit_timestamp,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            last_active_commit_time=oldest_commit_timestamp,
        )

        return fg_commit
