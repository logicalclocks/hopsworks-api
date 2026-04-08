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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import project_api
from hopsworks_common.core.constants import HAS_POLARS
from hopsworks_common.core.type_systems import convert_offline_type_to_pyarrow_type
from hsfs import feature_group, feature_group_commit, util
from hsfs.core import feature_group_api, variable_api


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from hsfs.constructor import hudi_feature_group_alias

# Note: Avoid importing optional Delta dependencies at module import time.
# They are imported on-demand inside methods to provide friendly errors only
# when the functionality is used.
_logger = logging.getLogger(__name__)


class DeltaEngine:
    DELTA_SPARK_FORMAT = "delta"
    DELTA_QUERY_TIME_TRAVEL_AS_OF_INSTANT = "timestampAsOf"
    DELTA_ENABLE_CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELTA_DOT_PREFIX = "delta."
    APPEND = "append"

    def __init__(
        self,
        feature_store_id: int,
        feature_store_name: str,
        feature_group: feature_group.FeatureGroup,
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

    _ALLOWED_OPERATIONS = ("insert", "upsert")

    def save_delta_fg(
        self,
        dataset: pd.DataFrame | pa.Table | pl.DataFrame,
        write_options: dict[str, Any] | None,
        validation_id: int | None = None,
        operation: str = "upsert",
    ) -> feature_group_commit.FeatureGroupCommit:
        operation = operation.lower()
        if operation not in self._ALLOWED_OPERATIONS:
            raise ValueError(
                f"Unsupported operation '{operation}'. "
                f"Allowed values are: {self._ALLOWED_OPERATIONS}."
            )
        if self._spark_session is not None:
            _logger.debug(
                f"Saving Delta dataset using spark to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_delta_dataset(dataset, write_options, operation)
        else:
            _logger.debug(
                f"Saving Delta dataset using delta-rs to feature group {self._feature_group.name} v{self._feature_group.version}"
            )
            fg_commit = self._write_delta_rs_dataset(
                dataset, write_options=write_options, operation=operation
            )
        fg_commit.validation_id = validation_id
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def register_temporary_table(
        self,
        delta_fg_alias,
        read_options: dict[str, Any] | None = None,
        is_cdc_query: bool = False,
    ):
        location = self._feature_group.prepare_spark_location()
        _logger.debug(
            f"Registering temporary table for Delta feature group {self._feature_group.name} v{self._feature_group.version} at location {location}"
        )

        delta_options = self._setup_delta_read_opts(
            delta_fg_alias, read_options=read_options
        )
        if not is_cdc_query:
            self._spark_session.read.format(self.DELTA_SPARK_FORMAT).options(
                **delta_options
            ).load(location).createOrReplaceTempView(delta_fg_alias.alias)
        else:
            from pyspark.sql.functions import col

            # CDC query - remove duplicates for upserts and do not include deleted rows
            # to match behavior of other engines
            self._spark_session.read.format(self.DELTA_SPARK_FORMAT).options(
                **delta_options
            ).load(location).filter(
                col("_change_type").isin("update_postimage", "insert")
            ).createOrReplaceTempView(delta_fg_alias.alias)

    def _setup_delta_read_opts(
        self,
        delta_fg_alias: hudi_feature_group_alias.HudiFeatureGroupAlias,
        read_options: dict[str, Any] | None = None,
    ):
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
        elif delta_fg_alias.left_feature_group_start_timestamp is not None:
            # change data feed query with start and end time
            _delta_commit_start_time = util.get_delta_datestr_from_timestamp(
                delta_fg_alias.left_feature_group_start_timestamp,
            )

            delta_options = {
                "readChangeFeed": "true",
                "startingTimestamp": _delta_commit_start_time,
            }
            if delta_fg_alias.left_feature_group_end_timestamp is not None:
                _delta_commit_end_time = util.get_delta_datestr_from_timestamp(
                    delta_fg_alias.left_feature_group_end_timestamp,
                )
                delta_options["endingTimestamp"] = _delta_commit_end_time

        if read_options:
            for key in read_options:
                if isinstance(key, str) and key.startswith(self.DELTA_DOT_PREFIX):
                    # delta read options do not have the "delta." prefix
                    delta_options[key[len(self.DELTA_DOT_PREFIX) :]] = read_options[key]
                else:
                    delta_options[key] = read_options[key]

        _logger.debug(
            f"Delta read options for feature group {self._feature_group.name} v{self._feature_group.version}: {delta_options}"
        )

        return delta_options

    def delete_record(self, delete_df):
        storage_options = None
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
            location = self._get_delta_rs_location()
            storage_options = self._get_delta_rs_storage_options()
            try:
                from deltalake import DeltaTable as DeltaRsTable
                from deltalake.exceptions import TableNotFoundError
            except ImportError as e:
                raise ImportError(
                    "Delta Lake (deltalake) is required for non-Spark operations. "
                    "Install 'hops-deltalake' to enable Delta RS features."
                ) from e
            try:
                fg_source_table = DeltaRsTable(
                    location, storage_options=storage_options
                )
                is_delta_table = True
            except TableNotFoundError:
                is_delta_table = False

        if not is_delta_table:
            raise FeatureStoreException(
                f"Feature group {self._feature_group.name} is not DELTA enabled "
            )
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
        fg_commit = self._get_last_commit_metadata(
            self._spark_session, location, storage_options=storage_options
        )
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def _write_delta_dataset(self, dataset, write_options, operation="upsert"):
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
        elif operation == "insert":
            _logger.debug(
                f"Insert operation requested for {location}. Using append mode, skipping merge."
            )
            (
                dataset.write.format(DeltaEngine.DELTA_SPARK_FORMAT)
                .options(**write_options)
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
        if not self._feature_group._is_hopsfs_storage():
            _logger.debug(
                "Non-HopsFS storage connector detected, skipping HopsFS-specific delta-rs setup"
            )
            return
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
            project_username = f"{_client.project_name}__{user_name}"
            _logger.debug(f"Setting LIBHDFS_DEFAULT_USER to {project_username}")
            os.environ["LIBHDFS_DEFAULT_USER"] = project_username

    def _get_delta_rs_location(self):
        if not self._feature_group._is_hopsfs_storage():
            location = self._feature_group.location
            _logger.debug(f"Non-HopsFS storage, using location as-is: {location}")
            return location

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

    def _get_delta_rs_storage_options(self) -> dict[str, str]:
        """Build delta-rs storage_options from the feature group's storage connector.

        Returns an empty dict for HopsFS (handled separately via env vars) and for
        feature groups without a connector.
        For S3, ADLS, and GCS connectors the relevant credential keys are returned.
        """
        from hsfs import storage_connector as sc

        connector = self._feature_group.storage_connector
        if connector is None or connector.type == sc.StorageConnector.HOPSFS:
            return {}
        if connector.type == sc.StorageConnector.S3:
            opts = {}
            if connector.access_key:
                opts["AWS_ACCESS_KEY_ID"] = connector.access_key
            if connector.secret_key:
                opts["AWS_SECRET_ACCESS_KEY"] = connector.secret_key
            if connector.session_token:
                opts["AWS_SESSION_TOKEN"] = connector.session_token
            if connector.region:
                opts["AWS_REGION"] = connector.region
            return opts
        if connector.type == sc.StorageConnector.ADLS:
            opts = {}
            if connector.account_name:
                opts["AZURE_STORAGE_ACCOUNT_NAME"] = connector.account_name
            if connector.application_id:
                opts["AZURE_CLIENT_ID"] = connector.application_id
            if connector.service_credential:
                opts["AZURE_CLIENT_SECRET"] = connector.service_credential
            if connector.directory_id:
                opts["AZURE_TENANT_ID"] = connector.directory_id
            return opts
        if connector.type == sc.StorageConnector.GCS:
            opts = {}
            if connector.key_path:
                # key_path is a HopsFS path; download it locally for external clients
                from hsfs import engine

                local_key_path = engine.get_instance().add_file(connector.key_path)
                opts["GOOGLE_SERVICE_ACCOUNT_PATH"] = local_key_path
            return opts
        return {}

    @staticmethod
    def _get_partition_values(
        dataset, partition_key: list[str]
    ) -> dict[str, list[str]]:
        """Return {col: [unique_str_values]} for each partition column.

        Reads only the partition columns from the in-memory Arrow table — no
        Parquet I/O.  Used by both _can_use_append (to query file_uris) and the
        merge path (to build DataFusion partition-pruning IN filters).
        """
        import pyarrow.compute as pc

        return {
            col: [str(v) for v in pc.unique(dataset.column(col)).to_pylist()]
            for col in partition_key
        }

    def _can_use_append(self, fg_source_table, dataset) -> bool:
        """Return True if a plain append is safe (no partition overlap with existing data).

        When the feature group has a partition key and none of the incoming
        partition values are present in the existing Delta table, a merge is
        unnecessary: every row is a new insert so appending produces the same
        result with far less memory (no existing-partition scan for the join).

        Falls back to False (i.e. use merge) on any error so correctness is
        never compromised.
        """
        partition_key = self._feature_group.partition_key
        if not partition_key:
            return False
        try:
            partition_values = self._get_partition_values(dataset, partition_key)
            filters = [(col, "in", vals) for col, vals in partition_values.items()]
            matching_files = fg_source_table.file_uris(partition_filters=filters)
            no_overlap = len(matching_files) == 0
            _logger.debug(
                f"Partition overlap check: incoming partitions={partition_values}, "
                f"existing matching files={len(matching_files)}, use_append={no_overlap}"
            )
            return no_overlap
        except Exception as e:
            _logger.debug(f"Partition overlap check failed, falling back to merge: {e}")
            return False

    def _write_delta_rs_dataset(
        self,
        dataset: pa.Table | pl.DataFrame | pd.DataFrame,
        write_options: dict[str, Any] | None = None,
        operation: str = "upsert",
    ):
        """Write a dataset to a Delta table using delta-rs.

        Parameters:
            dataset: Dataset to write to the Delta table.
        """
        try:
            from deltalake import DeltaTable as DeltaRsTable
            from deltalake import write_deltalake as deltars_write
            from deltalake.exceptions import TableNotFoundError
        except ImportError as e:
            raise ImportError(
                "Delta Lake (deltalake) and its dependencies are required for non-Spark operations. "
                "Install 'hops-deltalake' to enable Delta RS features."
            ) from e

        location = self._get_delta_rs_location()
        storage_options = self._get_delta_rs_storage_options()

        is_polars_df = False
        if HAS_POLARS:
            import polars as pl

            if isinstance(dataset, pl.DataFrame):
                is_polars_df = True
                _logger.debug("Converting DataFrame to Arrow Table for Delta write")
                dataset = dataset.to_arrow()

        if not is_polars_df:
            dataset = self._prepare_df_for_delta(dataset)

        append_requested = operation == "insert" or (
            isinstance(write_options, dict)
            and str(write_options.get("mode", "")).lower() == self.APPEND
        )

        try:
            fg_source_table = DeltaRsTable(location, storage_options=storage_options)
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
            configuration = {
                self.DELTA_ENABLE_CHANGE_DATA_FEED: (write_options or {}).get(
                    self.DELTA_ENABLE_CHANGE_DATA_FEED, "true"
                )
            }
            deltars_write(
                location,
                dataset,
                partition_by=self._feature_group.partition_key,
                configuration=configuration,
                storage_options=storage_options or None,
            )
        else:
            if (
                isinstance(write_options, dict)
                and self.DELTA_ENABLE_CHANGE_DATA_FEED in write_options
            ):
                fg_source_table.alter.set_table_properties(
                    {
                        self.DELTA_ENABLE_CHANGE_DATA_FEED: write_options.get(
                            self.DELTA_ENABLE_CHANGE_DATA_FEED
                        )
                    }
                )
            if append_requested:
                deltars_write(
                    location,
                    dataset,
                    mode=self.APPEND,
                    storage_options=storage_options or None,
                )
                _logger.debug(
                    f"Explicit append mode requested for {location}. Skipping merge operation."
                )
                return self._get_last_commit_metadata(
                    self._spark_session, location, storage_options=storage_options
                )
            # Optimisation: if the feature group is partitioned and none of the
            # incoming partition values already exist in the table, a plain append
            # is equivalent to a merge (no rows to update) but avoids loading all
            # existing partition data into memory for the join.
            use_append = self._can_use_append(fg_source_table, dataset)
            if use_append:
                deltars_write(
                    location,
                    dataset,
                    mode="append",
                    partition_by=self._feature_group.partition_key,
                    storage_options=storage_options or None,
                )
            else:
                source_alias = (
                    f"{self._feature_group.name}_{self._feature_group.version}_source"
                )
                updates_alias = (
                    f"{self._feature_group.name}_{self._feature_group.version}_updates"
                )
                # Extract partition values from the in-memory dataset so
                # _generate_merge_query can emit literal IN filters that let DataFusion
                # prune Parquet files to only the overlapping partitions.  Zero I/O —
                # the values come directly from the Arrow table already in memory.
                partition_values = (
                    self._get_partition_values(
                        dataset, self._feature_group.partition_key
                    )
                    if self._feature_group.partition_key
                    else None
                )
                merge_query_str = self._generate_merge_query(
                    source_alias, updates_alias, partition_values
                )
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
        return self._get_last_commit_metadata(
            self._spark_session, location, storage_options=storage_options
        )

    @staticmethod
    def _prepare_df_for_delta(df, timestamp_precision="us"):
        try:
            import pandas as pd
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
        if not isinstance(df, pd.DataFrame):
            return df
        # `df` is already a shallow copy from convert_to_default_dataframe, so we do not
        # need a full deep copy here.  Column assignments on a shallow copy update only the
        # copy's column reference and never mutate the caller's original DataFrame.
        for col in df.select_dtypes(include=["datetime64"]).columns:
            # For timezone-aware timestamps, convert to UTC and remove timezone info
            if hasattr(df[col].dtype, "tz") and df[col].dtype.tz is not None:
                df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

        # Convert to basic PyArrow table first
        _logger.debug("Converting DataFrame to basic PyArrow Table")
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Cast timestamp columns to the specified precision and float16 to float32
        _logger.debug("Casting timestamp and float16 columns if needed")
        new_cols = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_timestamp(field.type):
                _precision_order = {"s": 0, "ms": 1, "us": 2, "ns": 3}
                if _precision_order.get(timestamp_precision, -1) < _precision_order.get(
                    field.type.unit, -1
                ):
                    warnings.warn(
                        f"Casting timestamp column '{field.name}' from '{field.type.unit}'"
                        f" to '{timestamp_precision}' will lose precision.",
                        UserWarning,
                        stacklevel=1,
                    )
                # Cast to specified precision (safe=False to allow for loss of precision)
                new_cols.append(col.cast(pa.timestamp(timestamp_precision), safe=False))
            elif pa.types.is_float16(field.type):  # delta lake do not support float16
                # Convert float16 to float32
                warnings.warn(
                    f"Casting float16 column '{field.name}' to float32 for Delta Lake compatibility.",
                    UserWarning,
                    stacklevel=1,
                )
                new_cols.append(col.cast(pa.float32()))
            elif pa.types.is_date(field.type) and field.type != pa.date32():
                # Delta Lake statistics use Date32 (days since epoch); date64
                # (milliseconds since epoch) causes a parse error in the kernel.
                new_cols.append(col.cast(pa.date32()))
            else:
                new_cols.append(col)

        # Create new table with modified columns
        _logger.debug("Creating new PyArrow Table with modified columns")
        return pa.Table.from_arrays(new_cols, names=table.column_names)

    def save_empty_delta_table_pyspark(self, write_options=None):
        """Create an empty Delta table with the schema from the feature group features.

        This method builds a DDL schema string from the feature group's features
        and creates an empty DataFrame with that schema, then writes it to the
        feature group location using Delta format.

        Parameters:
            write_options:
                Optional dictionary of write options for Delta.
                - key `delta.enableChangeDataFeed` set to a *string* value of true or false to enable or disable cdf operations on the feature group delta table.
                  Set to true by default on FG created after 4.6.
        """
        # Build DDL schema string from features
        ddl_fields = []
        for _feature in self._feature_group.features:
            if _feature.type:
                ddl_fields.append(f"{_feature.name} {_feature.type}")
            else:
                raise FeatureStoreException(
                    f"Feature '{_feature.name}' does not have a type defined. "
                    "Cannot create Delta table schema."
                )

        ddl_schema = ", ".join(ddl_fields)

        # Create empty DataFrame using the DDL string
        empty_df = self._spark_session.createDataFrame([], ddl_schema)

        self._write_delta_dataset(empty_df, write_options or {})

    def save_empty_delta_table_python(self, write_options=None):
        """Create an empty Delta table with the schema from the feature group features using delta-rs.

        This method converts feature types directly to PyArrow types without requiring Spark,
        creates an empty PyArrow table with that schema, and writes it to the feature group
        location using delta-rs write_deltalake.

        Supports simple types, array types, and struct types.

        Parameters:
            write_options:
                Optional dictionary of write options for Delta.
                - key `delta.enableChangeDataFeed` set to a *string* value of true or false to enable or disable cdf operations on the feature group delta table.
                  Set to true by default on FG created after 4.6.
        """
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "PyArrow is required to create empty Delta tables."
            ) from e

        # Build PyArrow schema directly from features
        pyarrow_fields = []
        for _feature in self._feature_group.features:
            if not _feature.type:
                raise FeatureStoreException(
                    f"Feature '{_feature.name}' does not have a type defined. "
                    "Cannot create Delta table schema."
                )
            try:
                pyarrow_type = convert_offline_type_to_pyarrow_type(_feature.type)
                pyarrow_fields.append(
                    pa.field(_feature.name, pyarrow_type, nullable=True)
                )
            except Exception as e:
                raise FeatureStoreException(
                    f"Failed to convert type '{_feature.type}' for feature '{_feature.name}': {str(e)}"
                ) from e

        pyarrow_schema = pa.schema(pyarrow_fields)
        _logger.debug(
            f"Created PyArrow schema with {len(pyarrow_fields)} fields for feature group {self._feature_group.name} v{self._feature_group.version}"
        )

        # Create empty PyArrow table from schema
        empty_arrow_table = pyarrow_schema.empty_table()

        self._write_delta_rs_dataset(empty_arrow_table, write_options=write_options)

    def save_empty_table(self, write_options=None):
        if self._spark_session is not None:
            self.save_empty_delta_table_pyspark(write_options=write_options)
        else:
            self.save_empty_delta_table_python(write_options=write_options)

    def vacuum(self, retention_hours: int):
        location = self._feature_group.prepare_spark_location()
        _logger.debug(
            f"Vacuuming Delta table for feature group {self._feature_group.name} v{self._feature_group.version} at location {location} with retention {retention_hours} hours"
        )
        retention = (
            f"RETAIN {retention_hours} HOURS" if retention_hours is not None else ""
        )
        self._spark_session.sql(f"VACUUM '{location}' {retention}")

    def _generate_merge_query(
        self,
        source_alias,
        updates_alias,
        partition_values: dict[str, list[str]] | None = None,
    ):
        _logger.debug(
            f"Generating merge query for feature group {self._feature_group.name} v{self._feature_group.version} from source alias {source_alias} and updates alias {updates_alias}"
        )
        merge_query_list = []
        primary_key = self._feature_group.primary_key.copy()

        # add event time to primary key for upserts
        if self._feature_group.event_time is not None:
            primary_key.append(self._feature_group.event_time)

        # add partition key for upserts
        if self._feature_group.partition_key:
            primary_key = primary_key + self._feature_group.partition_key

        for pk in primary_key:
            merge_query_list.append(f"{source_alias}.{pk} == {updates_alias}.{pk}")
        merge_query_str = " AND ".join(merge_query_list)

        # Append literal partition-value IN filters so DataFusion can prune
        # which Parquet files to read — avoids scanning the entire table when
        # only a subset of partitions are being updated.
        if partition_values:
            for col, vals in partition_values.items():
                vals_sql = ", ".join(f"'{v}'" for v in vals)
                merge_query_str += f" AND {source_alias}.{col} IN ({vals_sql})"

        _logger.debug(f"Merge query: {merge_query_str}")
        return merge_query_str

    @staticmethod
    def _get_last_commit_metadata(
        spark_context, base_path, storage_options: dict | None = None
    ):
        """Retrieve oldest and last data-changing commits (MERGE/WRITE) from a Delta table.

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
            fg_source_table = DeltaRsTable(
                base_path, storage_options=storage_options or {}
            )
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

        # Depending on operation, set the relevant metrics
        if operation == "WRITE":
            rows_inserted = (
                operation_metrics.get("numOutputRows")
                or operation_metrics.get("num_added_rows")
                or 0
            )
        elif operation == "MERGE":
            rows_inserted = (
                operation_metrics.get("numTargetRowsInserted")
                or operation_metrics.get("num_target_rows_inserted")
                or 0
            )
            rows_updated = (
                operation_metrics.get("numTargetRowsUpdated")
                or operation_metrics.get("num_target_rows_updated")
                or 0
            )
            rows_deleted = (
                operation_metrics.get("numTargetRowsDeleted")
                or operation_metrics.get("num_target_rows_deleted")
                or 0
            )

        _logger.debug(
            f"Commit metrics {commit_timestamp} - inserted: {rows_inserted}, updated: {rows_updated}, deleted: {rows_deleted}"
        )

        return feature_group_commit.FeatureGroupCommit(
            commitid=None,
            commit_date_string=commit_date_string,
            commit_time=commit_timestamp,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            last_active_commit_time=oldest_commit_timestamp,
        )
