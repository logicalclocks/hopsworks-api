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

from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group_commit, util
from hsfs.core import feature_group_api


try:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    from deltalake import DeltaTable as DeltaRsTable
    from deltalake import write_deltalake as deltars_write
    from deltalake.exceptions import TableNotFoundError
except ImportError:
    pass

try:
    from delta.tables import DeltaTable
except ImportError:
    pass


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
        self._feature_group = feature_group
        self._spark_context = spark_context
        self._spark_session = spark_session
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name

        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def save_delta_fg(self, dataset, write_options, validation_id=None):
        if self._spark_session is not None:
            fg_commit = self._write_delta_dataset(dataset, write_options)
        else:
            fg_commit = self._write_delta_rs_dataset(dataset)
        fg_commit.validation_id = validation_id
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def register_temporary_table(self, delta_fg_alias, read_options):
        location = self._feature_group.prepare_spark_location()

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

        return delta_options

    def delete_record(self, delete_df):
        if self._spark_session is not None:
            location = self._feature_group.prepare_spark_location()
            fg_source_table = DeltaTable.forPath(self._spark_session, location)
            is_delta_table = DeltaTable.isDeltaTable(self._spark_session, location)
        else:
            location = self._feature_group.location.replace("hopsfs", "hdfs")
            try:
                fg_source_table = DeltaRsTable(location)
                is_delta_table = True
            except TableNotFoundError:
                is_delta_table = False

        if not is_delta_table:
            raise FeatureStoreException(
                f"This is no data available in Feature group {self._feature_group.name}, or it not DELTA enabled "
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
                fg_commit = self._get_last_commit_metadata(
                    self._spark_session, location
                )
            else:
                fg_source_table.merge(
                    source=delete_df,
                    predicate=merge_query_str,
                    source_alias=updates_alias,
                    target_alias=source_alias,
                ).when_matched_delete().execute()
                fg_commit = self._get_last_commit_metadata_delta_rs(location)
        return self._feature_group_api.commit(self._feature_group, fg_commit)

    def _write_delta_dataset(self, dataset, write_options):
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

    def _write_delta_rs_dataset(self, dataset):
        location = self._feature_group.location.replace("hopsfs", "hdfs")
        if isinstance(dataset, pl.DataFrame):
            dataset = dataset.to_arrow()
        else:
            dataset = self._prepare_df_for_delta(dataset)

        try:
            fg_source_table = DeltaRsTable(location)
            is_delta_table = True
        except TableNotFoundError:
            is_delta_table = False

        if not is_delta_table:
            deltars_write(location, dataset)
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
        return self._get_last_commit_metadata_delta_rs(location)

    @staticmethod
    def _prepare_df_for_delta(df, timestamp_precision="us"):
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
        table = pa.Table.from_pandas(df_copy, preserve_index=False)

        # Cast timestamp columns to the specified precision
        new_cols = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_timestamp(field.type):
                # Cast to specified precision
                new_cols.append(col.cast(pa.timestamp(timestamp_precision)))
            else:
                new_cols.append(col)

        # Create new table with modified columns
        return pa.Table.from_arrays(new_cols, names=table.column_names)

    def vacuum(self, retention_hours: int):
        location = self._feature_group.prepare_spark_location()
        retention = (
            f"RETAIN {retention_hours} HOURS" if retention_hours is not None else ""
        )
        self._spark_session.sql(f"VACUUM '{location}' {retention}")

    def _generate_merge_query(self, source_alias, updates_alias):
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
        megrge_query_str = " AND ".join(merge_query_list)
        return megrge_query_str

    @staticmethod
    def _get_last_commit_metadata_delta_rs(base_path):
        fg_source_table = DeltaRsTable(base_path)

        last_commit = fg_source_table.history()[0]
        version = last_commit["version"]
        commit_timestamp = util.convert_event_time_to_timestamp(
            last_commit["timestamp"]
        )
        commit_date_string = util.get_hudi_datestr_from_timestamp(commit_timestamp)
        operation_metrics = last_commit["operationMetrics"]

        # Get info about the oldest remaining commit
        oldest_commit = (
            pd.DataFrame(fg_source_table.history())
            .sort_values("version")
            .iloc[0]
            .to_dict()
        )
        oldest_commit_timestamp = util.convert_event_time_to_timestamp(
            oldest_commit["timestamp"]
        )
        if version == 0:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["num_added_rows"],
                rows_updated=0,
                rows_deleted=0,
                last_active_commit_time=oldest_commit_timestamp,
            )
        else:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["num_target_rows_inserted"],
                rows_updated=operation_metrics["num_target_rows_updated"],
                rows_deleted=operation_metrics["num_target_rows_deleted"],
                last_active_commit_time=oldest_commit_timestamp,
            )

        return fg_commit

    @staticmethod
    def _get_last_commit_metadata(spark_context, base_path):
        fg_source_table = DeltaTable.forPath(spark_context, base_path)

        # Get info about the latest commit
        last_commit = fg_source_table.history(1).first().asDict()
        version = last_commit["version"]
        commit_timestamp = util.convert_event_time_to_timestamp(
            last_commit["timestamp"]
        )
        commit_date_string = util.get_hudi_datestr_from_timestamp(commit_timestamp)
        operation_metrics = last_commit["operationMetrics"]

        # Get info about the oldest remaining commit
        oldest_commit = fg_source_table.history().orderBy("version").first().asDict()
        oldest_commit_timestamp = util.convert_event_time_to_timestamp(
            oldest_commit["timestamp"]
        )

        if version == 0:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["numOutputRows"],
                rows_updated=0,
                rows_deleted=0,
                last_active_commit_time=oldest_commit_timestamp,
            )
        else:
            fg_commit = feature_group_commit.FeatureGroupCommit(
                commitid=None,
                commit_date_string=commit_date_string,
                commit_time=commit_timestamp,
                rows_inserted=operation_metrics["numTargetRowsInserted"],
                rows_updated=operation_metrics["numTargetRowsUpdated"],
                rows_deleted=operation_metrics["numTargetRowsDeleted"],
                last_active_commit_time=oldest_commit_timestamp,
            )

        return fg_commit
