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

from typing import TYPE_CHECKING

from hopsworks_apigen import public


if TYPE_CHECKING:
    from hopsworks_common import job
    from hopsworks_common.core.sink_job_configuration import (
        SinkJobConfiguration,
        TableIngestionTarget,
    )
    from hopsworks_common.execution import Execution
    from hsfs import feature_group as fg


def _target_overrides(sink_job_conf: SinkJobConfiguration | None) -> dict:
    """Extract the fields a feature group's sink config overrides on the shared job.

    Only the settings the user actually changed from the library defaults become
    per-target overrides, so a feature group with a bare config inherits the
    job-level defaults rather than pinning every field to its default value.
    """
    from hopsworks_common.core.sink_job_configuration import SinkJobConfiguration

    if sink_job_conf is None:
        return {}

    reference = SinkJobConfiguration()
    overrides: dict = {}
    for attr, keyword in (
        ("_write_mode", "write_mode"),
        ("_batch_size", "batch_size"),
        ("_sql_source_fetch_chunk_size", "sql_source_fetch_chunk_size"),
        ("_source_read_workers", "source_read_workers"),
        ("_data_processing_workers", "data_processing_workers"),
        ("_max_upload_batch_size_mb", "max_upload_batch_size_mb"),
        ("_sql_table_num_partitions", "sql_table_num_partitions"),
        ("_transform_script_path", "transform_script_path"),
    ):
        value = getattr(sink_job_conf, attr)
        if value is not None and value != getattr(reference, attr):
            overrides[keyword] = value

    if sink_job_conf.column_mappings:
        overrides["column_mappings"] = sink_job_conf.column_mappings

    loading_config = sink_job_conf._loading_config
    default_loading = reference._loading_config
    if (
        loading_config is not None
        and hasattr(loading_config, "to_dict")
        and hasattr(default_loading, "to_dict")
        and loading_config.to_dict() != default_loading.to_dict()
    ):
        overrides["loading_config"] = loading_config

    if sink_job_conf._endpoint_config is not None:
        overrides["endpoint_config"] = sink_job_conf._endpoint_config

    return overrides


@public("hopsworks.core.MultiTableIngestionJob")
class MultiTableIngestionJob:
    """A multi-table ingestion job assembled before it is created on the server.

    Collect the feature groups to ingest, either by attaching the job to them with
    `sink_job=` when creating each feature group, or by calling
    [`MultiTableIngestionJob.add_target`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob.add_target]
    directly, then call
    [`MultiTableIngestionJob.save`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob.save]
    to create one job that copies every collected table.
    Targets are only sent to the server on `save`, so a partial or failed set of
    feature groups never leaves a half-built job behind.

    Obtain one from
    [`DataSource.new_ingestion_job`][hsfs.core.data_source.DataSource.new_ingestion_job].
    """

    def __init__(
        self,
        data_source,
        name: str,
        *,
        table_parallelism: int = 1,
        environment_name: str | None = None,
        transform_script_path: str | None = None,
        write_mode: str | None = None,
        batch_size: int | None = None,
        sql_source_fetch_chunk_size: int | None = None,
        source_read_workers: int | None = None,
        data_processing_workers: int | None = None,
        max_upload_batch_size_mb: int | None = None,
        sql_table_num_partitions: int | None = None,
        schedule_config=None,
    ):
        """Initialize a multi-table ingestion job builder.

        Parameters:
            data_source: The data source every target reads from.
            name: Name of the ingestion job to create.
            table_parallelism: How many tables run at the same time; `1` runs them sequentially.
            environment_name: Python environment the job runs in.
            transform_script_path: Default transformation script path for targets that do not set their own.
            write_mode: Default write mode (`APPEND` or `MERGE`) for targets that do not set their own.
            batch_size: Default write batch size.
            sql_source_fetch_chunk_size: Default source fetch chunk size for SQL sources.
            source_read_workers: Default number of source read workers.
            data_processing_workers: Default number of data processing workers.
            max_upload_batch_size_mb: Default maximum upload batch size in MB.
            sql_table_num_partitions: Default number of read partitions for SQL sources.
            schedule_config: Optional schedule for the job.
        """
        self._data_source = data_source
        self._name = name
        self._table_parallelism = table_parallelism
        self._schedule_config = schedule_config
        defaults = {
            "environment_name": environment_name,
            "transform_script_path": transform_script_path,
            "write_mode": write_mode,
            "batch_size": batch_size,
            "sql_source_fetch_chunk_size": sql_source_fetch_chunk_size,
            "source_read_workers": source_read_workers,
            "data_processing_workers": data_processing_workers,
            "max_upload_batch_size_mb": max_upload_batch_size_mb,
            "sql_table_num_partitions": sql_table_num_partitions,
        }
        # drop unset knobs so SinkJobConfiguration applies its own defaults
        self._job_level_defaults = {
            key: value for key, value in defaults.items() if value is not None
        }
        self._targets: list[TableIngestionTarget] = []
        self._target_index: dict[int, int] = {}
        self._job: job.Job | None = None

    @public
    def add_target(
        self,
        feature_group: fg.FeatureGroup | None = None,
        *,
        feature_group_id: int | None = None,
        **overrides,
    ) -> MultiTableIngestionJob:
        """Add a feature group as a target of this ingestion job.

        A target already added for the same feature group is replaced, so adding a
        feature group twice (for example on a re-run) never duplicates it.
        Any keyword override accepted by
        [`TableIngestionTarget`][hopsworks.core.sink_job_configuration.TableIngestionTarget]
        applies to this target only.

        Parameters:
            feature_group: The feature group to ingest into.
            feature_group_id: The id of the feature group, as an alternative to passing the object.
            **overrides: Per-target overrides such as `write_mode`, `batch_size`, or `resource_config`.

        Returns:
            This ingestion job, so calls can be chained.
        """
        from hopsworks_common.core.sink_job_configuration import TableIngestionTarget

        self._add_target_object(
            TableIngestionTarget(
                feature_group=feature_group,
                feature_group_id=feature_group_id,
                **overrides,
            )
        )
        return self

    def _add_target_object(self, target: TableIngestionTarget) -> None:
        key = target._feature_group_id
        if key in self._target_index:
            self._targets[self._target_index[key]] = target
        else:
            self._target_index[key] = len(self._targets)
            self._targets.append(target)

    def _attach_feature_group(
        self,
        feature_group: fg.FeatureGroup,
        sink_job_conf: SinkJobConfiguration | None,
    ) -> None:
        from hopsworks_common.core.sink_job_configuration import TableIngestionTarget

        self._add_target_object(
            TableIngestionTarget(
                feature_group=feature_group,
                **_target_overrides(sink_job_conf),
            )
        )

    @property
    def targets(self) -> list[TableIngestionTarget]:
        """The feature-group targets collected so far."""
        return self._targets

    @property
    def name(self) -> str:
        """Name of the ingestion job."""
        return self._name

    @property
    def job(self) -> job.Job | None:
        """The created job, or `None` until [`MultiTableIngestionJob.save`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob.save] has run."""
        return self._job

    @public
    def save(self) -> job.Job:
        """Create the ingestion job on the server from the collected targets.

        Sends every collected target in a single request, so the job is created
        atomically.
        Calling `save` again re-creates the job with the current set of targets.

        Returns:
            The created ingestion job.

        Raises:
            ValueError: If no targets have been collected yet.
        """
        from hopsworks_common.core.job_api import JobApi
        from hopsworks_common.core.sink_job_configuration import SinkJobConfiguration

        if not self._targets:
            raise ValueError(
                "The ingestion job has no targets; attach at least one feature "
                "group (with sink_job=...) or call add_target(...) before saving."
            )

        storage_connector = self._data_source._storage_connector
        sink_job_conf = SinkJobConfiguration(
            name=self._name,
            targets=self._targets,
            table_parallelism=self._table_parallelism,
            schedule_config=self._schedule_config,
            **self._job_level_defaults,
        )
        sink_job_conf._set_extra_params(
            featurestore_id=storage_connector._featurestore_id,
            storage_connector_id=storage_connector.id,
        )

        job_api = JobApi()
        self._job = job_api.create(self._name, sink_job_conf)
        if sink_job_conf.schedule_config:
            job_api.create_or_update_schedule_job(
                self._name, sink_job_conf.schedule_config
            )
        return self._job

    @public
    def run(self, await_termination: bool = False) -> Execution:
        """Run the ingestion job, creating it first if it has not been saved.

        Parameters:
            await_termination: Whether to block until the run finishes.

        Returns:
            The started execution.
        """
        if self._job is None:
            self.save()
        return self._job.run(await_termination=await_termination)
