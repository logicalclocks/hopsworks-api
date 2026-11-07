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

import copy
import json
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common.client.exceptions import JobExecutionException


if TYPE_CHECKING:
    from hopsworks_common import job
    from hopsworks_common.execution import Execution
    from hsfs import storage_connector as sc_mod


DTO_TYPE = "ingestionJobConfiguration"
MODE_FILES = "FILES"
DEFAULT_FILES_PER_PAGE = 100
_JOB_NAME_PATTERN = re.compile(r"[^A-Za-z0-9_]")
_INDEX_NAME_PATTERN = re.compile(r"[^a-z0-9_]")
_MAX_INDEX_NAME = 63

# Settings that `update` may change; everything else is fixed at creation.
_EDITABLE = (
    "prefix",
    "include",
    "exclude",
    "mirror_deletions",
    "index",
    "max_file_size_mb",
    "fail_on_first_error",
    "files_per_page",
    "resources",
    "schedule",
    "environment_name",
)


def _default_job_name(connector_name: str, dataset: str) -> str:
    """The job name `ingest_files` and the UI use when none is given: `<connector>_<dataset>_files` with every character outside `[A-Za-z0-9_]` replaced by `_`."""
    return _JOB_NAME_PATTERN.sub("_", f"{connector_name}_{dataset}_files")


def _default_index_name(dataset: str) -> str:
    """The index feature group name the backend applies when none is given: `files_<dataset>`, lowercased, with every character outside `[a-z0-9_]` replaced by `_`, at most 63 characters."""
    return _INDEX_NAME_PATTERN.sub("_", f"files_{dataset}".lower())[:_MAX_INDEX_NAME]


def _validate_dataset(dataset: Any) -> str:
    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError("dataset must be a non-empty dataset name.")
    return dataset.strip()


def _validate_prefix(prefix: Any) -> str:
    if prefix is None:
        return ""
    if not isinstance(prefix, str):
        raise ValueError("prefix must be a string, relative to the bucket root.")
    if prefix.startswith("/"):
        raise ValueError(
            "prefix must not start with '/'; it is relative to the bucket or container root."
        )
    if ".." in prefix.split("/"):
        raise ValueError("prefix must not contain '..' segments.")
    return prefix


def _validate_globs(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a list of glob strings.")
    return list(value)


def _validate_index(index: Any) -> bool | str:
    if isinstance(index, bool):
        return index
    if isinstance(index, str):
        if not index.strip():
            raise ValueError("index must be True, False or a feature group name.")
        return index.strip()
    raise ValueError("index must be True, False or a feature group name.")


def _validate_max_file_size(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("max_file_size_mb must be None or a non-negative integer.")
    return value


def _validate_files_per_page(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("files_per_page must be a positive integer.")
    return value


def _validate_resources(resources: Any) -> dict[str, Any] | None:
    if resources is None:
        return None
    if not isinstance(resources, dict):
        raise ValueError(
            'resources must be a dict such as {"cores": 1, "memory": 2048, "gpus": 0}.'
        )
    return dict(resources)


def _validate_schedule(schedule: Any) -> str | None:
    if schedule is None:
        return None
    if not isinstance(schedule, str) or not schedule.strip():
        raise ValueError("schedule must be a cron expression string or None.")
    return schedule.strip()


@public("hopsworks.core.FilesIngestionJob")
class FilesIngestionJob:
    """An ingestion job that copies the files under a prefix of an S3, GCS or ADLS data source into a project dataset.

    Each run lists the prefix, copies new and changed files into the dataset, skips unchanged ones and, with mirror deletions, removes files that are gone from the source.
    Its state (manifest, lease, audit log and per-run stats) lives under `DataSources/<connector>/files/<dataset>`.
    Obtain one with
    [`StorageConnector.ingest_files`][hsfs.storage_connector.StorageConnector.ingest_files],
    which creates it on the server, or list existing ones with
    [`StorageConnector.get_files_ingestion_jobs`][hsfs.storage_connector.StorageConnector.get_files_ingestion_jobs].
    """

    def __init__(
        self,
        storage_connector: sc_mod.StorageConnector | None,
        dataset: str,
        *,
        prefix: str = "",
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        mirror_deletions: bool = False,
        index: bool | str = False,
        max_file_size_mb: int | None = None,
        schedule: str | None = None,
        resources: dict[str, Any] | None = None,
        name: str | None = None,
        fail_on_first_error: bool = False,
        files_per_page: int = DEFAULT_FILES_PER_PAGE,
        environment_name: str | None = None,
        storage_connector_id: int | None = None,
        featurestore_id: int | None = None,
        connector_name: str | None = None,
        job: job.Job | None = None,
    ):
        """Initialize a files ingestion job, without creating it on the server.

        Parameters:
            storage_connector: The S3, GCS or ADLS data source the files are read from; `None` when the ids are given instead.
            dataset: Name of the dataset the files are copied into.
            prefix: Prefix inside the bucket or container, relative to its root; `""` for the whole bucket.
            include: Glob patterns of the files to copy; every file when empty.
            exclude: Glob patterns of the files to skip.
            mirror_deletions: Delete files from the dataset that are gone from the source.
            index: `True` to keep an index feature group named `files_<dataset>`, a name to use instead, `False` for none.
            max_file_size_mb: Files larger than this are skipped; `None` or `0` copies any size.
            schedule: Quartz cron expression of the job schedule; `None` for none.
            resources: Pod resources of a run, e.g. `{"cores": 1, "memory": 2048, "gpus": 0}`.
            name: Job name; `<connector>_<dataset>_files` when `None`.
            fail_on_first_error: Stop the run at the first file that fails instead of continuing.
            files_per_page: Number of files listed per request.
            environment_name: Python environment the job runs in; the cluster default when `None`.
            storage_connector_id: Id of the data source, when no connector object is given.
            featurestore_id: Id of the feature store the data source belongs to, when no connector object is given.
            connector_name: Name of the data source, when no connector object is given.
            job: The server-side job this object describes, when it already exists.
        """
        self._storage_connector = storage_connector
        if storage_connector is not None:
            storage_connector_id = storage_connector.id
            featurestore_id = storage_connector._featurestore_id
            connector_name = storage_connector.name
        self._storage_connector_id = storage_connector_id
        self._featurestore_id = featurestore_id
        self._connector_name = connector_name
        self._dataset = _validate_dataset(dataset)
        self._prefix = _validate_prefix(prefix)
        self._include = _validate_globs(include, "include")
        self._exclude = _validate_globs(exclude, "exclude")
        self._mirror_deletions = bool(mirror_deletions)
        self._index = _validate_index(index)
        self._max_file_size_mb = _validate_max_file_size(max_file_size_mb)
        self._schedule = _validate_schedule(schedule)
        self._resources = _validate_resources(resources)
        self._fail_on_first_error = bool(fail_on_first_error)
        self._files_per_page = _validate_files_per_page(files_per_page)
        self._environment_name = environment_name
        if name is None:
            if connector_name is None:
                raise ValueError("name is required when no data source is given.")
            name = _default_job_name(connector_name, self._dataset)
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty job name.")
        self._name = name.strip()
        self._job = job
        # The job's configuration as the server holds it, so a save keeps the fields
        # this class does not model (description, envVars, defaultArgs, ...).
        self._server_config: dict[str, Any] | None = None
        self._remember_server_config()

    def _remember_server_config(self) -> None:
        config = self._job.config if self._job is not None else None
        self._server_config = (
            copy.deepcopy(config) if isinstance(config, dict) else None
        )

    @public
    @classmethod
    def from_job(cls, job: job.Job) -> FilesIngestionJob:
        """Describe an existing FILES-mode ingestion job.

        Parameters:
            job: A job whose configuration is an `ingestionJobConfiguration` with `mode` `FILES`.

        Returns:
            The files ingestion job backed by that job.

        Raises:
            ValueError: If the job is not a FILES-mode ingestion job.
        """
        config = job.config if isinstance(job.config, dict) else {}
        if (
            config.get("type") != DTO_TYPE
            or config.get("mode") != MODE_FILES
            or not isinstance(config.get("filesConfig"), dict)
        ):
            raise ValueError(
                f"Job '{job.name}' is not a files ingestion job (mode FILES)."
            )
        files = config["filesConfig"]
        if not files.get("datasetName"):
            raise ValueError(
                f"Job '{job.name}' is a files ingestion job without a datasetName."
            )
        index_name = files.get("indexFeatureGroupName")
        index: bool | str = (
            index_name if (files.get("indexFiles") and index_name) else False
        )
        if files.get("indexFiles") and not index_name:
            index = True
        schedule = job.job_schedule.cron_expression if job.job_schedule else None
        return cls(
            None,
            files["datasetName"],
            prefix=files.get("prefix") or "",
            include=files.get("include") or [],
            exclude=files.get("exclude") or [],
            mirror_deletions=bool(files.get("mirrorDeletions")),
            index=index,
            max_file_size_mb=files.get("maxFileSizeMB"),
            schedule=schedule,
            resources=config.get("resourceConfig"),
            name=job.name,
            fail_on_first_error=bool(files.get("failOnFirstError")),
            files_per_page=files.get("filesPerPage") or DEFAULT_FILES_PER_PAGE,
            environment_name=config.get("environmentName"),
            storage_connector_id=files.get("storageConnectorId"),
            featurestore_id=config.get("featurestoreId"),
            job=job,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the job as the `ingestionJobConfiguration` body of a FILES-mode job.

        The settings this class models are laid over the configuration the server holds, key by key inside `filesConfig`, so fields it does not model survive a save.

        Returns:
            The request body sent to the jobs endpoint.
        """
        body = self._modelled_config()
        if self._server_config is None:
            return body
        merged = copy.deepcopy(self._server_config)
        server_files = merged.get("filesConfig")
        merged.update({k: v for k, v in body.items() if k != "filesConfig"})
        merged["filesConfig"] = {
            **(server_files if isinstance(server_files, dict) else {}),
            **body["filesConfig"],
        }
        return merged

    def _modelled_config(self) -> dict[str, Any]:
        index_files = self._index is not False
        body: dict[str, Any] = {
            "type": DTO_TYPE,
            "appName": self._name,
            "jobType": "INGESTION",
            "featurestoreId": self._featurestore_id,
            "mode": MODE_FILES,
            "filesConfig": {
                "storageConnectorId": self._storage_connector_id,
                "prefix": self._prefix,
                "include": list(self._include),
                "exclude": list(self._exclude),
                "datasetName": self._dataset,
                "mirrorDeletions": self._mirror_deletions,
                "indexFiles": index_files,
                "indexFeatureGroupName": (
                    self._index if isinstance(self._index, str) else None
                ),
                "maxFileSizeMB": self._max_file_size_mb,
                "filesPerPage": self._files_per_page,
                "failOnFirstError": self._fail_on_first_error,
            },
        }
        if self._resources is not None:
            body["resourceConfig"] = dict(self._resources)
        if self._environment_name is not None:
            body["environmentName"] = self._environment_name
        return body

    def json(self) -> str:
        return json.dumps(self.to_dict())

    def __repr__(self) -> str:
        return (
            f"FilesIngestionJob(name={self._name!r}, dataset={self._dataset!r}, "
            f"prefix={self._prefix!r}, saved={self._job is not None})"
        )

    @public
    @property
    def name(self) -> str:
        """Name of the ingestion job."""
        return self._name

    @public
    @property
    def job(self) -> job.Job | None:
        """The job on the server, or `None` until [`FilesIngestionJob.save`][hsfs.core.files_ingestion.FilesIngestionJob.save] has run."""
        return self._job

    @public
    @property
    def dataset_name(self) -> str:
        """Name of the dataset the files are copied into."""
        return self._dataset

    @public
    @property
    def prefix(self) -> str:
        """Prefix inside the bucket or container, relative to its root."""
        return self._prefix

    @public
    @property
    def include(self) -> list[str]:
        """Glob patterns of the files to copy; every file when empty."""
        return list(self._include)

    @public
    @property
    def exclude(self) -> list[str]:
        """Glob patterns of the files to skip."""
        return list(self._exclude)

    @public
    @property
    def mirror_deletions(self) -> bool:
        """Whether files gone from the source are deleted from the dataset."""
        return self._mirror_deletions

    @public
    @property
    def index_feature_group(self) -> str | None:
        """Name of the index feature group, or `None` when the files are not indexed."""
        if self._index is False:
            return None
        if self._index is True:
            return _default_index_name(self._dataset)
        return self._index

    @public
    @property
    def max_file_size_mb(self) -> int | None:
        """Size cap of the files copied, in MB; `None` or `0` copies any size."""
        return self._max_file_size_mb

    @public
    @property
    def storage_connector_id(self) -> int | None:
        """Id of the data source the files are read from."""
        return self._storage_connector_id

    @public
    @property
    def schedule(self) -> str | None:
        """Cron expression of the job schedule, or `None`."""
        return self._schedule

    @public
    def save(self) -> job.Job:
        """Create the job on the server, or update it with the current settings.

        Sends the job configuration in one request, then creates, updates or removes the schedule.
        Only data owners of the project may create or update files ingestion jobs.

        Returns:
            The created or updated job.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the data source cannot copy files, the dataset belongs to another files job, or the caller is not a data owner.
        """
        from hopsworks_common.core.job_api import JobApi

        job_api = JobApi()
        self._job = job_api.create(self._name, self)
        self._remember_server_config()
        self._apply_schedule(job_api)
        return self._job

    def _apply_schedule(self, job_api) -> None:
        from hopsworks_common.job_schedule import JobSchedule

        existing = self._job.job_schedule if self._job is not None else None
        if self._schedule is None:
            if existing is not None:
                job_api.delete_schedule_job(self._name)
                self._job._job_schedule = None
            return
        if existing is not None and existing.cron_expression == self._schedule:
            return
        job_schedule = JobSchedule(
            id=existing.id if existing is not None else None,
            start_date_time=(
                existing.start_date_time
                if existing is not None and existing.start_date_time
                else datetime.now(tz=timezone.utc)
            ),
            # a paused schedule stays paused when its expression changes
            enabled=existing.enabled if existing is not None else True,
            cron_expression=self._schedule,
        )
        self._job._job_schedule = job_api.create_or_update_schedule_job(
            self._name, job_schedule
        )

    @public
    def run(self, await_termination: bool = False) -> Execution:
        """Start a run of the job, creating the job first if it has not been saved.

        A run in which some files failed to copy ends as `FAILED` by design, with the successful copies kept; when awaiting, such a run is returned like any other instead of raising, so its
        [`files_progress`][hopsworks.execution.Execution.files_progress]
        can be read for the counters and the failed files.

        Parameters:
            await_termination: Whether to block until the run finishes.

        Returns:
            The started execution, or the finished one when awaiting; its `files_progress` carries the copy counters once the run has finished.
        """
        from hopsworks_common.core import execution_api
        from hopsworks_common.engine import execution_engine

        if self._job is None:
            self.save()
        execution = self._job.run(await_termination=False)
        if not await_termination or execution is None:
            return execution
        try:
            finished = execution_engine.ExecutionEngine()._wait_until_finished(
                self._job, execution
            )
        except JobExecutionException:
            finished = None
        if finished is None:
            finished = execution_api.ExecutionApi()._get(self._job, execution.id)
        return finished

    @public
    @property
    def last_stats(self) -> dict[str, Any] | None:
        """The `filesProgress` of the most recent execution, as [`Execution.files_progress`][hopsworks.execution.Execution.files_progress] describes it: `status`, `listed`, `copied`, `updated`, `skipped`, `deleted`, `failed`, `oversized`, `bytesCopied`, `failedFiles`, `auditFile`, `auditPath` and more; `None` before the first run and while the latest run has not written its result yet."""
        if self._job is None:
            return None
        from hopsworks_common.core.job_api import JobApi

        execution = JobApi().last_execution(self._job)
        # a collection response is a list, empty when the job never ran
        if isinstance(execution, list):
            execution = execution[0] if execution else None
        if execution is None:
            return None
        return execution.files_progress

    @public
    def update(self, **fields: Any) -> FilesIngestionJob:
        """Change settings of the job and save it; they apply from the next run.

        The keyword arguments are any of `prefix`, `include`, `exclude`, `mirror_deletions`, `index`, `max_file_size_mb`, `fail_on_first_error`, `files_per_page`, `resources`, `schedule` (`None` removes it) and `environment_name`.
        Fields not given keep their value.
        The data source and the dataset cannot change; create another job for them.
        An index feature group name can only be set while indexing was off.

        Returns:
            This job, saved.

        Raises:
            ValueError: If a field cannot be changed, such as `dataset` or `name`, or a value is invalid.
        """
        unknown = sorted(set(fields) - set(_EDITABLE))
        if unknown:
            raise ValueError(
                f"Cannot update {', '.join(unknown)} of files ingestion job "
                f"{self._name}; updatable fields are {', '.join(_EDITABLE)}."
            )
        if "prefix" in fields:
            self._prefix = _validate_prefix(fields["prefix"])
        if "include" in fields:
            self._include = _validate_globs(fields["include"], "include")
        if "exclude" in fields:
            self._exclude = _validate_globs(fields["exclude"], "exclude")
        if "mirror_deletions" in fields:
            self._mirror_deletions = bool(fields["mirror_deletions"])
        if "index" in fields:
            self._index = _validate_index(fields["index"])
        if "max_file_size_mb" in fields:
            self._max_file_size_mb = _validate_max_file_size(fields["max_file_size_mb"])
        if "fail_on_first_error" in fields:
            self._fail_on_first_error = bool(fields["fail_on_first_error"])
        if "files_per_page" in fields:
            self._files_per_page = _validate_files_per_page(fields["files_per_page"])
        if "resources" in fields:
            self._resources = _validate_resources(fields["resources"])
        if "schedule" in fields:
            self._schedule = _validate_schedule(fields["schedule"])
        if "environment_name" in fields:
            self._environment_name = fields["environment_name"]
        self.save()
        return self

    @public
    def delete(self, delete_files: bool = False, delete_index: bool = False) -> None:
        """Delete the job, its executions and its state directory.

        Danger: Deletes the copied files and the index when asked
            With `delete_files` the dataset and every file in it are deleted; with `delete_index` the index feature group is, when this job created it.
            Neither is recoverable from Hopsworks afterwards.

        Parameters:
            delete_files: Also delete the dataset the files were copied into.
            delete_index: Also delete the index feature group this job created.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hopsworks_common.core.job_api import JobApi

        JobApi()._delete_files_job(self._name, delete_files, delete_index)
        self._job = None
