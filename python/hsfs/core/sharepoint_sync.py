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

import datetime
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import execution
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import job_api
from hsfs.core import sharepoint_api


if TYPE_CHECKING:
    from hopsworks_common.execution import Execution
    from hsfs.feature_group import FeatureGroup


# Fields of SharePointSyncDTO that PUT /syncs/{id} accepts; driveId and datasetName are fixed at creation.
_UPDATABLE = (
    "include_patterns",
    "exclude_patterns",
    "max_file_size_mb",
    "mirror_deletions",
    "verify_checksums",
    "checksum_every",
    "transfers",
    "index_files",
    "index_feature_group",
    "cron_expression",
    "folder_path",
    "take_ownership",
)

_STATUS_FIELDS = (
    "state",
    "paused",
    "scheduled",
    "cron_expression",
    "active_execution_id",
    "resync_requested_at",
    "last_run_execution_id",
    "last_run_started",
    "last_run_finished",
    "last_success",
    "last_error",
    "last_audit_file",
    "files_total",
    "bytes_total",
)

_AUDIT_PAGE = 1000


def _is_execution(response: Any) -> bool:
    return isinstance(response, dict) and response.get("id") is not None


@public
class SharePointSync:
    """A sync that copies a folder of a SharePoint document library into a dataset of the project.

    The copy runs as an ordinary Python job, either once when started or on a cron schedule, with rsync semantics from SharePoint to Hopsworks.
    Create one with [`SharePointConnector.create_sync`][hsfs.storage_connector.SharePointConnector.create_sync] or [`SharePointConnector.copy_folder`][hsfs.storage_connector.SharePointConnector.copy_folder].

    Every method that changes the sync needs the Data Owner role; reading its status and audit log needs project membership.
    """

    def __init__(
        self,
        id: int | None = None,
        connector_name: str | None = None,
        drive_id: str | None = None,
        drive_name: str | None = None,
        folder_item_id: str | None = None,
        folder_path: str | None = None,
        dataset_name: str | None = None,
        dataset_id: int | None = None,
        job_id: int | None = None,
        job_name: str | None = None,
        scheduled: bool | None = None,
        cron_expression: str | None = None,
        paused: bool | None = None,
        alert_receiver: str | None = None,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        max_file_size_mb: int | None = None,
        mirror_deletions: bool | None = None,
        verify_checksums: bool | None = None,
        checksum_every: int | None = None,
        transfers: int | None = None,
        index_files: bool | None = None,
        index_feature_group: str | None = None,
        index_feature_group_id: int | None = None,
        state: str | None = None,
        active_execution_id: int | None = None,
        resync_requested_at: Any = None,
        last_run_execution_id: int | None = None,
        last_run_started: Any = None,
        last_run_finished: Any = None,
        last_success: Any = None,
        last_error: str | None = None,
        last_audit_file: str | None = None,
        audit_path: str | None = None,
        files_total: int | None = None,
        bytes_total: int | None = None,
        created_by: Any = None,
        created_on: Any = None,
        featurestore_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        self._id = id
        self._connector_name = connector_name
        self._drive_id = drive_id
        self._drive_name = drive_name
        self._folder_item_id = folder_item_id
        self._folder_path = folder_path
        self._dataset_name = dataset_name
        self._dataset_id = dataset_id
        self._job_id = job_id
        self._job_name = job_name
        self._scheduled = scheduled
        self._cron_expression = cron_expression
        self._paused = paused
        self._alert_receiver = alert_receiver
        self._include_patterns = include_patterns or []
        self._exclude_patterns = exclude_patterns or []
        self._max_file_size_mb = max_file_size_mb
        self._mirror_deletions = mirror_deletions
        self._verify_checksums = verify_checksums
        self._checksum_every = checksum_every
        self._transfers = transfers
        self._index_files = index_files
        self._index_feature_group = index_feature_group
        self._index_feature_group_id = index_feature_group_id
        self._state = state
        self._active_execution_id = active_execution_id
        self._resync_requested_at = resync_requested_at
        self._last_run_execution_id = last_run_execution_id
        self._last_run_started = last_run_started
        self._last_run_finished = last_run_finished
        self._last_success = last_success
        self._last_error = last_error
        self._last_audit_file = last_audit_file
        self._audit_path = audit_path
        self._files_total = files_total
        self._bytes_total = bytes_total
        self._created_by = created_by
        self._created_on = created_on
        self._featurestore_id = featurestore_id
        self._sharepoint_api = sharepoint_api.SharePointApi()

    @classmethod
    def from_response_json(
        cls,
        json_dict: dict[str, Any],
        featurestore_id: int,
        connector_name: str | None = None,
    ) -> SharePointSync:
        decamelized = humps.decamelize(json_dict)
        if connector_name and not decamelized.get("connector_name"):
            decamelized["connector_name"] = connector_name
        decamelized["featurestore_id"] = featurestore_id
        return cls(**decamelized)

    def update_from_response_json(self, json_dict: dict[str, Any]) -> SharePointSync:
        decamelized = humps.decamelize(json_dict)
        decamelized.setdefault("connector_name", self._connector_name)
        self.__init__(**{**decamelized, "featurestore_id": self._featurestore_id})
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "connectorName": self._connector_name,
            "driveId": self._drive_id,
            "driveName": self._drive_name,
            "folderItemId": self._folder_item_id,
            "folderPath": self._folder_path,
            "datasetName": self._dataset_name,
            "datasetId": self._dataset_id,
            "jobId": self._job_id,
            "jobName": self._job_name,
            "scheduled": self._scheduled,
            "cronExpression": self._cron_expression,
            "paused": self._paused,
            "alertReceiver": self._alert_receiver,
            "includePatterns": self._include_patterns,
            "excludePatterns": self._exclude_patterns,
            "maxFileSizeMb": self._max_file_size_mb,
            "mirrorDeletions": self._mirror_deletions,
            "verifyChecksums": self._verify_checksums,
            "checksumEvery": self._checksum_every,
            "transfers": self._transfers,
            "indexFiles": self._index_files,
            "indexFeatureGroup": self._index_feature_group,
            "indexFeatureGroupId": self._index_feature_group_id,
            "state": self._state,
            "activeExecutionId": self._active_execution_id,
            "resyncRequestedAt": self._resync_requested_at,
            "lastRunExecutionId": self._last_run_execution_id,
            "lastRunStarted": self._last_run_started,
            "lastRunFinished": self._last_run_finished,
            "lastSuccess": self._last_success,
            "lastError": self._last_error,
            "lastAuditFile": self._last_audit_file,
            "auditPath": self._audit_path,
            "filesTotal": self._files_total,
            "bytesTotal": self._bytes_total,
            "createdBy": self._created_by,
            "createdOn": self._created_on,
        }

    def __repr__(self) -> str:
        return (
            f"SharePointSync(id={self._id!r}, dataset_name={self._dataset_name!r}, "
            f"folder_path={self._folder_path!r}, state={self._state!r})"
        )

    def _apply(self, response: Any) -> SharePointSync:
        if isinstance(response, dict) and response.get("id") is not None:
            self.update_from_response_json(response)
        return self

    def _action(self, action: str, query_params: dict[str, Any] | None = None) -> Any:
        return self._sharepoint_api._sync_action(
            self._featurestore_id,
            self._connector_name,
            self._id,
            action,
            query_params,
        )

    @public
    def refresh(self) -> SharePointSync:
        """Reload the sync from Hopsworks.

        Returns:
            This sync, with its fields as the backend reports them now.
        """
        return self.update_from_response_json(
            self._sharepoint_api._get_sync(
                self._featurestore_id, self._connector_name, self._id
            )
        )

    @public
    def status(self) -> dict[str, Any]:
        """Reload the sync and return its run state and counters.

        A run that ended without reporting a result, because its pod was killed or crashed, is reported as `FAILED` with `last_error` saying so.

        Returns:
            `state`, `paused`, `scheduled`, `cron_expression`, `active_execution_id`, `resync_requested_at`, the `last_run_*` fields, `last_success`, `last_error`, `last_audit_file`, `files_total` and `bytes_total`.
        """
        self.refresh()
        return {field: getattr(self, f"_{field}") for field in _STATUS_FIELDS}

    @public
    def run(self) -> Execution | Any:
        """Start an execution of the sync job now, also while the sync is paused or has no schedule.

        The execution runs as the caller, who must be a Data Owner.
        When a run of this sync is already in progress, the new execution finishes immediately without copying anything.

        Returns:
            The started execution, or the backend's response when it does not describe one.
        """
        response = self._action("run")
        if not _is_execution(response):
            return response
        if not self._job_name:
            self.refresh()
        return self._execution(response)

    def _execution(self, response: dict[str, Any]) -> Execution:
        job = job_api.JobApi().get(self._job_name)
        return execution.Execution.from_response_json(response, job=job)

    @public
    def retry(self) -> SharePointSync:
        """Resume a sync left in `CREATING` or `DELETING` by a failed create or delete.

        Returns:
            This sync, updated.
        """
        return self._apply(self._action("retry"))

    @public
    def pause(self) -> SharePointSync:
        """Disable the schedule, keeping it so that [`resume`][hsfs.core.sharepoint_sync.SharePointSync.resume] restores it.

        Returns:
            This sync, updated.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the sync has no schedule.
        """
        self._action("pause")
        return self.refresh()

    @public
    def resume(self) -> SharePointSync:
        """Re-enable a paused schedule.

        Returns:
            This sync, updated.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the sync has no schedule.
        """
        self._action("resume")
        return self.refresh()

    @public
    def force_full_resync(self, run: bool = True) -> Execution | None:
        """Make the next run compare file contents by checksum, not only size and modification time.

        A run normally detects a changed file by its size or modification time, so a file rewritten with the same size and time is only caught by a checksum run.
        The sync is reloaded afterwards, so `resync_requested_at` shows the pending request.

        Parameters:
            run: Start the run now; otherwise the next scheduled or manual run does the full comparison.

        Returns:
            The started execution, or `None` when `run` is not set.
        """
        response = self._action("resync", {"run": run})
        self.refresh()
        return self._execution(response) if _is_execution(response) else None

    @public
    def update(self, **fields: Any) -> SharePointSync:
        """Change settings of the sync; they apply from the next run.

        The keyword arguments are any of `include_patterns`, `exclude_patterns`, `max_file_size_mb`, `mirror_deletions`, `verify_checksums`, `checksum_every`, `transfers`, `index_files`, `index_feature_group`, `cron_expression` (`None` removes the schedule), `folder_path` and `take_ownership`.
        Fields not given keep their value.
        Under mirror semantics, files that a new filter or size cap excludes are deleted from the dataset on the next run.
        `index_files=False` stops indexing and keeps the feature group; a new `index_feature_group` starts a new one, which the next run fills with every copied file still in the folder.
        `take_ownership=True` re-creates the schedule as the caller, so scheduled runs run as the caller, which adopts a sync whose creator left the project.

        Returns:
            This sync, updated.

        Raises:
            ValueError: If a field cannot be changed, such as `drive_id` or `dataset_name`.
        """
        unknown = sorted(set(fields) - set(_UPDATABLE))
        if unknown:
            raise ValueError(
                f"Cannot update {', '.join(unknown)} of SharePoint sync {self._id}; "
                f"updatable fields are {', '.join(_UPDATABLE)}."
            )
        return self._apply(
            self._sharepoint_api._update_sync(
                self._featurestore_id,
                self._connector_name,
                self._id,
                humps.camelize(fields),
            )
        )

    @public
    def set_schedule(self, cron_expression: str | None) -> SharePointSync:
        """Add, change or remove the schedule.

        Parameters:
            cron_expression:
                Quartz cron expression in UTC, with seconds first, e.g. `"0 0 * ? * *"` for hourly or `"0 0 2 ? * *"` for daily at 02:00.
                `None` removes the schedule, so the sync runs only when started.
                Runs closer together than the cluster's minimum interval (5 minutes by default) are refused.

        Returns:
            This sync, updated.
        """
        return self.update(cron_expression=cron_expression)

    @public
    def set_folder(self, folder_path: str) -> SharePointSync:
        """Point the sync at another folder of the same document library.

        Under mirror semantics the next run deletes the files of the old folder from the dataset and copies the new one.

        Parameters:
            folder_path: Folder path inside the library, `""` for the library root.

        Returns:
            This sync, updated.
        """
        return self.update(folder_path=folder_path)

    @public
    def delete(self, keep_files: bool = False, force: bool = False) -> bool:
        """Delete the sync and its job.

        With `force`, the backend stops a run in progress first; when that run does not stop in time the delete is left pending.
        A pending delete keeps the sync in state `DELETING`, and [`retry`][hsfs.core.sharepoint_sync.SharePointSync.retry] or another `delete` finishes it.

        Danger: Deletes the synced dataset unless `keep_files` is set
            Without `keep_files`, the dataset and the sync's audit log are deleted with every file in them, and so is the index feature group of a sync that indexes its files.
            Files are not recoverable from Hopsworks afterwards; only SharePoint still has them.

        Parameters:
            keep_files: Keep the dataset, the audit log and the index feature group; the audit log is then no longer rotated.
            force: Stop a run in progress instead of refusing the delete.

        Returns:
            `True` when the sync is deleted, `False` when the delete is still pending.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If a run is in progress and `force` is not set.
        """
        self._sharepoint_api._delete_sync(
            self._featurestore_id,
            self._connector_name,
            self._id,
            keep_files,
            force,
        )
        # The client maps both 204 (deleted) and an empty 202 (stop timed out) to None.
        try:
            self.refresh()
        except RestAPIError as err:
            if err.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND:
                return True
            raise
        return False

    @public
    def get_audit(self, day: str | datetime.date | None = None) -> list[dict[str, Any]]:
        """Return the paths changed by the runs of one day, as recorded in the audit log.

        Runs that changed nothing write no audit rows.
        The backend keeps a cluster-wide number of days, 7 by default.

        Parameters:
            day: Day in UTC, as `YYYY-MM-DD` or a date; today when not given.

        Returns:
            One dict per changed path with `date`, `execution_id`, `op` (`added`, `updated`, `deleted` or `renamed`), `kind` (`file` or `dir`), `path` and `src_path` (the old path of a rename).
        """
        if isinstance(day, datetime.date):
            day = day.isoformat()
        rows: list[dict[str, Any]] = []
        while True:
            page = self._sharepoint_api._get_audit(
                self._featurestore_id,
                self._connector_name,
                self._id,
                day,
                len(rows),
                _AUDIT_PAGE,
            )
            items = sharepoint_api._items(page)
            rows.extend(humps.decamelize(items))
            count = page.get("count") if isinstance(page, dict) else None
            if len(items) < _AUDIT_PAGE or (count is not None and len(rows) >= count):
                return rows

    @public
    @property
    def id(self) -> int | None:
        """Id of the sync, unique in the project."""
        return self._id

    @public
    @property
    def connector_name(self) -> str | None:
        """Name of the SharePoint data source the sync reads from."""
        return self._connector_name

    @public
    @property
    def drive_id(self) -> str | None:
        """Graph id of the document library, fixed at creation."""
        return self._drive_id

    @public
    @property
    def drive_name(self) -> str | None:
        """Display name of the document library."""
        return self._drive_name

    @public
    @property
    def folder_item_id(self) -> str | None:
        """Graph id of the synced folder, empty for the library root."""
        return self._folder_item_id

    @public
    @property
    def folder_path(self) -> str | None:
        """Path of the synced folder inside the library, empty for the library root."""
        return self._folder_path

    @public
    @property
    def dataset_name(self) -> str | None:
        """Top-level dataset of the project the files are copied into, fixed at creation."""
        return self._dataset_name

    @public
    @property
    def dataset_id(self) -> int | None:
        """Id of the synced dataset, `None` once it was deleted outside the sync."""
        return self._dataset_id

    @public
    @property
    def job_id(self) -> int | None:
        """Id of the Python job that runs the sync."""
        return self._job_id

    @public
    @property
    def job_name(self) -> str | None:
        """Name of the Python job that runs the sync, `sharepoint_<connector>_<dataset>`."""
        return self._job_name

    @public
    @property
    def scheduled(self) -> bool | None:
        """Whether the sync has a schedule; a sync without one copies only when started."""
        return self._scheduled

    @public
    @property
    def cron_expression(self) -> str | None:
        """Cron expression of the schedule, `None` for a copy-once sync."""
        return self._cron_expression

    @public
    @property
    def paused(self) -> bool | None:
        """Whether the schedule is disabled."""
        return self._paused

    @public
    @property
    def alert_receiver(self) -> str | None:
        """Alert receiver notified when a run of the sync job fails or is killed."""
        return self._alert_receiver

    @public
    @property
    def include_patterns(self) -> list[str]:
        """Filter patterns, in rclone syntax, of the files to copy; empty copies everything not excluded."""
        return self._include_patterns

    @public
    @property
    def exclude_patterns(self) -> list[str]:
        """Filter patterns, in rclone syntax, of the files to skip."""
        return self._exclude_patterns

    @public
    @property
    def max_file_size_mb(self) -> int | None:
        """Files larger than this many MB are skipped; 0 copies files of any size."""
        return self._max_file_size_mb

    @public
    @property
    def mirror_deletions(self) -> bool | None:
        """Whether files gone from SharePoint, or added in Hopsworks, are deleted from the dataset."""
        return self._mirror_deletions

    @public
    @property
    def verify_checksums(self) -> bool | None:
        """Whether every run compares file contents by checksum."""
        return self._verify_checksums

    @public
    @property
    def checksum_every(self) -> int | None:
        """Number of runs between checksum runs when `verify_checksums` is off."""
        return self._checksum_every

    @public
    @property
    def transfers(self) -> int | None:
        """Number of files downloaded in parallel; the cluster default when `None`."""
        return self._transfers

    @public
    @property
    def index_files(self) -> bool | None:
        """Whether each run indexes the dataset's files in a feature group."""
        return self._index_files

    @public
    @property
    def index_feature_group(self) -> str | None:
        """Name of the index feature group (version 1), or `None` when the sync does not index."""
        return self._index_feature_group

    @public
    @property
    def index_feature_group_id(self) -> int | None:
        """Id of the index feature group, or `None` until a run has created it."""
        return self._index_feature_group_id

    @public
    def get_index_feature_group(self) -> FeatureGroup | None:
        """Get the feature group that indexes the sync's files.

        It holds one row per file in the dataset: SharePoint id, path, link, type, size, times, authors, quickXorHash and path in HopsFS, partitioned by `ingestion_date`, the day the file was first indexed.
        The first run after indexing is turned on creates it.

        Returns:
            The index feature group, or `None` until a run has created it.

        Raises:
            ValueError: If the sync does not index its files.
        """
        if not self._index_feature_group:
            raise ValueError(f"SharePoint sync {self._id} does not index its files.")
        from hsfs.core import feature_store_api

        return (
            feature_store_api.FeatureStoreApi()
            ._get(self._featurestore_id)
            .get_feature_group(self._index_feature_group, 1)
        )

    @public
    @property
    def state(self) -> str | None:
        """`CREATING`, `IDLE`, `RUNNING`, `FAILED` or `DELETING`."""
        return self._state

    @public
    @property
    def active_execution_id(self) -> int | None:
        """Execution that holds the sync's run claim, if a run is in progress."""
        return self._active_execution_id

    @public
    @property
    def resync_requested_at(self) -> Any:
        """When a full resync was requested that no run has done yet."""
        return self._resync_requested_at

    @public
    @property
    def last_run_execution_id(self) -> int | None:
        """Execution of the most recent run that reported a result."""
        return self._last_run_execution_id

    @public
    @property
    def last_run_started(self) -> Any:
        """Start of the most recent run."""
        return self._last_run_started

    @public
    @property
    def last_run_finished(self) -> Any:
        """End of the most recent run."""
        return self._last_run_finished

    @public
    @property
    def last_success(self) -> Any:
        """End of the most recent run that copied everything without errors."""
        return self._last_success

    @public
    @property
    def last_error(self) -> str | None:
        """Error of the most recent failed or partial run."""
        return self._last_error

    @public
    @property
    def last_audit_file(self) -> str | None:
        """Audit file written by the most recent run that changed files, relative to the project."""
        return self._last_audit_file

    @public
    @property
    def audit_path(self) -> str | None:
        """Directory of the sync's audit log, with one `date=YYYY-MM-DD` partition per day of CSV files."""
        return self._audit_path

    @public
    @property
    def files_total(self) -> int | None:
        """Number of files in the dataset after the most recent run."""
        return self._files_total

    @public
    @property
    def bytes_total(self) -> int | None:
        """Size of the files in the dataset after the most recent run."""
        return self._bytes_total

    @public
    @property
    def created_by(self) -> Any:
        """User who created the sync; scheduled runs run as this user."""
        return self._created_by

    @public
    @property
    def created_on(self) -> Any:
        """When the sync was created."""
        return self._created_on
