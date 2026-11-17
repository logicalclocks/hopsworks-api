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
import json
from unittest.mock import Mock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsfs.core import sharepoint_sync


_SYNC = [
    "project",
    119,
    "featurestores",
    67,
    "storageconnectors",
    "finance_sharepoint",
    "sharepoint",
    "syncs",
    5,
]

_DTO = {
    "id": 5,
    "connectorName": "finance_sharepoint",
    "driveId": "b!1",
    "driveName": "Documents",
    "folderItemId": "item-1",
    "folderPath": "Reports/2026",
    "datasetName": "sharepoint",
    "datasetId": 11,
    "jobId": 21,
    "jobName": "sharepoint_finance_sharepoint_sharepoint",
    "scheduled": True,
    "cronExpression": "0 0 * ? * *",
    "paused": False,
    "alertReceiver": "ops",
    "includePatterns": ["*.pdf"],
    "excludePatterns": [],
    "maxFileSizeMb": 1024,
    "mirrorDeletions": True,
    "verifyChecksums": False,
    "checksumEvery": 24,
    "transfers": None,
    "indexFiles": True,
    "indexFeatureGroup": "sharepoint_sharepoint",
    "indexFeatureGroupId": 31,
    "state": "IDLE",
    "activeExecutionId": None,
    "lastRunExecutionId": 812,
    "lastError": None,
    "lastAuditFile": "DataSources/finance_sharepoint/audit/sharepoint/date=2026-10-20/101500Z_812.csv",
    "auditPath": "/Projects/finance/DataSources/finance_sharepoint/audit/sharepoint",
    "filesTotal": 996,
    "bytesTotal": 123456789,
}


def _client(mocker, response=None, side_effect=None):
    client_mock = Mock()
    client_mock._project_id = 119
    client_mock._send_request.return_value = response
    if side_effect is not None:
        client_mock._send_request.side_effect = side_effect
    mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
    return client_mock


def _sync() -> sharepoint_sync.SharePointSync:
    return sharepoint_sync.SharePointSync.from_response_json(_DTO, 67)


def test_from_response_json():
    sync = _sync()

    assert sync.id == 5
    assert sync.connector_name == "finance_sharepoint"
    assert sync.drive_name == "Documents"
    assert sync.folder_path == "Reports/2026"
    assert sync.scheduled is True
    assert sync.cron_expression == "0 0 * ? * *"
    assert sync.include_patterns == ["*.pdf"]
    assert sync.max_file_size_mb == 1024
    assert sync.audit_path.endswith("/audit/sharepoint")
    assert sync.files_total == 996
    assert sync.to_dict()["lastAuditFile"] == _DTO["lastAuditFile"]
    assert sync.index_feature_group_id == 31
    assert sync.to_dict()["indexFeatureGroupId"] == 31


def test_status_reloads_the_sync(mocker):
    client = _client(mocker, {**_DTO, "state": "FAILED", "lastError": "boom"})
    sync = _sync()

    status = sync.status()

    assert client._send_request.call_args.args == ("GET", _SYNC)
    assert status["state"] == "FAILED"
    assert status["last_error"] == "boom"
    assert status["files_total"] == 996
    assert sync.state == "FAILED"


def test_run_returns_the_execution(mocker):
    client = _client(mocker, {"id": 900, "state": "INITIALIZING"})
    job = Mock()
    get_job = mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)

    execution = _sync().run()

    assert client._send_request.call_args.args == ("POST", [*_SYNC, "run"])
    get_job.assert_called_once_with("sharepoint_finance_sharepoint_sharepoint")
    assert execution.id == 900
    assert execution._job is job


def test_run_hands_back_a_response_that_is_no_execution(mocker):
    _client(mocker, None)

    assert _sync().run() is None


_EXECUTION = {
    "id": 901,
    "state": "INITIALIZING",
    "finalStatus": "UNDEFINED",
    "submissionTime": "2026-10-22T10:15:00.000Z",
    "args": "",
}


def test_retry_updates_the_sync_from_its_response(mocker):
    client = _client(mocker, {**_DTO, "state": "IDLE"})
    sync = sharepoint_sync.SharePointSync.from_response_json(
        {**_DTO, "state": "CREATING"}, 67
    )

    assert sync.retry() is sync

    assert client._send_request.call_args.args == ("POST", [*_SYNC, "retry"])
    assert sync.state == "IDLE"


@pytest.mark.parametrize("action", ["pause", "resume"])
def test_pause_and_resume_reload_the_sync_after_no_content(mocker, action):
    paused = action == "pause"
    client = _client(mocker, side_effect=[None, {**_DTO, "paused": paused}])
    sync = sharepoint_sync.SharePointSync.from_response_json(
        {**_DTO, "paused": not paused}, 67
    )

    assert getattr(sync, action)() is sync

    calls = client._send_request.call_args_list
    assert [c.args for c in calls] == [("POST", [*_SYNC, action]), ("GET", _SYNC)]
    assert sync.paused is paused


def test_force_full_resync_returns_the_execution_and_keeps_the_sync(mocker):
    requested = "2026-10-22T10:15:00.000Z"
    client = _client(
        mocker, side_effect=[_EXECUTION, {**_DTO, "resyncRequestedAt": requested}]
    )
    job = Mock()
    get_job = mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)
    sync = _sync()

    execution = sync.force_full_resync()

    calls = client._send_request.call_args_list
    assert [c.args for c in calls] == [("POST", [*_SYNC, "resync"]), ("GET", _SYNC)]
    assert calls[0].kwargs["query_params"] == {"run": True}
    get_job.assert_called_once_with("sharepoint_finance_sharepoint_sharepoint")
    assert execution.id == 901
    assert execution._job is job
    assert sync.id == 5
    assert sync.dataset_name == "sharepoint"
    assert sync.resync_requested_at == requested


def test_force_full_resync_without_run_returns_none(mocker):
    requested = "2026-10-22T10:15:00.000Z"
    client = _client(
        mocker, side_effect=[None, {**_DTO, "resyncRequestedAt": requested}]
    )
    get_job = mocker.patch("hopsworks_common.core.job_api.JobApi.get")
    sync = _sync()

    assert sync.force_full_resync(run=False) is None

    calls = client._send_request.call_args_list
    assert [c.args for c in calls] == [("POST", [*_SYNC, "resync"]), ("GET", _SYNC)]
    assert calls[0].kwargs["query_params"] == {"run": False}
    get_job.assert_not_called()
    assert sync.id == 5
    assert sync.resync_requested_at == requested


def test_set_schedule_none_sends_an_explicit_null(mocker):
    client = _client(mocker, {**_DTO, "scheduled": False, "cronExpression": None})
    sync = _sync()

    sync.set_schedule(None)

    call = client._send_request.call_args
    assert call.args == ("PUT", _SYNC)
    assert json.loads(call.kwargs["data"]) == {"cronExpression": None}
    assert sync.scheduled is False


def test_set_folder(mocker):
    client = _client(mocker, _DTO)

    _sync().set_folder("Reports/2027")

    assert json.loads(client._send_request.call_args.kwargs["data"]) == {
        "folderPath": "Reports/2027"
    }


def test_update_sends_only_the_given_fields(mocker):
    client = _client(mocker, _DTO)

    _sync().update(exclude_patterns=["tmp/**"], max_file_size_mb=0, take_ownership=True)

    assert json.loads(client._send_request.call_args.kwargs["data"]) == {
        "excludePatterns": ["tmp/**"],
        "maxFileSizeMb": 0,
        "takeOwnership": True,
    }


def test_update_sends_the_index_fields(mocker):
    client = _client(mocker, _DTO)

    _sync().update(index_files=True, index_feature_group="docs")

    assert json.loads(client._send_request.call_args.kwargs["data"]) == {
        "indexFiles": True,
        "indexFeatureGroup": "docs",
    }


def test_get_index_feature_group_needs_indexing():
    sync = sharepoint_sync.SharePointSync.from_response_json(
        dict(_DTO, indexFiles=False, indexFeatureGroup=None, indexFeatureGroupId=None),
        67,
    )
    with pytest.raises(ValueError, match="does not index"):
        sync.get_index_feature_group()


def test_update_refuses_immutable_fields(mocker):
    client = _client(mocker, _DTO)

    with pytest.raises(ValueError, match="dataset_name"):
        _sync().update(dataset_name="other")

    client._send_request.assert_not_called()


def _not_found():
    response = Mock()
    response.status_code = 404
    response.json.return_value = {}
    return RestAPIError("url", response)


def test_delete_returns_true_once_the_sync_is_gone(mocker):
    client = _client(mocker, side_effect=[None, _not_found()])

    assert _sync().delete(keep_files=True, force=True) is True

    delete, get = client._send_request.call_args_list
    assert delete.args == ("DELETE", _SYNC)
    assert delete.kwargs["query_params"] == {"keepFiles": True, "force": True}
    assert get.args == ("GET", _SYNC)


def test_delete_returns_false_while_the_sync_is_still_deleting(mocker):
    client = _client(mocker, side_effect=[None, dict(_DTO, state="DELETING")])
    sync = _sync()

    assert sync.delete(force=True) is False

    assert client._send_request.call_count == 2
    assert sync.state == "DELETING"


def test_get_audit_pages_through_the_rows(mocker):
    row = {
        "date": "2026-10-20",
        "executionId": 812,
        "op": "added",
        "kind": "file",
        "path": "a.csv",
        "srcPath": "",
    }
    pages = [
        {"count": 1001, "items": [row] * sharepoint_sync._AUDIT_PAGE},
        {"count": 1001, "items": [{**row, "op": "renamed", "srcPath": "b.csv"}]},
    ]
    client = _client(mocker, side_effect=pages)

    rows = _sync().get_audit(day=datetime.date(2026, 10, 20))

    assert len(rows) == 1001
    assert rows[-1] == {
        "date": "2026-10-20",
        "execution_id": 812,
        "op": "renamed",
        "kind": "file",
        "path": "a.csv",
        "src_path": "b.csv",
    }
    calls = client._send_request.call_args_list
    assert calls[0].args == ("GET", [*_SYNC, "audit"])
    assert calls[0].kwargs["query_params"] == {
        "offset": 0,
        "limit": sharepoint_sync._AUDIT_PAGE,
        "day": "2026-10-20",
    }
    assert calls[1].kwargs["query_params"]["offset"] == sharepoint_sync._AUDIT_PAGE


def test_get_audit_without_a_day_leaves_it_to_the_backend(mocker):
    client = _client(mocker, {"count": 0, "items": []})

    assert _sync().get_audit() == []
    assert "day" not in client._send_request.call_args.kwargs["query_params"]
