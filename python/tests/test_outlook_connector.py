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

import pytest
from hsfs import storage_connector
from hsfs.core import data_source as ds
from hsfs.storage_connector import OutlookConnector


READ_RESPONSE = {
    "type": "featurestoreOutlookConnectorDTO",
    "id": 4,
    "name": "support outlook",
    "featurestoreId": 67,
    "storageConnectorType": "OUTLOOK",
    "mailbox": "support@contoso.com",
    "authType": "CLIENT_CREDENTIALS",
    "tenantId": "contoso.onmicrosoft.com",
    "clientId": "app-id",
    "credentialsSet": True,
}


def _connector():
    return storage_connector.StorageConnector.from_response_json(READ_RESPONSE)


def test_a_read_has_no_secrets():
    sc = _connector()
    assert isinstance(sc, OutlookConnector)
    assert (sc.mailbox, sc.auth_type, sc.tenant_id, sc.client_id) == (
        "support@contoso.com",
        "CLIENT_CREDENTIALS",
        "contoso.onmicrosoft.com",
        "app-id",
    )
    assert sc._client_secret is None and sc._refresh_token is None


def test_an_update_sends_only_the_secrets_set_locally():
    sc = _connector()
    payload = sc.to_dict()
    assert payload["type"] == "featurestoreOutlookConnectorDTO"
    assert "clientSecret" not in payload and "refreshToken" not in payload
    sc.set_credentials("OAUTH", "public-app", refresh_token="token-value")
    payload = sc.to_dict()
    assert (payload["authType"], payload["clientId"]) == ("OAUTH", "public-app")
    assert payload["refreshToken"] == "token-value"
    assert "tenantId" not in payload and "clientSecret" not in payload
    assert "token-value" not in repr(sc)


def test_get_access_token_carries_the_graph_path(mocker):
    get = mocker.patch(
        "hsfs.core.storage_connector_api.StorageConnectorApi._get_mail_access_token",
        return_value={
            "accessToken": "eyJ0.x",
            "expiresAt": 1761400000000,
            "mailbox": "support@contoso.com",
            "userPath": "users/support%40contoso.com",
        },
    )
    token = _connector().get_access_token()
    get.assert_called_once_with(67, "support outlook", "outlook")
    assert token.user_path == "users/support%40contoso.com"
    assert "eyJ0" not in repr(token)


def test_get_folders_maps_ids_to_paths(mocker):
    mocker.patch(
        "hsfs.core.storage_connector_api.StorageConnectorApi._get_outlook_folders",
        return_value=[
            {"id": "f1", "name": "Inbox", "path": "Inbox", "wellKnownName": "inbox"},
            {"id": "f2", "name": "Projects", "path": "Inbox/Projects"},
        ],
    )
    assert _connector().get_folders() == {"f1": "Inbox", "f2": "Inbox/Projects"}


def test_sync_sends_folders_as_labels_and_no_query(mocker):
    put = mocker.patch(
        "hsfs.core.storage_connector_api.StorageConnectorApi._put_mail_sync_job"
    )
    job = mocker.MagicMock()
    mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)
    _connector().sync(folders=["f1", "f2"], schedule="hourly")
    fs_id, name, service, job_name, body = put.call_args.args
    assert (fs_id, name, service, job_name) == (
        67,
        "support outlook",
        "outlook",
        "support_outlook_outlook_sync",
    )
    assert (body["labels"], body["query"]) == (["f1", "f2"], "")
    job.schedule.assert_called_once_with(cron_expression="0 0 * ? * * *")


def test_list_sync_jobs(mocker):
    get = mocker.patch(
        "hsfs.core.storage_connector_api.StorageConnectorApi._get_mail_sync_jobs",
        return_value=[{"jobName": "j", "featureGroupName": "fg", "path": "p"}],
    )
    assert [j.job_name for j in _connector().list_sync_jobs()] == ["j"]
    get.assert_called_once_with(67, "support outlook", "outlook")


@pytest.mark.parametrize("method", ["get_databases", "get_tables"])
def test_browsing_is_refused(method):
    with pytest.raises(ValueError):
        getattr(_connector(), method)()


def test_a_data_source_syncs_through_the_outlook_connector(mocker):
    sc = _connector()
    sync = mocker.patch.object(OutlookConnector, "sync", return_value="job")
    assert ds.DataSource(storage_connector=sc).sync(schedule="daily") == "job"
    sync.assert_called_once_with(schedule="daily")
