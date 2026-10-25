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

from datetime import datetime, timezone

import pytest
from hsfs import storage_connector
from hsfs.core import data_source as ds
from hsfs.storage_connector import GmailConnector


READ_RESPONSE = {
    "type": "featurestoreGmailConnectorDTO",
    "id": 3,
    "name": "support inbox",
    "featurestoreId": 67,
    "storageConnectorType": "GMAIL",
    "mailbox": "support@example.com",
    "authType": "OAUTH",
    "clientId": "client.apps.googleusercontent.com",
    "credentialsSet": True,
}


def _connector():
    return storage_connector.StorageConnector.from_response_json(READ_RESPONSE)


class TestGmailConnector:
    def test_a_read_has_no_secrets(self):
        sc = _connector()
        assert isinstance(sc, GmailConnector)
        assert sc.mailbox == "support@example.com"
        assert sc.credentials_set is True
        assert sc.client_id == "client.apps.googleusercontent.com"
        assert sc._client_secret is None and sc._refresh_token is None

    def test_an_update_sends_only_the_secrets_set_locally(self):
        sc = _connector()
        sc.mailbox = "help@example.com"
        payload = sc.to_dict()
        assert payload["type"] == "featurestoreGmailConnectorDTO"
        assert payload["mailbox"] == "help@example.com"
        assert (
            "clientSecret" not in payload
            and "refreshToken" not in payload
            and "keyPath" not in payload
        )
        sc.set_credentials("SERVICE_ACCOUNT", key_path="Resources/sa.json")
        payload = sc.to_dict()
        assert payload["authType"] == "SERVICE_ACCOUNT"
        assert payload["keyPath"] == "Resources/sa.json"
        assert "clientId" not in payload

    def test_repr_never_shows_credentials(self):
        sc = GmailConnector(
            None,
            "inbox",
            67,
            mailbox="a@example.com",
            auth_type="OAUTH",
            client_id="cid",
            client_secret="secret-value",
            refresh_token="token-value",
        )
        assert "secret-value" not in repr(sc) and "token-value" not in repr(sc)

    def test_get_access_token(self, mocker):
        mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._get_gmail_access_token",
            return_value={
                "accessToken": "ya29.x",
                "expiresAt": 1761400000000,
                "mailbox": "support@example.com",
                "scope": "s",
            },
        )
        token = _connector().get_access_token()
        assert token.token == "ya29.x"
        assert token.expires_at == 1761400000000
        assert "ya29" not in repr(token)

    def test_sync_creates_starts_and_schedules_the_job(self, mocker):
        put = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._put_gmail_sync_job"
        )
        job = mocker.MagicMock()
        get = mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)

        result = _connector().sync(
            query="from:bob",
            labels=["INBOX"],
            loading_strategy="FULL_LOAD",
            initial_value=datetime(2026, 1, 1, tzinfo=timezone.utc),
            schedule="hourly",
        )

        fs_id, name, job_name, body = put.call_args.args
        assert (fs_id, name, job_name) == (
            67,
            "support inbox",
            "support_inbox_gmail_sync",
        )
        assert body == {
            "featureGroupName": None,
            "path": None,
            "query": "from:bob",
            "labels": ["INBOX"],
            "loadingStrategy": "FULL_LOAD",
            "initialValue": 1767225600000,
            "environmentName": None,
        }
        get.assert_called_once_with("support_inbox_gmail_sync")
        job.run.assert_called_once_with(await_termination=False)
        job.schedule.assert_called_once_with(cron_expression="0 0 * ? * * *")
        assert result is job

    def test_daily_runs_at_midnight_utc(self, mocker):
        mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._put_gmail_sync_job"
        )
        job = mocker.MagicMock()
        mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)
        _connector().sync(
            feature_group_name="support",
            attachments_path="Resources/att",
            schedule="daily",
            start=False,
        )
        job.schedule.assert_called_once_with(cron_expression="0 0 0 * * ? *")
        job.run.assert_not_called()

    def test_sync_without_start_or_schedule_only_saves_the_job(self, mocker):
        mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._put_gmail_sync_job"
        )
        job = mocker.MagicMock()
        mocker.patch("hopsworks_common.core.job_api.JobApi.get", return_value=job)
        _connector().sync(job_name="nightly", start=False)
        job.run.assert_not_called()
        job.schedule.assert_not_called()

    @pytest.mark.parametrize(
        "kwargs", [{"schedule": "weekly"}, {"loading_strategy": "INCREMENTAL_ID"}]
    )
    def test_sync_refuses_unknown_schedules_and_strategies(self, kwargs, mocker):
        put = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._put_gmail_sync_job"
        )
        with pytest.raises(ValueError):
            _connector().sync(**kwargs)
        put.assert_not_called()

    def test_list_sync_jobs(self, mocker):
        mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._get_gmail_sync_jobs",
            return_value=[
                {
                    "jobName": "nightly",
                    "path": "Resources/mail",
                    "featureGroupName": "support_examples",
                    "loadingStrategy": "INCREMENTAL_TIMESTAMP",
                    "labels": ["INBOX"],
                    "connectorName": "support inbox",
                    "mailbox": "support@example.com",
                }
            ],
        )
        jobs = _connector().list_sync_jobs()
        assert [(j.job_name, j.feature_group_name, j.labels) for j in jobs] == [
            ("nightly", "support_examples", ["INBOX"])
        ]

    @pytest.mark.parametrize("method", ["get_databases", "get_tables"])
    def test_browsing_is_refused(self, method):
        with pytest.raises(ValueError):
            getattr(_connector(), method)()

    def test_preview_is_refused(self):
        sc = _connector()
        with pytest.raises(ValueError, match="sync"):
            sc.get_data(ds.DataSource(storage_connector=sc))


class TestDataSourceSync:
    def test_forwards_to_the_gmail_connector(self, mocker):
        sc = _connector()
        sync = mocker.patch.object(GmailConnector, "sync", return_value="job")
        assert ds.DataSource(storage_connector=sc).sync(schedule="daily") == "job"
        sync.assert_called_once_with(schedule="daily")

    def test_other_data_sources_are_refused(self):
        hopsfs = storage_connector.HopsFSConnector(1, "files", 67)
        with pytest.raises(ValueError, match="Gmail"):
            ds.DataSource(storage_connector=hopsfs).sync()
