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

import json
from unittest.mock import MagicMock

import pytest
from hopsworks_common.core.trino_catalog_api import TrinoCatalogApi


def _patch_client(mocker, send_request_return=None) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 119
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.trino_catalog_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestTrinoCatalogApi:
    """The request each call makes, since the contract with the backend is the whole of this class."""

    def test_get_catalogs_is_project_scoped(self, mocker):
        client = _patch_client(mocker, [{"name": "p__pg"}])

        result = TrinoCatalogApi().get_catalogs()

        assert result == [{"name": "p__pg"}]
        call = client._send_request.call_args
        assert call.args[0] == "GET"
        assert call.args[1] == ["project", 119, "trino", "catalog"]

    def test_get_catalog_names_the_catalog_in_the_path(self, mocker):
        client = _patch_client(mocker, {})

        TrinoCatalogApi().get_catalog("p__pg")

        call = client._send_request.call_args
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]

    def test_get_catalog_template_passes_the_data_source_as_query_params(self, mocker):
        # The template is a GET so that asking for it creates nothing; the source it derives from
        # therefore travels as query params rather than a body.
        client = _patch_client(mocker, {"supported": True})

        TrinoCatalogApi().get_catalog_template("snow", 67)

        call = client._send_request.call_args
        assert call.args[0] == "GET"
        assert call.args[1] == ["project", 119, "trino", "catalog", "template"]
        assert call.kwargs["query_params"] == {
            "dataSourceName": "snow",
            "featurestoreId": 67,
        }

    def test_create_omits_the_data_source_when_hand_written(self, mocker):
        # Their absence is the signal that the properties are to be used exactly as given: sending
        # them as nulls would make the backend try to resolve credentials from a source named null.
        client = _patch_client(mocker, {})

        TrinoCatalogApi().create_catalog(
            "p__pg", "postgresql", {"connection-url": "jdbc:x"}
        )

        body = json.loads(client._send_request.call_args.kwargs["data"])
        assert body == {
            "name": "p__pg",
            "connectorType": "postgresql",
            "properties": {"connection-url": "jdbc:x"},
        }

    def test_create_carries_the_data_source_when_derived(self, mocker):
        client = _patch_client(mocker, {})

        TrinoCatalogApi().create_catalog(
            "p__snow",
            "snowflake",
            {"connection-url": "jdbc:snowflake://x"},
            data_source_name="snow",
            featurestore_id=67,
        )

        body = json.loads(client._send_request.call_args.kwargs["data"])
        assert body["dataSourceName"] == "snow"
        assert body["featurestoreId"] == 67

    def test_create_refuses_a_half_specified_data_source(self, mocker):
        # One without the other cannot be resolved server-side; failing here beats an opaque
        # backend error after the request is already sent.
        client = _patch_client(mocker)

        with pytest.raises(ValueError, match="passed together"):
            TrinoCatalogApi().create_catalog(
                "p__snow", "snowflake", {}, data_source_name="snow"
            )
        with pytest.raises(ValueError, match="passed together"):
            TrinoCatalogApi().create_catalog(
                "p__snow", "snowflake", {}, featurestore_id=67
            )
        client._send_request.assert_not_called()

    def test_update_uses_put_on_the_named_catalog(self, mocker):
        client = _patch_client(mocker, {})

        TrinoCatalogApi().update_catalog("p__pg", "postgresql", {})

        call = client._send_request.call_args
        assert call.args[0] == "PUT"
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]
        # The backend does not re-derive credentials on update (masked values keep the stored
        # secret instead), so an update never carries a data source.
        body = json.loads(call.kwargs["data"])
        assert "dataSourceName" not in body
        assert "featurestoreId" not in body

    def test_delete_uses_delete_on_the_named_catalog(self, mocker):
        client = _patch_client(mocker)

        TrinoCatalogApi().delete_catalog("p__pg")

        call = client._send_request.call_args
        assert call.args[0] == "DELETE"
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]

    def test_test_connection_posts_to_the_test_endpoint(self, mocker):
        client = _patch_client(mocker)

        TrinoCatalogApi().test_connection("p__pg", "postgresql", {})

        call = client._send_request.call_args
        assert call.args[0] == "POST"
        assert call.args[1] == [
            "project",
            119,
            "trino",
            "catalog",
            "test-connection",
        ]

    # Not project-scoped, and deliberately the sync-and-restart endpoint: a plain restart would load
    # nothing, because a newly created catalog is only a record until it is written.
    def test_restart_calls_the_admin_sync_and_restart_endpoint(self, mocker):
        client = _patch_client(mocker, {"restarted": True, "quarantined": []})

        result = TrinoCatalogApi().restart()

        assert result == {"restarted": True, "quarantined": []}
        call = client._send_request.call_args
        assert call.args[0] == "POST"
        assert call.args[1] == ["admin", "trino", "catalogs", "sync-and-restart"]
