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
from unittest.mock import Mock, patch

from hopsworks_common.core.trino_catalog_api import TrinoCatalogApi


class TestTrinoCatalogApi:
    """The request each call makes, since the contract with the backend is the whole of this class."""

    def _client(self, response=None):
        client = Mock()
        client._project_id = 119
        client._send_request.return_value = response
        return client

    def _call(self, response, fn):
        client = self._client(response)
        with patch(
            "hopsworks_common.core.trino_catalog_api.client._get_instance",
            return_value=client,
        ):
            result = fn(TrinoCatalogApi())
        return result, client._send_request.call_args

    def test_get_catalogs_is_project_scoped(self):
        result, call = self._call([{"name": "p__pg"}], lambda api: api.get_catalogs())
        assert result == [{"name": "p__pg"}]
        assert call.args[0] == "GET"
        assert call.args[1] == ["project", 119, "trino", "catalog"]

    def test_get_catalog_names_the_catalog_in_the_path(self):
        _, call = self._call({}, lambda api: api.get_catalog("p__pg"))
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]

    def test_get_catalog_template_passes_the_data_source_as_query_params(self):
        # The template is a GET so that asking for it creates nothing; the source it derives from
        # therefore travels as query params rather than a body.
        _, call = self._call(
            {"supported": True},
            lambda api: api.get_catalog_template("snow", 67),
        )
        assert call.args[0] == "GET"
        assert call.args[1] == ["project", 119, "trino", "catalog", "template"]
        assert call.kwargs["query_params"] == {
            "dataSourceName": "snow",
            "featurestoreId": 67,
        }

    def test_create_omits_the_data_source_when_hand_written(self):
        # Their absence is the signal that the properties are to be used exactly as given: sending
        # them as nulls would make the backend try to resolve credentials from a source named null.
        _, call = self._call(
            {},
            lambda api: api.create_catalog("p__pg", "postgresql", {"connection-url": "jdbc:x"}),
        )
        body = json.loads(call.kwargs["data"])
        assert body == {
            "name": "p__pg",
            "connectorType": "postgresql",
            "properties": {"connection-url": "jdbc:x"},
        }

    def test_create_carries_the_data_source_when_derived(self):
        _, call = self._call(
            {},
            lambda api: api.create_catalog(
                "p__snow",
                "snowflake",
                {"connection-url": "jdbc:snowflake://x"},
                data_source_name="snow",
                featurestore_id=67,
            ),
        )
        body = json.loads(call.kwargs["data"])
        assert body["dataSourceName"] == "snow"
        assert body["featurestoreId"] == 67

    def test_update_uses_put_on_the_named_catalog(self):
        _, call = self._call({}, lambda api: api.update_catalog("p__pg", "postgresql", {}))
        assert call.args[0] == "PUT"
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]

    def test_delete_uses_delete_on_the_named_catalog(self):
        _, call = self._call(None, lambda api: api.delete_catalog("p__pg"))
        assert call.args[0] == "DELETE"
        assert call.args[1] == ["project", 119, "trino", "catalog", "p__pg"]

    def test_test_connection_posts_to_the_test_endpoint(self):
        _, call = self._call(None, lambda api: api.test_connection("p__pg", "postgresql", {}))
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
    def test_restart_calls_the_admin_sync_and_restart_endpoint(self):
        result, call = self._call(
            {"restarted": True, "quarantined": []}, lambda api: api.restart()
        )
        assert result == {"restarted": True, "quarantined": []}
        assert call.args[0] == "POST"
        assert call.args[1] == ["admin", "trino", "catalogs", "sync-and-restart"]
