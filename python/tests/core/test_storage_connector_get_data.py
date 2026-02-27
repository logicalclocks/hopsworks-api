#
#   Copyright 2025 Hopsworks AB
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
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hsfs.core import data_source
from hsfs.storage_connector import CRMAndAnalyticsConnector, CRMSource, RestConnector


class TestStorageConnectorGetData:
    def test_rest_connector_requires_table(self, mocker):
        connector = RestConnector(id=1, name="rest", featurestore_id=1)
        source = data_source.DataSource(table=None, rest_endpoint=None)

        with pytest.raises(ValueError, match="REST data sources require a table name"):
            connector.get_data(source)

    def test_rest_connector_sets_default_rest_endpoint(self, mocker):
        connector = RestConnector(id=1, name="rest", featurestore_id=1)
        source = data_source.DataSource(table="table_a", rest_endpoint=None)
        mocker.patch.object(connector, "_get_no_sql_data", return_value="result")

        data = connector.get_data(source)

        assert data == "result"
        assert isinstance(source.rest_endpoint, RestEndpointConfig)
        connector._get_no_sql_data.assert_called_once_with(source, True)

    def test_crm_connector_requires_table(self, mocker):
        connector = CRMAndAnalyticsConnector(
            id=1,
            name="crm",
            featurestore_id=1,
            crm_type=CRMSource.HUBSPOT,
        )
        source = data_source.DataSource(table=None, rest_endpoint=None)

        with pytest.raises(ValueError, match="CRM data sources require a table name"):
            connector.get_data(source)
