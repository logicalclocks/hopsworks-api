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
from hopsworks_common.client.exceptions import (
    DataSourceException,
    FeatureStoreException,
)
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hsfs.core import data_source
from hsfs.core.data_source_data import DataSourceData
from hsfs.storage_connector import (
    CRMAndAnalyticsConnector,
    CRMSource,
    HopsFSConnector,
    RestConnector,
    SqlConnector,
)


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

    def test_sql_connector_raises_on_failed_schema_fetch(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(query="SELECT * FROM events")
        mocker.patch.object(
            connector._data_source_api,
            "_get_data",
            return_value=DataSourceData(
                schema_fetch_failed=True, schema_fetch_logs="clickhouse said no"
            ),
        )

        with pytest.raises(DataSourceException) as e_info:
            connector.get_data(source)

        assert "clickhouse said no" in str(e_info.value)
        assert "SELECT * FROM events" in str(e_info.value)
        assert "data source 'sql'" in str(e_info.value)
        assert isinstance(e_info.value, FeatureStoreException)

    def test_failed_schema_fetch_names_the_table(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(table="events", database="loadtest")
        mocker.patch.object(
            connector._data_source_api,
            "_get_data",
            return_value=DataSourceData(
                schema_fetch_failed=True, schema_fetch_logs="clickhouse said no"
            ),
        )

        with pytest.raises(DataSourceException, match="table 'events'"):
            connector.get_data(source)

    def test_failed_schema_fetch_names_the_path(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(path="s3://bucket/events")
        mocker.patch.object(
            connector._data_source_api,
            "_get_data",
            return_value=DataSourceData(
                schema_fetch_failed=True, schema_fetch_logs="clickhouse said no"
            ),
        )

        with pytest.raises(DataSourceException, match="path 's3://bucket/events'"):
            connector.get_data(source)

    def test_failed_schema_fetch_falls_back_to_the_connector(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource()
        mocker.patch.object(
            connector._data_source_api,
            "_get_data",
            return_value=DataSourceData(
                schema_fetch_failed=True, schema_fetch_logs="clickhouse said no"
            ),
        )

        with pytest.raises(DataSourceException) as e_info:
            connector.get_data(source)

        assert "for data source 'sql'" in str(e_info.value)
        assert "None" not in str(e_info.value)

    def test_failed_schema_fetch_shortens_a_long_query(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(query="SELECT " + "col, " * 100 + "1")
        mocker.patch.object(
            connector._data_source_api,
            "_get_data",
            return_value=DataSourceData(
                schema_fetch_failed=True, schema_fetch_logs="clickhouse said no"
            ),
        )

        with pytest.raises(DataSourceException) as e_info:
            connector.get_data(source)

        assert "..." in str(e_info.value)
        assert len(str(e_info.value).splitlines()[0]) < 200

    def test_sql_connector_returns_data_on_success(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(query="SELECT * FROM events")
        expected = DataSourceData()
        mocker.patch.object(
            connector._data_source_api, "_get_data", return_value=expected
        )

        assert connector.get_data(source) is expected

    def test_sql_connector_passes_through_empty_response(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(query="SELECT * FROM events")
        mocker.patch.object(connector._data_source_api, "_get_data", return_value=None)

        assert connector.get_data(source) is None

    def test_infer_metadata_refuses_an_empty_response(self, mocker):
        connector = SqlConnector(id=1, name="sql", featurestore_id=1)
        source = data_source.DataSource(table="events")
        mocker.patch.object(connector._data_source_api, "_get_data", return_value=None)
        mock_infer = mocker.patch.object(connector._data_source_api, "_infer_metadata")

        with pytest.raises(DataSourceException, match="table 'events'"):
            connector.infer_metadata(source)

        assert mock_infer.call_count == 0

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

    def test_crm_get_tables_sets_storage_connector(self, mocker):
        connector = CRMAndAnalyticsConnector(
            id=1,
            name="crm",
            featurestore_id=1,
            crm_type=CRMSource.HUBSPOT,
        )
        mocker.patch.object(
            connector._data_source_api,
            "_get_crm_resources",
            return_value=type(
                "CRMResources", (), {"supported_resources": ["contacts", "deals"]}
            )(),
        )

        tables = connector.get_tables()

        assert [table.table for table in tables] == ["contacts", "deals"]
        assert all(table.storage_connector is connector for table in tables)


class TestStorageConnectorGetDataBatch:
    def _crm_connector(self):
        return CRMAndAnalyticsConnector(
            id=1,
            name="crm",
            featurestore_id=1,
            crm_type=CRMSource.HUBSPOT,
        )

    def test_unsupported_connector_type_raises(self):
        connector = HopsFSConnector(id=1, name="hopsfs", featurestore_id=1)

        with pytest.raises(ValueError, match="only supported for CRM"):
            connector.get_data_batch([data_source.DataSource(table="t")])

    def test_empty_data_sources_raises(self):
        with pytest.raises(ValueError, match="At least one data source"):
            self._crm_connector().get_data_batch([])

    def test_requires_table_on_every_source(self):
        sources = [
            data_source.DataSource(table="contacts"),
            data_source.DataSource(table=None),
        ]

        with pytest.raises(ValueError, match="require a table name"):
            self._crm_connector().get_data_batch(sources)

    def test_rest_connector_sets_default_rest_endpoint(self, mocker):
        connector = RestConnector(id=1, name="rest", featurestore_id=1)
        sources = [
            data_source.DataSource(table="a", rest_endpoint=None),
            data_source.DataSource(table="b", rest_endpoint=None),
        ]
        mocker.patch.object(
            connector._data_source_api,
            "_start_no_sql_schema_fetch",
            return_value={"a": DataSourceData(), "b": DataSourceData()},
        )

        connector.get_data_batch(sources)

        assert all(
            isinstance(source.rest_endpoint, RestEndpointConfig) for source in sources
        )

    def test_polls_until_all_resources_finished(self, mocker):
        connector = self._crm_connector()
        sources = [
            data_source.DataSource(table="contacts"),
            data_source.DataSource(table="deals"),
        ]
        in_progress = {
            "contacts": DataSourceData(schema_fetch_in_progress=True),
            "deals": DataSourceData(schema_fetch_in_progress=True),
        }
        done = {"contacts": DataSourceData(), "deals": DataSourceData()}
        api_mock = mocker.patch.object(
            connector._data_source_api,
            "_start_no_sql_schema_fetch",
            side_effect=[in_progress, in_progress, done],
        )
        mocker.patch("hsfs.storage_connector.time.sleep")

        results = connector.get_data_batch(sources, use_cached=False)

        assert results == done
        assert api_mock.call_count == 3
        # the initial call forwards use_cached, the polling calls never force
        assert api_mock.call_args_list[0].args == (connector, sources, False)
        assert api_mock.call_args_list[1].args == (connector, sources)

    def test_failed_resource_raises_with_logs(self, mocker):
        connector = self._crm_connector()
        sources = [
            data_source.DataSource(table="contacts"),
            data_source.DataSource(table="deals"),
        ]
        mocker.patch.object(
            connector._data_source_api,
            "_start_no_sql_schema_fetch",
            return_value={
                "contacts": DataSourceData(),
                "deals": DataSourceData(
                    schema_fetch_failed=True, schema_fetch_logs="boom"
                ),
            },
        )

        with pytest.raises(DataSourceException, match="1 of 2") as excinfo:
            connector.get_data_batch(sources)

        assert "deals" in str(excinfo.value)
        assert "boom" in str(excinfo.value)
