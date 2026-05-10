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

import json
from unittest.mock import MagicMock, patch

import pytest
from hopsworks_common.client.exceptions import (
    PlatformIntelligenceException,
    RestAPIError,
)
from hsfs import feature, storage_connector
from hsfs.core import data_source, data_source_api
from hsfs.core import data_source_data as dsd
from hsfs.core import inferred_metadata as im


class TestDataSource:
    def test_update_storage_connector_redshift(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.RedshiftConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database_name == ds.database
        assert sc._database_group == ds.group
        assert sc._table_name == ds.table

    def test_update_storage_connector_snowflake(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.SnowflakeConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database == ds.database
        assert sc._schema == ds.group
        assert sc._table == ds.table

    def test_update_storage_connector_bigquery(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.BigQueryConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._query_project == ds.database
        assert sc._dataset == ds.group
        assert sc._query_table == ds.table

    def test_update_storage_connector_sql(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.SqlConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database == ds.database

    def test_update_storage_connector_other(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        # Remove attributes that are objects with changing memory addresses
        ignore_keys = ["_data_source_api", "_storage_connector_api"]

        sc = storage_connector.S3Connector(1, "test", 100)
        sc_dict = {k: v for k, v in vars(sc).items() if k not in ignore_keys}

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc_dict == {k: v for k, v in vars(sc).items() if k not in ignore_keys}

    def test_update_storage_connector_none(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        # Act / Assert
        ds._update_storage_connector(None)


class TestInferredMetadata:
    def test_from_response_json_decamelizes(self):
        # Arrange
        payload = {
            "features": [
                {
                    "originalName": "USER_ID",
                    "newName": "user_id",
                    "type": "bigint",
                    "description": "Unique user identifier.",
                },
                {
                    "originalName": "TS",
                    "newName": "event_time",
                    "type": "timestamp",
                    "description": "When the event occurred.",
                },
            ],
            "suggestedPrimaryKey": ["user_id"],
            "suggestedEventTime": "event_time",
        }

        # Act
        inferred = im.InferredMetadata.from_response_json(payload)

        # Assert
        assert len(inferred.features) == 2
        assert inferred.features[0].original_name == "USER_ID"
        assert inferred.features[0].new_name == "user_id"
        assert inferred.features[0].type == "bigint"
        assert inferred.suggested_primary_key == ["user_id"]
        assert inferred.suggested_event_time == "event_time"


class TestDataSourceApiInferMetadata:
    def test_builds_columns_payload_from_preview(self):
        # Arrange — features and a 2-row preview where each row.values is a list
        # of {value0=col_name, value1=cell_value} pairs (mirrors the Java DTO).
        features = [
            feature.Feature(name="col_a", type="string"),
            feature.Feature(name="col_b", type="bigint"),
        ]
        preview = [
            {
                "values": [
                    {"value0": "col_a", "value1": "x1"},
                    {"value0": "col_b", "value1": "1"},
                ]
            },
            {
                "values": [
                    {"value0": "col_a", "value1": "x2"},
                    {"value0": "col_b", "value1": "2"},
                ]
            },
        ]
        preview_data = dsd.DataSourceData(features=features, preview=preview)

        sc = MagicMock()
        sc._featurestore_id = 99
        sc._name = "my_conn"

        api = data_source_api.DataSourceApi()

        captured: dict = {}

        class _StubClient:
            _project_id = 1

            def _send_request(self, method, path_params, **kwargs):
                captured["method"] = method
                captured["path_params"] = path_params
                captured["data"] = kwargs.get("data")
                return {
                    "features": [],
                    "suggestedPrimaryKey": [],
                    "suggestedEventTime": None,
                }

        # Act
        with patch(
            "hsfs.core.data_source_api.client.get_instance",
            return_value=_StubClient(),
        ):
            result = api.infer_metadata(sc, preview_data)

        # Assert — endpoint shape and per-column samples are correct
        assert captured["method"] == "POST"
        assert captured["path_params"][-2:] == ["data_source", "infer-metadata"]
        body = json.loads(captured["data"])
        assert body == {
            "columns": [
                {"name": "col_a", "type": "string", "values": ["x1", "x2"]},
                {"name": "col_b", "type": "bigint", "values": ["1", "2"]},
            ]
        }
        assert isinstance(result, im.InferredMetadata)

    def _make_rest_api_error(self, error_code: int) -> RestAPIError:
        # Build a real RestAPIError without going through HTTP — we just need
        # the parsed-error-object path to set self.error_code to our value.
        response = MagicMock()
        response.json.return_value = {"errorCode": error_code, "errorMsg": "x"}
        response.status_code = 400
        response.reason = "Bad Request"
        response.content = b""
        return RestAPIError("http://test/url", response)

    def test_llm_not_configured_raises_platform_intelligence_exception(self):
        # Arrange — backend returns BrewerErrorCode.LLM_NOT_CONFIGURED (520012)
        sc_mock = MagicMock()
        sc_mock._featurestore_id = 1
        sc_mock._name = "c"
        preview_data = dsd.DataSourceData(
            features=[feature.Feature(name="x", type="string")], preview=[]
        )
        api = data_source_api.DataSourceApi()

        class _StubClient:
            _project_id = 1

            def _send_request(self, *_args, **_kwargs):
                raise data_source_api._BREWER_LLM_NOT_CONFIGURED  # placeholder

        # Act / Assert
        with (
            patch(
                "hsfs.core.data_source_api.client.get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(520012)
                    ),
                ),
            ),
            pytest.raises(PlatformIntelligenceException) as excinfo,
        ):
            api.infer_metadata(sc_mock, preview_data)

        assert excinfo.value.reason == PlatformIntelligenceException.NOT_CONFIGURED
        assert "not enabled" in str(excinfo.value).lower()

    def test_inference_failed_raises_platform_intelligence_exception(self):
        # Arrange — backend returns BrewerErrorCode.METADATA_INFERENCE_FAILED (520013)
        sc_mock = MagicMock()
        sc_mock._featurestore_id = 1
        sc_mock._name = "c"
        preview_data = dsd.DataSourceData(
            features=[feature.Feature(name="x", type="string")], preview=[]
        )
        api = data_source_api.DataSourceApi()

        with (
            patch(
                "hsfs.core.data_source_api.client.get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(520013)
                    ),
                ),
            ),
            pytest.raises(PlatformIntelligenceException) as excinfo,
        ):
            api.infer_metadata(sc_mock, preview_data)

        assert excinfo.value.reason == PlatformIntelligenceException.INFERENCE_FAILED

    def test_unrelated_rest_error_is_not_translated(self):
        # Arrange — any other backend error must surface as RestAPIError, not PIE.
        sc_mock = MagicMock()
        sc_mock._featurestore_id = 1
        sc_mock._name = "c"
        preview_data = dsd.DataSourceData(
            features=[feature.Feature(name="x", type="string")], preview=[]
        )
        api = data_source_api.DataSourceApi()

        with (
            patch(
                "hsfs.core.data_source_api.client.get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(170000)
                    ),
                ),
            ),
            pytest.raises(RestAPIError),
        ):
            api.infer_metadata(sc_mock, preview_data)
