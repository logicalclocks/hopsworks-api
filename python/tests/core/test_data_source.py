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

    def test_format_preserved_in_constructor(self):
        # Arrange / Act
        ds = data_source.DataSource(path="s3://bucket/path", format="parquet")

        # Assert
        assert ds.format == "parquet"

    def test_format_round_trips_via_from_response_json(self):
        # Arrange
        payload = {"path": "s3://bucket/path", "format": "delta"}

        # Act
        ds = data_source.DataSource.from_response_json(payload)

        # Assert
        assert ds.format == "delta"

    def test_from_response_json_items_null_is_single(self):
        # A DTO can carry `items: null` (e.g. the data source embedded in a
        # feature group inside a QueryDTO) — that is a single data source, not a
        # list, and must not be iterated.
        payload = {"items": None, "count": None, "path": "s3://bucket/path"}

        # Act
        ds = data_source.DataSource.from_response_json(payload)

        # Assert
        assert isinstance(ds, data_source.DataSource)
        assert ds.path == "s3://bucket/path"

    def test_from_response_json_items_list(self):
        # A populated `items` collection still deserializes into a list.
        payload = {"items": [{"path": "a"}, {"path": "b"}]}

        # Act
        result = data_source.DataSource.from_response_json(payload)

        # Assert
        assert isinstance(result, list)
        assert [d.path for d in result] == ["a", "b"]

    def test_from_response_json_items_empty_list(self):
        # An empty `items` collection (e.g. a list endpoint with `count: 0`) is
        # still a list payload and must deserialize to an empty list, not a
        # single data source.
        payload = {"items": [], "count": 0}

        # Act
        result = data_source.DataSource.from_response_json(payload)

        # Assert
        assert result == []

    def test_format_emitted_in_to_dict(self):
        # Arrange
        ds = data_source.DataSource(path="s3://bucket/path", format="hudi")

        # Act
        result = ds.to_dict()

        # Assert
        assert result["format"] == "hudi"

    def test_format_none_by_default(self):
        # Arrange / Act
        ds = data_source.DataSource()

        # Assert
        assert ds.format is None
        assert ds.to_dict()["format"] is None

    def test_spreadsheet_id_preserved_in_constructor(self):
        # Arrange / Act
        ds = data_source.DataSource(table="Sheet1", spreadsheet_id="test_spreadsheet")

        # Assert
        assert ds.spreadsheet_id == "test_spreadsheet"

    def test_spreadsheet_id_round_trips_via_from_response_json(self):
        # Arrange
        payload = {"table": "Sheet1", "spreadsheetId": "test_spreadsheet"}

        # Act
        ds = data_source.DataSource.from_response_json(payload)

        # Assert
        assert ds.spreadsheet_id == "test_spreadsheet"

    def test_spreadsheet_id_emitted_in_to_dict(self):
        # Arrange
        ds = data_source.DataSource(table="Sheet1", spreadsheet_id="test_spreadsheet")

        # Act
        result = ds.to_dict()

        # Assert
        assert result["spreadsheetId"] == "test_spreadsheet"

    def test_spreadsheet_id_none_by_default(self):
        # Arrange / Act
        ds = data_source.DataSource()

        # Assert
        assert ds.spreadsheet_id is None
        assert ds.to_dict()["spreadsheetId"] is None


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
            "suggestedDescription": "User events, one row per event.",
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
        assert inferred.suggested_description == "User events, one row per event."
        assert inferred.to_dict()["suggestedDescription"] == (
            "User events, one row per event."
        )


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
            "hsfs.core.data_source_api.client._get_instance",
            return_value=_StubClient(),
        ):
            result = api._infer_metadata(sc, preview_data)

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

    def test_data_source_data_from_response_json_builds_feature_objects(self):
        # Regression for the infer_metadata break (HWORKS-2876, d830e12f6):
        # DataSourceData.from_response_json decamelized the backend JSON and
        # passed the feature entries straight through, so `features` held raw
        # dicts instead of Feature objects. Assert from_response_json — the path
        # the API actually uses — yields Feature objects with readable name/type.
        preview_data = dsd.DataSourceData.from_response_json(
            {
                "features": [
                    {"name": "col_a", "type": "string"},
                    {"name": "col_b", "type": "bigint"},
                ],
                "preview": [],
            }
        )

        assert all(isinstance(f, feature.Feature) for f in preview_data.features)
        assert [f.name for f in preview_data.features] == ["col_a", "col_b"]
        assert [f.type for f in preview_data.features] == ["string", "bigint"]

    def test_infer_metadata_handles_data_source_data_from_response_json(self):
        # Regression for the infer_metadata break (HWORKS-2876, d830e12f6):
        # _infer_metadata reads feature.name / feature.type, which raised
        # "AttributeError: 'dict' object has no attribute 'name'" when
        # DataSourceData.features held raw dicts. Build the preview the same way
        # the client does at runtime — via from_response_json — so the regression
        # would resurface here rather than only in production.
        preview_data = dsd.DataSourceData.from_response_json(
            {
                "features": [
                    {"name": "col_a", "type": "string"},
                    {"name": "col_b", "type": "bigint"},
                ],
                "preview": [
                    {
                        "values": [
                            {"value0": "col_a", "value1": "x1"},
                            {"value0": "col_b", "value1": "1"},
                        ]
                    },
                ],
            }
        )

        sc = MagicMock()
        sc._featurestore_id = 99
        sc._name = "my_conn"

        api = data_source_api.DataSourceApi()

        captured: dict = {}

        class _StubClient:
            _project_id = 1

            def _send_request(self, method, path_params, **kwargs):
                captured["data"] = kwargs.get("data")
                return {
                    "features": [],
                    "suggestedPrimaryKey": [],
                    "suggestedEventTime": None,
                }

        # Act — must not raise AttributeError reading feature.name / feature.type.
        with patch(
            "hsfs.core.data_source_api.client._get_instance",
            return_value=_StubClient(),
        ):
            result = api._infer_metadata(sc, preview_data)

        # Assert — per-column samples are read off Feature objects, not dicts.
        body = json.loads(captured["data"])
        assert body == {
            "columns": [
                {"name": "col_a", "type": "string", "values": ["x1"]},
                {"name": "col_b", "type": "bigint", "values": ["1"]},
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
                "hsfs.core.data_source_api.client._get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(520012)
                    ),
                ),
            ),
            pytest.raises(PlatformIntelligenceException) as excinfo,
        ):
            api._infer_metadata(sc_mock, preview_data)

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
                "hsfs.core.data_source_api.client._get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(520013)
                    ),
                ),
            ),
            pytest.raises(PlatformIntelligenceException) as excinfo,
        ):
            api._infer_metadata(sc_mock, preview_data)

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
                "hsfs.core.data_source_api.client._get_instance",
                return_value=MagicMock(
                    _project_id=1,
                    _send_request=MagicMock(
                        side_effect=self._make_rest_api_error(170000)
                    ),
                ),
            ),
            pytest.raises(RestAPIError),
        ):
            api._infer_metadata(sc_mock, preview_data)


class TestDataSourceNewIngestionJob:
    def test_new_ingestion_job_returns_empty_builder(self):
        from hsfs.core.multi_table_ingestion import MultiTableIngestionJob

        sc = storage_connector.RedshiftConnector(9, "sc", 7)
        ds = data_source.DataSource(storage_connector=sc)

        job = ds.new_ingestion_job(name="crm_ingestion", table_parallelism=2)

        assert isinstance(job, MultiTableIngestionJob)
        assert job.name == "crm_ingestion"
        assert job.targets == []

    def test_new_ingestion_job_without_storage_connector_raises(self):
        ds = data_source.DataSource()
        with pytest.raises(ValueError, match="storage connector"):
            ds.new_ingestion_job(name="x")


class TestDataSourceEstimateIngestionResources:
    def test_estimate_builds_payload_and_returns_estimate(self):
        sc = storage_connector.RedshiftConnector(9, "sc", 7)
        ds = data_source.DataSource(storage_connector=sc)

        fake_fg = MagicMock()
        fake_fg.id = 33

        with patch("hsfs.core.data_source_api.DataSourceApi") as MockApi:
            api = MockApi.return_value
            api._estimate_ingestion_resources.return_value = {
                "recommendedMemoryMb": 4096,
                "recommendedCpuCores": 2.0,
            }

            result = ds.estimate_ingestion_resources(
                fake_fg, write_mode="MERGE", batch_size=200000
            )

            posted_sc, payload = api._estimate_ingestion_resources.call_args[0]

        assert result["recommendedMemoryMb"] == 4096
        assert posted_sc is sc
        assert payload["featuregroupId"] == 33
        assert payload["featurestoreId"] == 7
        assert payload["writeMode"] == "MERGE"
        assert payload["batchSize"] == 200000
        # unset knobs are not sent, so the backend applies its own defaults
        assert "sourceReadWorkers" not in payload

    def test_estimate_accepts_feature_group_id(self):
        sc = storage_connector.RedshiftConnector(9, "sc", 7)
        ds = data_source.DataSource(storage_connector=sc)

        with patch("hsfs.core.data_source_api.DataSourceApi") as MockApi:
            api = MockApi.return_value
            api._estimate_ingestion_resources.return_value = {}
            ds.estimate_ingestion_resources(feature_group_id=5)
            payload = api._estimate_ingestion_resources.call_args[0][1]

        assert payload["featuregroupId"] == 5

    def test_estimate_without_feature_group_raises(self):
        sc = storage_connector.RedshiftConnector(9, "sc", 7)
        ds = data_source.DataSource(storage_connector=sc)
        with pytest.raises(ValueError, match="feature_group"):
            ds.estimate_ingestion_resources()
