#
#   Copyright 2023 Hopsworks AB
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
from unittest.mock import MagicMock

import pandas as pd
import pytest
from hsfs import feature_group, feature_view, storage_connector, training_dataset
from hsfs.constructor import fs_query
from hsfs.core import arrow_flight_client
from hsfs.engine import python
from hsfs.feature_store import FeatureStore
from hsfs.storage_connector import HopsFSConnector, StorageConnector


class TestArrowFlightClient:
    @pytest.fixture(autouse=True)
    def run_around_tests(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        arrow_flight_client.get_instance()._enabled_on_cluster = True
        arrow_flight_client.get_instance()._disabled_for_session = False
        yield
        arrow_flight_client.get_instance()._enabled_on_cluster = False
        arrow_flight_client.get_instance()._disabled_for_session = True

    def _arrange_engine_mocks(self, mocker, backend_fixtures):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        json_query = backend_fixtures["fs_query"]["get_basic_info"]["response"]
        q = fs_query.FsQuery.from_response_json(json_query)
        mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi.construct_query",
            return_value=q,
        )

    def _arrange_featuregroup_mocks(self, backend_fixtures):
        json_fg = backend_fixtures["feature_group"]["get_stream_list"]["response"]
        fg_list = feature_group.FeatureGroup.from_response_json(json_fg)
        return fg_list[0]

    def _arrange_featureview_mocks(self, mocker, backend_fixtures):
        json_fv = backend_fixtures["feature_view"]["get"]["response"]
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")
        fv = feature_view.FeatureView.from_response_json(json_fv)
        json_td = backend_fixtures["training_dataset"]["get_basic_info"]["response"]
        td = training_dataset.TrainingDataset.from_response_json(json_td)[0]
        td.training_dataset_type = "IN_MEMORY_TRAINING_DATASET"
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata",
            return_value=td,
        )
        mocker.patch(
            "hsfs.core.feature_view_api.FeatureViewApi.get_training_dataset_by_version",
            return_value=td,
        )

        fg = self._arrange_featuregroup_mocks(backend_fixtures)
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query",
            return_value=fg.select_all(),
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions"
        )

        # required for batch query
        batch_scoring_server = mocker.MagicMock()
        batch_scoring_server.training_dataset_version = 1
        batch_scoring_server._transformation_functions = None
        fv._FeatureView__batch_scoring_server = batch_scoring_server
        mocker.patch("hsfs.feature_view.FeatureView.init_batch_scoring")

        return fv

    def _arrange_dataset_reads(self, mocker, backend_fixtures, data_format):
        # required for reading tds from path
        json_td = backend_fixtures["training_dataset"]["get_basic_info"]["response"]
        td_hopsfs = training_dataset.TrainingDataset.from_response_json(json_td)[0]
        td_hopsfs.training_dataset_type = "HOPSFS_TRAINING_DATASET"
        td_hopsfs.storage_connector = HopsFSConnector(0, "", 0, hopsfs_path="/path")
        td_hopsfs.data_format = data_format
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata",
            return_value=td_hopsfs,
        )
        mocker.patch("hsfs.storage_connector.StorageConnector.refetch")
        inode_path = mocker.MagicMock()
        inode_path.path = "/path/test.parquet"
        mocker.patch(
            "hsfs.core.dataset_api.DatasetApi._list_dataset_path",
            return_value=(1, [inode_path]),
        )
        mocker.patch("hsfs.engine.python.Engine.split_labels", return_value=None)

    def test_read_feature_group(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fg.read()

        # Assert
        assert mock_read_query.call_count == 1

    def test_read_query(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fg = self._arrange_featuregroup_mocks(backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )
        query = fg.select_all()

        # Act
        query.read()

        # Assert
        assert mock_read_query.call_count == 1

    def test_training_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fv.training_data()

        # Assert
        assert mock_read_query.call_count == 1

    def test_batch_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        mock_read_query = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_query"
        )

        # Act
        fv.get_batch_data()

        # Assert
        assert mock_read_query.call_count == 1

    def test_get_training_data_featureview(self, mocker, backend_fixtures):
        # Arrange
        self._arrange_engine_mocks(mocker, backend_fixtures)
        fv = self._arrange_featureview_mocks(mocker, backend_fixtures)
        self._arrange_dataset_reads(mocker, backend_fixtures, "parquet")
        mock_read_path = mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient.read_path",
            return_value=pd.DataFrame(),
        )

        # Act
        fv.get_training_data(1)

        # Assert
        assert mock_read_path.call_count == 1

    def test_supports(self):
        # Arrange
        connector = storage_connector.BigQueryConnector(0, "BigQueryConnector", 99)
        external_feature_group = feature_group.ExternalFeatureGroup(
            storage_connector=connector, primary_key=[""]
        )

        # Act
        supported = arrow_flight_client.supports([external_feature_group])

        # Assert
        assert supported

    class FakeConnector(StorageConnector):
        def __init__(self):
            self._type = "Fake"

        def spark_options(self):
            pass

    def test_supports_unsupported(self):
        # Arrange
        external_feature_group = feature_group.ExternalFeatureGroup(
            storage_connector=self.FakeConnector(), primary_key=[""]
        )

        # Act
        supported = arrow_flight_client.supports([external_feature_group])

        # Assert
        assert not supported

    def test_supports_mixed_featuregroups(self):
        # Arrange
        connector = storage_connector.BigQueryConnector(0, "BigQueryConnector", 99)
        external_feature_group = feature_group.ExternalFeatureGroup(
            storage_connector=connector, primary_key=[""]
        )
        mock_feature_group = MagicMock(spec=feature_group.FeatureGroup)

        # Act
        supported = arrow_flight_client.supports(
            [external_feature_group, mock_feature_group]
        )

        # Assert
        assert supported

    def test_supports_mixed_featuregroups_unsupported(self):
        # Arrange
        external_feature_group = feature_group.ExternalFeatureGroup(
            storage_connector=self.FakeConnector(), primary_key=[""]
        )
        mock_feature_group = MagicMock(spec=feature_group.FeatureGroup)

        # Act
        supported = arrow_flight_client.supports(
            [external_feature_group, mock_feature_group]
        )

        # Assert
        assert not supported

    def test_supports_spine_unsupported(self):
        # Arrange
        mock_feature_group = MagicMock(spec=feature_group.FeatureGroup)
        mock_sping = MagicMock(spec=feature_group.SpineGroup)

        # Act
        supported = arrow_flight_client.supports([mock_feature_group, mock_sping])

        # Assert
        assert not supported

    def test_override_hostname(self, mocker, backend_fixtures):
        # Arrange
        client = arrow_flight_client.ArrowFlightClient.__new__(
            arrow_flight_client.ArrowFlightClient
        )
        client.host_url = "grpc+tls://hqs.service.hopsworks.ai:5005"
        client._service_discovery_domain = "hopsworks.ai"

        mocker.patch(
            "hsfs.core.arrow_flight_client.ArrowFlightClient._extract_certs",
            return_value=(None, None, None),
        )

        flight_client_mock = mocker.patch("pyarrow.flight.FlightClient")

        # Act
        client._initialize_flight_client()

        # Assert
        # Assert that FlightClient was initialized with the correct parameters
        flight_client_mock.assert_called_once()
        args, kwargs = flight_client_mock.call_args
        assert kwargs.get("override_hostname") == "flyingduck.service.hopsworks.ai"
