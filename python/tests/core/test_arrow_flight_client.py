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
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pyarrow
import pytest
from hsfs import feature_group, feature_view, storage_connector, training_dataset
from hsfs.constructor import fs_query
from hsfs.core import arrow_flight_client
from hsfs.core import data_source as ds
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
        td_hopsfs.data_source.storage_connector = HopsFSConnector(
            0, "", 0, hopsfs_path="/path"
        )
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
            primary_key=[""], data_source=ds.DataSource(storage_connector=connector)
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
            primary_key=[""],
            data_source=ds.DataSource(storage_connector=self.FakeConnector()),
        )

        # Act
        supported = arrow_flight_client.supports([external_feature_group])

        # Assert
        assert not supported

    def test_supports_mixed_featuregroups(self):
        # Arrange
        connector = storage_connector.BigQueryConnector(0, "BigQueryConnector", 99)
        external_feature_group = feature_group.ExternalFeatureGroup(
            primary_key=[""], data_source=ds.DataSource(storage_connector=connector)
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
            primary_key=[""],
            data_source=ds.DataSource(storage_connector=self.FakeConnector()),
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


@pytest.fixture
def fake_hqs_module(monkeypatch):
    """Inject a fake `hqs` package into sys.modules so HQSLocalClient can be exercised
    without the real flyingduck library installed. Yields the fake module.
    """
    fake_engine = MagicMock(name="hqs._engine")
    fake_client_obj = MagicMock(name="hqs.HQSClient.instance")
    fake_client_obj._engine = fake_engine
    fake_client_obj.hopsfs = MagicMock(name="hopsfs")

    fake_hqs_client_class = MagicMock(name="hqs.HQSClient")
    fake_hqs_client_class.from_pod_environment = MagicMock(return_value=fake_client_obj)

    fake_hqs = SimpleNamespace(__version__="5.0.0", HQSClient=fake_hqs_client_class)
    fake_reader_writer_module = SimpleNamespace(
        ArrowDatasetReaderWriter=MagicMock(name="ArrowDatasetReaderWriter")
    )
    fake_duckdb = SimpleNamespace(__version__="1.5.2")
    fake_pfs = SimpleNamespace(HadoopFileSystem=MagicMock(return_value=MagicMock()))

    monkeypatch.setitem(sys.modules, "hqs", fake_hqs)
    monkeypatch.setitem(
        sys.modules,
        "hqs.arrow_dataset_reader_writer",
        fake_reader_writer_module,
    )
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    # `import fsspec.implementations.arrow as pfs` walks fsspec → fsspec.implementations
    # → fsspec.implementations.arrow, so the intermediate package must also be present.
    monkeypatch.setitem(
        sys.modules,
        "fsspec.implementations",
        SimpleNamespace(arrow=fake_pfs),
    )
    monkeypatch.setitem(sys.modules, "fsspec.implementations.arrow", fake_pfs)
    yield fake_hqs


class TestShouldUseLocalHqs:
    def test_external_user_returns_false(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_external", return_value=True
        )
        remote = MagicMock()
        remote._enabled_on_cluster = True
        assert arrow_flight_client._should_use_local_hqs(remote) is False

    def test_cluster_flag_disabled_returns_false(self, mocker, fake_hqs_module):
        mocker.patch(
            "hopsworks_common.client._is_external", return_value=False
        )
        remote = MagicMock()
        remote._enabled_on_cluster = False
        assert arrow_flight_client._should_use_local_hqs(remote) is False

    def test_hqs_not_importable_returns_false(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_external", return_value=False
        )
        # find_spec returns None when the module is not installed.
        mocker.patch(
            "importlib.util.find_spec",
            side_effect=lambda name: None if name == "hqs" else MagicMock(),
        )
        remote = MagicMock()
        remote._enabled_on_cluster = True
        assert arrow_flight_client._should_use_local_hqs(remote) is False

    def test_duckdb_not_importable_returns_false(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_external", return_value=False
        )
        mocker.patch(
            "importlib.util.find_spec",
            side_effect=lambda name: None if name == "duckdb" else MagicMock(),
        )
        remote = MagicMock()
        remote._enabled_on_cluster = True
        assert arrow_flight_client._should_use_local_hqs(remote) is False

    def test_all_gates_pass_returns_true(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_external", return_value=False
        )
        # All find_spec calls return a spec — both hqs and duckdb are "installed"
        mocker.patch("importlib.util.find_spec", return_value=MagicMock())
        remote = MagicMock()
        remote._enabled_on_cluster = True
        assert arrow_flight_client._should_use_local_hqs(remote) is True


class TestGetInstanceRouting:
    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        arrow_flight_client.close()
        yield
        arrow_flight_client.close()

    def test_external_returns_arrow_flight(self, mocker):
        mocker.patch("hopsworks_common.client._is_external", return_value=True)
        # Construction does network probes — patch them out
        mocker.patch.object(
            arrow_flight_client.ArrowFlightClient,
            "__init__",
            return_value=None,
        )
        instance = arrow_flight_client.get_instance()
        assert isinstance(instance, arrow_flight_client.ArrowFlightClient)

    def test_internal_with_local_hqs_swaps_to_local(self, mocker, fake_hqs_module):
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mocker.patch("importlib.util.find_spec", return_value=MagicMock())
        # Construct a real-ish ArrowFlightClient stub with the cluster flag on
        remote_stub = MagicMock(spec=arrow_flight_client.ArrowFlightClient)
        remote_stub._enabled_on_cluster = True
        mocker.patch.object(
            arrow_flight_client,
            "ArrowFlightClient",
            return_value=remote_stub,
        )
        instance = arrow_flight_client.get_instance()
        assert isinstance(instance, arrow_flight_client.HQSLocalClient)
        # The HQSLocalClient should hold the remote stub as its fallback.
        assert instance._flight_fallback is remote_stub


class TestHQSLocalClient:
    @pytest.fixture
    def local_client(self, fake_hqs_module):
        fallback = MagicMock(spec=arrow_flight_client.ArrowFlightClient)
        return arrow_flight_client.HQSLocalClient(flight_fallback=fallback), fallback

    def test_unsigned_read_query_runs_locally(self, local_client):
        client, fallback = local_client
        query = MagicMock()
        query.hqs_payload_signature = None
        query.hqs_payload = json.dumps({"query_string": "x", "features": {}})
        arrow_table = pyarrow.Table.from_pydict({"col": [1, 2, 3]})
        client._hqs_client.execute.return_value = arrow_table

        result = client.read_query(query, None, dataframe_type="default")

        client._hqs_client.execute.assert_called_once()
        fallback.read_query.assert_not_called()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_signed_read_query_forwards_to_flight(self, local_client):
        client, fallback = local_client
        query = MagicMock()
        query.hqs_payload_signature = "sig"
        fallback.read_query.return_value = "remote-result"

        result = client.read_query(query, None, dataframe_type="default")

        client._hqs_client.execute.assert_not_called()
        fallback.read_query.assert_called_once_with(query, None, "default")
        assert result == "remote-result"

    def test_signed_create_training_dataset_forwards_to_flight(self, local_client):
        client, fallback = local_client
        query = MagicMock()
        query.hqs_payload_signature = "sig"
        fallback.create_training_dataset.return_value = "remote-tds"

        result = client.create_training_dataset(
            MagicMock(), MagicMock(), query, None
        )

        fallback.create_training_dataset.assert_called_once()
        assert result == "remote-tds"

    def test_disable_for_session_drops_fallback(self, mocker, local_client):
        client, fallback = local_client
        # Patch the global ArrowFlightClient constructor invoked by _disable_for_session
        mocker.patch.object(
            arrow_flight_client.ArrowFlightClient,
            "__init__",
            return_value=None,
        )
        client._disable_for_session()
        assert client._flight_fallback is None
        assert client._disabled_for_session is True

    def test_uses_from_pod_environment(self, fake_hqs_module):
        fallback = MagicMock(spec=arrow_flight_client.ArrowFlightClient)
        arrow_flight_client.HQSLocalClient(flight_fallback=fallback)
        # Memory cap is delegated to the library's classmethod
        fake_hqs_module.HQSClient.from_pod_environment.assert_called_once()
        # Bare HQSClient(...) should not be invoked directly
        fake_hqs_module.HQSClient.assert_not_called()
