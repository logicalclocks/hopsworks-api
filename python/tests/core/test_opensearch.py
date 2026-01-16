#
#   Copyright 2024 Hopsworks AB
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

import pytest
from hopsworks_common.core.opensearch import ProjectOpenSearchClient
from hsfs.client.exceptions import VectorDatabaseException
from hsfs.core.opensearch import OpenSearchClientSingleton, OpensearchRequestOption


class TestOpenSearchClientSingleton:
    @pytest.mark.parametrize(
        "message, expected_reason, expected_info",
        [
            (
                "[knn] requires k <= 5",
                VectorDatabaseException.REQUESTED_K_TOO_LARGE,
                {VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K: 5},
            ),
            (
                # Added bracket to number
                "[knn] requires k <= [5]",
                VectorDatabaseException.REQUESTED_K_TOO_LARGE,
                {},
            ),
            (
                "Result window is too large, from + size must be less than or equal to: [10000] but was [80000]",
                VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE,
                {VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N: 10000},
            ),
            (
                # Removed the bracket from numbers
                "Result window is too large, from + size must be less than or equal to: 10000 but was 80000",
                VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE,
                {},
            ),
            (
                "Some other error message",
                VectorDatabaseException.OTHERS,
                {},
            ),
        ],
    )
    def test_create_vector_database_exception(
        self, message, expected_reason, expected_info
    ):
        # Use a lightweight ProjectOpenSearchClient instance that does not
        # require an initialized Hopsworks client.
        target = ProjectOpenSearchClient(opensearch_client=None)
        exception = target._create_vector_database_exception(message)
        assert isinstance(exception, VectorDatabaseException)
        assert exception.reason == expected_reason
        assert exception.info == expected_info

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """Ensure clean singleton state for each test.

        Resets the singleton instance before and after each test.
        """
        OpenSearchClientSingleton._instance = None
        yield
        OpenSearchClientSingleton._instance = None

    def test_uses_default_client_when_no_federated_connector(self, mocker):
        """If there is no `federated_opensearch` connector for the feature store.

        The OpenSearch client for that feature store should be the same object
        as the default one.
        """
        # Arrange: mock OpenSearchApi.get_default_py_config so we don't hit real cluster
        mock_default_cfg = {"hosts": [{"host": "default-host", "port": 9200}]}
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi.get_default_py_config",
            return_value=mock_default_cfg,
        )

        # Ensure no federated connector is found
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchClientSingleton._get_federated_opensearch_config",
            return_value=None,
        )

        # Also mock OpenSearch client class to avoid real connections
        mock_opensearch_cls = mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearch", autospec=True
        )

        # Return different MagicMock instances for each construction so we can
        # detect reuse; we will assert on identity via the singleton cache.
        first_default_client = MagicMock(name="default_client")
        second_client = MagicMock(name="second_client")
        mock_opensearch_cls.side_effect = [first_default_client, second_client]

        # Act: get default client (no feature_store_id)
        default_wrapper = OpenSearchClientSingleton()
        default_client = default_wrapper.get_opensearch_client()

        # Act: get client for a specific feature store with no federated connector
        fs_id = 123
        fs_wrapper = OpenSearchClientSingleton(feature_store_id=fs_id)
        fs_client = fs_wrapper.get_opensearch_client()

        # Assert: the feature-store-specific client is the same as the default one
        assert default_client is fs_client, (
            "When no federated_opensearch connector exists, the feature-store client must reuse the default client instance."
        )

    def test_uses_different_client_when_federated_connector_available(self, mocker):
        """If a `federated_opensearch` connector exists for the feature store.

        The OpenSearch client for that feature store should be a different object
        than the default one.
        """
        # Arrange: mock default config
        mock_default_cfg = {"hosts": [{"host": "default-host", "port": 9200}]}
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi.get_default_py_config",
            return_value=mock_default_cfg,
        )

        # Federated connector config for the feature store
        federated_cfg = {"hosts": [{"host": "federated-host", "port": 9200}]}
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchClientSingleton._get_federated_opensearch_config",
            return_value=federated_cfg,
        )
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi._get_authorization_token",
            return_value="test-token",
        )

        # Mock OpenSearch client class
        mock_opensearch_cls = mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearch", autospec=True
        )

        # First call -> default client, second call -> federated client
        default_client = MagicMock(name="default_client")
        federated_client = MagicMock(name="federated_client")
        mock_opensearch_cls.side_effect = [default_client, federated_client]

        # Act: get default client (no feature_store_id)
        default_wrapper = OpenSearchClientSingleton()
        default_client_from_wrapper = default_wrapper.get_opensearch_client()

        # Act: get client for a specific feature store with federated connector
        fs_id = 456
        fs_wrapper = OpenSearchClientSingleton(feature_store_id=fs_id)
        fs_client_from_wrapper = fs_wrapper.get_opensearch_client()

        # Assert: the feature-store-specific client is NOT the same as the default one
        assert default_client_from_wrapper is not fs_client_from_wrapper, (
            "When a federated_opensearch connector exists, the feature-store client must NOT reuse the default client instance."
        )

    @pytest.mark.parametrize(
        "create_fs_id, close_opensearch_client, expect_close_calls",
        [
            (123, False, 0),
            (123, True, 1),
            (None, False, 0),
        ],
        ids=[
            "invalidate_specific_feature_store_no_close",
            "invalidate_specific_feature_store_with_close",
            "invalidate_all_default_client_noop",
        ],
    )
    def test_invalidate_cache(
        self,
        mocker,
        create_fs_id,
        close_opensearch_client,
        expect_close_calls,
    ):
        """Test that invalidate_cache clears cache and creates new client on next access."""
        # Arrange: mock default config
        mock_default_cfg = {"hosts": [{"host": "default-host", "port": 9200}]}
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi.get_default_py_config",
            return_value=mock_default_cfg,
        )

        # No federated connector
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchClientSingleton._get_federated_opensearch_config",
            return_value=None,
        )

        # Mock OpenSearch client class
        mock_opensearch_cls = mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearch", autospec=True
        )

        first_client = MagicMock(name="first_client")
        second_client = MagicMock(name="second_client")
        mock_opensearch_cls.side_effect = [first_client, second_client]

        # Act: create client
        wrapper1 = OpenSearchClientSingleton(feature_store_id=create_fs_id)
        client1 = wrapper1.get_opensearch_client()

        # Invalidate cache
        OpenSearchClientSingleton.invalidate_cache(
            feature_store_id=create_fs_id,
            close_opensearch_client=close_opensearch_client,
        )

        # Get client again - should create a new one
        wrapper2 = OpenSearchClientSingleton(feature_store_id=create_fs_id)
        client2 = wrapper2.get_opensearch_client()

        assert first_client.close.call_count == expect_close_calls
        # Expect a new client when close_opensearch_client=True; otherwise reuse.
        if close_opensearch_client:
            assert client1 is not client2
        else:
            assert client1 is client2

    def test_invalidate_cache_no_op_when_no_instance(self):
        """Test that invalidate_cache is a no-op when singleton not initialized."""
        # Ensure no instance exists
        OpenSearchClientSingleton._instance = None

        # Should not raise any exception
        OpenSearchClientSingleton.invalidate_cache()
        OpenSearchClientSingleton.invalidate_cache(feature_store_id=123)

    def test_invalidate_cache_closes_federated_client_and_recreates(self, mocker):
        """invalidate_cache should close federated client, clear caches, and recreate."""
        # Arrange: default config plus two federated configs to detect cache refresh
        mock_default_cfg = {"hosts": [{"host": "default-host", "port": 9200}]}
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi.get_default_py_config",
            return_value=mock_default_cfg,
        )
        federated_cfg_v1 = {"hosts": [{"host": "fed-v1", "port": 9200}]}
        federated_cfg_v2 = {"hosts": [{"host": "fed-v2", "port": 9200}]}
        mock_get_federated = mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchClientSingleton._get_federated_opensearch_config",
            side_effect=[federated_cfg_v1, federated_cfg_v2],
        )
        mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearchApi._get_authorization_token",
            return_value="test-token",
        )

        # Mock OpenSearch client to return distinct instances
        mock_opensearch_cls = mocker.patch(
            "hopsworks_common.core.opensearch.OpenSearch", autospec=True
        )
        federated_client_v1 = MagicMock(name="federated_client_v1")
        federated_client_v2 = MagicMock(name="federated_client_v2")
        mock_opensearch_cls.side_effect = [federated_client_v1, federated_client_v2]

        fs_id = 321

        # Act: create initial federated client
        wrapper1 = OpenSearchClientSingleton(feature_store_id=fs_id)
        client1 = wrapper1.get_opensearch_client()

        # Invalidate cache with close_opensearch_client=True
        OpenSearchClientSingleton.invalidate_cache(
            feature_store_id=fs_id, close_opensearch_client=True
        )

        # Act: fetch client again; should be recreated with fresh config
        wrapper2 = OpenSearchClientSingleton(feature_store_id=fs_id)
        client2 = wrapper2.get_opensearch_client()

        # Assert: old client closed, caches cleared (federated config refetched), new client created
        assert federated_client_v1.close.call_count == 1
        assert client1 is not client2
        assert mock_get_federated.call_count == 2


class TestOpensearchRequestOption:
    def test_version_1_no_options(self):
        OpensearchRequestOption.get_version = lambda: (1, 1)
        options = OpensearchRequestOption.get_options({})
        assert options == {"timeout": "30s"}

    def test_version_1_with_options_timeout_int(self):
        OpensearchRequestOption.get_version = lambda: (1, 1)
        options = OpensearchRequestOption.get_options({"timeout": 45})
        assert options == {"timeout": "45s"}

    def test_version_2_3_no_options(self):
        OpensearchRequestOption.get_version = lambda: (2, 3)
        options = OpensearchRequestOption.get_options({})
        assert options == {"timeout": 30}

    def test_version_2_3_with_options(self):
        OpensearchRequestOption.get_version = lambda: (2, 3)
        options = OpensearchRequestOption.get_options({"timeout": 50})
        assert options == {"timeout": 50}
