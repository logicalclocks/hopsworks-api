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

from hopsworks_common.connection import Connection
from hopsworks_common.search_results import (
    DeploymentSearchResult,
    FeatureGroupSearchResult,
    FeatureViewSearchResult,
    ModelSearchResult,
    TrainingDatasetSearchResult,
)


_DATA = {
    "name": "entity",
    "version": 1,
    "parentProjectId": 1,
    "parentProjectName": "proj",
}


class TestSearchResults:
    """SearchResultItem.get() resolves the entity through the (internal) Connection.

    The Connection's feature-store accessor is private (`_get_feature_store`), so a
    `spec=Connection` mock makes a call to the old public name raise AttributeError
    — these tests catch a stale-call regression in the `.get()` delegation.
    """

    def _patch_connection(self, mocker):
        conn = mocker.MagicMock(spec=Connection)
        fs = mocker.MagicMock()
        conn._get_feature_store.return_value = fs
        mocker.patch("hopsworks_common.client._get_connection", return_value=conn)
        return conn, fs

    def test_feature_group_search_result_get(self, mocker):
        conn, fs = self._patch_connection(mocker)
        fs.get_feature_group.return_value = "FG"

        result = FeatureGroupSearchResult(_DATA).get()

        assert result == "FG"
        conn._get_feature_store.assert_called_once_with("proj")
        fs.get_feature_group.assert_called_once_with("entity", version=1)

    def test_feature_view_search_result_get(self, mocker):
        conn, fs = self._patch_connection(mocker)
        fs.get_feature_view.return_value = "FV"

        result = FeatureViewSearchResult(_DATA).get()

        assert result == "FV"
        conn._get_feature_store.assert_called_once_with("proj")
        fs.get_feature_view.assert_called_once_with("entity", version=1)

    def test_training_dataset_search_result_get(self, mocker):
        conn, fs = self._patch_connection(mocker)
        fs.get_training_dataset.return_value = "TD"

        result = TrainingDatasetSearchResult(_DATA).get()

        assert result == "TD"
        conn._get_feature_store.assert_called_once_with("proj")
        fs.get_training_dataset.assert_called_once_with("entity", version=1)

    def test_model_search_result_get(self, mocker):
        conn = mocker.MagicMock(spec=Connection)
        mr = mocker.MagicMock()
        conn._get_model_registry.return_value = mr
        mr.get_model.return_value = "MODEL"
        mocker.patch("hopsworks_common.client._get_connection", return_value=conn)

        result = ModelSearchResult(_DATA).get()

        assert result == "MODEL"
        conn._get_model_registry.assert_called_once_with("proj")
        mr.get_model.assert_called_once_with("entity", version=1)

    def test_deployment_search_result_get(self, mocker):
        conn = mocker.MagicMock(spec=Connection)
        ms = mocker.MagicMock()
        conn._get_model_serving.return_value = ms
        ms.get_deployment.return_value = "DEPLOYMENT"
        mocker.patch("hopsworks_common.client._get_connection", return_value=conn)

        result = DeploymentSearchResult(_DATA).get()

        assert result == "DEPLOYMENT"
        conn._get_model_serving.assert_called_once_with()
        ms.get_deployment.assert_called_once_with("entity")
