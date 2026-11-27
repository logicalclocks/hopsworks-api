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

import copy
import warnings
from unittest.mock import Mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import feature_group as fg_mod
from hsfs.core import feature_group_api


class TestFeatureGroupApi:
    def test_get_smart_with_infer_type(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99
        fg_api = feature_group_api.FeatureGroupApi()
        side_effects = [
            [backend_fixtures["feature_group"]["get"]["response"]],
            [backend_fixtures["external_feature_group"]["get"]["response"]],
            [backend_fixtures["spine_group"]["get"]["response"]],
        ]
        client_mock = Mock()
        client_mock.configure_mock(**{"_send_request.side_effect": side_effects})
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=client_mock,
        )
        mocker.patch("hsfs.engine._get_instance")

        print(client_mock.side_effect)

        # Act
        stream_fg = fg_api._get(feature_store_id, "stream_fg", version=1)
        external_fg = fg_api._get(feature_store_id, "external_fg", version=1)
        spine_fg = fg_api._get(feature_store_id, "spine_fg", version=1)

        # Assert
        assert isinstance(stream_fg, fg_mod.FeatureGroup)
        assert isinstance(external_fg, fg_mod.ExternalFeatureGroup)
        assert isinstance(spine_fg, fg_mod.SpineGroup)

    def test_get_all_builds_the_class_of_each_type(self, mocker, backend_fixtures):
        fg_api = feature_group_api.FeatureGroupApi()
        items = [
            backend_fixtures["feature_group"]["get"]["response"],
            backend_fixtures["external_feature_group"]["get"]["response"],
            backend_fixtures["spine_group"]["get"]["response"],
            {"type": "unknownFeaturegroupDTO", "name": "future"},
        ]
        client_mock = Mock()
        client_mock._send_request.return_value = {"items": items}
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        mocker.patch("hsfs.engine._get_instance")

        fgs = fg_api._get_all(99)

        assert [type(fg) for fg in fgs] == [
            fg_mod.FeatureGroup,
            fg_mod.ExternalFeatureGroup,
            fg_mod.SpineGroup,
        ]

    def test_check_features(self, mocker, backend_fixtures):
        # Arrange
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = fg_mod.FeatureGroup.from_response_json(json)

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg_api._check_features(fg)

        # Assert
        assert len(warning_record) == 0

    def test_check_features_no_features(self, mocker, backend_fixtures):
        # Arrange
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get_basic_info_no_features"][
            "response"
        ]
        fg = fg_mod.FeatureGroup.from_response_json(json)

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg_api._check_features(fg)

        # Assert
        assert len(warning_record) == 1

    def test_test_data_source_access_posts_to_the_test_endpoint(
        self, mocker, backend_fixtures
    ):
        # Arrange
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["external_feature_group"]["get"]["response"]
        fg = fg_mod.ExternalFeatureGroup.from_response_json(json)
        client_mock = Mock()
        client_mock._project_id = 119
        client_mock._send_request.return_value = {"status": "OK"}
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)

        # Act
        result = fg_api._test_data_source_access(fg)

        # Assert
        assert result == {"status": "OK"}
        client_mock._send_request.assert_called_once_with(
            "POST",
            [
                "project",
                119,
                "featurestores",
                fg.feature_store_id,
                "featuregroups",
                fg.id,
                "datasource",
                "test",
            ],
        )

    def test_delete_content_refreshes_the_recreated_feature_group(
        self, mocker, backend_fixtures
    ):
        # Arrange: clear recreates the feature group under a new id
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = fg_mod.FeatureGroup.from_response_json(copy.deepcopy(json))
        old_id = fg.id
        recreated = copy.deepcopy(json)
        recreated["id"] = old_id + 1
        client_mock = Mock()
        client_mock._project_id = 1
        client_mock._send_request.return_value = recreated
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)

        # Act
        feature_group_api.FeatureGroupApi()._delete_content(fg)

        # Assert
        assert client_mock._send_request.call_args.args[1][-2:] == [old_id, "clear"]
        assert fg.id == old_id + 1

    def test_sync_metastore(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 1
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        fg = Mock()
        fg.id = 42
        fg.feature_store_id = 99

        feature_group_api.FeatureGroupApi()._sync_metastore(fg)

        client_mock._send_request.assert_called_once_with(
            "POST",
            [
                "project",
                1,
                "featurestores",
                99,
                "featuregroups",
                42,
                "metastoresync",
            ],
            timeout=60,
        )

    @staticmethod
    def _delete(mocker, backend_version="5.3.0", **kwargs):
        client_mock = Mock()
        client_mock._project_id = 119
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        mocker.patch(
            "hopsworks_common.client._get_connection",
            return_value=mocker.Mock(backend_version=backend_version),
        )
        fg = Mock(feature_store_id=67, id=13)
        feature_group_api.FeatureGroupApi()._delete(fg, **kwargs)
        return client_mock._send_request

    @pytest.mark.parametrize(
        "kwargs, query_params",
        [
            ({}, None),
            ({"delete_data": False}, {"deleteData": False}),
            (
                {"force": True, "delete_feature_views": True, "delete_data": False},
                {"force": True, "deleteFeatureViews": True, "deleteData": False},
            ),
        ],
    )
    def test_delete_query_params(self, mocker, kwargs, query_params):
        send_request = self._delete(mocker, **kwargs)

        send_request.assert_called_once_with(
            "DELETE",
            ["project", 119, "featurestores", 67, "featuregroups", 13],
            query_params=query_params,
        )

    def test_delete_keeping_data_refuses_a_backend_older_than_5_3(self, mocker):
        client_mock = Mock()
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        mocker.patch(
            "hopsworks_common.client._get_connection",
            return_value=mocker.Mock(backend_version="5.2.4"),
        )

        with pytest.raises(FeatureStoreException, match="5.2.4"):
            feature_group_api.FeatureGroupApi()._delete(Mock(), delete_data=False)

        client_mock._send_request.assert_not_called()

    def test_delete_with_data_runs_against_an_older_backend(self, mocker):
        send_request = self._delete(mocker, backend_version="5.2.4")

        send_request.assert_called_once()
