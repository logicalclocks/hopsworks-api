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

import warnings
from unittest.mock import MagicMock, Mock

import pytest
from hopsworks_common.client.exceptions import (
    PlatformIntelligenceException,
    RestAPIError,
)
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


class TestFeatureGroupApiSave:
    def _arrange(self, mocker, backend_fixtures):
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get"]["response"]
        fg = fg_mod.FeatureGroup.from_response_json(json)
        mocker.patch.object(fg, "update_from_response_json", return_value=fg)
        client_mock = Mock()
        client_mock._project_id = 119
        client_mock._send_request.return_value = {}
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        return fg_api, fg, client_mock

    def test_save_does_not_emit_skip_duplicate_check_by_default(
        self, mocker, backend_fixtures
    ):
        # Arrange
        fg_api, fg, client_mock = self._arrange(mocker, backend_fixtures)

        # Act
        fg_api._save(fg)

        # Assert
        query_params = client_mock._send_request.call_args.kwargs["query_params"]
        assert "skipDuplicateCheck" not in query_params

    def test_save_emits_skip_duplicate_check_when_flag_set(
        self, mocker, backend_fixtures
    ):
        # Arrange
        fg_api, fg, client_mock = self._arrange(mocker, backend_fixtures)
        fg._not_check_duplicate = True

        # Act
        fg_api._save(fg)

        # Assert
        query_params = client_mock._send_request.call_args.kwargs["query_params"]
        assert query_params["skipDuplicateCheck"] is True


class TestFeatureGroupApiCheckDuplicates:
    def _arrange(self, mocker, backend_fixtures, response=None, side_effect=None):
        fg_api = feature_group_api.FeatureGroupApi()
        json = backend_fixtures["feature_group"]["get"]["response"]
        fg = fg_mod.FeatureGroup.from_response_json(json)
        client_mock = Mock()
        client_mock._project_id = 119
        if side_effect is not None:
            client_mock._send_request.side_effect = side_effect
        else:
            client_mock._send_request.return_value = response or {}
        mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
        return fg_api, fg, client_mock

    def _make_rest_api_error(self, error_code: int) -> RestAPIError:
        # Build a real RestAPIError without going through HTTP — we just need
        # the parsed-error-object path to set self.error_code to our value.
        response = MagicMock()
        response.json.return_value = {"errorCode": error_code, "errorMsg": "x"}
        response.status_code = 400
        response.reason = "Bad Request"
        response.content = b""
        return RestAPIError("http://test/url", response)

    def test_check_duplicates_builds_path_and_decamelizes(
        self, mocker, backend_fixtures
    ):
        # Arrange
        fg_api, fg, client_mock = self._arrange(
            mocker,
            backend_fixtures,
            response={
                "status": "SUCCEEDED",
                "suspectedDuplicates": True,
                "matches": [{"featureGroupName": "customer_profiles"}],
            },
        )

        # Act
        result = fg_api._check_duplicates(fg)

        # Assert — a plain read is a GET on the duplicates subresource
        call_args = client_mock._send_request.call_args
        assert call_args.args[0] == "GET"
        assert call_args.args[1] == [
            "project",
            119,
            "featurestores",
            fg.feature_store_id,
            "featuregroups",
            fg.id,
            "duplicates",
        ]
        assert result["suspected_duplicates"] is True
        assert result["matches"][0]["feature_group_name"] == "customer_profiles"

    def test_check_duplicates_recheck_posts_to_recheck_subresource(
        self, mocker, backend_fixtures
    ):
        # Arrange — a recheck mutates the stored result, so it must never
        # travel on GET (observers are auto-authorized for GETs server-side).
        fg_api, fg, client_mock = self._arrange(
            mocker, backend_fixtures, response={"status": "SUCCEEDED"}
        )

        # Act
        fg_api._check_duplicates(fg, recheck=True)

        # Assert
        call_args = client_mock._send_request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1][-2:] == ["duplicates", "recheck"]

    @pytest.mark.parametrize(
        "error_code, reason",
        [
            (520012, PlatformIntelligenceException.NOT_CONFIGURED),
            (520016, PlatformIntelligenceException.DUPLICATE_CHECK_FAILED),
            (520017, PlatformIntelligenceException.DUPLICATE_CHECK_DISABLED),
            (520018, PlatformIntelligenceException.DUPLICATE_CHECK_NOT_FOUND),
            (520019, PlatformIntelligenceException.DUPLICATE_CHECK_ALREADY_RUNNING),
            (520020, PlatformIntelligenceException.DUPLICATE_CHECK_NOT_OWNER),
        ],
    )
    def test_check_duplicates_translates_platform_intelligence_errors(
        self, mocker, backend_fixtures, error_code, reason
    ):
        # Arrange
        fg_api, fg, _ = self._arrange(
            mocker,
            backend_fixtures,
            side_effect=self._make_rest_api_error(error_code),
        )

        # Act / Assert
        with pytest.raises(PlatformIntelligenceException) as excinfo:
            fg_api._check_duplicates(fg)

        assert excinfo.value.reason == reason

    def test_check_duplicates_unrelated_rest_error_is_not_translated(
        self, mocker, backend_fixtures
    ):
        # Arrange — any other backend error must surface as RestAPIError, not PIE.
        fg_api, fg, _ = self._arrange(
            mocker,
            backend_fixtures,
            side_effect=self._make_rest_api_error(270009),
        )

        # Act / Assert
        with pytest.raises(RestAPIError):
            fg_api._check_duplicates(fg)
