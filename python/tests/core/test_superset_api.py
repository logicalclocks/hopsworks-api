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

from unittest.mock import Mock

import pytest
import requests
from hopsworks_common.core.superset_api import SupersetApi


class TestSupersetApi:
    @pytest.fixture
    def mock_project(self):
        """Create a mock project."""
        project = Mock()
        project.id = 42
        return project

    @pytest.fixture
    def api(self, mocker, mock_project):
        """Create a SupersetApi with mocked dependencies."""
        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "example.local"
        mocker.patch(
            "hopsworks_common.core.superset_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.superset_api.client._is_external",
            return_value=False,
        )
        return SupersetApi(project=mock_project)

    @pytest.fixture
    def authed_api(self, api, mocker):
        """SupersetApi with session and CSRF tokens already cached."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "sess-tok"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )
        api._session_token = "sess-tok"
        api._csrf_token = "csrf-tok"
        return api

    def _mock_response(self, status_code=200, json_data=None, cookies=None):
        """Build a mock requests.Response."""
        resp = Mock(spec=requests.Response)
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.cookies = cookies or {}
        resp.raise_for_status = Mock()
        return resp

    # ------------------------------------------------------------------ #
    #  Session token caching                                              #
    # ------------------------------------------------------------------ #

    def test_session_token_fetched_on_first_request(self, api, mocker):
        """Session token is fetched lazily on the first request."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "new-sess"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )
        resp = self._mock_response(json_data={"result": []})
        api._http = Mock()
        api._http.request.return_value = resp

        api._request("GET", "/api/v1/dataset/")

        client_mock._send_request.assert_called_once_with(
            "GET", ["project", 42, "superset", "login"]
        )
        assert api._session_token == "new-sess"

    def test_session_token_reused_on_subsequent_requests(self, authed_api):
        """Cached session token is reused without re-fetching."""
        resp = self._mock_response(json_data={"result": []})
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        authed_api._request("GET", "/api/v1/dataset/")
        authed_api._request("GET", "/api/v1/chart/")

        assert authed_api._http.request.call_count == 2
        for c in authed_api._http.request.call_args_list:
            assert "session=sess-tok" in c.kwargs["headers"]["Cookie"]

    # ------------------------------------------------------------------ #
    #  CSRF token injection                                               #
    # ------------------------------------------------------------------ #

    def test_csrf_token_included_for_post(self, authed_api):
        """POST requests include the X-CSRFToken header."""
        resp = self._mock_response(json_data={"id": 1})
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        authed_api._request("POST", "/api/v1/dataset/", json_data={"table_name": "t"})

        headers = authed_api._http.request.call_args.kwargs["headers"]
        assert headers["X-CSRFToken"] == "csrf-tok"

    def test_csrf_token_included_for_put(self, authed_api):
        """PUT requests include the X-CSRFToken header."""
        resp = self._mock_response(json_data={"result": {}})
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        authed_api._request("PUT", "/api/v1/dataset/1", json_data={"sql": "SELECT 1"})

        headers = authed_api._http.request.call_args.kwargs["headers"]
        assert headers["X-CSRFToken"] == "csrf-tok"

    def test_csrf_token_included_for_delete(self, authed_api):
        """DELETE requests include the X-CSRFToken header."""
        resp = self._mock_response(status_code=204, json_data={})
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        authed_api._request("DELETE", "/api/v1/dataset/1")

        headers = authed_api._http.request.call_args.kwargs["headers"]
        assert headers["X-CSRFToken"] == "csrf-tok"

    def test_csrf_token_not_included_for_get(self, authed_api):
        """GET requests do not include the X-CSRFToken header."""
        resp = self._mock_response(json_data={"result": []})
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        authed_api._request("GET", "/api/v1/dataset/")

        headers = authed_api._http.request.call_args.kwargs["headers"]
        assert "X-CSRFToken" not in headers

    def test_csrf_token_fetched_lazily_for_mutating_request(self, api, mocker):
        """CSRF token is fetched when first mutating request is made."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "sess-tok"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )
        api._session_token = "sess-tok"

        csrf_resp = self._mock_response(json_data={"result": "fresh-csrf"})
        csrf_resp.cookies = {}
        post_resp = self._mock_response(json_data={"id": 1})

        api._http = Mock()
        api._http.get.return_value = csrf_resp
        api._http.request.return_value = post_resp

        api._request("POST", "/api/v1/dataset/", json_data={"table_name": "t"})

        api._http.get.assert_called_once()
        assert api._csrf_token == "fresh-csrf"

    # ------------------------------------------------------------------ #
    #  401 retry behaviour                                                #
    # ------------------------------------------------------------------ #

    def test_401_retries_with_new_session_token(self, authed_api, mocker):
        """On 401, re-authenticates and retries the request."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "new-sess"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )

        first_resp = self._mock_response(status_code=401)
        second_resp = self._mock_response(json_data={"result": []})

        authed_api._http = Mock()
        authed_api._http.request.side_effect = [first_resp, second_resp]

        result = authed_api._request("GET", "/api/v1/dataset/")

        assert authed_api._http.request.call_count == 2
        assert authed_api._session_token == "new-sess"
        assert result == {"result": []}

    def test_401_retry_refreshes_csrf_for_mutating_request(self, authed_api, mocker):
        """On 401 for a POST, both session and CSRF tokens are refreshed."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "new-sess"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )

        csrf_resp = self._mock_response(json_data={"result": "new-csrf"})
        csrf_resp.cookies = {}
        authed_api._http = Mock()
        authed_api._http.get.return_value = csrf_resp

        first_resp = self._mock_response(status_code=401)
        second_resp = self._mock_response(json_data={"id": 1})
        authed_api._http.request.side_effect = [first_resp, second_resp]

        authed_api._request("POST", "/api/v1/dataset/", json_data={"table_name": "t"})

        assert authed_api._csrf_token == "new-csrf"
        retry_headers = authed_api._http.request.call_args_list[1].kwargs["headers"]
        assert retry_headers["X-CSRFToken"] == "new-csrf"
        assert retry_headers["Cookie"] == "session=new-sess"

    # ------------------------------------------------------------------ #
    #  Session cookie rotation during CSRF fetch                          #
    # ------------------------------------------------------------------ #

    def test_csrf_fetch_updates_session_cookie_on_rotation(self, api, mocker):
        """Session cookie is updated when server rotates it during CSRF fetch."""
        client_mock = Mock()
        client_mock._send_request.return_value = {"accessToken": "original-sess"}
        mocker.patch(
            "hopsworks_common.core.superset_api.client.get_instance",
            return_value=client_mock,
        )
        api._session_token = "original-sess"

        csrf_resp = self._mock_response(json_data={"result": "csrf-tok"})
        csrf_resp.cookies = {"session": "rotated-sess"}

        api._http = Mock()
        api._http.get.return_value = csrf_resp

        api._get_csrf_token()

        assert api._session_token == "rotated-sess"

    # ------------------------------------------------------------------ #
    #  204 No Content handling                                            #
    # ------------------------------------------------------------------ #

    def test_204_returns_empty_dict(self, authed_api):
        """A 204 response returns an empty dict instead of calling .json()."""
        resp = self._mock_response(status_code=204)
        authed_api._http = Mock()
        authed_api._http.request.return_value = resp

        result = authed_api._request("DELETE", "/api/v1/dataset/1")

        assert result == {}
        resp.json.assert_not_called()

    # ------------------------------------------------------------------ #
    #  URL validation guards                                              #
    # ------------------------------------------------------------------ #

    def test_get_superset_url_raises_on_external_client(self, authed_api, mocker):
        """Raises RuntimeError when called from an external client."""
        mocker.patch(
            "hopsworks_common.core.superset_api.client._is_external",
            return_value=True,
        )

        with pytest.raises(RuntimeError, match="only available from within"):
            authed_api._get_superset_url()

    def test_get_superset_url_raises_on_empty_domain(self, authed_api, mocker):
        """Raises RuntimeError when service discovery domain is empty."""
        mocker.patch(
            "hopsworks_common.core.superset_api.client._is_external",
            return_value=False,
        )
        authed_api._service_discovery_domain = ""

        with pytest.raises(RuntimeError, match="not configured"):
            authed_api._get_superset_url()
