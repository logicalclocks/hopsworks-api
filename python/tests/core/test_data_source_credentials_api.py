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
from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException, RestAPIError
from hsfs import storage_connector
from hsfs.core import data_source_api


_PATH = [
    "project",
    119,
    "featurestores",
    67,
    "storageconnectors",
    "oracle-sales",
    "credentials",
]


def _connector(backend_fixtures):
    json_dict = backend_fixtures["storage_connector"]["get_oracle_provided"]["response"]
    return storage_connector.StorageConnector.from_response_json(json_dict)


def _client(mocker, response=None, side_effect=None):
    client_mock = Mock()
    client_mock._project_id = 119
    client_mock._send_request.return_value = response
    if side_effect is not None:
        client_mock._send_request.side_effect = side_effect
    mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
    return client_mock


def _rest_error(error_code, usr_msg):
    response = Mock()
    response.json.return_value = {"errorCode": error_code, "usrMsg": usr_msg}
    response.status_code = 400
    response.reason = "Bad Request"
    response.content = b""
    return RestAPIError("url", response)


class TestDataSourceCredentialsApi:
    def test_get_credentials_reads_the_binding(self, mocker, backend_fixtures):
        client_mock = _client(mocker, {"status": "MISSING"})
        sc = _connector(backend_fixtures)

        binding = sc.get_credentials()

        client_mock._send_request.assert_called_once_with("GET", _PATH)
        assert binding == {"status": "MISSING"}
        assert sc.user_credentials == {"status": "MISSING"}

    def test_validate_credentials_posts_the_dto_and_stores_nothing(
        self, mocker, backend_fixtures
    ):
        client_mock = _client(mocker, {"valid": True})
        sc = _connector(backend_fixtures)

        assert sc.validate_credentials(user="SCOTT", password="tiger") is True

        method, path = client_mock._send_request.call_args.args
        assert (method, path) == ("POST", [*_PATH, "validate"])
        body = json.loads(client_mock._send_request.call_args.kwargs["data"])
        assert body == {
            "username": {"envVarName": "DS_ORACLE_SALES_67_USER", "value": "SCOTT"},
            "password": {"secretName": "ds_oracle_sales_67_password", "value": "tiger"},
        }

    def test_validate_credentials_returns_false_on_a_rejection(
        self, mocker, backend_fixtures
    ):
        _client(mocker, {"valid": False, "errorCode": "ORA-01017", "message": "bad"})
        sc = _connector(backend_fixtures)

        assert sc.validate_credentials(user="SCOTT", password="wrong") is False

    def test_set_credentials_puts_references_and_values(self, mocker, backend_fixtures):
        binding = {"status": "VALID", "usernameEnvVar": "MY_USER"}
        reread = dict(
            backend_fixtures["storage_connector"]["get_oracle_provided"]["response"],
            userCredentials=binding,
        )
        client_mock = _client(mocker, side_effect=[binding, reread])
        sc = _connector(backend_fixtures)

        result = sc.set_credentials(
            user_env_var="MY_USER",
            password_secret="my_pwd",
            wallet_path="/Projects/p/Users/u/.datasources/oracle-sales/wallet.zip",
            wallet_password="wp",
        )

        put = client_mock._send_request.call_args_list[0]
        assert put.args == ("PUT", _PATH)
        body = json.loads(put.kwargs["data"])
        assert body == {
            "username": {"envVarName": "MY_USER"},
            "password": {"secretName": "my_pwd"},
            "walletPath": "/Projects/p/Users/u/.datasources/oracle-sales/wallet.zip",
            "walletPassword": {
                "secretName": "ds_oracle_sales_67_wallet_password",
                "value": "wp",
            },
        }
        assert result == {"status": "VALID", "username_env_var": "MY_USER"}
        assert sc.user_credentials == result

    def test_set_credentials_makes_the_new_credentials_usable_at_once(
        self, mocker, backend_fixtures
    ):
        fixtures = backend_fixtures["storage_connector"]
        sc = storage_connector.StorageConnector.from_response_json(
            fixtures["get_oracle_provided_missing"]["response"]
        )
        binding = fixtures["get_oracle_provided"]["response"]["userCredentials"]
        client_mock = _client(
            mocker,
            side_effect=[binding, fixtures["get_oracle_provided"]["response"]],
        )

        sc.set_credentials(user="scott", password="tiger")

        reread = client_mock._send_request.call_args_list[1]
        assert reread.args == ("GET", _PATH[:-1])
        opts = sc.spark_options()
        assert (opts["user"], opts["password"]) == ("scott", "tiger")
        assert sc.wallet_password == "wallet_pass"
        assert sc.user_credentials["status"] == "VALID"

    def test_set_credentials_rotates_the_credentials_of_the_same_object(
        self, mocker, backend_fixtures
    ):
        fixtures = backend_fixtures["storage_connector"]
        sc = _connector(backend_fixtures)
        assert sc.spark_options()["user"] == "scott"
        rotated = dict(
            fixtures["get_oracle_provided"]["response"],
            user="scott2",
            password="lion",
            walletPath=None,
            walletPassword=None,
        )
        _client(mocker, side_effect=[{"status": "VALID"}, rotated])

        sc.set_credentials(user_env_var="MY_USER", password_secret="my_pwd")

        opts = sc.spark_options()
        assert (opts["user"], opts["password"]) == ("scott2", "lion")
        assert sc.wallet_path is None
        assert sc.wallet_password is None

    def test_set_credentials_raises_with_the_backend_message_when_rejected(
        self, mocker, backend_fixtures
    ):
        _client(
            mocker,
            side_effect=_rest_error(270336, "ORA-01017: invalid username/password"),
        )
        sc = _connector(backend_fixtures)

        with pytest.raises(FeatureStoreException, match="ORA-01017"):
            sc.set_credentials(user="SCOTT", password="wrong")

    def test_set_credentials_lets_other_errors_through(self, mocker, backend_fixtures):
        _client(mocker, side_effect=_rest_error(270337, "not supported"))
        sc = _connector(backend_fixtures)

        with pytest.raises(RestAPIError):
            sc.set_credentials(user="SCOTT", password="tiger")

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"user": "SCOTT"}, {"password": "tiger"}, {"user_env_var": "X"}],
    )
    def test_a_username_and_a_password_are_required(
        self, mocker, backend_fixtures, kwargs
    ):
        client_mock = _client(mocker)
        sc = _connector(backend_fixtures)

        with pytest.raises(ValueError, match="username"):
            sc.set_credentials(**kwargs)

        client_mock._send_request.assert_not_called()

    def test_delete_credentials(self, mocker, backend_fixtures):
        client_mock = _client(mocker)
        sc = _connector(backend_fixtures)

        sc.delete_credentials()

        client_mock._send_request.assert_called_once_with("DELETE", _PATH)
        assert sc.user_credentials == {"status": "MISSING"}
        assert sc.user is None
        assert sc.password is None


class TestMissingCredentialsFromTheBackend:
    def test_browse_rephrases_a_270334_naming_set_credentials(
        self, mocker, backend_fixtures
    ):
        _client(mocker, side_effect=_rest_error(270334, "not provided"))
        sc = _connector(backend_fixtures)

        with pytest.raises(FeatureStoreException, match="set_credentials"):
            data_source_api.DataSourceApi()._get_databases(sc)

    def test_browse_lets_other_errors_through(self, mocker, backend_fixtures):
        _client(mocker, side_effect=_rest_error(270042, "not found"))
        sc = _connector(backend_fixtures)

        with pytest.raises(RestAPIError):
            data_source_api.DataSourceApi()._get_databases(sc)
