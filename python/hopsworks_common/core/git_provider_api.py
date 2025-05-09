#
#   Copyright 2022 Logical Clocks AB
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

from hopsworks_common import client, git_provider
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.engine import git_engine


class GitProviderApi:
    def __init__(self):
        self._git_engine = git_engine.GitEngine()

    def _get_providers(self):
        _client = client.get_instance()
        path_params = ["users", "git", "provider"]

        return git_provider.GitProvider.from_response_json(
            _client._send_request("GET", path_params)
        )

    def _get_default_configured_provider(self):
        providers = self._get_providers()
        if providers is None or len(providers) == 0:
            raise GitException("No git provider is configured")
        elif len(providers) == 1:
            return providers[0].git_provider
        else:
            raise GitException(
                "Multiple git providers are configured. Set the provider keyword to specify the provider to use"
            )

    def _get_provider(self, provider: str, host: str = None):
        _client = client.get_instance()
        path_params = ["users", "git", "provider"]

        providers = git_provider.GitProvider.from_response_json(
            _client._send_request("GET", path_params)
        )
        matching = []
        for p in providers:
            if p.git_provider.lower() == provider.lower():
                if host is None or p.host == host:
                    matching.append(p)
        if len(matching) == 1:
            return matching[0]
        elif len(matching) > 1:
            raise GitException(
                "Multiple git providers are configured. Set the host keyword to specify the provider to use"
            )
        return None

    def _set_provider(self, provider: str, username: str, token: str, host: str):
        _client = client.get_instance()
        path_params = ["users", "git", "provider"]

        provider_config = {
            "gitProvider": provider,
            "username": username,
            "token": token,
            "host": host,
        }

        headers = {"content-type": "application/json"}
        return git_provider.GitProvider.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(provider_config)
            )
        )

    def _delete_provider(self, provider: str, host: str):
        _client = client.get_instance()
        path_params = ["users", "secrets", "{}_token_{}".format(provider.lower(), host)]
        _client._send_request("DELETE", path_params)
        path_params = ["users", "secrets", "{}_username_{}".format(provider.lower(), host)]
        _client._send_request("DELETE", path_params)
