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

from hopsworks_apigen import public
from hopsworks_common import client, decorators, env_var


@public("hopsworks.core.env_var_api.EnvVarsApi")
class EnvVarsApi:
    """API for managing user account environment variables in Hopsworks."""

    @public
    def get_env_vars(self) -> list[env_var.EnvVar]:
        """Get all account environment variables."""
        _client = client.get_instance()
        return env_var.EnvVar.from_response_json(
            _client._send_request("GET", ["users", "envvars"])
        )

    @public
    @decorators.catch_not_found("hopsworks_common.env_var.EnvVar", fallback_return=None)
    def get_env_var(self, name: str) -> env_var.EnvVar | None:
        """Get an account environment variable."""
        _client = client.get_instance()
        env_vars = env_var.EnvVar.from_response_json(
            _client._send_request("GET", ["users", "envvars", name])
        )
        return env_vars[0] if env_vars else None

    @public
    def get(self, name: str) -> str | None:
        """Get an account environment variable value."""
        env_var_obj = self.get_env_var(name)
        return env_var_obj.value if env_var_obj else None

    @public
    def create_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Create an account environment variable.

        Raises a ``RestAPIError`` with code ``ENV_VAR_RESERVED_NAME`` when the
        name is reserved and ``ENV_VAR_INVALID_NAME`` when the name is invalid.
        """
        return self._upsert("POST", ["users", "envvars"], name, value)

    @public
    def update_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Update the value of an existing account environment variable."""
        return self._upsert("PUT", ["users", "envvars", name], name, value)

    @public
    def set_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Create the env var if missing, otherwise update its value."""
        existing = self.get_env_var(name)
        if existing is None:
            return self.create_env_var(name, value)
        return self.update_env_var(name, value)

    def _upsert(
        self, method: str, path: list[str], name: str, value: str
    ) -> env_var.EnvVar:
        _client = client.get_instance()
        headers = {"content-type": "application/json"}
        payload = {"name": name, "value": value}
        env_vars = env_var.EnvVar.from_response_json(
            _client._send_request(
                method,
                path,
                headers=headers,
                data=json.dumps(payload),
            )
        )
        return env_vars[0]

    @public
    def delete(self, name: str):
        """Delete an account environment variable."""
        _client = client.get_instance()
        _client._send_request("DELETE", ["users", "envvars", name])

    @public
    def delete_all(self):
        """Delete all account environment variables."""
        _client = client.get_instance()
        _client._send_request("DELETE", ["users", "envvars"])
