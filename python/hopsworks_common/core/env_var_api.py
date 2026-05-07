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
    """Manage user account environment variables in Hopsworks.

    Account env vars are encrypted at rest and automatically injected into
    every runtime the user starts (jobs, deployments, apps, Jupyter, terminal).
    Per-runtime env vars override account-level on collision.

    Example:
        ```python
        import hopsworks
        hopsworks.login()
        api = hopsworks.get_env_vars_api()

        api.create_env_var("OPENAI_API_KEY", "sk-...")
        api.create_env_var("HF_TOKEN", "hf_...")

        # Print only names — values are typically credentials, don't log them.
        for v in api.get_env_vars():
            print(v.name)

        api.delete_env_var("OPENAI_API_KEY")
        ```
    """

    @public
    def get_env_vars(self, include_value: bool = True) -> list[env_var.EnvVar]:
        """Return all account-level env vars for the authenticated user.

        The backend omits values from the list response by default to reduce
        accidental exposure (UI, logs, proxies). The SDK opts back in here so
        existing callers see ``EnvVar.value`` populated; pass
        ``include_value=False`` if you only need names (e.g. building a UI
        list before drilling into a single var).

        Parameters:
            include_value: When ``False``, the returned EnvVars have no value
                set. Default ``True`` preserves the existing behavior.

        Returns:
            List of [`EnvVar`][hopsworks.env_var.EnvVar] objects, possibly empty.
        """
        _client = client.get_instance()
        params = {"includeValue": "true" if include_value else "false"}
        return env_var.EnvVar.from_response_json(
            _client._send_request("GET", ["users", "envvars"], query_params=params)
        )

    @public
    @decorators.catch_not_found("hopsworks_common.env_var.EnvVar", fallback_return=None)
    def get_env_var(self, name: str) -> env_var.EnvVar | None:
        """Look up a single env var by name.

        Returns ``None`` when no env var with that name exists, instead of
        raising — convenient for "set if missing" patterns.

        Parameters:
            name: Variable name (e.g. ``"OPENAI_API_KEY"``).

        Returns:
            The matching [`EnvVar`][hopsworks.env_var.EnvVar], or ``None``.
        """
        _client = client.get_instance()
        env_vars = env_var.EnvVar.from_response_json(
            _client._send_request("GET", ["users", "envvars", name])
        )
        return env_vars[0] if env_vars else None

    @public
    def get(self, name: str) -> str | None:
        """Return just the value of an env var, or ``None`` if missing.

        Convenience wrapper around [`get_env_var`][hopsworks.core.env_var_api.EnvVarsApi.get_env_var].

        Example:
            ```python
            api.get("OPENAI_API_KEY")  # -> "sk-..." or None
            ```

        Parameters:
            name: Variable name.

        Returns:
            The env var's value, or ``None`` when no var with that name exists.
        """
        env_var_obj = self.get_env_var(name)
        return env_var_obj.value if env_var_obj else None

    @public
    def create_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Add a new account-level env var.

        Example:
            ```python
            api.create_env_var("OPENAI_API_KEY", "sk-...")
            ```

        Parameters:
            name: Variable name. Must match ``^[A-Za-z_][A-Za-z0-9_]*$`` and
                not be reserved by the platform (``API_KEY``, ``HOPS_*``,
                ``HOPSWORKS_*``, etc — see ``ReservedEnvVars`` in the backend).
            value: Variable value. Up to 8192 characters.

        Returns:
            The created [`EnvVar`][hopsworks.env_var.EnvVar].

        Raises:
            hopsworks.client.exceptions.RestAPIError: ``ENV_VAR_RESERVED_NAME``,
                ``ENV_VAR_INVALID_NAME``, ``ENV_VAR_VALUE_TOO_LARGE``, or
                ``ENV_VAR_LIMIT_EXCEEDED`` (default cap is 64 vars per user).
        """
        return self._upsert("POST", ["users", "envvars"], name, value)

    @public
    def update_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Replace the value of an existing env var.

        Use [`set_env_var`][hopsworks.core.env_var_api.EnvVarsApi.set_env_var]
        to upsert (create-if-missing) instead.

        Parameters:
            name: Variable name. Must already exist.
            value: New value. Up to 8192 characters.

        Returns:
            The updated [`EnvVar`][hopsworks.env_var.EnvVar].

        Raises:
            hopsworks.client.exceptions.RestAPIError: ``ENV_VAR_NOT_FOUND``
                if no env var with that name exists.
        """
        return self._upsert("PUT", ["users", "envvars", name], name, value)

    @public
    def set_env_var(self, name: str, value: str) -> env_var.EnvVar:
        """Upsert: create the env var if missing, else update its value.

        Example:
            ```python
            # Idempotent — safe to call from setup scripts
            api.set_env_var("HF_TOKEN", os.environ["HF_TOKEN"])
            ```

        Parameters:
            name: Variable name.
            value: Variable value.

        Returns:
            The created or updated [`EnvVar`][hopsworks.env_var.EnvVar].
        """
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
    def delete_env_var(self, name: str) -> None:
        """Remove a single env var from the account.

        Example:
            ```python
            api.delete_env_var("OPENAI_API_KEY")
            ```

        Parameters:
            name: Variable name to remove.

        Raises:
            hopsworks.client.exceptions.RestAPIError: ``ENV_VAR_NOT_FOUND``
                if no env var with that name exists.
        """
        _client = client.get_instance()
        _client._send_request("DELETE", ["users", "envvars", name])

    @public
    def delete(self, name: str) -> None:
        """Alias for [`delete_env_var`][hopsworks.core.env_var_api.EnvVarsApi.delete_env_var].

        Kept for parity with [`SecretsApi.delete`][hopsworks.core.secret_api.SecretsApi].
        New code should prefer ``delete_env_var``.

        Parameters:
            name: Variable name to remove.
        """
        self.delete_env_var(name)

    @public
    def delete_all(self) -> None:
        """Remove all account-level env vars for the authenticated user.

        Returns silently if there are none.

        Example:
            ```python
            api.delete_all()
            assert api.get_env_vars() == []
            ```
        """
        _client = client.get_instance()
        _client._send_request("DELETE", ["users", "envvars"])
