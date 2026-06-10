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
from __future__ import annotations

import base64
import getpass
import json
from pathlib import Path

from hopsworks_apigen import public
from hopsworks_common import client, decorators, secret
from hopsworks_common.core import project_api


# Mirrors the static MAX_SECRET_VALUE_LENGTH on SecretsController in hopsworks-ee.
# Derived from the VARBINARY(10000) column the encrypted ciphertext is persisted
# to, minus AES/GCM salt+IV+auth-tag; widening it requires a schema change, so
# this constant tracks the backend by hand rather than being fetched at runtime.
MAX_SECRET_VALUE_LENGTH = 9000


@public("hopsworks.core.secret_api.SecretsApi")
class SecretsApi:
    """API for managing secrets in Hopsworks.

    You can get an instance of this class with [`hopsworks.get_secrets_api`][hopsworks.get_secrets_api].
    """

    def __init__(
        self,
    ):
        self._project_api = project_api.ProjectApi()

    @public
    def get_secrets(self) -> list[secret.Secret]:
        """Get all secrets.

        Returns:
            List of all accessible secrets.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request
        """
        _client = client._get_instance()
        path_params = [
            "users",
            "secrets",
        ]
        return secret.Secret.from_response_json(
            _client._send_request("GET", path_params)
        )

    @public
    @decorators._catch_not_found("hopsworks_common.secret.Secret", fallback_return=None)
    def get_secret(self, name: str, owner: str | None = None) -> secret.Secret | None:
        """Get a secret.

        Parameters:
            name: Name of the secret.
            owner:
                Username of the owner for a secret shared with the current project.
                Users can find their username in the Account Settings > Profile section.

        Returns:
            The Secret object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        query_params = None
        if owner is None:
            path_params = [
                "users",
                "secrets",
                name,
            ]
        else:
            query_params = {"name": name, "owner": owner}
            path_params = [
                "users",
                "secrets",
                "shared",
            ]

        return secret.Secret.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )[0]

    @public
    def get(self, name: str, owner: str | None = None) -> str:
        """Get the secret's value.

        If the secret does not exist, it prompts the user to create the secret if the application is running interactively.

        Parameters:
            name: Name of the secret.
            owner: Email of the owner for a secret shared with the current project.

        Returns:
            The secret value.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        secret_obj = self.get_secret(name=name, owner=owner)
        if secret_obj:
            return secret_obj.value
        secret_input = getpass.getpass(
            prompt="\nCould not find secret, enter value here to create it: "
        )
        return self.create_secret(name, secret_input).value

    @public
    def create_secret(
        self, name: str, value: str, project: str | None = None
    ) -> secret.Secret:
        """Create a new secret.

        ```python
        import hopsworks

        project = hopsworks.login()

        secrets_api = hopsworks.get_secrets_api()

        secret = secrets_api.create_secret("my_secret", "Fk3MoPlQXCQvPo")
        ```

        Parameters:
            name: Name of the secret.
            value: The secret value.
            project: Name of the project to share the secret with.

        Returns:
            The Secret object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        secret_config = {"name": name, "secret": value}

        if project is None:
            secret_config["visibility"] = "PRIVATE"
        else:
            scope_project = self._project_api._get_project(project)
            secret_config["scope"] = scope_project.id
            secret_config["visibility"] = "PROJECT"

        path_params = [
            "users",
            "secrets",
        ]

        headers = {"content-type": "application/json"}

        _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(secret_config)
        )

        created_secret = self.get_secret(name)
        print(f"Secret created successfully, explore it at {created_secret.get_url()}")
        return created_secret

    @public
    def create_secret_from_file(
        self, name: str, local_path: str, project: str | None = None
    ) -> secret.Secret:
        """Create a new secret from the contents of a local file.

        The file is base64-encoded in memory and stored as the secret value.
        Reads return the same base64 string and the caller is responsible for
        decoding it back to the original bytes.

        ```python
        import base64
        import hopsworks

        project = hopsworks.login()

        secrets_api = hopsworks.get_secrets_api()

        secret = secrets_api.create_secret_from_file("my_key", "~/.ssh/id_ed25519")
        raw_bytes = base64.b64decode(secrets_api.get("my_key"))
        ```

        Parameters:
            name: Name of the secret.
            local_path: Path to the file whose contents become the secret value.
            project: Name of the project to share the secret with.

        Returns:
            The Secret object.

        Raises:
            ValueError: If the base64-encoded length exceeds the server-side ceiling.
            FileNotFoundError: If `local_path` does not exist.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        path = Path(local_path).expanduser()
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        if len(encoded) > MAX_SECRET_VALUE_LENGTH:
            raise ValueError(
                f"File at {local_path} is too large after base64 encoding "
                f"({len(encoded)} characters, max {MAX_SECRET_VALUE_LENGTH}). "
                "Pick a smaller file."
            )
        return self.create_secret(name, encoded, project=project)

    def _delete(self, name: str):
        """Delete the secret.

        Parameters:
            name: Name of the secret.
        """
        _client = client._get_instance()
        path_params = [
            "users",
            "secrets",
            name,
        ]
        _client._send_request("DELETE", path_params)
