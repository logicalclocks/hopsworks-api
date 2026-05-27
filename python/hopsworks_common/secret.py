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

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hopsworks_common.core import secret_api


@public("hopsworks.secret.Secret")
class Secret:
    """Represents a secret in Hopsworks.

    Use [`SecretsApi`][hopsworks.core.secret_api.SecretsApi] to manage secrets; namely you can create a secret with [`SecretsApi.create`][hopsworks.core.secret_api.SecretsApi.create_secret] and get secrets with [`SecretsApi.get_secret`][hopsworks.core.secret_api.SecretsApi.get_secret] and [`SecretsApi.get_secrets`][hopsworks.core.secret_api.SecretsApi.get_secrets].
    """

    NOT_FOUND_ERROR_CODE = 160048

    def __init__(
        self,
        name=None,
        secret=None,
        added_on=None,
        visibility=None,
        scope=None,
        owner=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._name = name
        self._secret = secret
        self._added_on = added_on
        self._visibility = visibility
        self._scope = scope
        self._owner = owner
        self._secret_api = secret_api.SecretsApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "items" not in json_decamelized or len(json_decamelized["items"]) == 0:
            return []
        return [cls(**secret) for secret in json_decamelized["items"]]

    @public
    @property
    def name(self):
        """Name of the secret."""
        return self._name

    @public
    @property
    def value(self):
        """Value of the secret."""
        return self._secret

    @public
    @property
    def created(self):
        """Date when secret was created."""
        return self._added_on

    @public
    @property
    def visibility(self):
        """Visibility of the secret."""
        return self._visibility

    @public
    @property
    def scope(self):
        """Scope of the secret."""
        return self._scope

    @public
    @property
    def owner(self):
        """Owner of the secret."""
        return self._owner

    @public
    def delete(self):
        """Delete the secret.

        Danger: Potentially dangerous operation
            This operation deletes the secret and may break applications using it.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request
        """
        return self._secret_api._delete(self.name)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        if self._owner is not None:
            return f"Secret({self._name!r}, {self._visibility!r}, {self._owner!r})"
        return f"Secret({self._name!r}, {self._visibility!r})"

    @public
    def get_url(self):
        """Get url to the secret in Hopsworks."""
        path = "/account/secrets"
        return util.get_hostname_replaced_url(path)
