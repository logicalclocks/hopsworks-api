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

from hopsworks import util
from hopsworks.core import secret_api


class Secret:
    def __init__(
        self,
        name=None,
        secret=None,
        added_on=None,
        visibility=None,
        scope=None,
        owner=None,
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
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def name(self):
        """Name of the secret"""
        return self._name

    @property
    def secret(self):
        """Value of the secret"""
        return self._secret

    @property
    def added_on(self):
        """Date when secret was created"""
        return self._added_on

    @property
    def visibility(self):
        """Visibility of the secret"""
        return self._visibility

    @property
    def scope(self):
        """Scope of the secret"""
        return self._scope

    @property
    def owner(self):
        """Owner of the secret"""
        return self._owner

    @property
    def delete(self):
        """Delete the secret
        !!! danger "Potentially dangerous operation"
            This operation deletes the secret and may break applications using it.
        # Raises
            `RestAPIError`.
        """
        return self._secret_api._delete(self.name)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Secret({self._name!r}, {self._visibility!r}, {self._owner!r})"
