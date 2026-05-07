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

import json

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hopsworks_common.core import env_var_api


@public("hopsworks.env_var.EnvVar")
class EnvVar:
    """Represents a user account environment variable in Hopsworks."""

    NOT_FOUND_ERROR_CODE = 160070

    def __init__(
        self,
        name=None,
        value=None,
        added_on=None,
        updated_on=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._name = name
        self._value = value
        self._added_on = added_on
        self._updated_on = updated_on
        self._env_var_api = env_var_api.EnvVarsApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "items" not in json_decamelized or len(json_decamelized["items"]) == 0:
            return []
        return [cls(**env_var) for env_var in json_decamelized["items"]]

    @public
    @property
    def name(self):
        """Name of the environment variable."""
        return self._name

    @public
    @property
    def value(self):
        """Value of the environment variable."""
        return self._value

    @public
    @property
    def created(self):
        """Date when the environment variable was created."""
        return self._added_on

    @public
    @property
    def updated(self):
        """Date when the environment variable was last updated."""
        return self._updated_on

    @public
    def delete(self):
        """Remove this env var from the user's account.

        Equivalent to ``api.delete_env_var(self.name)``.
        """
        return self._env_var_api.delete_env_var(self.name)

    def to_dict(self):
        """Serialise to a JSON-friendly dict, redacting the value.

        Used by ``util.Encoder`` when ``json()`` / ``str(env_var)`` /
        logging serialises an EnvVar. The value is intentionally redacted
        because env vars typically hold credentials/tokens and we don't
        want them landing in logs / notebook outputs / error reports.
        Use ``env_var.value`` directly if you need the plaintext.
        """
        return {
            "name": self._name,
            "value": "***" if self._value is not None else None,
            "added_on": self._added_on,
            "updated_on": self._updated_on,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"EnvVar({self._name!r})"

    @public
    def get_url(self):
        """Get url to the environment variables page in Hopsworks."""
        return util.get_hostname_replaced_url("/account/env-variables")
