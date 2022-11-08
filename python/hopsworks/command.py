#
#   Copyright 2022 Hopsworks AB
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

import humps


class Command:
    def __init__(
        self,
        status=None,
        op=None,
        install_type=None,
        error_message=None,
        type=None,
        href=None,
        count=None,
    ):
        self._op = op
        self._install_type = install_type
        self._status = status
        self._error_message = error_message
        self._count = count

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized:
            return cls(**json_decamelized)
        elif json_decamelized["count"] == 0:
            return []
        else:
            return [cls(**command) for command in json_decamelized["items"]]

    @property
    def status(self):
        return self._status

    @property
    def error_message(self):
        return self._error_message
