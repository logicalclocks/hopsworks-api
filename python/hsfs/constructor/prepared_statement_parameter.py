#
#  Copyright 2021. Logical Clocks AB
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import humps
from hsfs import util


class PreparedStatementParameter:
    def __init__(
        self,
        name: Optional[str] = None,
        index: Optional[int] = None,
        type: Optional[str] = None,
        href: Optional[str] = None,
        **kwargs,
    ) -> None:
        self._name = name
        self._index = index

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> PreparedStatementParameter:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def update_from_response_json(
        self, json_dict: Dict[str, Any]
    ) -> PreparedStatementParameter:
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self._name, "index": self._index}

    def __repr__(self) -> str:
        return humps.decamelize(json.dumps(self.to_dict(), sort_keys=True, indent=4))

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def index(self) -> Optional[int]:
        return self._index

    @name.setter
    def name(self, name: Optional[str]) -> None:
        self._name = name

    @index.setter
    def prepared_statement_index(self, index: Optional[int]) -> None:
        self._index = index
