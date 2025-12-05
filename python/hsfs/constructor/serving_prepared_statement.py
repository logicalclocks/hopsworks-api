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
from typing import Any

import humps
from hsfs import util
from hsfs.constructor import prepared_statement_parameter


class ServingPreparedStatement:
    def __init__(
        self,
        feature_group_id: int | None = None,
        prepared_statement_index: int | None = None,
        prepared_statement_parameters: list[
            prepared_statement_parameter.PreparedStatementParameter
        ]
        | None = None,
        query_online: str | None = None,
        prefix: str | None = None,
        type: str | None = None,
        items: list[dict[str, Any]] | None = None,
        count: int | None = None,
        href: str | None = None,
        **kwargs,
    ) -> None:
        self._feature_group_id = feature_group_id
        self._prepared_statement_index = prepared_statement_index
        # use setter to ensure that the parameters are sorted by index
        self.prepared_statement_parameters = prepared_statement_parameters
        self._query_online = query_online
        self._prefix = prefix

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> list[ServingPreparedStatement]:
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        return [cls(**pstm_dto) for pstm_dto in json_decamelized["items"]]

    def update_from_response_json(
        self, json_dict: dict[str, Any]
    ) -> ServingPreparedStatement:
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        return {
            "preparedStatementIndex": self._prepared_statement_index,
            "preparedStatementParameters": self._prepared_statement_parameters,
            "queryOnline": self._query_online,
        }

    def __repr__(self) -> str:
        repr_dict = humps.decamelize(self.to_dict())
        repr_dict["feature_group_id"] = self._feature_group_id
        repr_dict["prefix"] = self._prefix
        repr_dict["prepared_statement_parameters"] = [
            pstm_param.__repr__() for pstm_param in self._prepared_statement_parameters
        ]
        return json.dumps(repr_dict, sort_keys=True, indent=4)

    @property
    def feature_group_id(self) -> int | None:
        return self._feature_group_id

    @property
    def prepared_statement_index(self) -> int | None:
        return self._prepared_statement_index

    @property
    def prepared_statement_parameters(
        self,
    ) -> list[prepared_statement_parameter.PreparedStatementParameter] | None:
        return self._prepared_statement_parameters

    @property
    def query_online(self) -> str | None:
        return self._query_online

    @property
    def prefix(self) -> str | None:
        return self._prefix

    @feature_group_id.setter
    def feature_group_id(self, feature_group_id: int | None) -> None:
        self._feature_group_id = feature_group_id

    @prepared_statement_index.setter
    def prepared_statement_index(self, prepared_statement_index: int | None) -> None:
        self._prepared_statement_index = prepared_statement_index

    @prepared_statement_parameters.setter
    def prepared_statement_parameters(
        self,
        prepared_statement_parameters: list[
            prepared_statement_parameter.PreparedStatementParameter
        ]
        | list[dict[str, Any]],
    ) -> None:
        if isinstance(prepared_statement_parameters[0], dict):
            prepared_statement_parameters = [
                prepared_statement_parameter.PreparedStatementParameter.from_response_json(
                    pstm_param
                )
                for pstm_param in prepared_statement_parameters
            ]

        self._prepared_statement_parameters = sorted(
            prepared_statement_parameters, key=lambda x: x.index
        )

    @query_online.setter
    def query_online(self, query_online: str | None) -> None:
        self._query_online = query_online

    @prefix.setter
    def prefix(self, prefix: str | None) -> None:
        self._prefix = prefix
