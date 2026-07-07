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
        query_online_scan: str | None = None,
        prefix: str | None = None,
        collect_n: int | None = None,
        collect_feature_name: str | None = None,
        collect_order_by: str | None = None,
        collect_ascending: bool | None = None,
        collect_filter_applied: bool | None = None,
        collect_source_features: list[str] | None = None,
        collect_filters: list[dict[str, Any]] | None = None,
        snowflake_template: bool | None = None,
        snowflake_templates: list[str] | None = None,
        query_ronsql: str | None = None,
        ronsql_database: str | None = None,
        aggregate_window: int | None = None,
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
        # Direct single-entity scan for collect statements (scan_vectors on the SQL
        # client): reads the ordered index backward and stops at the bound row cap,
        # unlike query_online's windowed ROW_NUMBER plan. The trailing ? binds
        # min(limit, collect_n). None from backends predating the field.
        self._query_online_scan = query_online_scan
        self._prefix = prefix
        # collect ("most recent N rows per entity"): when set, this statement returns up to
        # collect_n rows per entity to fold into list-typed features (see online_store_sql_engine).
        # pyhumps decamelizes the wire key "collectN" to "collectn" (a trailing single
        # capital does not split), so absorb that form when the canonical kwarg is absent.
        if collect_n is None:
            collect_n = kwargs.get("collectn")
        self._collect_n = collect_n
        # Name of the feature-view feature the client folds the collect rows into
        # (v2 C1: <fg_name>_collect, typed array<struct<...>>).
        self._collect_feature_name = collect_feature_name
        # Order column and direction of the collect: the client sorts the returned rows by
        # this column before folding (SQL subquery/CTE-scan output order is not guaranteed),
        # newest-first by default, oldest-first when collect_ascending.
        self._collect_order_by = collect_order_by
        self._collect_ascending = collect_ascending
        # True when the feature view's filters apply to this collect statement: the /scan
        # fallback cannot express them, so it must not serve filtered collects.
        self._collect_filter_applied = collect_filter_applied
        # The statement's source columns (primary keys, then struct fields, order column
        # first): the /scan fallback projects exactly these so it never reads columns the
        # feature view did not select.
        self._collect_source_features = collect_source_features
        # The feature view's filters in structured form ({feature, condition, value}),
        # present only when every condition is expressible as an RDRS /scan filter; the
        # /scan fallback applies them so filtered collects stay correct on REST.
        self._collect_filters = collect_filters
        # True when this statement serves a snowflake nested subtree (FSTORE-2060):
        # template outputs are already aliased to the prefixed feature-view names and
        # overlay verbatim into the vector.
        self._snowflake_template = snowflake_template
        # The templates: one combined statement for an all-INNER subtree, or one
        # chain statement per nested join when the subtree has LEFT joins (so a hop
        # miss only loses the unreachable nodes' features).
        self._snowflake_templates = snowflake_templates
        # RonSQL template + target database for serving this statement via RDRS /ronsql
        # (v3 online path); the client substitutes typed literals for the `?` markers.
        self._query_ronsql = query_ronsql
        self._ronsql_database = ronsql_database
        # When set, query_ronsql is a pushdown-aggregation statement whose trailing `?`
        # is the window bound: the client substitutes now - aggregate_window (seconds).
        self._aggregate_window = aggregate_window

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
    def query_online_scan(self) -> str | None:
        return self._query_online_scan

    @property
    def prefix(self) -> str | None:
        return self._prefix

    @property
    def collect_n(self) -> int | None:
        return self._collect_n

    @property
    def collect_feature_name(self) -> str | None:
        return self._collect_feature_name

    @property
    def collect_order_by(self) -> str | None:
        return self._collect_order_by

    @property
    def collect_ascending(self) -> bool | None:
        return self._collect_ascending

    @property
    def collect_filter_applied(self) -> bool | None:
        return self._collect_filter_applied

    @property
    def collect_source_features(self) -> list[str] | None:
        return self._collect_source_features

    @property
    def collect_filters(self) -> list[dict[str, Any]] | None:
        return self._collect_filters

    @property
    def snowflake_template(self) -> bool | None:
        return self._snowflake_template

    @property
    def snowflake_templates(self) -> list[str] | None:
        return self._snowflake_templates

    @property
    def query_ronsql(self) -> str | None:
        return self._query_ronsql

    @property
    def ronsql_database(self) -> str | None:
        return self._ronsql_database

    @property
    def aggregate_window(self) -> int | None:
        return self._aggregate_window

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
        | list[dict[str, Any]]
        | None,
    ) -> None:
        # a statement can carry no parameters (backend edge cases must fail as a
        # typed serving error downstream, not as a parse crash here)
        if not prepared_statement_parameters:
            self._prepared_statement_parameters = []
            return
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
