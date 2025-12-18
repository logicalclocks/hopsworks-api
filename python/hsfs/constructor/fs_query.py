#
#   Copyright 2020 Logical Clocks AB
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

from typing import Any, TypeVar

import humps
from hsfs import engine
from hsfs.constructor import external_feature_group_alias, hudi_feature_group_alias


class FsQuery:
    def __init__(
        self,
        query: str,
        on_demand_feature_groups: list[dict[str, Any]] | None,
        hudi_cached_feature_groups: list[dict[str, Any]] | None,
        query_online: str | None = None,
        pit_query: str | None = None,
        pit_query_asof: str | None = None,
        hqs_payload: str | None = None,
        hqs_payload_signature: str | None = None,
        href: str | None = None,
        expand: list[str] | None = None,
        items: list[dict[str, Any]] | None = None,
        type: str | None = None,
        delta_cached_feature_groups: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> None:
        self._query = query
        self._query_online = query_online
        self._pit_query = pit_query
        self._pit_query_asof = pit_query_asof

        self._hqs_payload = hqs_payload
        self._hqs_payload_signature = hqs_payload_signature

        if on_demand_feature_groups is not None:
            self._on_demand_fg_aliases = [
                external_feature_group_alias.ExternalFeatureGroupAlias.from_response_json(
                    fg
                )
                for fg in on_demand_feature_groups
            ]
        else:
            self._on_demand_fg_aliases = []

        if hudi_cached_feature_groups is not None:
            self._hudi_cached_feature_groups = [
                hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(fg)
                for fg in hudi_cached_feature_groups
            ]
        else:
            self._hudi_cached_feature_groups = []

        if delta_cached_feature_groups is not None:
            self._delta_cached_feature_groups = [
                hudi_feature_group_alias.HudiFeatureGroupAlias.from_response_json(fg)
                for fg in delta_cached_feature_groups
            ]
        else:
            self._delta_cached_feature_groups = []

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> FsQuery:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def query(self) -> str:
        return self._query

    @property
    def query_online(self) -> str | None:
        return self._query_online

    @property
    def pit_query(self) -> str | None:
        return self._pit_query

    @property
    def pit_query_asof(self) -> str | None:
        return self._pit_query_asof

    @property
    def on_demand_fg_aliases(
        self,
    ) -> list[external_feature_group_alias.ExternalFeatureGroupAlias]:
        return self._on_demand_fg_aliases

    @property
    def hudi_cached_feature_groups(
        self,
    ) -> list[hudi_feature_group_alias.HudiFeatureGroupAlias]:
        return self._hudi_cached_feature_groups

    @property
    def hqs_payload(self) -> Optional[str]:
        return self._hqs_payload

    @property
    def hqs_payload_signature(self) -> Optional[str]:
        return self._hqs_payload_signature

    def register_external(
        self,
        spine: TypeVar("pyspark.sql.DataFrame") | TypeVar("pyspark.RDD") | None = None,
    ) -> None:
        if self._on_demand_fg_aliases is None:
            return

        for external_fg_alias in self._on_demand_fg_aliases:
            if type(external_fg_alias.on_demand_feature_group).__name__ == "SpineGroup":
                external_fg_alias.on_demand_feature_group.dataframe = spine
            engine.get_instance().register_external_temporary_table(
                external_fg_alias.on_demand_feature_group,
                external_fg_alias.alias,
            )

    def register_hudi_tables(
        self,
        feature_store_id: int,
        feature_store_name: str,
        read_options: dict[str, Any] | None,
    ) -> None:
        for hudi_fg in self._hudi_cached_feature_groups:
            engine.get_instance().register_hudi_temporary_table(
                hudi_fg, feature_store_id, feature_store_name, read_options
            )

    def register_delta_tables(
        self,
        feature_store_id: int,
        feature_store_name: str,
        read_options: dict[str, Any] | None,
        is_cdc_query: bool = False,
    ) -> None:
        for delta_fg in self._delta_cached_feature_groups:
            engine.get_instance().register_delta_temporary_table(
                delta_fg_alias=delta_fg,
                feature_store_id=feature_store_id,
                feature_store_name=feature_store_name,
                read_options=read_options,
                is_cdc_query=is_cdc_query,
            )
