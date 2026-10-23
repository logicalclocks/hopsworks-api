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


import copy

from hsfs.constructor import (
    external_feature_group_alias,
    fs_query,
    hudi_feature_group_alias,
)


class TestFsQuery:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["fs_query"]["get"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        # Assert
        assert q.query == "test_query"
        assert len(q._on_demand_fg_aliases) == 1
        assert isinstance(
            q._on_demand_fg_aliases[0],
            external_feature_group_alias.ExternalFeatureGroupAlias,
        )
        assert len(q._hudi_cached_feature_groups) == 1
        assert isinstance(
            q._hudi_cached_feature_groups[0],
            hudi_feature_group_alias.HudiFeatureGroupAlias,
        )
        assert q.query_online == "test_query_online"
        assert q.pit_query == "test_pit_query"
        assert q.pushdown_query == "test_pushdown_query"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["fs_query"]["get_basic_info"]["response"]

        # Act
        q = fs_query.FsQuery.from_response_json(json)

        # Assert
        assert q.query == "test_query"
        assert len(q._on_demand_fg_aliases) == 0
        assert len(q._hudi_cached_feature_groups) == 0
        assert q.query_online is None
        assert q.pit_query is None
        assert q.pushdown_query is None

    def _query_with_options(self, backend_fixtures, *options):
        json = copy.deepcopy(backend_fixtures["fs_query"]["get"]["response"])
        json["on_demand_feature_groups"] = json["on_demand_feature_groups"] * len(
            options
        )
        q = fs_query.FsQuery.from_response_json(json)
        for alias, option in zip(q._on_demand_fg_aliases, options, strict=True):
            alias.on_demand_feature_group._options = option
        return q

    def test_pushdown_options_shared(self, backend_fixtures):
        q = self._query_with_options(
            backend_fixtures, {"sfRole": "ANALYST"}, {"sfRole": "ANALYST"}
        )

        assert q._pushdown_options() == {"sfRole": "ANALYST"}

    def test_pushdown_options_none_and_empty_agree(self, backend_fixtures):
        q = self._query_with_options(backend_fixtures, None, {})

        assert q._pushdown_options() == {}

    def test_pushdown_options_differ(self, backend_fixtures):
        q = self._query_with_options(backend_fixtures, {"sfRole": "ANALYST"}, None)

        assert q._pushdown_options() is None

    def test_pushdown_options_message_names_the_feature_groups_and_keys_not_values(
        self, backend_fixtures
    ):
        q = self._query_with_options(
            backend_fixtures,
            {"sfRole": "SECRET_ROLE", "sfWarehouse": "WH_S"},
            {"sfRole": "SECRET_ROLE", "sfWarehouse": "WH_L", "sfSchema": "S2"},
        )

        message = q._pushdown_options_message()

        assert "set different read options (sfSchema, sfWarehouse)" in message
        assert "sfRole" not in message
        assert "SECRET_ROLE" not in message
        assert "WH_S" not in message
        assert "Reading each feature group separately." in message

    def _parsed_query(self, backend_fixtures, *options):
        # The backend sends options as a list of name/value pairs on the feature group.
        json = copy.deepcopy(backend_fixtures["fs_query"]["get"]["response"])
        json["on_demand_feature_groups"] = [
            copy.deepcopy(json["on_demand_feature_groups"][0]) for _ in options
        ]
        for entry, option in zip(
            json["on_demand_feature_groups"], options, strict=True
        ):
            entry["on_demand_feature_group"]["options"] = [
                {"name": name, "value": value} for name, value in option.items()
            ]
        return fs_query.FsQuery.from_response_json(json)

    def test_pushdown_options_parsed_from_the_backend_response(self, backend_fixtures):
        q = self._parsed_query(
            backend_fixtures,
            {"sfRole": "ANALYST", "sfWarehouse": "WH_S"},
            {"sfRole": "ANALYST", "sfWarehouse": "WH_S"},
        )

        assert q._pushdown_options() == {"sfRole": "ANALYST", "sfWarehouse": "WH_S"}

    def test_pushdown_options_parsed_from_the_backend_response_differ(
        self, backend_fixtures
    ):
        q = self._parsed_query(
            backend_fixtures,
            {"sfRole": "ANALYST", "sfWarehouse": "WH_S"},
            {"sfRole": "ANALYST", "sfWarehouse": "WH_L"},
        )

        assert q._pushdown_options() is None
        assert "(sfWarehouse)" in q._pushdown_options_message()
