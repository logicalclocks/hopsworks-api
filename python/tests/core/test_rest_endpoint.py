#
#   Copyright 2025 Hopsworks AB
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

import pytest
from hopsworks_common.core import rest_endpoint


class TestRestEndpoint:
    def test_query_params_accepts_dict(self):
        endpoint = rest_endpoint.RestEndpointConfig(
            relative_url="/resource",
            query_params={"limit": 10, "offset": 5},
        )

        assert len(endpoint.query_params) == 2
        assert endpoint.query_params[0].key == "limit"
        assert endpoint.query_params[0].value == 10
        assert endpoint.query_params[1].key == "offset"
        assert endpoint.query_params[1].value == 5

    def test_pagination_factory_returns_correct_class(self):
        pagination = rest_endpoint.PaginationConfig.from_type(
            rest_endpoint.PaginationType.OFFSET, limit=25
        )

        assert isinstance(pagination, rest_endpoint.OffsetPaginationConfig)
        assert pagination.to_dict()["limit"] == 25

    def test_pagination_from_response_requires_type(self):
        with pytest.raises(ValueError):
            rest_endpoint.PaginationConfig.from_response_json({"limit": 5})

    def test_cursor_pagination_validates_options(self):
        with pytest.raises(ValueError):
            rest_endpoint.CursorPaginationConfig(
                cursor_param="cursor", cursor_body_path="payload.cursor"
            )

    def test_rest_endpoint_config_to_dict(self):
        pagination = rest_endpoint.JsonLinkPaginationConfig(
            next_url_path="pagination.next"
        )
        endpoint = rest_endpoint.RestEndpointConfig(
            relative_url="/items",
            query_params=[rest_endpoint.QueryParams(name="page_size", value=50)],
            pagination_config=pagination,
        )

        serialized = endpoint.to_dict()
        assert serialized["relativeUrl"] == "/items"
        assert serialized["queryParams"] == [{"name": "page_size", "value": 50}]
        assert serialized["paginationConfig"]["type"] == "JSON_LINK"
        assert serialized["paginationConfig"]["nextUrlPath"] == "pagination.next"
