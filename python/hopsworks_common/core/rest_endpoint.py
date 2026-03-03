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
from __future__ import annotations

import json
from enum import Enum
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util


class PaginationType(str, Enum):
    JSON_LINK = "json_link"
    HEADER_LINK = "header_link"
    HEADER_CURSOR = "header_cursor"
    OFFSET = "offset"
    PAGE_NUMBER = "page_number"
    CURSOR = "cursor"
    SINGLE_PAGE = "single_page"
    AUTO = "auto"


@public("hopsworks.core.rest_endpoint.QueryParams")
class QueryParams:
    """Represents an arbitrary name/value pair used by REST endpoint configurations."""

    def __init__(self, name: str, value: Any) -> None:
        self._name = name
        self._value = value

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> QueryParams | None:
        if json_dict is None:
            return None
        json_decamelized: dict[str, Any] = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "value": self._value,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self) -> str:
        return f"QueryParams({self._name!r}, {self._value!r})"

    @public
    @property
    def name(self) -> str:
        return self._name

    @property
    def key(self) -> str:
        return self._name

    @public
    @property
    def value(self) -> Any:
        return self._value


@public("hopsworks.core.rest_endpoint.PaginationConfig")
class PaginationConfig:
    """Base pagination configuration that specific strategies inherit from."""

    pagination_type: PaginationType | None = None

    def __init__(self, **extra_params: Any) -> None:
        self._extra_params = {
            key: value
            for key, value in (extra_params or {}).items()
            if value is not None
        }

    @classmethod
    def from_type(
        cls,
        pagination_type: str | PaginationType | None,
        **kwargs,
    ) -> PaginationConfig | None:
        if pagination_type is None:
            return None

        pagination_enum = PaginationType(pagination_type)
        pagination_class = _PAGINATION_CLASS_MAP.get(pagination_enum)
        if pagination_class is None:
            raise ValueError(
                f"Unsupported pagination type '{pagination_enum.value}'. "
                f"Supported types: {[ptype.value for ptype in _PAGINATION_CLASS_MAP]}"
            )
        return pagination_class(**kwargs)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> PaginationConfig | None:
        if json_dict is None:
            return None

        json_decamelized: dict[str, Any] = humps.decamelize(json_dict)
        pagination_type = json_decamelized.pop("type", None)
        if pagination_type is None:
            raise ValueError("Pagination configuration must include the 'type' field.")
        return cls.from_type(pagination_type, **json_decamelized)

    def to_dict(self) -> dict[str, Any]:
        payload = self._merge_payload(self._payload())
        payload_camelized = humps.camelize(payload) if payload else {}

        if self.pagination_type is not None:
            payload_camelized["type"] = self.pagination_type.value
        return payload_camelized

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self) -> str:
        payload = self._merge_payload(self._payload())
        pagination_type = self.pagination_type.value if self.pagination_type else None
        return (
            f"{self.__class__.__name__}(type={pagination_type!r}, config={payload!r})"
        )

    def _payload(self) -> dict[str, Any]:
        """Return pagination-specific options. Subclasses should override."""
        return {}

    def _merge_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        merged = {key: value for key, value in payload.items() if value is not None}
        for key, value in self._extra_params.items():
            merged.setdefault(key, value)
        return merged


@public("hopsworks.core.rest_endpoint.JsonLinkPaginationConfig")
class JsonLinkPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.JSON_LINK

    def __init__(self, next_url_path: str, **kwargs) -> None:
        if not next_url_path:
            raise ValueError("JsonLinkPaginationConfig requires 'next_url_path'.")
        super().__init__(**kwargs)
        self._next_url_path = next_url_path

    def _payload(self) -> dict[str, Any]:
        return {
            "next_url_path": self._next_url_path,
        }


@public("hopsworks.core.rest_endpoint.HeaderLinkPaginationConfig")
class HeaderLinkPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.HEADER_LINK

    def __init__(self, links_next_key: str = "next", **kwargs) -> None:
        super().__init__(**kwargs)
        self._links_next_key = links_next_key or "next"

    def _payload(self) -> dict[str, Any]:
        return {
            "links_next_key": self._links_next_key,
        }


@public("hopsworks.core.rest_endpoint.HeaderCursorPaginationConfig")
class HeaderCursorPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.HEADER_CURSOR

    def __init__(
        self,
        cursor_key: str = "next",
        cursor_param: str = "cursor",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._cursor_key = cursor_key or "next"
        self._cursor_param = cursor_param or "cursor"

    def _payload(self) -> dict[str, Any]:
        return {
            "cursor_key": self._cursor_key,
            "cursor_param": self._cursor_param,
        }


@public("hopsworks.core.rest_endpoint.OffsetPaginationConfig")
class OffsetPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.OFFSET

    def __init__(
        self,
        limit: int | None = None,
        offset: int = 0,
        offset_param: str = "offset",
        offset_body_path: str | None = None,
        limit_param: str = "limit",
        limit_body_path: str | None = None,
        total_path: str | None = None,
        maximum_offset: int | None = None,
        stop_after_empty_page: bool = True,
        has_more_path: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._limit = limit
        self._offset = offset
        self._offset_param = offset_param or "offset"
        self._offset_body_path = offset_body_path
        self._limit_param = limit_param or "limit"
        self._limit_body_path = limit_body_path
        self._total_path = total_path
        self._maximum_offset = maximum_offset
        self._stop_after_empty_page = stop_after_empty_page
        self._has_more_path = has_more_path

    def _payload(self) -> dict[str, Any]:
        return {
            "limit": self._limit,
            "offset": self._offset,
            "offset_param": self._offset_param,
            "offset_body_path": self._offset_body_path,
            "limit_param": self._limit_param,
            "limit_body_path": self._limit_body_path,
            "total_path": self._total_path,
            "maximum_offset": self._maximum_offset,
            "stop_after_empty_page": self._stop_after_empty_page,
            "has_more_path": self._has_more_path,
        }


@public("hopsworks.core.rest_endpoint.PageNumberPaginationConfig")
class PageNumberPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.PAGE_NUMBER

    def __init__(
        self,
        base_page: int = 0,
        page_param: str = "page",
        total_path: str | None = None,
        maximum_page: int | None = None,
        stop_after_empty_page: bool = True,
        has_more_path: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._base_page = base_page
        self._page_param = page_param or "page"
        self._total_path = total_path
        self._maximum_page = maximum_page
        self._stop_after_empty_page = stop_after_empty_page
        self._has_more_path = has_more_path

    def _payload(self) -> dict[str, Any]:
        return {
            "base_page": self._base_page,
            "page_param": self._page_param,
            "total_path": self._total_path,
            "maximum_page": self._maximum_page,
            "stop_after_empty_page": self._stop_after_empty_page,
            "has_more_path": self._has_more_path,
        }


@public("hopsworks.core.rest_endpoint.CursorPaginationConfig")
class CursorPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.CURSOR

    def __init__(
        self,
        cursor_path: str = "cursors.next",
        cursor_param: str | None = None,
        cursor_body_path: str | None = None,
        **kwargs,
    ) -> None:
        if cursor_param and cursor_body_path:
            raise ValueError(
                "CursorPaginationConfig cannot set both 'cursor_param' and 'cursor_body_path'."
            )
        super().__init__(**kwargs)
        self._cursor_path = cursor_path or "cursors.next"
        if cursor_param is None and cursor_body_path is None:
            cursor_param = "cursor"
        self._cursor_param = cursor_param
        self._cursor_body_path = cursor_body_path

    def _payload(self) -> dict[str, Any]:
        return {
            "cursor_path": self._cursor_path,
            "cursor_param": self._cursor_param,
            "cursor_body_path": self._cursor_body_path,
        }


@public("hopsworks.core.rest_endpoint.SinglePagePaginationConfig")
class SinglePagePaginationConfig(PaginationConfig):
    pagination_type = PaginationType.SINGLE_PAGE

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


@public("hopsworks.core.rest_endpoint.AutoPaginationConfig")
class AutoPaginationConfig(PaginationConfig):
    pagination_type = PaginationType.AUTO

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


_PAGINATION_CLASS_MAP: dict[PaginationType, type[PaginationConfig]] = {
    PaginationType.JSON_LINK: JsonLinkPaginationConfig,
    PaginationType.HEADER_LINK: HeaderLinkPaginationConfig,
    PaginationType.HEADER_CURSOR: HeaderCursorPaginationConfig,
    PaginationType.OFFSET: OffsetPaginationConfig,
    PaginationType.PAGE_NUMBER: PageNumberPaginationConfig,
    PaginationType.CURSOR: CursorPaginationConfig,
    PaginationType.SINGLE_PAGE: SinglePagePaginationConfig,
    PaginationType.AUTO: AutoPaginationConfig,
}


@public("hopsworks.core.rest_endpoint.RestEndpointConfig")
class RestEndpointConfig:
    """Configuration describing how a REST resource should be accessed."""

    def __init__(
        self,
        relative_url: str | None = None,
        query_params: list[QueryParams] | dict[str, Any] | None = None,
        pagination_config: PaginationConfig | dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        self._relative_url = relative_url
        self._query_params = self._init_query_params(query_params)
        self._pagination_config = self._init_pagination_config(pagination_config)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> RestEndpointConfig | None:
        if json_dict is None:
            return None

        json_decamelized: dict[str, Any] = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self._relative_url is not None:
            payload["relativeUrl"] = self._relative_url
        if self._query_params:
            payload["queryParams"] = [param.to_dict() for param in self._query_params]
        if self._pagination_config is not None:
            payload["paginationConfig"] = self._pagination_config.to_dict()
        return payload

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self) -> str:
        return (
            "RestEndpointConfig("
            f"relative_url={self._relative_url!r}, "
            f"query_params={self._query_params!r}, "
            f"pagination_config={self._pagination_config!r}"
            ")"
        )

    def _init_query_params(
        self, query_params: list[QueryParams] | dict[str, Any] | None
    ) -> list[QueryParams]:
        if query_params is None:
            return []

        if isinstance(query_params, dict):
            return [
                QueryParams(name=key, value=value)
                for key, value in query_params.items()
            ]

        if isinstance(query_params, list):
            normalized: list[QueryParams] = []
            for param in query_params:
                if isinstance(param, QueryParams):
                    normalized.append(param)
                elif isinstance(param, dict):
                    normalized.append(QueryParams(**param))
                else:
                    raise TypeError(
                        "Query parameters must be provided as QueryParams instances or dictionaries."
                    )
            return normalized

        raise TypeError(
            "query_params must be a list of QueryParams, a dictionary, or None."
        )

    def _init_pagination_config(
        self, pagination_config: PaginationConfig | dict[str, Any] | None
    ) -> PaginationConfig | None:
        if pagination_config is None:
            return None
        if isinstance(pagination_config, PaginationConfig):
            return pagination_config
        if isinstance(pagination_config, str):
            return PaginationConfig.from_type(pagination_config)
        if isinstance(pagination_config, dict):
            return PaginationConfig.from_response_json(pagination_config)
        raise TypeError(
            "pagination_config must be a PaginationConfig or a dictionary representation."
        )

    @public
    @property
    def relative_url(self) -> str | None:
        return self._relative_url

    @public
    @property
    def query_params(self) -> list[QueryParams]:
        return self._query_params

    @public
    @property
    def pagination_config(self) -> PaginationConfig | None:
        return self._pagination_config
