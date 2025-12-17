from typing import Dict, List, Optional


class RequestParams:
    def __init__(
        self,
        name: str,
        value: str,
    ):
        self._name = name
        self._value = value

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, value: str) -> None:
        self._value = value


class RESTEndpointConfig:
    def __init__(
        self,
        path: Optional[str] = None,
        query_params: Optional[List[RequestParams]] = [],
    ):

        self._path = path
        self._query_params = query_params

    @property
    def path(self) -> Optional[str]:
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @property
    def query_params(self) -> List[RequestParams]:
        return self._query_params

    @query_params.setter
    def query_params(self, query_params: List[RequestParams]) -> None:
        self._query_params = query_params
