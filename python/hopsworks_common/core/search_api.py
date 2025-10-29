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

import json
import logging
from typing import Dict, List, Literal, Optional, Union, get_args
from urllib.parse import quote

from hopsworks_common import client
from hopsworks_common.search_results import FeaturestoreSearchResult
from hopsworks_common.util import Encoder


DOC_TYPE_ARG = Literal[
    "FEATUREGROUP", "FEATUREVIEW", "TRAININGDATASET", "FEATURE", "ALL"
]

class TagSearchFilter:
    def __init__(
            self,
            name: str,
            key: str,
            value: str
    ):
        self._log = logging.getLogger(__name__)
        self._name = name
        self._key = key
        self._value = value

    @property
    def name(self):
        """Name of the tag"""
        return self._name

    @property
    def key(self):
        """Key(property) of the tag"""
        return self._key

    @property
    def value(self):
        """Value of the tag key"""
        return self._value

    def to_dict(self):
        """Convert TagSearchFilter to dictionary"""
        return {
            "name": self._name,
            "key": self._key,
            "value": self._value
        }

    def json(self):
        return json.dumps(self, cls=Encoder)

    @classmethod
    def from_dict(cls, tag_dict: Dict[str, str]) -> "TagSearchFilter":
        """Create a TagSearchFilter from a dictionary"""
        return cls(
            name=tag_dict.get("name", ""),
            key=tag_dict.get("key", ""),
            value=tag_dict.get("value", "")
        )

class KeywordSearchFilter:
    def __init__(
            self,
            value: str
    ):
        self._log = logging.getLogger(__name__)
        self._value = value

    @property
    def value(self):
        """Value of the keyword"""
        return self._value

    def to_dict(self):
        """Convert KeywordSearchFilter to dictionary"""
        return {
            "value": self._value
        }

    def json(self):
        return json.dumps(self, cls=Encoder)

    @classmethod
    def from_dict(cls, keyword_dict: Dict[str, str]) -> "KeywordSearchFilter":
        """Create a KeywordSearchFilter from a dictionary"""
        return cls(
            value=keyword_dict.get("value", "")
        )


class SearchApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    def feature_store(
            self,
            search_term: str = None,
            keyword_filter: Optional[Union[str, List[str]]] = None,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]] = None,
            offset: int = 0,
            limit: int = 100,
            global_search: bool = False,
    ) -> FeaturestoreSearchResult:
        """
        Search for feature groups, feature views, training datasets and features.

        # Arguments
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries, or an array of TagSearchFilter objects.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects
        # Returns
            `FeaturestoreSearchResult`: The search results containing lists of metadata objects for feature groups, feature views, training datasets, and features.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        # Example
```python
        import hopsworks

        project = hopsworks.login()
        search_api = project.get_search_api()

        # Simple search
        result = search_api.feature_store("search-term")

        # Access results
        for fg_meta in result.feature_groups:
            print(f"Feature Group: {fg_meta.name} v{fg_meta.version}")
            print(f"Description: {fg_meta.description}")
            print(f"Highlights: {fg_meta.highlights}")

            # Get the same FeatureGroup object as returned by featurestore.get_feature_group
            fg = fg_meta.get()

        # Search with a single keyword (string)
        result = search_api.feature_store("search-term", keyword_filter="ml")

        # Search with multiple keywords (array of strings)
        result = search_api.feature_store("search-term", keyword_filter=["ml", "production"])

        # Search with tag filter as a single dictionary
        result = search_api.feature_store(
            "search-term",
            tag_filter={"name": "tag1", "key": "environment", "value": "production"}
        )

        # Search with tag filter as an array of dictionaries
        result = search_api.feature_store(
            "search-term",
            tag_filter=[
                {"name": "tag1", "key": "environment", "value": "production"},
                {"name": "tag2", "key": "version", "value": "v1.0"}
            ]
        )

        # Search with TagSearchFilter objects
        from hopsworks_common.core.search_api import TagSearchFilter
        tags = [
            TagSearchFilter(name="tag1", key="environment", value="production"),
            TagSearchFilter(name="tag2", key="version", value="v1.0")
        ]
        result = search_api.feature_store("search-term", tag_filter=tags)

        # Search with both keyword_filter and tag_filter
        result = search_api.feature_store(
            "search-term",
            keyword_filter=["ml", "production"],
            tag_filter=tags
        )
```
        """
        return self._search(
            search_term,
            doc_type="ALL",
            keyword_filter=keyword_filter,
            tag_filter=tag_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

    def feature_groups(
            self,
            search_term: str = None,
            keyword_filter: Optional[Union[str, List[str]]] = None,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]] = None,
            offset: int = 0,
            limit: int = 100,
            global_search: bool = False,
    ) -> List:
        """
        Search for feature groups only.

        # Arguments
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries, or an array of TagSearchFilter objects.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects
        # Returns
            `List`: A list of metadata objects for feature groups matching the search criteria.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        # Example
```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for feature groups
        fg_metas = fs.feature_groups("customer")

        for fg_meta in fg_metas:
            print(f"Feature Group: {fg_meta.name} v{fg_meta.version}")

            # Get the same FeatureGroup object as returned by featurestore.get_feature_group
            fg = fg_meta.get()
```
        """
        result = self._search(
            search_term,
            doc_type="FEATUREGROUP",
            keyword_filter=keyword_filter,
            tag_filter=tag_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )
        return result.feature_groups

    def feature_views(
            self,
            search_term: str = None,
            keyword_filter: Optional[Union[str, List[str]]] = None,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]] = None,
            offset: int = 0,
            limit: int = 100,
            global_search: bool = False,
    ) -> List:
        """
        Search for feature views only.

        # Arguments
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries, or an array of TagSearchFilter objects.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects
        # Returns
            `List`: A list of metadata objects for feature views matching the search criteria.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        # Example
```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for feature views
        fs_metas = search_api.feature_views("customer")

        for fv_meta in fv_metas:
            print(f"Feature View: {fv_meta.name} v{fv_meta.version}")

            # Get the same FeatureView object as returned by featurestore.get_feature_view
            fv = fv_meta.get()
```
        """
        result = self._search(
            search_term,
            doc_type="FEATUREVIEW",
            keyword_filter=keyword_filter,
            tag_filter=tag_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )
        return result.feature_views

    def training_datasets(
            self,
            search_term: str = None,
            keyword_filter: Optional[Union[str, List[str]]] = None,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]] = None,
            offset: int = 0,
            limit: int = 100,
            global_search: bool = False,
    ) -> List:
        """
        Search for training datasets only.

        # Arguments
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries, or an array of TagSearchFilter objects.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects
        # Returns
            `List`: A list of metadata objects for training datasets matching the search criteria.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        # Example
```python
        import hopsworks

        project = hopsworks.login()
        search_api = project.get_search_api()

        # Search for training datasets
        td_metas = search_api.training_datasets("model")

        for td_meta in td_metas:
            print(f"Training Dataset: {td_meta.name} v{td_meta.version}")

            # Get the same TrainingDataset object as returned by featurestore.get_training_dataset
            td = td_meta.get()
```
        """
        result = self._search(
            search_term,
            doc_type="TRAININGDATASET",
            keyword_filter=keyword_filter,
            tag_filter=tag_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )
        return result.training_datasets

    def features(
            self,
            search_term: str = None,
            keyword_filter: Optional[Union[str, List[str]]] = None,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]] = None,
            offset: int = 0,
            limit: int = 100,
            global_search: bool = False,
    ) -> List:
        """
        Search for features only.

        # Arguments
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries, or an array of TagSearchFilter objects.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects
        # Returns
            `List`: A list of features matching the search criteria.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        # Example
```python
        import hopsworks

        project = hopsworks.login()
        search_api = project.get_search_api()

        # Search for features
        features = search_api.features("age")

        for feature in features:
            print(f"Feature: {feature.name}")
```
        """
        result = self._search(
            search_term,
            doc_type="FEATURE",
            keyword_filter=keyword_filter,
            tag_filter=tag_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )
        return result.features

    def _normalize_keyword_filter(
            self,
            keyword_filter: Optional[Union[str, List[str]]]
    ) -> Optional[List[KeywordSearchFilter]]:
        """
        Normalize keyword_filter input to a list of KeywordSearchFilter objects.

        Accepts:
        - None: returns None
        - Single string: converts to [KeywordSearchFilter]
        - List of strings: converts each to KeywordSearchFilter
        """
        if keyword_filter is None:
            return None

        # If single string, convert to list
        if isinstance(keyword_filter, str):
            keyword_filter = [keyword_filter]

        # Convert all items to KeywordSearchFilter objects
        normalized_keywords = []
        if isinstance(keyword_filter, list):
            for item in keyword_filter:
                if not isinstance(item, str):
                    raise ValueError(
                        f"Invalid keyword filter item. Expected string, got {type(item)}"
                    )
                normalized_keywords.append(KeywordSearchFilter(value=item))
            return normalized_keywords

        raise ValueError(
            f"Invalid keyword_filter type. Expected str or List[str], got {type(keyword_filter)}"
        )

    def _normalize_tag_filter(
            self,
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]]
    ) -> Optional[List[TagSearchFilter]]:
        """
        Normalize tag_filter input to a list of TagSearchFilter objects.

        Accepts:
        - None: returns None
        - Single dict: converts to [TagSearchFilter]
        - List of dicts: converts to [TagSearchFilter]
        - List of TagSearchFilter: returns as-is
        - List of mixed dicts and TagSearchFilter: normalizes all to TagSearchFilter
        """
        if tag_filter is None:
            return None

        # If single dictionary, convert to list
        if isinstance(tag_filter, dict):
            tag_filter = [tag_filter]

        # Convert all items to TagSearchFilter objects
        normalized_tags = []
        for tag in tag_filter:
            if isinstance(tag, TagSearchFilter):
                normalized_tags.append(tag)
            elif isinstance(tag, dict):
                normalized_tags.append(TagSearchFilter.from_dict(tag))
            else:
                raise ValueError(
                    f"Invalid tag filter item. Expected dict or TagSearchFilter, got {type(tag)}"
                )

        return normalized_tags

    def _search(
            self,
            search_term: str,
            doc_type: DOC_TYPE_ARG,
            keyword_filter: Optional[Union[str, List[str]]],
            tag_filter: Optional[Union[Dict[str, str], List[Union[Dict[str, str], TagSearchFilter]]]],
            offset: int,
            limit: int,
            global_search: bool = False,
    ) -> FeaturestoreSearchResult:
        if doc_type not in get_args(DOC_TYPE_ARG):
            raise ValueError(
                f"doc_type must be one of the following {get_args(DOC_TYPE_ARG)}."
            )

        # Normalize keyword_filter to list of KeywordSearchFilter objects
        normalized_keywords = self._normalize_keyword_filter(keyword_filter)

        # Normalize tag_filter to list of TagSearchFilter objects
        normalized_tags = self._normalize_tag_filter(tag_filter)

        _client = client.get_instance()
        if global_search:
            path_params = [
                "elastic",
                "featurestore"
            ]
        else:
            path_params = [
                "project",
                _client._project_id,
                "elastic",
                "featurestore"
            ]

        headers = {"content-type": "application/json"}
        query_params = {
            "searchTerm": search_term,
            "docType": doc_type,
            "from": offset,
            "size": limit
        }

        # Add keyword filter if provided
        if normalized_keywords:
            # Convert list of KeywordSearchFilter objects to list of dictionaries
            keywords_dict = [keyword.to_dict() for keyword in normalized_keywords]
            # Serialize to JSON string and URL encode
            query_params["keywords"] = quote(json.dumps(keywords_dict), safe='')

        # Add tag filter if provided
        if normalized_tags:
            # Convert list of TagSearchFilter objects to list of dictionaries
            tags_dict = [tag.to_dict() for tag in normalized_tags]
            # Serialize to JSON string and URL encode
            query_params["tags"] = quote(json.dumps(tags_dict), safe='')

        result = _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )

        return FeaturestoreSearchResult(result)
