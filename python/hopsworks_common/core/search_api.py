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

import logging
from typing import Literal, get_args

from hopsworks_common import client, search_results
from hopsworks_common.internal.aliases import public


DOC_TYPE_ARG = Literal[
    "FEATUREGROUP", "FEATUREVIEW", "TRAININGDATASET", "FEATURE", "ALL"
]
FILTER_BY_ARG = Literal["tag", "tag_name", "tag_key", "tag_value", "keyword"]


@public("hopsworks.core.search_api")
class SearchApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    def featurestore_search(
        self,
        search_term: str,
        filter_by: FILTER_BY_ARG = None,
        offset: int = 0,
        limit: int = 100,
    ) -> search_results.FeaturestoreSearchResult:
        """Search for feature groups, feature views, training datasets and features.

        ```python
        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search("search-term")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()
        ```

        Parameters:
            search_term: The term to search for.
            filter_by: Filter results by a specific field.
            offset: The number of results to skip.
            limit: The number of search results to return.

        Returns:
            The matching results from all feature stores in the project including shared feature stores.

        Raises:
            ValueError: If the search term is not provided or if the filter_by is not one of the allowed values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._search(
            search_term, "ALL", filter_by=filter_by, offset=offset, limit=limit
        )

    # FEATUREGROUP
    def featuregroup_search(
        self,
        search_term: str,
        filter_by: FILTER_BY_ARG = None,
        offset: int = 0,
        limit: int = 100,
    ) -> search_results.FeatureGroupSearchResult:
        """Search for feature group.

        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search("search_term")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()
        ```

        Parameters:
            search_term: The term to search for.
            filter_by: Filter results by a specific field.
            offset: The number of results to skip.
            limit: The number of search results to return.

        Returns:
            The matching feature groups from all feature stores in the project including shared feature stores.

        Raises:
            ValueError: If the search term is not provided or if the filter_by is not one of the allowed values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return search_results.FeatureGroupSearchResult(
            self._search(
                search_term,
                "FEATUREGROUP",
                filter_by=filter_by,
                offset=offset,
                limit=limit,
            )
        )

    # FEATUREVIEW
    def featureview_search(
        self,
        search_term: str,
        filter_by: FILTER_BY_ARG = None,
        offset: int = 0,
        limit: int = 100,
    ) -> search_results.FeatureViewSearchResult:
        """Search for feature views.

        ```python
        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search("search_term")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()
        ```

        Parameters:
            search_term: The term to search for.
            filter_by: Filter results by a specific field (default is None).
            offset: The number of results to skip (default is 0).
            limit: The number of search results to return (default is 100).

        Returns:
            The matching feature views from all feature stores in the project including shared feature stores.

        Raises:
            ValueError: If the search term is not provided or if the filter_by is not one of the allowed values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return search_results.FeatureViewSearchResult(
            self._search(
                search_term,
                "FEATUREVIEW",
                filter_by=filter_by,
                offset=offset,
                limit=limit,
            )
        )

    # TRAININGDATASET
    def trainingdataset_search(
        self,
        search_term: str,
        filter_by: FILTER_BY_ARG = None,
        offset: int = 0,
        limit: int = 100,
    ) -> search_results.TrainingDatasetSearchResult:
        """Search for training datasets.

        ```python
        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search("search_term")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()
        ```

        Parameters:
            search_term: The term to search for.
            filter_by: Filter results by a specific field.
            offset: The number of results to skip.
            limit: The number of search results to return.

        Returns:
            The matching training datasets from all feature stores in the project including shared feature stores.

        Raises:
            ValueError: If the search term is not provided or if the filter_by is not one of the allowed values.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return search_results.TrainingDatasetSearchResult(
            self._search(
                search_term,
                "TRAININGDATASET",
                filter_by=filter_by,
                offset=offset,
                limit=limit,
            )
        )

    # FEATURE
    def feature_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ) -> search_results.FeatureSearchResult:
        """Search for features.

        ```python
        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.feature_search("search_term")

        # get feature group instance
        feature_group = result.features[0].get_feature_group()

        # get feature instance
        feature = result.features[0].get_feature()
        ```

        Parameters:
            search_term: The term to search for.
            offset: The number of results to skip.
            limit: The number of search results to return.

        Returns:
            The matching features from all feature stores in the project including shared feature stores.

        Raises:
            ValueError: If the search term is not provided.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        result = self._search(
            search_term, "FEATURE", filter_by=None, offset=offset, limit=limit
        )

        return search_results.FeatureSearchResult(result)

    def _search(
        self,
        search_term: str,
        doc_type: DOC_TYPE_ARG,
        filter_by: FILTER_BY_ARG,
        offset: int,
        limit: int,
    ):
        if not search_term:
            raise ValueError("Search term not provided.")
        if doc_type not in get_args(DOC_TYPE_ARG):
            raise ValueError(
                f"doc_type must be one of the following {get_args(DOC_TYPE_ARG)}."
            )
        if filter_by is not None and filter_by not in get_args(FILTER_BY_ARG):
            raise ValueError(
                f"filter_by must be one of the following {get_args(FILTER_BY_ARG)}."
            )

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "elastic",
            "featurestore",
            search_term,
        ]
        headers = {"content-type": "application/json"}
        query_params = {"docType": doc_type, "from": offset, "size": limit}

        result = _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )

        if filter_by == "tag":
            return search_results.FeaturestoreSearchResultByTag.from_response_json(
                result
            )
        if filter_by == "tag_name":
            return search_results.FeaturestoreSearchResultByTagName.from_response_json(
                result
            )
        if filter_by == "tag_key":
            return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                result
            )
        if filter_by == "tag_value":
            return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                result
            )
        if filter_by == "keyword":
            return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
                result
            )

        # Default case
        return search_results.FeaturestoreSearchResult.from_response_json(result)
