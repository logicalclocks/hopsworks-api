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


DOC_TYPE_ARG = Literal["FEATUREGROUP", "FEATUREVIEW", "TRAININGDATASET", "FEATURE","ALL"]


class SearchApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    def featurestore_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ):
        """
        Search for feature groups, feature views, training datasets and features.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search("search-term")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "ALL", offset, limit)
        )

    def featurestore_search_by_tag(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search by tag.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search_by_tag("tag")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "ALL", offset, limit)
        )

    def featurestore_search_by_tag_key(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search by tag key.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search_by_tag_key("tag_key")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "ALL", offset, limit)
        )

    def featurestore_search_by_tag_value(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search by tag value.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search_by_tag_value("tag_value")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "ALL", offset, limit)
        )

    def featurestore_search_by_keyword(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search by keyword.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featurestore_search_by_keyword("keyword")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "ALL", offset, limit)
        )

    # FEATUREGROUP
    def featuregroup_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ):
        """
        Search for feature group.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search("search_term")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATUREGROUP", offset, limit)
        )
        return search_results.FeatureGroupSearchResult(result)

    def featuregroup_search_by_tag(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature group by tag.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search_by_tag("tag")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "FEATUREGROUP", offset, limit)
        )
        return search_results.FeatureGroupSearchResult(result)

    def featuregroup_search_by_tag_key(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature group by tag key.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search_by_tag_key("tag_key")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "FEATUREGROUP", offset, limit)
        )
        return search_results.FeatureGroupSearchResult(result)

    def featuregroup_search_by_tag_value(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature group by tag value.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search_by_tag_value("tag_value")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "FEATUREGROUP", offset, limit)
        )
        return search_results.FeatureGroupSearchResult(result)

    def featuregroup_search_by_keyword(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature group by keyword.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featuregroup_search_by_keyword("keyword")

        # get feature group instance
        featuregroup = result.featuregroups[0].get_feature_group()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "FEATUREGROUP", offset, limit)
        )
        return search_results.FeatureGroupSearchResult(result)

    # FEATUREVIEW
    def featureview_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ):
        """
        Search for feature views.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search("search_term")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATUREVIEW", offset, limit)
        )
        return search_results.FeatureViewSearchResult(result)

    def featureview_search_by_tag(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature views by tag.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search_by_tag("tag")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "FEATUREVIEW", offset, limit)
        )
        return search_results.FeatureViewSearchResult(result)

    def featureview_search_by_tag_key(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature views by tag key.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search_by_tag_key("tag_key")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "FEATUREVIEW", offset, limit)
        )
        return search_results.FeatureViewSearchResult(result)

    def featureview_search_by_tag_value(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature views by tag value.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search_by_tag_value("tag_value")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "FEATUREVIEW", offset, limit)
        )
        return search_results.FeatureViewSearchResult(result)

    def featureview_search_by_keyword(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for feature views by keyword.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.featureview_search_by_keyword("keyword")

        # get feature view instance
        featureview = result.feature_views[0].get_feature_view()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "FEATUREVIEW", offset, limit)
        )
        return search_results.FeatureViewSearchResult(result)

    # TRAININGDATASET
    def trainingdataset_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ):
        """
        Search for training datasets.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search("search_term")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "TRAININGDATASET", offset, limit)
        )
        return search_results.TrainingdatasetsSearchResult(result)

    def trainingdataset_search_by_tag(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for training datasets by tag.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search_by_tag("tag")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "TRAININGDATASET", offset, limit)
        )
        return search_results.TrainingdatasetsSearchResult(result)

    def trainingdataset_search_by_tag_key(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for training datasets by tag key.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search_by_tag_key("tag_key")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "TRAININGDATASET", offset, limit)
        )
        return search_results.TrainingdatasetsSearchResult(result)

    def trainingdataset_search_by_tag_value(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for training datasets by tag value.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search_by_tag_value("tag_value")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "TRAININGDATASET", offset, limit)
        )
        return search_results.TrainingdatasetsSearchResult(result)

    def trainingdataset_search_by_keyword(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 1,
    ):
        """
        Search for training datasets by keyword.
        ```python

        import hopsworks

        project = hopsworks.login()

        search_api = project.get_search_api()

        result = search_api.trainingdataset_search_by_keyword("keyword")

        # get training datasets instance
        trainingdataset = result.trainingdatasets[0].get_training_dataset()

        ```
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "TRAININGDATASET", offset, limit)
        )
        return search_results.TrainingdatasetsSearchResult(result)

    # FEATURE
    def feature_search(
        self,
        search_term: str,
        offset: int = 0,
        limit: int = 100,
    ):
        """
        Search for features.
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
        # Arguments
            search_term: the term to search for.
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
        # Returns
            `FeaturestoreSearchResult`: The results.
        # Raises
            `ValueError`: If the search term is not provided.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        result = search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATURE", offset, limit)
        )
        return search_results.FeatureSearchResult(result)


    def _search(self, search_term: str, doc_type: DOC_TYPE_ARG, offset: int, limit: int):
        if not search_term:
            raise ValueError("Search term not provided.")
        if doc_type not in get_args(DOC_TYPE_ARG):
            raise ValueError(
                f"doc_type must be one of the following {get_args(DOC_TYPE_ARG)}."
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
        return _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )
