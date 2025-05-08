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
        _from: int = 0,
        size: int = 100,
    ):
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "ALL", _from, size)
        )

    def featurestore_search_by_tag(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "ALL", _from, size)
        )

    def featurestore_search_by_tag_key(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "ALL", _from, size)
        )

    def featurestore_search_by_tag_value(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "ALL", _from, size)
        )

    def featurestore_search_by_keyword(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "ALL", _from, size)
        )

    # FEATUREGROUP
    def featuregroup_search(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 100,
    ):
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATUREGROUP", _from, size)
        ).featuregroups

    def featuregroup_search_by_tag(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "FEATUREGROUP", _from, size)
        ).featuregroups

    def featuregroup_search_by_tag_key(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "FEATUREGROUP", _from, size)
        ).featuregroups

    def featuregroup_search_by_tag_value(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "FEATUREGROUP", _from, size)
        ).featuregroups

    def featuregroup_search_by_keyword(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "FEATUREGROUP", _from, size)
        ).featuregroups

    # FEATUREVIEW
    def featureview_search(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 100,
    ):
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATUREVIEW", _from, size)
        ).feature_views

    def featureview_search_by_tag(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "FEATUREVIEW", _from, size)
        ).feature_views

    def featureview_search_by_tag_key(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "FEATUREVIEW", _from, size)
        ).feature_views

    def featureview_search_by_tag_value(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "FEATUREVIEW", _from, size)
        ).feature_views

    def featureview_search_by_keyword(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "FEATUREVIEW", _from, size)
        ).feature_views

    # TRAININGDATASET
    def trainingdataset_search(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 100,
    ):
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "TRAININGDATASET", _from, size)
        ).trainingdatasets

    def trainingdataset_search_by_tag(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTag.from_response_json(
            self._search(search_term, "TRAININGDATASET", _from, size)
        ).trainingdatasets

    def trainingdataset_search_by_tag_key(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagKey.from_response_json(
            self._search(search_term, "TRAININGDATASET", _from, size)
        ).trainingdatasets

    def trainingdataset_search_by_tag_value(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByTagValue.from_response_json(
            self._search(search_term, "TRAININGDATASET", _from, size)
        ).trainingdatasets

    def trainingdataset_search_by_keyword(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 1,
    ):
        return search_results.FeaturestoreSearchResultByKeyWord.from_response_json(
            self._search(search_term, "TRAININGDATASET", _from, size)
        ).trainingdatasets

    # FEATURE
    def feature_search(
        self,
        search_term: str,
        _from: int = 0,
        size: int = 100,
    ):
        return search_results.FeaturestoreSearchResult.from_response_json(
            self._search(search_term, "FEATURE", _from, size)
        ).features


    def _search(self, search_term: str, doc_type: DOC_TYPE_ARG, _from: int, size: int):
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
        query_params = {"docType": doc_type, "from": _from, "size": size}
        return _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )
