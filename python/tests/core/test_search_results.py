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

from hopsworks_common import search_results


class TestSearchResults:
    def test_search_result_from_response_json(self):
        json_dict = {
            "featuregroups": [
                {
                    "name": "test",
                    "version": "1.0",
                    "description": "test description",
                    "featurestore_id": "test_id",
                    "created": "2025-01-01T00:00:00Z",
                    "parent_project_id": "parent_id",
                    "parent_project_name": "parent_name",
                    "access_projects": ["project1", "project2"],
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                        "other_xattrs": None,
                    },
                    "creator": {
                        "username": "test_user",
                        "firstname": "Test",
                        "lastname": "User",
                        "email": "test@example.com",
                    },
                    "elastic_id": "elastic_id",
                },
            ],
            "feature_views": [
                {
                    "name": "test",
                    "version": "1.0",
                    "description": "test description",
                    "featurestore_id": "test_id",
                    "created": "2025-01-01T00:00:00Z",
                    "parent_project_id": "parent_id",
                    "parent_project_name": "parent_name",
                    "access_projects": ["project1", "project2"],
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                        "other_xattrs": None,
                    },
                    "creator": {
                        "username": "test_user",
                        "firstname": "Test",
                        "lastname": "User",
                        "email": "test@example.com",
                    },
                    "elastic_id": "elastic_id",
                }
            ],
            "trainingdatasets": [
                {
                    "name": "test",
                    "version": "1.0",
                    "description": "test description",
                    "featurestore_id": "test_id",
                    "created": "2025-01-01T00:00:00Z",
                    "parent_project_id": "parent_id",
                    "parent_project_name": "parent_name",
                    "access_projects": ["project1", "project2"],
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                        "other_xattrs": None,
                    },
                    "creator": {
                        "username": "test_user",
                        "firstname": "Test",
                        "lastname": "User",
                        "email": "test@example.com",
                    },
                    "elastic_id": "elastic_id",
                },
            ],
            "features": [
                {
                    "name": "test",
                    "version": "1.0",
                    "description": "test description",
                    "featurestore_id": "test_id",
                    "created": "2025-01-01T00:00:00Z",
                    "parent_project_id": "parent_id",
                    "parent_project_name": "parent_name",
                    "access_projects": ["project1", "project2"],
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                        "other_xattrs": None,
                    },
                    "creator": {
                        "username": "test_user",
                        "firstname": "Test",
                        "lastname": "User",
                        "email": "test@example.com",
                    },
                    "featuregroup": "test_fg",
                },
            ],
            "featuregroups_from": 0,
            "featuregroups_total": 1,
            "feature_views_from": 0,
            "feature_views_total": 1,
            "trainingdatasets_from": 0,
            "trainingdatasets_total": 1,
            "features_from": 0,
            "features_total": 1,
        }

        search_result_by_tag = (
            search_results.FeaturestoreSearchResultByTag.from_response_json(json_dict)
        )

        assert len(search_result_by_tag.featuregroups) == 0
        assert len(search_result_by_tag.feature_views) == 0
        assert len(search_result_by_tag.trainingdatasets) == 0
        assert len(search_result_by_tag.features) == 0

        search_result_by_tag_name = (
            search_results.FeaturestoreSearchResultByTagName.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_name.featuregroups) == 0
        assert len(search_result_by_tag_name.feature_views) == 0
        assert len(search_result_by_tag_name.trainingdatasets) == 0
        assert len(search_result_by_tag_name.features) == 0

        search_result_by_tag_key = (
            search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_key.featuregroups) == 0
        assert len(search_result_by_tag_key.feature_views) == 0
        assert len(search_result_by_tag_key.trainingdatasets) == 0
        assert len(search_result_by_tag_key.features) == 0

        search_result_by_tag_value = (
            search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_value.featuregroups) == 0
        assert len(search_result_by_tag_value.feature_views) == 0
        assert len(search_result_by_tag_value.trainingdatasets) == 0
        assert len(search_result_by_tag_value.features) == 0

        json_dict["featuregroups"][0]["highlights"]["tags"] = [
            {"key": "<em>tag1</em>", "value": None},
        ]

        search_result_by_tag = (
            search_results.FeaturestoreSearchResultByTag.from_response_json(json_dict)
        )

        assert len(search_result_by_tag.featuregroups) == 1
        assert len(search_result_by_tag.feature_views) == 0
        assert len(search_result_by_tag.trainingdatasets) == 0
        assert len(search_result_by_tag.features) == 0

        search_result_by_tag_name = (
            search_results.FeaturestoreSearchResultByTagName.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_name.featuregroups) == 1
        assert len(search_result_by_tag_name.feature_views) == 0
        assert len(search_result_by_tag_name.trainingdatasets) == 0
        assert len(search_result_by_tag_name.features) == 0

        search_result_by_tag_key = (
            search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_key.featuregroups) == 0
        assert len(search_result_by_tag_key.feature_views) == 0
        assert len(search_result_by_tag_key.trainingdatasets) == 0
        assert len(search_result_by_tag_key.features) == 0

        search_result_by_tag_value = (
            search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_value.featuregroups) == 0
        assert len(search_result_by_tag_value.feature_views) == 0
        assert len(search_result_by_tag_value.trainingdatasets) == 0
        assert len(search_result_by_tag_value.features) == 0

        json_dict["featuregroups"][0]["highlights"]["tags"] = []
        json_dict["feature_views"][0]["highlights"]["tags"] = [
            {"key": None, "value": '{"<em>tag1</em>": "value1"}'},
        ]

        search_result_by_tag = (
            search_results.FeaturestoreSearchResultByTag.from_response_json(json_dict)
        )

        assert len(search_result_by_tag.featuregroups) == 0
        assert len(search_result_by_tag.feature_views) == 1
        assert len(search_result_by_tag.trainingdatasets) == 0
        assert len(search_result_by_tag.features) == 0

        search_result_by_tag_name = (
            search_results.FeaturestoreSearchResultByTagName.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_name.featuregroups) == 0
        assert len(search_result_by_tag_name.feature_views) == 0
        assert len(search_result_by_tag_name.trainingdatasets) == 0
        assert len(search_result_by_tag_name.features) == 0

        search_result_by_tag_key = (
            search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_key.featuregroups) == 0
        assert len(search_result_by_tag_key.feature_views) == 1
        assert len(search_result_by_tag_key.trainingdatasets) == 0
        assert len(search_result_by_tag_key.features) == 0

        search_result_by_tag_value = (
            search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_value.featuregroups) == 0
        assert len(search_result_by_tag_value.feature_views) == 0
        assert len(search_result_by_tag_value.trainingdatasets) == 0
        assert len(search_result_by_tag_value.features) == 0

        json_dict["feature_views"][0]["highlights"]["tags"] = []
        json_dict["trainingdatasets"][0]["highlights"]["tags"] = [
            {"value": '{"tag1": "<em>value1</em>"}'},
        ]

        search_result_by_tag = (
            search_results.FeaturestoreSearchResultByTag.from_response_json(json_dict)
        )

        assert len(search_result_by_tag.featuregroups) == 0
        assert len(search_result_by_tag.feature_views) == 0
        assert len(search_result_by_tag.trainingdatasets) == 1
        assert len(search_result_by_tag.features) == 0

        search_result_by_tag_name = (
            search_results.FeaturestoreSearchResultByTagName.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_name.featuregroups) == 0
        assert len(search_result_by_tag_name.feature_views) == 0
        assert len(search_result_by_tag_name.trainingdatasets) == 0
        assert len(search_result_by_tag_name.features) == 0

        search_result_by_tag_key = (
            search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_key.featuregroups) == 0
        assert len(search_result_by_tag_key.feature_views) == 0
        assert len(search_result_by_tag_key.trainingdatasets) == 0
        assert len(search_result_by_tag_key.features) == 0

        search_result_by_tag_value = (
            search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_value.featuregroups) == 0
        assert len(search_result_by_tag_value.feature_views) == 0
        assert len(search_result_by_tag_value.trainingdatasets) == 1
        assert len(search_result_by_tag_value.features) == 0

        json_dict["trainingdatasets"][0]["highlights"]["tags"] = [
            {"key": "<em>tag1</em>", "value": '{"tag1": "value1"}'},
            {"key": None, "value": '{"tag1": "<em>value1</em>"}'},
            {"value": '{"<em>tag1</em>": "value1"}'},
        ]

        search_result_by_tag = (
            search_results.FeaturestoreSearchResultByTag.from_response_json(json_dict)
        )

        assert len(search_result_by_tag.featuregroups) == 0
        assert len(search_result_by_tag.feature_views) == 0
        assert len(search_result_by_tag.trainingdatasets) == 1
        assert len(search_result_by_tag.features) == 0

        search_result_by_tag_name = (
            search_results.FeaturestoreSearchResultByTagName.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_name.featuregroups) == 0
        assert len(search_result_by_tag_name.feature_views) == 0
        assert len(search_result_by_tag_name.trainingdatasets) == 1
        assert len(search_result_by_tag_name.features) == 0

        search_result_by_tag_key = (
            search_results.FeaturestoreSearchResultByTagKey.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_key.featuregroups) == 0
        assert len(search_result_by_tag_key.feature_views) == 0
        assert len(search_result_by_tag_key.trainingdatasets) == 1
        assert len(search_result_by_tag_key.features) == 0

        search_result_by_tag_value = (
            search_results.FeaturestoreSearchResultByTagValue.from_response_json(
                json_dict
            )
        )

        assert len(search_result_by_tag_value.featuregroups) == 0
        assert len(search_result_by_tag_value.feature_views) == 0
        assert len(search_result_by_tag_value.trainingdatasets) == 1
        assert len(search_result_by_tag_value.features) == 0
