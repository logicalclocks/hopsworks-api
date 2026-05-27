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
                    "name": "test_fg",
                    "version": 1,
                    "description": "test description",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                    },
                },
            ],
            "featureViews": [
                {
                    "name": "test_fv",
                    "version": 1,
                    "description": "test description",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                    },
                }
            ],
            "trainingdatasets": [
                {
                    "name": "test_td",
                    "version": 1,
                    "description": "test description",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                    },
                },
            ],
            "features": [
                {
                    "name": "test_feature",
                    "version": 1,
                    "description": "test description",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {
                        "description": "<em>Transaction</em> data",
                        "tags": [],
                    },
                },
            ],
            "featuregroupsFrom": 0,
            "featuregroupsTotal": 1,
            "featureViewsFrom": 0,
            "featureViewsTotal": 1,
            "trainingdatasetsFrom": 0,
            "trainingdatasetsTotal": 1,
            "featuresFrom": 0,
            "featuresTotal": 1,
        }

        result = search_results.FeaturestoreSearchResult(json_dict)

        assert len(result.feature_groups) == 1
        assert len(result.feature_views) == 1
        assert len(result.training_datasets) == 1
        assert len(result.features) == 1

        assert result.feature_groups[0].name == "test_fg"
        assert result.feature_views[0].name == "test_fv"
        assert result.training_datasets[0].name == "test_td"
        assert result.features[0].name == "test_feature"

        assert result.feature_groups_total == 1
        assert result.feature_views_total == 1
        assert result.training_datasets_total == 1
        assert result.features_total == 1

    def test_search_result_with_empty_response(self):
        json_dict = {
            "featuregroups": [],
            "featureViews": [],
            "trainingdatasets": [],
            "features": [],
            "featuregroupsFrom": 0,
            "featuregroupsTotal": 0,
            "featureViewsFrom": 0,
            "featureViewsTotal": 0,
            "trainingdatasetsFrom": 0,
            "trainingdatasetsTotal": 0,
            "featuresFrom": 0,
            "featuresTotal": 0,
        }

        result = search_results.FeaturestoreSearchResult(json_dict)

        assert len(result.feature_groups) == 0
        assert len(result.feature_views) == 0
        assert len(result.training_datasets) == 0
        assert len(result.features) == 0

    def test_search_result_with_highlights(self):
        json_dict = {
            "featuregroups": [
                {
                    "name": "test_fg",
                    "version": 1,
                    "description": "test description",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {
                        "name": "<em>test</em>_fg",
                        "description": "<em>Transaction</em> data",
                        "tags": [{"key": "tag1", "value": "value1"}],
                        "keywords": ["<em>keyword1</em>"],
                    },
                },
            ],
            "featureViews": [],
            "trainingdatasets": [],
            "features": [],
            "featuregroupsFrom": 0,
            "featuregroupsTotal": 1,
            "featureViewsFrom": 0,
            "featureViewsTotal": 0,
            "trainingdatasetsFrom": 0,
            "trainingdatasetsTotal": 0,
            "featuresFrom": 0,
            "featuresTotal": 0,
        }

        result = search_results.FeaturestoreSearchResult(json_dict)

        assert len(result.feature_groups) == 1
        fg = result.feature_groups[0]
        assert fg.highlights.name == "<em>test</em>_fg"
        assert fg.highlights.description == "<em>Transaction</em> data"
        assert fg.highlights.tags == [{"key": "tag1", "value": "value1"}]
        assert fg.highlights.keywords == ["<em>keyword1</em>"]
        assert fg.highlights.has_highlights()

    def test_search_result_project_info(self):
        json_dict = {
            "featuregroups": [
                {
                    "name": "test_fg",
                    "version": 1,
                    "parentProjectId": 123,
                    "parentProjectName": "my_project",
                    "highlights": {},
                },
            ],
            "featureViews": [],
            "trainingdatasets": [],
            "features": [],
            "featuregroupsFrom": 0,
            "featuregroupsTotal": 1,
            "featureViewsFrom": 0,
            "featureViewsTotal": 0,
            "trainingdatasetsFrom": 0,
            "trainingdatasetsTotal": 0,
            "featuresFrom": 0,
            "featuresTotal": 0,
        }

        result = search_results.FeaturestoreSearchResult(json_dict)

        fg = result.feature_groups[0]
        assert fg.project.id == 123
        assert fg.project.name == "my_project"
