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
from hopsworks_common import search_results
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import dataset_api


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

    def test_search_result_parses_jobs_and_datasets(self):
        json_dict = {
            "featuregroups": [],
            "featureViews": [],
            "trainingdatasets": [],
            "features": [],
            "jobs": [
                {
                    "name": "test_job",
                    "description": "ingestion job",
                    "jobType": "PYSPARK",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {"description": "<em>ingestion</em> job"},
                },
            ],
            "datasets": [
                {
                    "name": "test_dataset",
                    "description": "reports",
                    "parentProjectId": 1,
                    "parentProjectName": "test_project",
                    "highlights": {},
                },
            ],
            "jobsFrom": 0,
            "jobsTotal": 3,
            "datasetsFrom": 0,
            "datasetsTotal": 2,
        }

        result = search_results.FeaturestoreSearchResult(json_dict)

        assert len(result.jobs) == 1
        assert result.jobs[0].name == "test_job"
        assert result.jobs[0].job_type == "PYSPARK"
        assert result.jobs[0].project.name == "test_project"
        assert len(result.datasets) == 1
        assert result.datasets[0].name == "test_dataset"
        assert result.jobs_total == 3
        assert result.datasets_total == 2

    def test_search_result_without_jobs_and_datasets(self):
        # An old server sends no jobs/datasets fields at all.
        result = search_results.FeaturestoreSearchResult({})

        assert result.jobs == []
        assert result.datasets == []
        assert result.jobs_total == 0
        assert result.datasets_total == 0

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


class TestModelAndDeploymentSearchResults:
    """Model and deployment hits, and how each resolves back to a real object."""

    def _response(self):
        return {
            "models": [
                {
                    "name": "credit_scoring",
                    "version": 2,
                    "description": "logistic regression",
                    "framework": "PYTHON",
                    "parentProjectId": 77,
                    "parentProjectName": "other_project",
                    "highlights": {},
                },
            ],
            "deployments": [
                {
                    "name": "creditscoring",
                    "description": "serving the scorer",
                    "servingTool": "KSERVE",
                    "modelName": "credit_scoring",
                    "modelVersion": 2,
                    "modelFramework": "PYTHON",
                    "parentProjectId": 5,
                    "parentProjectName": "login_project",
                    "highlights": {},
                },
            ],
            "modelsFrom": 0,
            "modelsTotal": 4,
            "deploymentsFrom": 0,
            "deploymentsTotal": 1,
        }

    def test_models_and_deployments_are_parsed(self):
        result = search_results.FeaturestoreSearchResult(self._response())

        model = result.models[0]
        assert model.name == "credit_scoring"
        assert model.version == 2
        assert model.framework == "PYTHON"
        assert model.project.name == "other_project"
        assert result.models_total == 4

        deployment = result.deployments[0]
        assert deployment.name == "creditscoring"
        assert deployment.serving_tool == "KSERVE"
        assert deployment.model_name == "credit_scoring"
        assert deployment.model_version == 2
        assert result.deployments_total == 1

    def test_an_old_server_sends_neither(self):
        result = search_results.FeaturestoreSearchResult({})

        assert result.models == []
        assert result.deployments == []
        assert result.models_total == 0
        assert result.deployments_total == 0

    def test_a_model_resolves_in_its_registry_project(self, mocker):
        model = search_results.FeaturestoreSearchResult(self._response()).models[0]
        registry = mocker.Mock()
        connection = mocker.Mock()
        connection._get_model_registry.return_value = registry
        mocker.patch("hopsworks_common.client._get_connection", return_value=connection)

        model.get()

        # The registry project of the hit, which for a shared registry is not the login project.
        connection._get_model_registry.assert_called_once_with("other_project")
        registry.get_model.assert_called_once_with("credit_scoring", version=2)

    def test_a_model_without_a_project_fails_closed(self):
        model = search_results.ModelSearchResult({"name": "m", "version": 1})

        with pytest.raises(ValueError, match="carries no project"):
            model.get()

    def test_a_deployment_resolves_in_the_login_project(self, mocker):
        deployment = search_results.FeaturestoreSearchResult(
            self._response()
        ).deployments[0]
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_name="login_project"),
        )
        serving = mocker.Mock()
        connection = mocker.Mock()
        connection._get_model_serving.return_value = serving
        mocker.patch("hopsworks_common.client._get_connection", return_value=connection)

        deployment.get()

        serving.get_deployment.assert_called_once_with("creditscoring")

    def test_a_deployment_of_another_project_cannot_be_fetched(self, mocker):
        # Deployments are project-local, so there is no client call that could answer this.
        deployment = search_results.DeploymentSearchResult(
            {
                "name": "creditscoring",
                "parentProjectId": 77,
                "parentProjectName": "other_project",
            }
        )
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_name="login_project"),
        )

        with pytest.raises(ValueError, match="outside their own project"):
            deployment.get()


class TestDatasetSearchResultProjectAwareness:
    """A dataset hit resolves in its own project, not the login project."""

    def _hit(self):
        return search_results.DatasetSearchResult(
            {
                "name": "sales_raw",
                "description": "raw sales drops",
                "parentProjectId": 77,
                "parentProjectName": "other_project",
                "highlights": {},
            }
        )

    def test_path_is_the_dataset_name(self):
        assert self._hit().path == "sales_raw"

    def test_dataset_api_is_built_for_the_hit_project(self, mocker):
        api = self._hit()._dataset_api()
        # Explicitly the hit's project, so no path is built from the connection's.
        assert api._project_id == 77
        assert api._project_name == "other_project"

    def test_get_reads_the_hit_project(self, mocker):
        hit = self._hit()
        get = mocker.patch(
            "hopsworks_common.core.dataset_api.DatasetApi._get",
            return_value={"attributes": {"path": "/Projects/other_project/sales_raw"}},
        )
        assert hit.get()["attributes"]["path"].startswith("/Projects/other_project/")
        get.assert_called_once_with("sales_raw")

    def test_get_returns_none_when_gone(self, mocker):
        hit = self._hit()
        # RestAPIError parses the body, so the mock has to answer json().
        response = mocker.Mock(status_code=404)
        response.json.return_value = {}
        mocker.patch(
            "hopsworks_common.core.dataset_api.DatasetApi._get",
            side_effect=RestAPIError("url", response),
        )
        assert hit.get() is None

    def test_get_tags_metadata_reads_the_hit_project(self, mocker):
        hit = self._hit()
        tags = mocker.patch(
            "hopsworks_common.core.dataset_api.DatasetApi.get_tags_metadata",
            return_value={},
        )
        assert hit.get_tags_metadata() == {}
        tags.assert_called_once_with("sales_raw")

    def test_a_dataset_api_without_a_project_falls_back_to_the_connection(self, mocker):
        instance = mocker.Mock(_project_id=5, _project_name="login_project")
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        api = dataset_api.DatasetApi()
        assert api._pid() == 5
        assert api._pname() == "login_project"
