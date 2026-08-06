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

import logging
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


if TYPE_CHECKING:
    from hopsworks_common.core.dataset_api import DatasetApi
    from hopsworks_common.job import Job
    from hopsworks_common.tag import Tag
    from hsfs.feature_group import FeatureGroup
    from hsfs.feature_view import FeatureView
    from hsfs.training_dataset import TrainingDataset
    from hsml.deployment import Deployment
    from hsml.model import Model


@public("hopsworks.core.search_api.Project")
class Project:
    """Represents a project associated with a search result."""

    def __init__(self, project_id: int, project_name: str):
        self._log = logging.getLogger(__name__)
        self._id = project_id
        self._name = project_name

    @public
    @property
    def id(self) -> int:
        """Project ID."""
        return self._id

    @public
    @property
    def name(self) -> str:
        """Project name."""
        return self._name

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the project.
        """
        return {"id": self._id, "name": self._name}

    def __repr__(self):
        return f"Project(id={self._id}, name='{self._name}')"


@public("hopsworks.core.search_api.Highlights")
class Highlights:
    """Container for search result highlights showing where matches occurred.

    The results are highlighted by wrapping the matched terms in `<em>` tags.
    Check the [OpenSearch Highlight Queries](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/) for more details.
    """

    def __init__(self, highlights_data: dict):
        self._log = logging.getLogger(__name__)
        self._raw_data = highlights_data
        self._name = highlights_data.get("name")
        self._description = highlights_data.get("description")
        self._tags = highlights_data.get("tags", [])
        self._keywords = highlights_data.get("keywords", [])
        self._features = highlights_data.get("features", [])
        self._source_feature_groups = highlights_data.get("sourceFeatureGroups", [])

    @public
    @property
    def name(self) -> str | None:
        """Highlighted name with the matched parts enwrapped in `<em>` tags."""
        return self._name

    @public
    @property
    def description(self) -> str | None:
        """Highlighted description with the matched parts enwrapped in `<em>` tags."""
        return self._description

    @public
    @property
    def tags(self) -> list:
        """List of highlighted tags with the matched parts enwrapped in `<em>` tags."""
        return self._tags

    @public
    @property
    def keywords(self) -> list:
        """Highlighted keywords with the matched parts enwrapped in `<em>` tags."""
        return self._keywords

    @public
    @property
    def features(self) -> list:
        """Highlighted features with the matched parts enwrapped in `<em>` tags."""
        return self._features

    @public
    @property
    def source_feature_groups(self) -> list:
        """Highlighted source feature groups with the matched parts enwrapped in `<em>` tags."""
        return self._source_feature_groups

    @public
    @property
    def raw_data(self) -> dict:
        """Raw highlights data."""
        return self._raw_data

    @public
    def has_highlights(self) -> bool:
        """Check if there are any highlights.

        Returns:
            Whether any of the highlight fields contain data.
        """
        return bool(
            self._name
            or self._description
            or self._tags
            or self._keywords
            or self._features
            or self._source_feature_groups
        )

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the highlights.
        """
        return {
            "name": self._name,
            "description": self._description,
            "tags": self._tags,
            "keywords": self._keywords,
            "features": self._features,
            "source_feature_groups": self._source_feature_groups,
        }

    def __repr__(self):
        highlights = []
        if self._name:
            highlights.append("name")
        if self._description:
            highlights.append("description")
        if self._tags:
            highlights.append("tags")
        if self._keywords:
            highlights.append("keywords")
        if self._features:
            highlights.append("features")
        if self._source_feature_groups:
            highlights.append("source_feature_groups")

        if highlights:
            return f"Highlights({', '.join(highlights)})"
        return "Highlights(none)"


@public("hopsworks.core.search_api.SearchResultItem")
class SearchResultItem:
    """Base class for search result items."""

    def __init__(self, data: dict):
        self._log = logging.getLogger(__name__)
        self._href = data.get("href")
        self._name = data.get("name")
        self._version = data.get("version")
        self._description = data.get("description")
        self._highlights = Highlights(data.get("highlights", {}))
        self._raw_data = data

        # Extract project information
        project_id = data.get("parentProjectId")
        project_name = data.get("parentProjectName")
        self._project = (
            Project(project_id, project_name) if project_id and project_name else None
        )

    @public
    @property
    def href(self):
        """URL to get the full resource."""
        return self._href

    @public
    @property
    def name(self):
        """Name of the resource."""
        return self._name

    @public
    @property
    def version(self):
        """Version of the resource."""
        return self._version

    @public
    @property
    def description(self):
        """Description of the resource."""
        return self._description

    @public
    @property
    def highlights(self) -> Highlights:
        """Search highlights showing matched terms."""
        return self._highlights

    @public
    @property
    def project(self) -> Project | None:
        """Parent project of this resource."""
        return self._project

    @public
    @property
    def raw_data(self):
        """Raw data from the search result."""
        return self._raw_data

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the search result item.
        """
        return {
            "href": self._href,
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "highlights": self._highlights.json(),
            "project": self._project.json() if self._project else None,
        }

    def __repr__(self):
        version_str = f", version={self._version}" if self._version else ""
        if self._description:
            if len(self._description) > 50:
                description_preview = f"{self._description[:50]}..."
            else:
                description_preview = self._description
            description_str = f", description='{description_preview}'"
        else:
            description_str = ""
        return f"{self.__class__.__name__}(name='{self._name}'{version_str}{description_str}, project='{self._project}', highlights='{self._highlights}')"


@public("hopsworks.core.search_api.FeatureGroupSearchResult")
class FeatureGroupSearchResult(SearchResultItem):
    """Search result for a Feature Group."""

    @public
    def get(self) -> FeatureGroup | None:
        """Retrieve the full FeatureGroup object.

        This uses the project associated with this search result to obtain a
        connection to the feature store and then fetches the Feature Group
        with the given name and version.

        Returns:
            The full Feature Group object corresponding to this search result.

        Raises:
            Exception: If the connection to the feature store fails or the
                Feature Group cannot be retrieved.
        """
        fs = client._get_connection()._get_feature_store(self.project.name)
        return fs.get_feature_group(self.name, version=self.version)


@public("hopsworks.core.search_api.FeatureViewSearchResult")
class FeatureViewSearchResult(SearchResultItem):
    """Search result for a Feature View."""

    @public
    def get(self) -> FeatureView | None:
        """Retrieve the full FeatureView object.

        This uses the project associated with this search result to obtain a
        connection to the feature store and then fetches the Feature View
        with the given name and version.

        Returns:
            The full FeatureView instance corresponding to this search result.

        Raises:
            Exception: If the connection to the feature store fails or the
                Feature View cannot be retrieved.
        """
        fs = client._get_connection()._get_feature_store(self.project.name)
        return fs.get_feature_view(self.name, version=self.version)


@public("hopsworks.core.search_api.TrainingDatasetSearchResult")
class TrainingDatasetSearchResult(SearchResultItem):
    """Search result for a Training Dataset."""

    @public
    def get(self) -> TrainingDataset | None:
        """Retrieve the full TrainingDataset object.

        This uses the project associated with this search result to obtain a
        connection to the feature store and then fetches the Training Dataset
        with the given name and version.

        Returns:
            The full TrainingDataset instance corresponding to this search result.

        Raises:
            Exception: If the connection to the feature store fails or the
                Training Dataset cannot be retrieved.
        """
        fs = client._get_connection()._get_feature_store(self.project.name)
        return fs.get_training_dataset(self.name, version=self.version)


@public("hopsworks.core.search_api.FeatureSearchResult")
class FeatureSearchResult(SearchResultItem):
    """Search result for a Feature."""


@public("hopsworks.core.search_api.JobSearchResult")
class JobSearchResult(SearchResultItem):
    """Search result for a Job."""

    def __init__(self, data: dict):
        super().__init__(data)
        self._job_type = data.get("jobType")

    @public
    @property
    def job_type(self) -> str | None:
        """Type of the job, e.g. `SPARK` or `PYTHON`."""
        return self._job_type

    @public
    def get(self) -> Job | None:
        """Retrieve the full Job object.

        The job is fetched from the project this search result belongs to,
        which is not necessarily the login project.

        Returns:
            The full Job object corresponding to this search result, or `None` if it no longer exists.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        from hopsworks_common.core import job_api

        return job_api.JobApi(
            project_id=self.project.id, project_name=self.project.name
        ).get_job(self.name)

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the search result item.
        """
        return {**super().json(), "job_type": self._job_type}


@public("hopsworks.core.search_api.DatasetSearchResult")
class DatasetSearchResult(SearchResultItem):
    """Search result for a Dataset.

    Every method here resolves in the project the hit belongs to, which is not
    necessarily the login project: a cross-project hit is the case dataset
    search exists to serve, and reading it against the login project would
    either 404 or, worse, answer about a same-named dataset of another project.
    """

    @public
    @property
    def path(self) -> str:
        """Path of the dataset within its project, which for a dataset root is its name."""
        return self._name

    def _dataset_api(self) -> DatasetApi:
        from hopsworks_common.core import dataset_api

        # Fail closed. Falling back to the connection's project would answer about a same-named
        # dataset of the wrong project, which reads as success; a hit without project metadata is a
        # hit nothing can be done with.
        if self.project is None:
            raise ValueError(
                "this search result carries no project, so its dataset cannot be resolved"
            )
        return dataset_api.DatasetApi(
            project_id=self.project.id, project_name=self.project.name
        )

    @public
    def get(self) -> dict | None:
        """Retrieve the dataset's metadata.

        Returns:
            The dataset metadata, or `None` if it no longer exists.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        try:
            return self._dataset_api()._get(self.path)
        except RestAPIError as e:
            if getattr(e.response, "status_code", None) == 404:
                return None
            raise

    @public
    def get_tags(self) -> dict[str, Any]:
        """Tags attached to the dataset, as name to value.

        Returns:
            Dictionary of tag names to values, empty when the dataset carries none.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._dataset_api().get_tags(self.path)

    @public
    def get_tags_metadata(self) -> dict[str, Tag]:
        """Tags attached to the dataset, with the time each was attached.

        Returns:
            Dictionary of tag names to [`Tag`][hopsworks.tag.Tag] objects, each carrying the time it was attached.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._dataset_api().get_tags_metadata(self.path)


@public("hopsworks.core.search_api.ModelSearchResult")
class ModelSearchResult(SearchResultItem):
    """Search result for a Model.

    A model resolves in the registry project the hit belongs to, which for a shared
    registry is not the login project.
    """

    def __init__(self, data: dict):
        super().__init__(data)
        self._framework = data.get("framework")

    @public
    @property
    def framework(self) -> str | None:
        """Framework the model was trained with, e.g. `PYTHON` or `TORCH`."""
        return self._framework

    @public
    def get(self) -> Model | None:
        """Retrieve the full Model object.

        The model is fetched from the registry project this search result belongs to.
        For that to work the registry has to be shared with the login project, which is
        the same condition under which the hit was visible.

        Returns:
            The full Model object corresponding to this search result, or `None` if it no longer exists.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        # Fail closed, for the reason given on DatasetSearchResult._dataset_api.
        if self.project is None:
            raise ValueError(
                "this search result carries no project, so its model cannot be resolved"
            )
        mr = client._get_connection()._get_model_registry(self.project.name)
        return mr.get_model(self.name, version=self.version)

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the search result item.
        """
        return {**super().json(), "framework": self._framework}


@public("hopsworks.core.search_api.DeploymentSearchResult")
class DeploymentSearchResult(SearchResultItem):
    """Search result for a Deployment."""

    def __init__(self, data: dict):
        super().__init__(data)
        self._serving_tool = data.get("servingTool")
        self._model_name = data.get("modelName")
        self._model_version = data.get("modelVersion")
        self._model_framework = data.get("modelFramework")

    @public
    @property
    def serving_tool(self) -> str | None:
        """Tool serving the deployment, e.g. `KSERVE`."""
        return self._serving_tool

    @public
    @property
    def model_name(self) -> str | None:
        """Name of the model the deployment serves, `None` when it serves no registered model."""
        return self._model_name

    @public
    @property
    def model_version(self) -> int | None:
        """Version of the model the deployment serves, `None` when it serves no registered model."""
        return self._model_version

    @public
    @property
    def model_framework(self) -> str | None:
        """Framework of the model the deployment serves."""
        return self._model_framework

    @public
    def get(self) -> Deployment | None:
        """Retrieve the full Deployment object.

        Returns:
            The full Deployment object corresponding to this search result, or `None` if it no longer exists.

        Raises:
            ValueError: If the deployment belongs to another project.
                Deployments are project-local, so there is no way to read one from outside its project.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if self.project is None:
            raise ValueError(
                "this search result carries no project, so its deployment cannot be resolved"
            )
        login_project = client._get_instance()._project_name
        if self.project.name != login_project:
            raise ValueError(
                f"deployment '{self.name}' belongs to project '{self.project.name}', "
                f"and deployments cannot be read from outside their own project; "
                f"log in to '{self.project.name}' to fetch it"
            )
        ms = client._get_connection()._get_model_serving()
        return ms.get_deployment(self.name)

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            A dictionary representation of the search result item.
        """
        return {
            **super().json(),
            "serving_tool": self._serving_tool,
            "model_name": self._model_name,
            "model_version": self._model_version,
            "model_framework": self._model_framework,
        }


@public("hopsworks.core.search_api.FeaturestoreSearchResult")
class FeaturestoreSearchResult:
    """Container for all featurestore search results."""

    def __init__(self, response_data: dict):
        self._log = logging.getLogger(__name__)
        self._feature_groups = [
            FeatureGroupSearchResult(fg)
            for fg in response_data.get("featuregroups", [])
        ]
        self._feature_views = [
            FeatureViewSearchResult(fv) for fv in response_data.get("featureViews", [])
        ]
        self._training_datasets = [
            TrainingDatasetSearchResult(td)
            for td in response_data.get("trainingdatasets", [])
        ]
        self._features = [
            FeatureSearchResult(f) for f in response_data.get("features", [])
        ]
        self._jobs = [JobSearchResult(j) for j in response_data.get("jobs", [])]
        self._datasets = [
            DatasetSearchResult(d) for d in response_data.get("datasets", [])
        ]
        self._models = [ModelSearchResult(m) for m in response_data.get("models", [])]
        self._deployments = [
            DeploymentSearchResult(d) for d in response_data.get("deployments", [])
        ]

        # Store metadata about result counts
        self._feature_groups_offset = response_data.get("featuregroupsFrom", 0)
        self._feature_groups_total = response_data.get("featuregroupsTotal", 0)
        self._feature_views_offset = response_data.get("featureViewsFrom", 0)
        self._feature_views_total = response_data.get("featureViewsTotal", 0)
        self._training_datasets_offset = response_data.get("trainingdatasetsFrom", 0)
        self._training_datasets_total = response_data.get("trainingdatasetsTotal", 0)
        self._features_offset = response_data.get("featuresFrom", 0)
        self._features_total = response_data.get("featuresTotal", 0)
        self._jobs_offset = response_data.get("jobsFrom", 0)
        self._jobs_total = response_data.get("jobsTotal", 0)
        self._datasets_offset = response_data.get("datasetsFrom", 0)
        self._datasets_total = response_data.get("datasetsTotal", 0)
        self._models_offset = response_data.get("modelsFrom", 0)
        self._models_total = response_data.get("modelsTotal", 0)
        self._deployments_offset = response_data.get("deploymentsFrom", 0)
        self._deployments_total = response_data.get("deploymentsTotal", 0)

    @public
    @property
    def feature_groups(self) -> list[FeatureGroupSearchResult]:
        """List of Feature Group search results."""
        return self._feature_groups

    @public
    @property
    def feature_views(self) -> list[FeatureViewSearchResult]:
        """List of Feature View search results."""
        return self._feature_views

    @public
    @property
    def training_datasets(self) -> list[TrainingDatasetSearchResult]:
        """List of Training Dataset search results."""
        return self._training_datasets

    @public
    @property
    def features(self) -> list[FeatureSearchResult]:
        """List of Feature search results."""
        return self._features

    @public
    @property
    def jobs(self) -> list[JobSearchResult]:
        """List of Job search results."""
        return self._jobs

    @public
    @property
    def datasets(self) -> list[DatasetSearchResult]:
        """List of Dataset search results."""
        return self._datasets

    @public
    @property
    def feature_groups_offset(self) -> int:
        """Total offset for the return list of feature groups within the whole result."""
        return self._feature_groups_offset

    @public
    @property
    def feature_views_offset(self) -> int:
        """Total offset for the return list of feature views within the whole result."""
        return self._feature_views_offset

    @public
    @property
    def training_datasets_offset(self) -> int:
        """Total offset for the return list of training datasets within the whole result."""
        return self._training_datasets_offset

    @public
    @property
    def features_offset(self) -> int:
        """Total offset for the return list of features within the whole result."""
        return self._features_offset

    @public
    @property
    def feature_groups_total(self) -> int:
        """Total number of Feature Groups matching the search."""
        return self._feature_groups_total

    @public
    @property
    def feature_views_total(self) -> int:
        """Total number of Feature Views matching the search."""
        return self._feature_views_total

    @public
    @property
    def training_datasets_total(self) -> int:
        """Total number of Training Datasets matching the search."""
        return self._training_datasets_total

    @public
    @property
    def features_total(self) -> int:
        """Total number of Features matching the search."""
        return self._features_total

    @public
    @property
    def jobs_offset(self) -> int:
        """Total offset for the returned list of jobs within the whole result."""
        return self._jobs_offset

    @public
    @property
    def jobs_total(self) -> int:
        """Total number of Jobs matching the search."""
        return self._jobs_total

    @public
    @property
    def datasets_offset(self) -> int:
        """Total offset for the returned list of datasets within the whole result."""
        return self._datasets_offset

    @public
    @property
    def datasets_total(self) -> int:
        """Total number of Datasets matching the search."""
        return self._datasets_total

    @public
    @property
    def models(self) -> list[ModelSearchResult]:
        """List of Model search results."""
        return self._models

    @public
    @property
    def models_offset(self) -> int:
        """Total offset for the returned list of models within the whole result."""
        return self._models_offset

    @public
    @property
    def models_total(self) -> int:
        """Total number of Models matching the search."""
        return self._models_total

    @public
    @property
    def deployments(self) -> list[DeploymentSearchResult]:
        """List of Deployment search results."""
        return self._deployments

    @public
    @property
    def deployments_offset(self) -> int:
        """Total offset for the returned list of deployments within the whole result."""
        return self._deployments_offset

    @public
    @property
    def deployments_total(self) -> int:
        """Total number of Deployments matching the search."""
        return self._deployments_total

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary.

        Returns:
            JSONDictionary representation of the object.
        """
        return {
            "featuregroups": [fg.json() for fg in self._feature_groups],
            "featuregroupsFrom": self._feature_groups_offset,
            "featuregroupsTotal": self._feature_groups_total,
            "featureviews": [fv.json() for fv in self._feature_views],
            "featureviewsFrom": self._feature_views_offset,
            "featureviewsTotal": self._feature_views_total,
            "trainingdatasets": [td.json() for td in self._training_datasets],
            "trainingdatasetsFrom": self._training_datasets_offset,
            "trainingdatasetsTotal": self._training_datasets_total,
            "features": [f.json() for f in self._features],
            "featuresFrom": self._features_offset,
            "featuresTotal": self._features_total,
            "jobs": [j.json() for j in self._jobs],
            "jobsFrom": self._jobs_offset,
            "jobsTotal": self._jobs_total,
            "datasets": [d.json() for d in self._datasets],
            "datasetsFrom": self._datasets_offset,
            "datasetsTotal": self._datasets_total,
            "models": [m.json() for m in self._models],
            "modelsFrom": self._models_offset,
            "modelsTotal": self._models_total,
            "deployments": [d.json() for d in self._deployments],
            "deploymentsFrom": self._deployments_offset,
            "deploymentsTotal": self._deployments_total,
        }

    def __repr__(self):
        return (
            f"FeaturestoreSearchResult("
            f"feature_groups={len(self._feature_groups)}/{self._feature_groups_total}, "
            f"feature_views={len(self._feature_views)}/{self._feature_views_total}, "
            f"training_datasets={len(self._training_datasets)}/{self._training_datasets_total}, "
            f"features={len(self._features)}/{self._features_total}, "
            f"jobs={len(self._jobs)}/{self._jobs_total}, "
            f"datasets={len(self._datasets)}/{self._datasets_total}, "
            f"models={len(self._models)}/{self._models_total}, "
            f"deployments={len(self._deployments)}/{self._deployments_total})"
        )
