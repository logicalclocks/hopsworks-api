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

from hopsworks_apigen import also_available_as, public
from hopsworks_common import client


@also_available_as("hopsworks.core.search_api.Project")
class Project:
    """Represents a project associated with a search result."""

    def __init__(self, project_id: int, project_name: str):
        self._log = logging.getLogger(__name__)
        self._id = project_id
        self._name = project_name

    @property
    def id(self) -> int:
        """Project ID."""
        return self._id

    @property
    def name(self) -> str:
        """Project name."""
        return self._name

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary."""
        return {"id": self._id, "name": self._name}

    def __repr__(self):
        return f"Project(id={self._id}, name='{self._name}')"


@also_available_as("hopsworks.core.search_api.Highlights")
class Highlights:
    """Container for search result highlights showing where matches occurred."""

    def __init__(self, highlights_data: dict):
        self._log = logging.getLogger(__name__)
        self._raw_data = highlights_data
        self._name = highlights_data.get("name")
        self._description = highlights_data.get("description")
        self._tags = highlights_data.get("tags", [])
        self._keywords = highlights_data.get("keywords", [])
        self._features = highlights_data.get("features", [])
        self._source_feature_groups = highlights_data.get("sourceFeatureGroups", [])

    @property
    def name(self) -> str | None:
        """Highlighted name with <em> tags showing matched terms."""
        return self._name

    @property
    def description(self) -> str | None:
        """Highlighted description with <em> tags showing matched terms."""
        return self._description

    @property
    def tags(self) -> list:
        """List of highlighted tags with <em> tags showing matched terms."""
        return self._tags

    @property
    def keywords(self) -> list:
        """Highlighted keywords with <em> tags showing matched terms."""
        return self._keywords

    @property
    def features(self) -> list:
        """Highlighted features with <em> tags showing matched terms."""
        return self._features

    @property
    def source_feature_groups(self) -> list:
        """Highlighted source feature groups with <em> tags showing matched terms."""
        return self._source_feature_groups

    @property
    def raw_data(self) -> dict:
        """Raw highlights data."""
        return self._raw_data

    def has_highlights(self) -> bool:
        """Check if there are any highlights."""
        return bool(
            self._name
            or self._description
            or self._tags
            or self._keywords
            or self._features
            or self._source_feature_groups
        )

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary."""
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


@also_available_as("hopsworks.core.search_api.SearchResultItem")
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

    @property
    def href(self):
        """URL to get the full resource."""
        return self._href

    @property
    def name(self):
        """Name of the resource."""
        return self._name

    @property
    def version(self):
        """Version of the resource."""
        return self._version

    @property
    def description(self):
        """Description of the resource."""
        return self._description

    @property
    def highlights(self) -> Highlights:
        """Search highlights showing matched terms."""
        return self._highlights

    @property
    def project(self) -> Project | None:
        """Parent project of this resource."""
        return self._project

    @property
    def raw_data(self):
        """Raw data from the search result."""
        return self._raw_data

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary."""
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

    def get(self):
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
        fs = client.get_connection().get_feature_store(self.project.name)
        return fs.get_feature_group(self.name, version=self.version)


@public("hopsworks.core.search_api.FeatureViewSearchResult")
class FeatureViewSearchResult(SearchResultItem):
    """Search result for a Feature View."""

    def get(self):
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
        fs = client.get_connection().get_feature_store(self.project.name)
        return fs.get_feature_view(self.name, version=self.version)


@public("hopsworks.core.search_api.TrainingDatasetSearchResult")
class TrainingDatasetSearchResult(SearchResultItem):
    """Search result for a Training Dataset."""

    def get(self):
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
        fs = client.get_connection().get_feature_store(self.project.name)
        return fs.get_training_dataset(self.name, version=self.version)


@public("hopsworks.core.search_api.FeatureSearchResult")
class FeatureSearchResult(SearchResultItem):
    """Search result for a Feature."""


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

        # Store metadata about result counts
        self._feature_groups_offset = response_data.get("featuregroupsFrom", 0)
        self._feature_groups_total = response_data.get("featuregroupsTotal", 0)
        self._feature_views_offset = response_data.get("featureViewsFrom", 0)
        self._feature_views_total = response_data.get("featureViewsTotal", 0)
        self._training_datasets_offset = response_data.get("trainingdatasetsFrom", 0)
        self._training_datasets_total = response_data.get("trainingdatasetsTotal", 0)
        self._features_offset = response_data.get("featuresFrom", 0)
        self._features_total = response_data.get("featuresTotal", 0)

    @property
    def feature_groups(self) -> list[FeatureGroupSearchResult]:
        """List of Feature Group search results."""
        return self._feature_groups

    @property
    def feature_views(self) -> list[FeatureViewSearchResult]:
        """List of Feature View search results."""
        return self._feature_views

    @property
    def training_datasets(self) -> list[TrainingDatasetSearchResult]:
        """List of Training Dataset search results."""
        return self._training_datasets

    @property
    def features(self) -> list[FeatureSearchResult]:
        """List of Feature search results."""
        return self._features

    @property
    def feature_groups_offset(self) -> int:
        """Total offset for the return list of feature groups within the whole result."""
        return self._feature_groups_offset

    @property
    def feature_views_offset(self) -> int:
        """Total offset for the return list of feature views within the whole result."""
        return self._feature_views_offset

    @property
    def training_datasets_offset(self) -> int:
        """Total offset for the return list of training datasets within the whole result."""
        return self._training_datasets_offset

    @property
    def features_offset(self) -> int:
        """Total offset for the return list of features within the whole result."""
        return self._features_offset

    @property
    def feature_groups_total(self) -> int:
        """Total number of Feature Groups matching the search."""
        return self._feature_groups_total

    @property
    def feature_views_total(self) -> int:
        """Total number of Feature Views matching the search."""
        return self._feature_views_total

    @property
    def training_datasets_total(self) -> int:
        """Total number of Training Datasets matching the search."""
        return self._training_datasets_total

    @property
    def features_total(self) -> int:
        """Total number of Features matching the search."""
        return self._features_total

    def json(self) -> dict:
        """Convert to JSON-serializable dictionary."""
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
        }

    def __repr__(self):
        return (
            f"FeaturestoreSearchResult("
            f"feature_groups={len(self._feature_groups)}/{self._feature_groups_total}, "
            f"feature_views={len(self._feature_views)}/{self._feature_views_total}, "
            f"training_datasets={len(self._training_datasets)}/{self._training_datasets_total}, "
            f"features={len(self._features)}/{self._features_total})"
        )
