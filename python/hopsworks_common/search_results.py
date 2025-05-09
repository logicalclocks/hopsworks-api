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
from typing import Optional

import humps
from hopsworks_common import util
from hsfs.core import feature_group_api, feature_view_api


class Creator:
    def __init__(
        self, username=None, firstname=None, lastname=None, email=None, **kwargs
    ):
        self._username = username
        self._firstname = firstname
        self._lastname = lastname
        self._email = email

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def username(self) -> str:
        return self._username

    @property
    def firstname(self) -> str:
        return self._firstname

    @property
    def lastname(self) -> str:
        return self._lastname

    @property
    def email(self) -> str:
        return self._email

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "firstname": self.firstname,
            "lastname": self.lastname,
            "email": self.email,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Creator(id={self._username}, name={self._firstname} {self._lastname})"


class Tag:
    def __init__(self, key=None, value=None, **kwargs):
        self._key = key
        self._value = value

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def key(self) -> str:
        return self._key

    @property
    def value(self) -> str:
        return self._value

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Tag(key={self.key}, description={self.value})"


class FeatureHighlights:
    def __init__(self, name=None, description=None, **kwargs):
        self._name = name
        self._description = description

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureHighlights(name={self.name}, description={self.description})"


class Highlight:
    def __init__(
        self,
        name=None,
        description=None,
        features=None,
        tags=None,
        other_xattrs=None,
        **kwargs,
    ):
        self._name = name
        self._description = description
        self._features = (
            [FeatureHighlights.from_response_json(f) for f in features]
            if features
            else []
        )
        self._tags = [Tag.from_response_json(tag) for tag in tags] if tags else []
        self._other_xattrs = other_xattrs

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def features(self) -> Optional[list]:
        return self._features

    @property
    def tags(self) -> Optional[list]:
        return self._tags

    @property
    def other_xattrs(self) -> Optional[dict]:
        return self._other_xattrs

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "features": [f.to_dict() for f in self.features],
            "tags": [tag.to_dict() for tag in self.tags],
            "other_xattrs": self.other_xattrs,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Highlight(name={self.name}, description={self.description}, features={self.features}, tags={self.tags}, other_xattrs={self.other_xattrs})"


class FeaturestoreResult:
    def __init__(
        self,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        created=None,
        parent_project_id=None,
        parent_project_name=None,
        access_projects=None,
        highlights: Highlight = None,
        creator: Creator = None,
        elastic_id=None,
        **kwargs,
    ):
        self._name = name
        self._version = version
        self._description = description
        self._featurestore_id = featurestore_id
        self._created = created
        self._parent_project_id = parent_project_id
        self._parent_project_name = parent_project_name
        self._access_projects = access_projects
        self._highlights = (
            Highlight.from_response_json(highlights) if highlights else None
        )
        self._creator = Creator.from_response_json(creator) if creator else None
        self._elastic_id = elastic_id

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> int:
        return self._version

    @property
    def description(self) -> str:
        return self._description

    @property
    def featurestore_id(self) -> int:
        return self._featurestore_id

    @property
    def created(self) -> str:
        return self._created

    @property
    def parent_project_id(self) -> int:
        return self._parent_project_id

    @property
    def parent_project_name(self) -> str:
        return self._parent_project_name

    @property
    def access_projects(self) -> Optional[dict]:
        return self._access_projects

    @property
    def highlights(self) -> Optional[Highlight]:
        return self._highlights

    @property
    def creator(self) -> Optional[Creator]:
        return self._creator

    @property
    def elastic_id(self) -> str:
        return self._elastic_id

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "featurestore_id": self.featurestore_id,
            "created": self.created,
            "parent_project_id": self.parent_project_id,
            "parent_project_name": self.parent_project_name,
            "access_projects": self.access_projects,
            "highlights": self.highlights.to_dict() if self.highlights else None,
            "creator": self.creator.to_dict() if self.creator else None,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeaturestoreResult(name={self.name}, version={self.version}, description={self.description}, featurestore_id={self.featurestore_id}, created={self.created}, parent_project_id={self.parent_project_id}, parent_project_name={self.parent_project_name}, access_projects={self.access_projects}, highlights={self.highlights}, creator={self.creator})"


class FeatureResult(FeaturestoreResult):
    def __init__(
        self,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        created=None,
        parent_project_id=None,
        parent_project_name=None,
        access_projects=None,
        highlights: Highlight = None,
        creator: Creator = None,
        elastic_id=None,
        featuregroup=None,
        **kwargs,
    ):
        super().__init__(
            name,
            version,
            description,
            featurestore_id,
            created,
            parent_project_id,
            parent_project_name,
            access_projects,
            highlights,
            creator,
            elastic_id,
            **kwargs,
        )
        self._featuregroup = featuregroup
        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def get_feature_group(self):
        return self._feature_group_api.get(
            self._featurestore_id, self._featuregroup, self._version
        )

    def get_feature(self):
        fg = self._feature_group_api.get(
            self._featurestore_id, self._featuregroup, self._version
        )
        return fg.get_feature(self.name)

    @property
    def featuregroup(self) -> str:
        return self._featuregroup

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "featurestore_id": self.featurestore_id,
            "created": self.created,
            "parent_project_id": self.parent_project_id,
            "parent_project_name": self.parent_project_name,
            "access_projects": self.access_projects,
            "highlights": self.highlights.to_dict() if self.highlights else None,
            "creator": self.creator.to_dict() if self.creator else None,
            "featuregroup": self.featuregroup,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureResult(name={self.name}, version={self.version}, description={self.description}, featurestore_id={self.featurestore_id}, created={self.created}, parent_project_id={self.parent_project_id}, parent_project_name={self.parent_project_name}, access_projects={self.access_projects}, highlights={self.highlights}, creator={self.creator}, featuregroup={self.featuregroup})"


class FeatureGroupResult(FeaturestoreResult):
    def __init__(
        self,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        created=None,
        parent_project_id=None,
        parent_project_name=None,
        access_projects=None,
        highlights: Highlight = None,
        creator: Creator = None,
        elastic_id=None,
        **kwargs,
    ):
        super().__init__(
            name,
            version,
            description,
            featurestore_id,
            created,
            parent_project_id,
            parent_project_name,
            access_projects,
            highlights,
            creator,
            elastic_id,
            **kwargs,
        )
        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def get_feature_group(self):
        return self._feature_group_api.get(
            self._featurestore_id, self._name, self._version
        )

    def __repr__(self) -> str:
        return f"FeatureGroupResult(name={self.name}, version={self.version}, description={self.description}, featurestore_id={self.featurestore_id}, created={self.created}, parent_project_id={self.parent_project_id}, parent_project_name={self.parent_project_name}, access_projects={self.access_projects}, highlights={self.highlights}, creator={self.creator})"



class FeatureViewResult(FeaturestoreResult):
    def __init__(
        self,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        created=None,
        parent_project_id=None,
        parent_project_name=None,
        access_projects=None,
        highlights: Highlight = None,
        creator: Creator = None,
        elastic_id=None,
        **kwargs,
    ):
        super().__init__(
            name,
            version,
            description,
            featurestore_id,
            created,
            parent_project_id,
            parent_project_name,
            access_projects,
            highlights,
            creator,
            elastic_id,
            **kwargs,
        )
        self._feature_view_api = feature_view_api.FeatureViewApi(self._featurestore_id)

    def get_feature_view(self):
        return self._feature_view_api.get_by_name_version(self._name, self._version)

    def __repr__(self) -> str:
        return f"FeatureViewResult(name={self.name}, version={self.version}, description={self.description}, featurestore_id={self.featurestore_id}, created={self.created}, parent_project_id={self.parent_project_id}, parent_project_name={self.parent_project_name}, access_projects={self.access_projects}, highlights={self.highlights}, creator={self.creator})"


class TrainingDatasetResult(FeaturestoreResult):
    def __init__(
        self,
        name=None,
        version=None,
        description=None,
        featurestore_id=None,
        created=None,
        parent_project_id=None,
        parent_project_name=None,
        access_projects=None,
        highlights: Highlight = None,
        creator: Creator = None,
        elastic_id=None,
        **kwargs,
    ):
        super().__init__(
            name,
            version,
            description,
            featurestore_id,
            created,
            parent_project_id,
            parent_project_name,
            access_projects,
            highlights,
            creator,
            elastic_id,
            **kwargs,
        )
        self._feature_view_api = feature_view_api.FeatureViewApi(self._featurestore_id)

    def get_training_dataset(self):
        return self._feature_view_api.get_training_datasets(self._name, self._version)

    def __repr__(self) -> str:
        return f"TrainingDatasetResult(name={self.name}, version={self.version}, description={self.description}, featurestore_id={self.featurestore_id}, created={self.created}, parent_project_id={self.parent_project_id}, parent_project_name={self.parent_project_name}, access_projects={self.access_projects}, highlights={self.highlights}, creator={self.creator})"


class FeaturestoreSearchResultBase:
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        self._featuregroups = featuregroups
        self._feature_views = feature_views
        self._trainingdatasets = trainingdatasets
        self._features = features
        self._featuregroups_from = featuregroups_from
        self._featuregroups_total = featuregroups_total
        self._feature_views_from = feature_views_from
        self._feature_views_total = feature_views_total
        self._trainingdatasets_from = trainingdatasets_from
        self._trainingdatasets_total = trainingdatasets_total
        self._features_from = features_from
        self._features_total = features_total

    @classmethod
    def from_response_json(cls, json_dict: dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def featuregroups(self) -> Optional[list]:
        """List of feature groups."""
        return self._featuregroups

    @property
    def feature_views(self) -> Optional[list]:
        """List of feature views."""
        return self._feature_views

    @property
    def trainingdatasets(self) -> Optional[list]:
        """List of training datasets."""
        return self._trainingdatasets

    @property
    def features(self) -> Optional[list]:
        """List of features."""
        return self._features

    @property
    def featuregroups_from(self) -> Optional[list]:
        """Results from."""
        return self._featuregroups_from

    @property
    def featuregroups_total(self) -> Optional[list]:
        """Total found."""
        return self._featuregroups_total

    @property
    def feature_views_from(self) -> Optional[list]:
        """Results from."""
        return self._feature_views_from

    @property
    def feature_views_total(self) -> Optional[list]:
        """Total found."""
        return self._feature_views_total

    @property
    def trainingdatasets_from(self) -> Optional[list]:
        """Results from."""
        return self._trainingdatasets_from

    @property
    def trainingdatasets_total(self) -> Optional[list]:
        """Total found."""
        return self._trainingdatasets_total

    @property
    def features_from(self) -> Optional[list]:
        """Results from."""
        return self._features_from

    @property
    def features_total(self) -> Optional[list]:
        """Total found."""
        return self._features_total

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "featuregroups": [fg.to_dict() for fg in self.featuregroups],
            "feature_views": [fv.to_dict() for fv in self.feature_views],
            "trainingdatasets": [td.to_dict() for td in self.trainingdatasets],
            "features": [f.to_dict() for f in self.features],
            "featuregroups_from": self.featuregroups_from,
            "featuregroups_total": self.featuregroups_total,
            "feature_views_from": self.feature_views_from,
            "feature_views_total": self.feature_views_total,
            "trainingdatasets_from": self.trainingdatasets_from,
            "trainingdatasets_total": self.trainingdatasets_total,
            "features_from": self.features_from,
            "features_total": self.features_total,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeaturestoreSearchResult(featuregroups={self.featuregroups}, feature_views={self.feature_views}, trainingdatasets={self.trainingdatasets}, features={self.features})"


class FeaturestoreSearchResult(FeaturestoreSearchResultBase):
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        _featuregroups = (
            [FeatureGroupResult.from_response_json(fg) for fg in featuregroups]
            if featuregroups
            else []
        )
        _feature_views = (
            [FeatureViewResult.from_response_json(fv) for fv in feature_views]
            if feature_views
            else []
        )
        _trainingdatasets = (
            [TrainingDatasetResult.from_response_json(td) for td in trainingdatasets]
            if trainingdatasets
            else []
        )
        _features = (
            [FeatureResult.from_response_json(f) for f in features] if features else []
        )
        super().__init__(
            _featuregroups,
            _feature_views,
            _trainingdatasets,
            _features,
            featuregroups_from,
            featuregroups_total,
            feature_views_from,
            feature_views_total,
            trainingdatasets_from,
            trainingdatasets_total,
            features_from,
            features_total,
        )


class FeaturestoreSearchResultByTag(FeaturestoreSearchResultBase):
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        _featuregroups = (
            [
                FeatureGroupResult.from_response_json(fg)
                for fg in featuregroups
                if "highlights" in fg and "tags" in fg["highlights"]
            ]
            if featuregroups
            else []
        )
        _feature_views = (
            [
                FeatureViewResult.from_response_json(fv)
                for fv in feature_views
                if "highlights" in fv and "tags" in fv["highlights"]
            ]
            if feature_views
            else []
        )
        _trainingdatasets = (
            [
                TrainingDatasetResult.from_response_json(td)
                for td in trainingdatasets
                if "highlights" in td and "tags" in td["highlights"]
            ]
            if trainingdatasets
            else []
        )
        _features = (
            [
                FeatureResult.from_response_json(f)
                for f in features
                if "highlights" in f and "tags" in f["highlights"]
            ]
            if features
            else []
        )
        super().__init__(
            _featuregroups,
            _feature_views,
            _trainingdatasets,
            _features,
            featuregroups_from,
            featuregroups_total,
            feature_views_from,
            feature_views_total,
            trainingdatasets_from,
            trainingdatasets_total,
            features_from,
            features_total,
        )


class FeaturestoreSearchResultByTagKey(FeaturestoreSearchResultBase):
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        _featuregroups = (
            [
                FeatureGroupResult.from_response_json(fg)
                for fg in featuregroups
                if "highlights" in fg
                and "tags" in fg["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[0]
                        for tag in fg["highlights"]["tags"]
                    ]
                )
            ]
            if featuregroups
            else []
        )
        _feature_views = (
            [
                FeatureViewResult.from_response_json(fv)
                for fv in feature_views
                if "highlights" in fv
                and "tags" in fv["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[0]
                        for tag in fv["highlights"]["tags"]
                    ]
                )
            ]
            if feature_views
            else []
        )
        _trainingdatasets = (
            [
                TrainingDatasetResult.from_response_json(td)
                for td in trainingdatasets
                if "highlights" in td
                and "tags" in td["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[0]
                        for tag in td["highlights"]["tags"]
                    ]
                )
            ]
            if trainingdatasets
            else []
        )
        _features = (
            [
                FeatureResult.from_response_json(f)
                for f in features
                if "highlights" in f
                and "tags" in f["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[0]
                        for tag in f["highlights"]["tags"]
                    ]
                )
            ]
            if features
            else []
        )
        super().__init__(
            _featuregroups,
            _feature_views,
            _trainingdatasets,
            _features,
            featuregroups_from,
            featuregroups_total,
            feature_views_from,
            feature_views_total,
            trainingdatasets_from,
            trainingdatasets_total,
            features_from,
            features_total,
        )


class FeaturestoreSearchResultByTagValue(FeaturestoreSearchResultBase):
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        _featuregroups = (
            [
                FeatureGroupResult.from_response_json(fg)
                for fg in featuregroups
                if "highlights" in fg
                and "tags" in fg["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[1]
                        for tag in fg["highlights"]["tags"]
                    ]
                )
            ]
            if featuregroups
            else []
        )
        _feature_views = (
            [
                FeatureViewResult.from_response_json(fv)
                for fv in feature_views
                if "highlights" in fv
                and "tags" in fv["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[1]
                        for tag in fv["highlights"]["tags"]
                    ]
                )
            ]
            if feature_views
            else []
        )
        _trainingdatasets = (
            [
                TrainingDatasetResult.from_response_json(td)
                for td in trainingdatasets
                if "highlights" in td
                and "tags" in td["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[1]
                        for tag in td["highlights"]["tags"]
                    ]
                )
            ]
            if trainingdatasets
            else []
        )
        _features = (
            [
                FeatureResult.from_response_json(f)
                for f in features
                if "highlights" in f
                and "tags" in f["highlights"]
                and any(
                    [
                        "<em>" in tag["value"].split(":")[1]
                        for tag in f["highlights"]["tags"]
                    ]
                )
            ]
            if features
            else []
        )
        super().__init__(
            _featuregroups,
            _feature_views,
            _trainingdatasets,
            _features,
            featuregroups_from,
            featuregroups_total,
            feature_views_from,
            feature_views_total,
            trainingdatasets_from,
            trainingdatasets_total,
            features_from,
            features_total,
        )


class FeaturestoreSearchResultByKeyWord(FeaturestoreSearchResultBase):
    def __init__(
        self,
        featuregroups=None,
        feature_views=None,
        trainingdatasets=None,
        features=None,
        featuregroups_from=None,
        featuregroups_total=None,
        feature_views_from=None,
        feature_views_total=None,
        trainingdatasets_from=None,
        trainingdatasets_total=None,
        features_from=None,
        features_total=None,
        **kwargs,
    ):
        _featuregroups = (
            [
                FeatureGroupResult.from_response_json(fg)
                for fg in featuregroups
                if "highlights" in fg
                and "other_xattrs" in fg["highlights"]
                and "xattr.keywords" in fg["highlights"]["other_xattrs"]
            ]
            if featuregroups
            else []
        )
        _feature_views = (
            [
                FeatureViewResult.from_response_json(fv)
                for fv in feature_views
                if "highlights" in fv
                and "other_xattrs" in fv["highlights"]
                and "xattr.keywords" in fv["highlights"]["other_xattrs"]
            ]
            if feature_views
            else []
        )
        _trainingdatasets = (
            [
                TrainingDatasetResult.from_response_json(td)
                for td in trainingdatasets
                if "highlights" in td
                and "other_xattrs" in td["highlights"]
                and "xattr.keywords" in td["highlights"]["other_xattrs"]
            ]
            if trainingdatasets
            else []
        )
        _features = (
            [
                FeatureResult.from_response_json(f)
                for f in features
                if "highlights" in f
                and "other_xattrs" in f["highlights"]
                and "xattr.keywords" in f["highlights"]["other_xattrs"]
            ]
            if features
            else []
        )
        super().__init__(
            _featuregroups,
            _feature_views,
            _trainingdatasets,
            _features,
            featuregroups_from,
            featuregroups_total,
            feature_views_from,
            feature_views_total,
            trainingdatasets_from,
            trainingdatasets_total,
            features_from,
            features_total,
        )


class FeatureGroupSearchResult:
    def __init__(self, result: FeaturestoreSearchResultBase):
        self._featuregroups = result.featuregroups
        self._featuregroups_from = result.featuregroups_from
        self._featuregroups_total = result.featuregroups_total

    @property
    def featuregroups(self) -> Optional[list]:
        """List of feature groups."""
        return self._featuregroups

    @property
    def featuregroups_from(self) -> Optional[list]:
        """Result from."""
        return self._featuregroups_from

    @property
    def featuregroups_total(self) -> Optional[list]:
        """Total feature groups found."""
        return self._featuregroups_total

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "featuregroups": [fg.to_dict() for fg in self.featuregroups],
            "featuregroups_from": self.featuregroups_from,
            "featuregroups_total": self.featuregroups_total,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureGroupSearchResult(featuregroups={self.featuregroups}, featuregroups_from={self.featuregroups_from}, featuregroups_total={self.featuregroups_total})"


class FeatureViewSearchResult:
    def __init__(self, result: FeaturestoreSearchResultBase):
        self._feature_views = result.feature_views
        self._feature_views_from = result.feature_views_from
        self._feature_views_total = result.feature_views_total

    @property
    def feature_views(self) -> Optional[list]:
        """List of feature views."""
        return self._feature_views

    @property
    def feature_views_from(self) -> Optional[list]:
        """Result from."""
        return self._feature_views_from

    @property
    def feature_views_total(self) -> Optional[list]:
        """Total feature views found."""
        return self._feature_views_total

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "feature_views": [fg.to_dict() for fg in self.feature_views],
            "feature_views_from": self.feature_views_from,
            "feature_views_total": self.feature_views_total,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureViewSearchResult(feature_views={self.feature_views}, feature_views_from={self.feature_views_from}, feature_views_total={self.feature_views_total})"


class FeatureSearchResult:
    def __init__(self, result: FeaturestoreSearchResultBase):
        self._features = result.features
        self._features_from = result.features_from
        self._features_total = result.features_total

    @property
    def features(self) -> Optional[list]:
        """List of featurs."""
        return self._features

    @property
    def features_from(self) -> Optional[list]:
        """Result from."""
        return self._features_from

    @property
    def features_total(self) -> Optional[list]:
        """Total features found."""
        return self._features_total

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "features": [fg.to_dict() for fg in self.features],
            "features_from": self.features_from,
            "features_total": self.features_total,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureSearchResult(features={self.features}, features_from={self.features_from}, features_total={self.features_total})"


class TrainingdatasetsSearchResult:
    def __init__(self, result: FeaturestoreSearchResultBase):
        self._trainingdatasets = result.trainingdatasets
        self._trainingdatasetsfrom = result.trainingdatasets_from
        self._trainingdatasets_total = result.trainingdatasets_total

    @property
    def trainingdatasets(self) -> Optional[list]:
        """List of featurs."""
        return self._trainingdatasets

    @property
    def trainingdatasets_from(self) -> Optional[list]:
        """Result from."""
        return self._trainingdatasets_from

    @property
    def trainingdatasets_total(self) -> Optional[list]:
        """Total training datasets found."""
        return self._trainingdatasets_total

    def json(self) -> dict:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict:
        return {
            "trainingdatasets": [td.to_dict() for td in self.trainingdatasets],
            "trainingdatasets_from": self.trainingdatasets_from,
            "trainingdatasets_total": self.trainingdatasets_total,
        }

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"TrainingdatasetsSearchResult(trainingdatasets={self.trainingdatasets}, trainingdatasets_from={self.trainingdatasets_from}, trainingdatasets_total={self.trainingdatasets_total})"
