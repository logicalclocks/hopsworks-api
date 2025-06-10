#
#   Copyright 2024 Hopsworks AB
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
from enum import Enum
from typing import Set

import humps


_logger = logging.getLogger(__name__)


class Artifact:
    class MetaType(Enum):
        DELETED = 1
        INACCESSIBLE = 2
        FAULTY = 3
        NOT_SUPPORTED = 4

    def __init__(
        self,
        model_registry_id,
        name,
        version,
        type,
        meta_type,
        href=None,
        exception_cause=None,
        **kwargs,
    ):
        self._model_registry_id = model_registry_id
        self._name = name
        self._version = version
        self._type = type
        self._meta_type = meta_type
        self._href = href
        self._exception_cause = exception_cause

    @property
    def model_registry_id(self):
        """Id of the model registry in which the artifact is located."""
        return self._model_registry_id

    @property
    def name(self):
        """Name of the artifact."""
        return self._name

    @property
    def version(self):
        """Version of the artifact"""
        return self._version

    def __str__(self):
        return {
            "model_registry_id": self._model_registry_id,
            "name": self._name,
            "version": self._version,
        }

    def __repr__(self):
        return (
            f"Artifact({self._model_registry_id!r}, {self._name!r}, "
            f"{self._version!r}, {self._type!r}, {self._meta_type!r}, "
            f"{self._href!r}, {self._exception_cause!r})"
        )

    @staticmethod
    def from_response_json(json_dict: dict):
        link_json = humps.decamelize(json_dict)
        href = None
        exception_cause = None
        if link_json.get("exception_cause") is not None:
            meta_type = Artifact.MetaType.FAULTY
            exception_cause = link_json.get("exception_cause")
        elif bool(link_json["deleted"]):
            meta_type = Artifact.MetaType.DELETED
        elif not bool(link_json["accessible"]):
            meta_type = Artifact.MetaType.INACCESSIBLE
            href = link_json["artifact"]["href"]
        else:
            meta_type = Artifact.MetaType.NOT_SUPPORTED
            href = link_json["artifact"]["href"]
        return Artifact(
            link_json["artifact"]["project"],
            link_json["artifact"]["name"],
            link_json["artifact"]["version"],
            link_json["artifact_type"],
            meta_type,
            href=href,
            exception_cause=exception_cause,
        )


class Links:
    def __init__(self, accessible=None, deleted=None, inaccessible=None, faulty=None):
        if accessible is None:
            self._accessible = []
        else:
            self._accessible = accessible
        if deleted is None:
            self._deleted = []
        else:
            self._deleted = deleted
        if inaccessible is None:
            self._inaccessible = []
        else:
            self._inaccessible = inaccessible
        if faulty is None:
            self._faulty = []
        else:
            self._faulty = faulty

    @property
    def deleted(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (feature views, training datasets) they represent.
        These entities have been removed from the feature store.
        """
        return self._deleted

    @property
    def inaccessible(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (feature views, training datasets) they represent.
        These entities exist in the feature store, however the user
        does not have access to them anymore.
        """
        return self._inaccessible

    @property
    def accessible(self):
        """List of [FeatureView|TrainingDataset objects] objects
        which are part of the provenance graph requested. These entities
        exist in the feature store and the user has access to them.
        """
        return self._accessible

    @property
    def faulty(self):
        """List of [Artifact objects] which contains
        minimal information (name, version) about the entities
        (feature views, training datasets) they represent.
        These entities exist in the feature store, however they are corrupted.
        """
        return self._faulty

    def is_empty(self):
        return (
            len(self.accessible) == 0
            and len(self.inaccessible) == 0
            and len(self.deleted) == 0
            and len(self.faulty) == 0
        )

    class Direction(Enum):
        UPSTREAM = 1
        DOWNSTREAM = 2

    class Type(Enum):
        FEATURE_VIEW = 1
        TRAINING_DATASET = 2

    def __str__(self, indent=None):
        return json.dumps(self, cls=ProvenanceEncoder, indent=indent)

    def __repr__(self):
        return (
            f"Links({self._accessible!r}, {self._deleted!r}"
            f", {self._inaccessible!r}, {self._faulty!r})"
        )

    @staticmethod
    def get_one_accessible_parent(links):
        if links is None:
            _logger.info("There is no parent information")
            return
        elif links.inaccessible or links.deleted:
            _logger.info(
                "The parent is deleted or inaccessible. For more details get the full provenance from `_provenance` method"
            )
            return None
        elif links.accessible:
            if len(links.accessible) > 1:
                msg = "Backend inconsistency - provenance returned more than one parent"
                raise Exception(msg)
            parent = links.accessible[0]
            if isinstance(parent, Artifact):
                msg = "The returned object is not a valid object. For more details get the full provenance from `_provenance` method"
                raise Exception(msg)
            return parent
        else:
            _logger.info("There is no parent information")
            return None

    @staticmethod
    def __parse_feature_views(links_json: dict, artifacts: Set[str]):
        from hsfs import feature_view
        from hsfs.core import explicit_provenance as hsfs_explicit_provenance

        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] in artifacts:
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                elif bool(link_json["node"]["accessible"]):
                    fv = feature_view.FeatureView.from_response_json(
                        link_json["node"]["artifact"]
                    )
                    links.accessible.append(fv)
                elif bool(link_json["node"]["deleted"]):
                    links.deleted.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                else:
                    links.inaccessible.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
            else:
                new_links = Links.__parse_feature_views(
                    link_json["upstream"], artifacts
                )
                links.faulty.extend(new_links.faulty)
                links.accessible.extend(new_links.accessible)
                links.inaccessible.extend(new_links.inaccessible)
                links.deleted.extend(new_links.deleted)
        return links

    @staticmethod
    def __parse_training_datasets(links_json: dict, artifacts: Set[str]):
        from hsfs import training_dataset
        from hsfs.core import explicit_provenance as hsfs_explicit_provenance

        links = Links()
        for link_json in links_json:
            if link_json["node"]["artifact_type"] in artifacts:
                if link_json["node"].get("exception_cause") is not None:
                    links._faulty.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                elif bool(link_json["node"]["accessible"]):
                    td = training_dataset.TrainingDataset.from_response_json_single(
                        link_json["node"]["artifact"]
                    )
                    links.accessible.append(td)
                elif bool(link_json["node"]["deleted"]):
                    links.deleted.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
                else:
                    links.inaccessible.append(
                        hsfs_explicit_provenance.Artifact.from_response_json(
                            link_json["node"]
                        )
                    )
        return links

    @staticmethod
    def from_response_json(json_dict: dict, direction: Direction, artifact: Type):
        """Parse explicit links from json response. There are three types of
        Links: UpstreamFeatureGroups, DownstreamFeatureGroups, DownstreamFeatureViews

        # Arguments
            links_json: json response from the explicit provenance endpoint
            direction: subset of links to parse - UPSTREAM/DOWNSTREAM
            type: subset of links to parse - FEATURE_VIEW/TRAINING_DATASET/MODEL

        # Returns
            A ProvenanceLink object for the selected parse type.
        """

        import importlib.util

        if not importlib.util.find_spec("hsfs"):
            raise ValueError(
                "hsfs is not installed in the environment - cannot parse feature store artifacts"
            )
        if not importlib.util.find_spec("hopsworks"):
            raise ValueError(
                "hopsworks is not installed in the environment - cannot switch from hsml connection to hsfs connection"
            )

        # make sure the hsfs connection is initialized so that the feature view/training dataset can actually be used after being returned
        import hopsworks

        if not hopsworks._connected_project:
            raise Exception(
                "hopsworks connection is not initialized - use hopsworks.login to connect if you want the ability to use provenance with connections between hsfs and hsml"
            )

        hopsworks._connected_project.get_feature_store()

        links = Links.__from_response_json_feature_store_artifacts(
            json_dict, direction, artifact
        )
        return links

    @staticmethod
    def __from_response_json_feature_store_artifacts(
        json_dict: dict, direction: Direction, artifact: Type
    ):
        links_json = humps.decamelize(json_dict)
        if direction == Links.Direction.UPSTREAM:
            if artifact == Links.Type.FEATURE_VIEW:
                return Links.__parse_feature_views(
                    links_json["upstream"],
                    {
                        "FEATURE_VIEW",
                    },
                )
            elif artifact == Links.Type.TRAINING_DATASET:
                return Links.__parse_training_datasets(
                    links_json["upstream"], {"TRAINING_DATASET"}
                )
        else:
            return Links()


class ProvenanceEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Links):
            return {
                "accessible": obj.accessible,
                "inaccessible": obj.inaccessible,
                "deleted": obj.deleted,
                "faulty": obj.faulty,
            }
        else:
            import importlib.util

            if importlib.util.find_spec("hsfs"):
                from hsfs import feature_view
                from hsfs.core import explicit_provenance as hsfs_explicit_provenance

                if isinstance(
                    obj,
                    (
                        feature_view.FeatureView,
                        hsfs_explicit_provenance.Artifact,
                    ),
                ):
                    return {
                        "feature_store_name": obj.feature_store_name,
                        "name": obj.name,
                        "version": obj.version,
                    }
            return json.JSONEncoder.default(self, obj)
