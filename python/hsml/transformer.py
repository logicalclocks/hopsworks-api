#
#   Copyright 2022 Logical Clocks AB
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
from __future__ import annotations

import humps
from hopsworks_apigen import public
from hopsworks_common import client, util
from hopsworks_common.constants import SCALING_CONFIG, Default
from hsml.deployable_component import DeployableComponent
from hsml.resources import TransformerResources
from hsml.scaling_config import TransformerScalingConfig


@public
class Transformer(DeployableComponent):
    """Metadata object representing a transformer to be used in a predictor."""

    @staticmethod
    def _get_raw_num_instances(resources):
        if resources is None:
            return None
        return (
            resources._num_instances
            if hasattr(resources, "_num_instances")
            else resources.num_instances
        )

    def __init__(
        self,
        script_file: str,
        resources: TransformerResources | dict | Default | None = None,  # base
        scaling_configuration: TransformerScalingConfig | dict | Default | None = None,
        env_vars: dict[str, str] | None = None,
        **kwargs,
    ):
        resources = self._validate_resources(
            util._get_obj_from_json(resources, TransformerResources)
        )
        # A transformer is built standalone, before it is attached to a predictor, so its Knative/Standard mode is not known here.
        # The provisional default assumes Knative mode (scale-to-zero when the cluster requires it) and is re-resolved mode-aware at serialization time, once the owning predictor is known (see _resolve_default_num_instances).
        self._num_instances_defaulted = (
            resources is None or self._get_raw_num_instances(resources) is None
        )
        resources = resources or self._get_default_resources()
        if self._get_raw_num_instances(resources) is None:
            resources._num_instances = self._get_default_num_instances()

        # Only an explicitly provided (or backend-hydrated) scaling config is stored and serialized: the backend synthesizes a mode-appropriate default when the key is absent, and a local default object would invite in-place edits that are silently never sent.
        # Default means "not provided": TransformerScalingConfig cannot be built without min_instances.
        if isinstance(scaling_configuration, Default):
            scaling_configuration = None
        self._scaling_configuration: TransformerScalingConfig | None = (
            util._get_obj_from_json(scaling_configuration, TransformerScalingConfig)
        )

        super().__init__(
            script_file, resources, scaling_configuration=self._scaling_configuration
        )

        self._env_vars = env_vars

    @public
    def describe(self):
        """Print a JSON description of the transformer."""
        util._pretty_print(self)

    @classmethod
    def _validate_resources(cls, resources):
        # The cluster's scale-to-zero requirement only applies to Knative deployments, and a transformer is built before the deployment mode is known.
        # A standard-mode deployment legitimately needs at least one instance, so enforcement is left to the backend, which validates the assembled deployment mode-aware.
        return resources

    @classmethod
    def _get_default_num_instances(cls):
        return (
            0  # enable scale-to-zero by default if required
            if client._is_scale_to_zero_required()
            else SCALING_CONFIG.MIN_NUM_INSTANCES
        )

    def _resolve_default_num_instances(self, effective_knative_mode: bool):
        # Re-resolve a defaulted instance count once the deployment mode is known.
        # The scale-to-zero default only applies to Knative mode; a Standard deployment needs at least one instance, and the backend cannot fix it up because the deprecated instances field always arrives with a value.
        # Explicitly provided values are never touched.
        if not self._num_instances_defaulted:
            return
        self._resources._num_instances = (
            self._get_default_num_instances()
            if effective_knative_mode
            else SCALING_CONFIG.MIN_NUM_INSTANCES
        )

    @classmethod
    def _get_default_resources(cls):
        return TransformerResources(cls._get_default_num_instances())

    @classmethod
    def from_json(cls, json_decamelized):
        sf, rc, sc, ev = cls.extract_fields_from_json(json_decamelized)
        return (
            Transformer(sf, rc, scaling_configuration=sc, env_vars=ev)
            if sf is not None
            else None
        )

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        sf = util._extract_field_from_json(
            json_decamelized, ["transformer", "script_file"]
        )
        if sf is None:
            return None, None, None, None
        sc = TransformerScalingConfig.from_json(json_decamelized)
        rc = TransformerResources.from_json(json_decamelized)
        env_vars = json_decamelized.pop("transformer_env_vars", None)
        ev = dict(e.split("=", 1) for e in env_vars) if env_vars else None
        return sf, rc, sc, ev

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        sf, rc, sc, ev = self.extract_fields_from_json(json_decamelized)
        self.__init__(sf, rc, scaling_configuration=sc, env_vars=ev)
        return self

    def to_dict(self):
        d = {"transformer": self._script_file, **self._resources.to_dict()}
        if self._scaling_configuration is not None:
            d = {**d, **self._scaling_configuration.to_dict()}
        if self._env_vars:
            d["transformerEnvVars"] = [f"{k}={v}" for k, v in self._env_vars.items()]
        return d

    @public
    @property
    def env_vars(self):
        """Environment variables of the transformer."""
        return self._env_vars

    @env_vars.setter
    def env_vars(self, env_vars: dict[str, str] | None):
        self._env_vars = env_vars

    @DeployableComponent.scaling_configuration.setter
    def scaling_configuration(
        self, scaling_configuration: TransformerScalingConfig | dict | Default | None
    ) -> None:
        # Mirror the constructor: accept a TransformerScalingConfig or a dict, and treat Default as not provided.
        if isinstance(scaling_configuration, Default):
            scaling_configuration = None
        self._scaling_configuration = util._get_obj_from_json(
            scaling_configuration, TransformerScalingConfig
        )

    def __repr__(self):
        return f"Transformer({self._script_file!r})"
