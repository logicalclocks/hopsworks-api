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
from hopsworks_common import client, util
from hopsworks_common.constants import PREDICTOR, SCALING_CONFIG, Default
from hsml.deployable_component import DeployableComponent
from hsml.resources import TransformerResources
from hsml.scaling_config import TransformerScalingConfig


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
        **kwargs,
    ):
        resources = (
            self._validate_resources(
                util.get_obj_from_json(resources, TransformerResources)
            )
            or self._get_default_resources()
        )
        if self._get_raw_num_instances(resources) is None:
            resources._num_instances = self._get_default_num_instances()

        self._scaling_configuration: TransformerScalingConfig = util.get_obj_from_json(
            scaling_configuration, TransformerScalingConfig
        ) or TransformerScalingConfig.get_default_scaling_configuration(
            serving_tool=PREDICTOR.SERVING_TOOL_KSERVE,
            min_instances=self._get_raw_num_instances(resources),
            component_type="transformer",
        )

        super().__init__(
            script_file, resources, scaling_configuration=self._scaling_configuration
        )

    def describe(self):
        """Print a JSON description of the transformer."""
        util.pretty_print(self)

    @classmethod
    def _validate_resources(cls, resources):
        if (
            resources is not None
            and cls._get_raw_num_instances(resources) != 0
            and client.is_scale_to_zero_required()
        ):
            # ensure scale-to-zero for kserve deployments when required
            raise ValueError(
                "Scale-to-zero is required for KServe deployments in this cluster. Please, set the number of transformer instances to 0."
            )
        return resources

    @classmethod
    def _get_default_num_instances(cls):
        return (
            0  # enable scale-to-zero by default if required
            if client.is_scale_to_zero_required()
            else SCALING_CONFIG.MIN_NUM_INSTANCES
        )

    @classmethod
    def _get_default_resources(cls):
        return TransformerResources(cls._get_default_num_instances())

    @classmethod
    def from_json(cls, json_decamelized):
        sf, rc, sc = cls.extract_fields_from_json(json_decamelized)
        return Transformer(sf, rc, scaling_configuration=sc) if sf is not None else None

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        sf = util.extract_field_from_json(
            json_decamelized, ["transformer", "script_file"]
        )
        if sf is None:
            return None, None, None
        sc = TransformerScalingConfig.from_json(json_decamelized)
        rc = TransformerResources.from_json(json_decamelized)
        return sf, rc, sc

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(*self.extract_fields_from_json(json_decamelized))
        return self

    def to_dict(self):
        return {"transformer": self._script_file, **self._resources.to_dict()}

    def __repr__(self):
        return f"Transformer({self._script_file!r})"
