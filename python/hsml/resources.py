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

import json
from abc import ABC, abstractmethod
from typing import Optional, Union

import humps
from hopsworks_common import util
from hopsworks_common.constants import RESOURCES, Default


class Resources:
    """Resource configuration for a predictor or transformer.

    # Arguments
        cores: Number of CPUs.
        memory: Memory (MB) resources.
        gpus: Number of GPUs.
    # Returns
        `Resources`. Resource configuration for a predictor or transformer.
    """

    def __init__(
        self,
        cores: int,
        memory: int,
        gpus: int,
        **kwargs,
    ):
        self._cores = cores
        self._memory = memory
        self._gpus = gpus

    def describe(self):
        """Print a description of the resource configuration"""
        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return Resources(**cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        kwargs = {}
        kwargs["cores"] = util.extract_field_from_json(json_decamelized, "cores")
        kwargs["memory"] = util.extract_field_from_json(json_decamelized, "memory")
        kwargs["gpus"] = util.extract_field_from_json(json_decamelized, "gpus")
        return kwargs

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {"cores": self._cores, "memory": self._memory, "gpus": self._gpus}

    @property
    def cores(self):
        """Number of CPUs to be allocated per instance"""
        return self._cores

    @cores.setter
    def cores(self, cores: int):
        self._cores = cores

    @property
    def memory(self):
        """Memory resources to be allocated per instance"""
        return self._memory

    @memory.setter
    def memory(self, memory: int):
        self._memory = memory

    @property
    def gpus(self):
        """Number of GPUs to be allocated per instance"""
        return self._gpus

    @gpus.setter
    def gpus(self, gpus: int):
        self._gpus = gpus

    def __repr__(self):
        return f"Resources(cores: {self._cores!r}, memory: {self._memory!r}, gpus: {self._gpus!r})"


class ComponentResources(ABC):
    """Resource configuration for a predictor or transformer.

    # Arguments
        num_instances: Number of instances.
        requests: Minimum resources to allocate for a deployment
        limits: Maximum resources to allocate for a deployment
    # Returns
        `ComponentResource`. Resource configuration for a predictor or transformer.
    """

    def __init__(
        self,
        num_instances: int,
        requests: Optional[Union[Resources, dict, Default]] = None,
        limits: Optional[Union[Resources, dict, Default]] = None,
    ):
        self._num_instances = num_instances
        self._requests = util.get_obj_from_json(requests, Resources) or Resources(
            RESOURCES.MIN_CORES, RESOURCES.MIN_MEMORY, RESOURCES.GPUS
        )
        self._fill_missing_resources(
            self._requests,
            RESOURCES.MIN_CORES,
            RESOURCES.MIN_MEMORY,
            RESOURCES.GPUS,
        )
        self._limits = util.get_obj_from_json(limits, Resources) or Resources(
            RESOURCES.MAX_CORES, RESOURCES.MAX_MEMORY, RESOURCES.GPUS
        )
        self._fill_missing_resources(
            self._limits,
            max(self._requests.cores, RESOURCES.MAX_CORES),
            max(self._requests.memory, RESOURCES.MAX_MEMORY),
            self._requests.gpus,
        )

    def describe(self):
        """Print a description of the resource configuration"""
        util.pretty_print(self)

    @classmethod
    def _fill_missing_resources(cls, resources, cores, memory, gpus):
        if resources.cores is None:
            resources.cores = cores
        if resources.memory is None:
            resources.memory = memory
        if resources.gpus is None:
            resources.gpus = gpus

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    @abstractmethod
    def from_json(cls, json_decamelized):
        pass

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        kwargs = {}

        # extract resources
        if cls.RESOURCES_CONFIG_KEY in json_decamelized:
            resources = json_decamelized.pop(cls.RESOURCES_CONFIG_KEY)
        elif "resources" in json_decamelized:
            resources = json_decamelized.pop("resources")
        else:
            resources = json_decamelized

        # extract resource fields
        kwargs["requests"] = util.extract_field_from_json(
            resources, "requests", as_instance_of=Resources
        )
        kwargs["limits"] = util.extract_field_from_json(
            resources, "limits", as_instance_of=Resources
        )

        # extract num instances
        if cls.NUM_INSTANCES_KEY in json_decamelized:
            kwargs["num_instances"] = json_decamelized.pop(cls.NUM_INSTANCES_KEY)
        elif "num_instances" in json_decamelized:
            kwargs["num_instances"] = json_decamelized.pop("num_instances")
        else:
            kwargs["num_instances"] = util.extract_field_from_json(
                resources, [cls.NUM_INSTANCES_KEY, "num_instances"]
            )

        return kwargs

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @abstractmethod
    def to_dict(self):
        pass

    @property
    def num_instances(self):
        """Number of instances"""
        return self._num_instances

    @num_instances.setter
    def num_instances(self, num_instances: int):
        self._num_instances = num_instances

    @property
    def requests(self):
        """Minimum resources to allocate"""
        return self._requests

    @requests.setter
    def requests(self, requests: Resources):
        self._resources = requests

    @property
    def limits(self):
        """Maximum resources to allocate"""
        return self._limits

    @limits.setter
    def limits(self, limits: Resources):
        self._limits = limits

    def __repr__(self):
        return f"ComponentResources(num_instances: {self._num_instances!r}, requests: {self._requests is not None!r}, limits: {self._limits is not None!r})"


class PredictorResources(ComponentResources):
    RESOURCES_CONFIG_KEY = "predictor_resources"
    NUM_INSTANCES_KEY = "requested_instances"

    def __init__(
        self,
        num_instances: int,
        requests: Optional[Union[Resources, dict]] = None,
        limits: Optional[Union[Resources, dict]] = None,
    ):
        super().__init__(num_instances, requests, limits)

    @classmethod
    def from_json(cls, json_decamelized):
        return PredictorResources(**cls.extract_fields_from_json(json_decamelized))

    def to_dict(self):
        return {
            humps.camelize(self.NUM_INSTANCES_KEY): self._num_instances,
            humps.camelize(self.RESOURCES_CONFIG_KEY): {
                "requests": (
                    self._requests.to_dict() if self._requests is not None else None
                ),
                "limits": self._limits.to_dict() if self._limits is not None else None,
            },
        }


class TransformerResources(ComponentResources):
    RESOURCES_CONFIG_KEY = "transformer_resources"
    NUM_INSTANCES_KEY = "requested_transformer_instances"

    def __init__(
        self,
        num_instances: int,
        requests: Optional[Union[Resources, dict]] = None,
        limits: Optional[Union[Resources, dict]] = None,
    ):
        super().__init__(num_instances, requests, limits)

    @classmethod
    def from_json(cls, json_decamelized):
        return TransformerResources(**cls.extract_fields_from_json(json_decamelized))

    def to_dict(self):
        return {
            humps.camelize(self.NUM_INSTANCES_KEY): self._num_instances,
            humps.camelize(self.RESOURCES_CONFIG_KEY): {
                "requests": (
                    self._requests.to_dict() if self._requests is not None else None
                ),
                "limits": self._limits.to_dict() if self._limits is not None else None,
            },
        }
