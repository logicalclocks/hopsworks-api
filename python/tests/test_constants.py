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

import inspect

from hsml import constants


class TestConstants:
    # NOTE
    # This class contains validations for constants and enum values.
    # The purpose of this class is to ensure that (1) we don't make undesired changes to contant values
    # that might break things somewhere else, and (2) we remember to update the pytests accordingly by
    # adding / removing / updating tests.

    # This class includes the following validations:
    # - Number of possible values of an Enum (to check for added/removed values)
    # - Exact values of contants (to check for modified values)

    # MODEL

    def test_model_framework_constants(self):
        # Arrange
        model_frameworks = {
            "FRAMEWORK_TENSORFLOW": "TENSORFLOW",
            "FRAMEWORK_TORCH": "TORCH",
            "FRAMEWORK_PYTHON": "PYTHON",
            "FRAMEWORK_SKLEARN": "SKLEARN",
            "FRAMEWORK_LLM": "LLM",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.MODEL,
            num_values=len(model_frameworks),
            expected_constants=model_frameworks,
            prefix="FRAMEWORK",
        )

    # MODEL_REGISTRY

    def test_model_registry_constants(self):
        # Arrange
        model_registry = {
            "HOPSFS_MOUNT_PREFIX": "/hopsfs/",
            "MODEL_FILES_DIR_NAME": "Files",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.MODEL_REGISTRY,
            num_values=len(model_registry),
            expected_constants=model_registry,
        )

    # MODEL_SERVING

    def test_model_serving_constants(self):
        # Arrange
        model_serving = {"MODELS_DATASET": "Models", "ARTIFACTS_DIR_NAME": "Artifacts"}

        # Assert
        self._check_added_modified_or_removed_values(
            constants.MODEL_SERVING,
            num_values=len(model_serving),
            expected_constants=model_serving,
        )

    # ARTIFACT_VERSION

    def test_artifact_version_constants(self):
        # Arrange
        artifact_versions = {"CREATE": "CREATE"}

        # Assert
        self._check_added_modified_or_removed_values(
            constants.ARTIFACT_VERSION,
            num_values=len(artifact_versions),
            expected_constants=artifact_versions,
        )

    # RESOURCES

    def test_resources_min_constants(self):
        # Arrange
        min_resources = {
            "MIN_NUM_INSTANCES": 1,
            "MIN_CORES": 0.2,
            "MIN_MEMORY": 32,
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.RESOURCES,
            num_values=len(min_resources),
            expected_constants=min_resources,
            prefix="MIN",
        )

    def test_resources_max_constants(self):
        # Arrange
        max_resources = {
            "MAX_CORES": 2,
            "MAX_MEMORY": 1024,
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.RESOURCES,
            num_values=len(max_resources),
            expected_constants=max_resources,
            prefix="MAX",
        )

    def test_resources_gpu_constants(self):
        # Assert
        assert hasattr(constants.RESOURCES, "GPUS")
        assert constants.RESOURCES.GPUS == 0

    # KAFKA_TOPIC

    def test_kafka_topic_names_constants(self):
        # Arrange
        kafka_topic_cons = {
            "NONE": "NONE",
            "CREATE": "CREATE",
            "NUM_REPLICAS": 1,
            "NUM_PARTITIONS": 1,
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.KAFKA_TOPIC,
            num_values=len(kafka_topic_cons),
            expected_constants=kafka_topic_cons,
        )

    # INFERENCE_LOGGER

    def test_inference_logger_constants(self):
        # Arrange
        if_modes = {
            "MODE_NONE": "NONE",
            "MODE_ALL": "ALL",
            "MODE_MODEL_INPUTS": "MODEL_INPUTS",
            "MODE_PREDICTIONS": "PREDICTIONS",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.INFERENCE_LOGGER,
            num_values=len(if_modes),
            expected_constants=if_modes,
            prefix="MODE",
        )

    # INFERENCE_BATCHER

    def test_inference_batcher_constants(self):
        # Arrange
        if_batcher = {"ENABLED": False}

        # Assert
        self._check_added_modified_or_removed_values(
            constants.INFERENCE_BATCHER,
            num_values=len(if_batcher),
            expected_constants=if_batcher,
        )

    # DEPLOYMENT

    def test_deployment_constants(self):
        # Arrange
        depl_actions = {"ACTION_START": "START", "ACTION_STOP": "STOP"}

        # Assert
        self._check_added_modified_or_removed_values(
            constants.DEPLOYMENT,
            num_values=len(depl_actions),
            expected_constants=depl_actions,
            prefix="ACTION",
        )

    # PREDICTOR

    def test_predictor_model_server_constants(self):
        # Arrange
        model_servers = {
            "MODEL_SERVER_PYTHON": "PYTHON",
            "MODEL_SERVER_TF_SERVING": "TENSORFLOW_SERVING",
            "MODEL_SERVER_VLLM": "VLLM",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.PREDICTOR,
            num_values=len(model_servers),
            expected_constants=model_servers,
            prefix="MODEL_SERVER",
        )

    def test_predictor_serving_tool_constants(self):
        # Arrange
        serving_tools = {
            "SERVING_TOOL_DEFAULT": "DEFAULT",
            "SERVING_TOOL_KSERVE": "KSERVE",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.PREDICTOR,
            num_values=len(serving_tools),
            expected_constants=serving_tools,
            prefix="SERVING_TOOL",
        )

    # PREDICTOR_STATE

    def test_predictor_state_status_constants(self):
        # Arrange
        predictor_states = {
            "STATUS_CREATING": "Creating",
            "STATUS_CREATED": "Created",
            "STATUS_STARTING": "Starting",
            "STATUS_FAILED": "Failed",
            "STATUS_RUNNING": "Running",
            "STATUS_IDLE": "Idle",
            "STATUS_UPDATING": "Updating",
            "STATUS_STOPPING": "Stopping",
            "STATUS_STOPPED": "Stopped",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.PREDICTOR_STATE,
            num_values=len(predictor_states),
            expected_constants=predictor_states,
            prefix="STATUS",
        )

    def test_predictor_state_condition_constants(self):
        # Arrange
        predictor_states = {
            "CONDITION_TYPE_STOPPED": "STOPPED",
            "CONDITION_TYPE_SCHEDULED": "SCHEDULED",
            "CONDITION_TYPE_INITIALIZED": "INITIALIZED",
            "CONDITION_TYPE_STARTED": "STARTED",
            "CONDITION_TYPE_READY": "READY",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.PREDICTOR_STATE,
            num_values=len(predictor_states),
            expected_constants=predictor_states,
            prefix="CONDITION",
        )

    # INFERENCE_ENDPOINTS

    def test_inference_endpoints_type_constants(self):
        # Arrange
        ie_types = {
            "ENDPOINT_TYPE_KUBE_CLUSTER": "KUBE_CLUSTER",
            "ENDPOINT_TYPE_LOAD_BALANCER": "LOAD_BALANCER",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.INFERENCE_ENDPOINTS,
            num_values=len(ie_types),
            expected_constants=ie_types,
            prefix="ENDPOINT_TYPE",
        )

    def test_inference_endpoints_port_constants(self):
        # Arrange
        ie_ports = {
            "PORT_NAME_HTTP": "HTTP",
            "PORT_NAME_HTTPS": "HTTPS",
            "PORT_NAME_STATUS_PORT": "STATUS",
            "PORT_NAME_TLS": "TLS",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.INFERENCE_ENDPOINTS,
            num_values=len(ie_ports),
            expected_constants=ie_ports,
            prefix="PORT_NAME",
        )

    def test_inference_endpoints_api_protocol_constants(self):
        # Arrange
        ie_api_protocols = {
            "API_PROTOCOL_REST": "REST",
            "API_PROTOCOL_GRPC": "GRPC",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.INFERENCE_ENDPOINTS,
            num_values=len(ie_api_protocols),
            expected_constants=ie_api_protocols,
            prefix="API_PROTOCOL",
        )

    # DEPLOYABLE_COMPONENT

    def test_inference_endpoints_deployable_component_constants(self):
        # Arrange
        depl_components = {
            "PREDICTOR": "predictor",
            "TRANSFORMER": "transformer",
        }

        # Assert
        self._check_added_modified_or_removed_values(
            constants.DEPLOYABLE_COMPONENT,
            num_values=len(depl_components),
            expected_constants=depl_components,
        )

    # Auxiliary methods

    def _check_added_modified_or_removed_values(
        self, cls, num_values, expected_constants=None, prefix=None
    ):
        cname = cls.__name__ + ("." + prefix if prefix is not None else "")
        const_dict = self._get_contants_name_value_dict(cls, prefix=prefix)
        # exact constants
        if expected_constants is not None:
            # constant names
            added_cnames = const_dict.keys() - expected_constants.keys()
            removed_cnames = expected_constants.keys() - const_dict.keys()

            assert len(added_cnames) == 0, (
                f"One or more constants were added under {cname} with names {added_cnames}. "
                + "If it was intentional, please add/remove/update tests accordingly (not only in this file, "
                + "but wherever it corresponds)."
            )

            assert len(removed_cnames) == 0, (
                f"One or more constants were removed under {cname} with names {removed_cnames}. "
                + "If it was intentional, please add/remove/update tests accordingly (not only in this file, "
                + "but wherever it corresponds)."
            )

            assert const_dict.keys() == expected_constants.keys(), (
                f"One or more constants under {cname} were modified from {removed_cnames} to {added_cnames}. "
                + "If it was intentional, please add/remove/update tests accordingly (not only in this file, "
                + "but wherever it corresponds)."
            )

            # constant values
            for cname, cvalue in expected_constants.items():
                full_cname = f"{cls.__name__}.{cname}"
                assert cvalue == const_dict[cname], (
                    f"The constant {full_cname} was modified from {cvalue} to {const_dict[cname]}. "
                    + "If it was intentional, please add/remove/update tests accordingly (not only in this file, "
                    + "but wherever it corresponds)."
                )
        else:
            # number of values
            assert len(const_dict) == num_values, (
                f"A constant was added/removed under {cname}. If it was intentional, please "
                + "add/remove/update tests accordingly (not only in this file, but wherever it corresponds)."
            )

    def _get_contants_name_value_dict(self, cls, prefix=None) -> dict:
        const_dict = dict()
        for m in inspect.getmembers(cls, lambda m: not (inspect.isroutine(m))):
            n = m[0]  # name
            if (prefix is not None and n.startswith(prefix)) or (
                prefix is None and not (n.startswith("__") and n.endswith("__"))
            ):
                const_dict[n] = m[1]  # value
        return const_dict
