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
#

import json
from typing import Dict, List, Union

from hsml import (
    client,
    decorators,
    deployable_component_logs,
    deployment,
    inference_endpoint,
    predictor_state,
)
from hsml.client.istio.utils.infer_type import (
    InferInput,
    InferOutput,
    InferRequest,
)
from hsml.constants import ARTIFACT_VERSION
from hsml.constants import INFERENCE_ENDPOINTS as IE


class ServingApi:
    def __init__(self):
        pass

    @decorators.catch_not_found("hsml.deployment.Deployment", fallback_return=None)
    def get_by_id(self, id: int):
        """Get the metadata of a deployment with a certain id.

        :param id: id of the deployment
        :type id: int
        :return: deployment metadata object
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            str(id),
        ]

        deployment_json = _client._send_request("GET", path_params)
        deployment_instance = deployment.Deployment.from_response_json(deployment_json)
        deployment_instance.model_registry_id = _client._project_id
        return deployment_instance

    @decorators.catch_not_found("hsml.deployment.Deployment", fallback_return=None)
    def get(self, name: str):
        """Get the metadata of a deployment with a certain name.

        :param name: name of the deployment
        :type name: str
        :return: deployment metadata object
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {"name": name}

        deployment_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        deployment_instance = deployment.Deployment.from_response_json(deployment_json)
        deployment_instance.model_registry_id = _client._project_id
        return deployment_instance

    def get_all(self, model_name: str = None, status: str = None):
        """Get the metadata of all deployments.

        :return: model metadata objects
        :rtype: List[Deployment]
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {
            "model": model_name,
            "status": status.capitalize() if status is not None else None,
        }
        deployments_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        deployment_instances = deployment.Deployment.from_response_json(
            deployments_json
        )
        for deployment_instance in deployment_instances:
            deployment_instance.model_registry_id = _client._project_id
        return deployment_instances

    def get_inference_endpoints(self):
        """Get inference endpoints.

        :return: inference endpoints for the current project.
        :rtype: List[InferenceEndpoint]
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "inference", "endpoints"]
        endpoints_json = _client._send_request("GET", path_params)
        return inference_endpoint.InferenceEndpoint.from_response_json(endpoints_json)

    def put(self, deployment_instance):
        """Save deployment metadata to model serving.

        :param deployment_instance: metadata object of deployment to be saved
        :type deployment_instance: Deployment
        :return: updated metadata object of the deployment
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        headers = {"content-type": "application/json"}

        if deployment_instance.artifact_version == ARTIFACT_VERSION.CREATE:
            deployment_instance.artifact_version = -1

        deployment_instance = deployment_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                data=deployment_instance.json(),
            )
        )
        deployment_instance.model_registry_id = _client._project_id
        return deployment_instance

    def post(self, deployment_instance, action: str):
        """Perform an action on the deployment

        :param action: action to perform on the deployment (i.e., START or STOP)
        :type action: str
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
        ]
        query_params = {"action": action}
        return _client._send_request("POST", path_params, query_params=query_params)

    def delete(self, deployment_instance):
        """Delete the deployment and metadata.

        :param deployment_instance: metadata object of the deployment to delete
        :type deployment_instance: Deployment
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
        ]
        return _client._send_request("DELETE", path_params)

    def get_state(self, deployment_instance):
        """Get the state of a given deployment

        :param deployment_instance: metadata object of the deployment to get state of
        :type deployment_instance: Deployment
        :return: predictor state
        :rtype: PredictorState
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            str(deployment_instance.id),
        ]
        deployment_json = _client._send_request("GET", path_params)
        return predictor_state.PredictorState.from_response_json(deployment_json)

    def reset_changes(self, deployment_instance):
        """Reset a given deployment to the original values in the Hopsworks instance

        :param deployment_instance: metadata object of the deployment to reset
        :type deployment_instance: Deployment
        :return: deployment with reset values
        :rtype: Deployment
        """

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "serving"]
        query_params = {"name": deployment_instance.name}
        deployment_json = _client._send_request(
            "GET", path_params, query_params=query_params
        )
        deployment_aux = deployment_instance.update_from_response_json(deployment_json)
        # TODO: remove when model_registry_id is added properly to deployments in backend
        deployment_aux.model_registry_id = _client._project_id
        return deployment_aux

    def send_inference_request(
        self,
        deployment_instance,
        data: Union[Dict, List[InferInput]],
        through_hopsworks: bool = False,
    ) -> Union[Dict, List[InferOutput]]:
        """Send inference requests to a deployment with a certain id

        :param deployment_instance: metadata object of the deployment to be used for the prediction
        :type deployment_instance: Deployment
        :param data: payload of the inference request
        :type data: Union[Dict, List[InferInput]]
        :param through_hopsworks: whether to send the inference request through the Hopsworks REST API or not
        :type through_hopsworks: bool
        :return: inference response
        :rtype: Union[Dict, List[InferOutput]]
        """
        if deployment_instance.api_protocol == IE.API_PROTOCOL_REST:
            # REST protocol, use hopsworks or istio client
            return self._send_inference_request_via_rest_protocol(
                deployment_instance, data, through_hopsworks
            )
        else:
            # gRPC protocol, use the deployment grpc channel
            return self._send_inference_request_via_grpc_protocol(
                deployment_instance, data
            )

    def _send_inference_request_via_rest_protocol(
        self,
        deployment_instance,
        data: Dict,
        through_hopsworks: bool = False,
    ) -> Dict:
        headers = {"content-type": "application/json"}
        if through_hopsworks:
            # use Hopsworks client
            _client = client.get_instance()
            path_params = self._get_hopsworks_inference_path(
                _client._project_id, deployment_instance
            )
            with_base_path_params = True
        else:
            _client = client.istio.get_instance()
            if _client is not None:
                # use istio client
                path_params = self._get_istio_inference_path(deployment_instance)
                # - add host header
                headers["host"] = self._get_inference_request_host_header(
                    deployment_instance.project_namespace,
                    deployment_instance.name,
                    client.get_knative_domain(),
                )
                with_base_path_params = False
            else:
                # fallback to Hopsworks client
                _client = client.get_instance()
                path_params = self._get_hopsworks_inference_path(
                    _client._project_id, deployment_instance
                )
                with_base_path_params = True

        # send inference request
        return _client._send_request(
            "POST",
            path_params,
            headers=headers,
            data=json.dumps(data),
            with_base_path_params=with_base_path_params,
        )

    def _send_inference_request_via_grpc_protocol(
        self, deployment_instance, data: List[InferInput]
    ) -> List[InferOutput]:
        # get grpc channel
        if deployment_instance._grpc_channel is None:
            # The gRPC channel is lazily initialized. The first call to deployment.predict() will initialize
            # the channel, which will be reused in all following calls on the same deployment object.
            # The gRPC channel is freed when calling deployment.stop()
            print("Initializing gRPC channel...")
            deployment_instance._grpc_channel = self._create_grpc_channel(
                deployment_instance
            )
        # build an infer request
        request = InferRequest(
            infer_inputs=data,
            model_name=deployment_instance.name,
        )

        # send infer request
        infer_response = deployment_instance._grpc_channel.infer(
            infer_request=request, headers=None
        )

        # extract infer outputs
        return infer_response.outputs

    def _create_grpc_channel(self, deployment_instance):
        _client = client.istio.get_instance()
        service_hostname = self._get_inference_request_host_header(
            deployment_instance.project_namespace,
            deployment_instance.name,
            client.get_knative_domain(),
        )
        return _client._create_grpc_channel(service_hostname)

    def is_kserve_installed(self):
        """Check if kserve is installed

        :return: whether kserve is installed
        :rtype: bool
        """

        _client = client.get_instance()
        path_params = ["variables", "kube_kserve_installed"]
        kserve_installed = _client._send_request("GET", path_params)
        return (
            "successMessage" in kserve_installed
            and kserve_installed["successMessage"] == "true"
        )

    def get_num_instances_limits(self):
        """Get number of instances limits for model serving"""

        _client = client.get_instance()

        path_params = ["variables", "kube_serving_min_num_instances"]
        min_instances = _client._send_request("GET", path_params)

        path_params = ["variables", "kube_serving_max_num_instances"]
        max_instances = _client._send_request("GET", path_params)

        return [
            int(min_instances["successMessage"]),
            int(max_instances["successMessage"]),
        ]

    def get_knative_domain(self):
        """Get the domain used by knative"""

        _client = client.get_instance()

        path_params = ["variables", "kube_knative_domain_name"]
        domain = _client._send_request("GET", path_params)

        return domain["successMessage"]

    def get_logs(self, deployment_instance, component, tail):
        """Get the logs of a deployment

        :param deployment_instance: metadata object of the deployment to get logs from
        :type deployment_instance: Deployment
        :param component: deployment component (e.g., predictor or transformer)
        :type component: str
        :param tail: number of tailing lines to retrieve
        :type tail: int
        :return: deployment logs
        :rtype: DeployableComponentLogs
        """

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "serving",
            deployment_instance.id,
            "logs",
        ]
        query_params = {"component": component, "tail": tail}
        return deployable_component_logs.DeployableComponentLogs.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    def _get_inference_request_host_header(
        self, project_namespace: str, deployment_name: str, domain: str
    ):
        return "{}.{}.{}".format(deployment_name, project_namespace, domain).lower()

    def _get_hopsworks_inference_path(self, project_id: int, deployment_instance):
        return [
            "project",
            project_id,
            "inference",
            "models",
            deployment_instance.name + ":predict",
        ]

    def _get_istio_inference_path(self, deployment_instance):
        return ["v1", "models", deployment_instance.name + ":predict"]
