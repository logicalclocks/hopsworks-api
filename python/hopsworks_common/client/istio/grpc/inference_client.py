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

import grpc
from hopsworks_apigen import also_available_as
from hopsworks_common.client.istio.grpc.proto.grpc_predict_v2_pb2_grpc import (
    GRPCInferenceServiceStub,
)
from hopsworks_common.client.istio.utils.infer_type import InferRequest, InferResponse


@also_available_as("hsml.client.istio.grpc.inference_client._PathPrefixInterceptor")
class _PathPrefixInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Interceptor that prepends a path prefix to gRPC method calls for path-based routing."""

    def __init__(self, path_prefix: str):
        self._path_prefix = path_prefix

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_method = self._path_prefix + client_call_details.method
        new_details = _ClientCallDetails(
            method=new_method,
            timeout=client_call_details.timeout,
            metadata=client_call_details.metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
            compression=client_call_details.compression,
        )
        return continuation(new_details, request)

@also_available_as("hsml.client.istio.grpc.inference_client._ClientCallDetails")
class _ClientCallDetails(
    grpc.ClientCallDetails,
):
    """Implementation of grpc.ClientCallDetails for use by interceptors."""

    def __init__(
        self, method, timeout, metadata, credentials, wait_for_ready, compression
    ):
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression

@also_available_as("hsml.client.istio.grpc.inference_client.GRPCInferenceServerClient")
class GRPCInferenceServerClient:
    def __init__(
        self,
        url,
        serving_api_key,
        path_prefix=None,
    ):
        channel_opt = [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ]

        # Authentication is done via API Key in the Authorization header
        self._channel = grpc.insecure_channel(url, options=channel_opt)

        # Apply path prefix interceptor for path-based routing
        if path_prefix:
            self._channel = grpc.intercept_channel(
                self._channel, _PathPrefixInterceptor(path_prefix)
            )

        self._client_stub = GRPCInferenceServiceStub(self._channel)
        self._serving_api_key = serving_api_key

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        """It is called during object garbage collection."""
        self.close()

    def close(self):
        """Close the client. Future calls to server will result in an Error."""
        self._channel.close()

    def infer(self, infer_request: InferRequest, headers=None, client_timeout=None):
        headers = {} if headers is None else headers
        headers["authorization"] = "ApiKey " + self._serving_api_key
        metadata = headers.items()

        # convert the InferRequest to a ModelInferRequest message
        request = infer_request.to_grpc()

        try:
            # send request
            model_infer_response = self._client_stub.ModelInfer(
                request=request, metadata=metadata, timeout=client_timeout
            )
        except grpc.RpcError as rpc_error:
            raise rpc_error

        # convert back the ModelInferResponse message to InferResponse
        return InferResponse.from_grpc(model_infer_response)
