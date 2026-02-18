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
from hopsworks_common.client.istio.grpc.proto.grpc_predict_v2_pb2_grpc import (
    GRPCInferenceServiceStub,
)
from hopsworks_common.client.istio.utils.infer_type import InferRequest, InferResponse


class GRPCInferenceServerClient:
    def __init__(
        self,
        url,
        serving_api_key,
        channel_args=None,
    ):
        if channel_args is not None:
            channel_opt = channel_args
        else:
            channel_opt = [
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ]

        # Authentication is done via API Key in the Authorization header
        self._channel = grpc.insecure_channel(url, options=channel_opt)
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
