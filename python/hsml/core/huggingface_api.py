#
#   Copyright 2026 Hopsworks AB
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
"""HTTP client for the backend's HuggingFace import endpoints.

Mirrors the three REST routes the model-registry resource exposes:

- ``POST /modelregistries/{id}/models/huggingface``               start_import
- ``GET  /modelregistries/{id}/models/huggingface/status/{job}``  get_status
- ``DELETE /modelregistries/{id}/models/huggingface/status/{job}`` cancel
"""

from __future__ import annotations

import json

from hsml import client


class HuggingFaceApi:
    def __init__(self):
        pass

    def _base_path(self, model_registry_id: int) -> list:
        _client = client.get_instance()
        return [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_registry_id),
            "models",
            "huggingface",
        ]

    def start_import(
        self,
        model_registry_id: int,
        hugging_face_model_id: str,
        hf_token: str | None = None,
        selected_formats: list[str] | None = None,
        selected_variants: list[str] | None = None,
        selected_filenames: list[str] | None = None,
    ) -> dict:
        """Kick off an asynchronous HuggingFace import.

        The backend returns 202 with ``{"jobId": "..."}``; the actual download runs
        in the background and is polled via :py:meth:`get_status`. ``selected_*``
        arguments line up with the JSON request body the UI sends — when the
        backend sees a non-empty ``selectedFilenames`` it imports exactly those
        files and ignores the other selection args.
        """
        body: dict = {"huggingFaceModelId": hugging_face_model_id}
        if hf_token:
            body["hfToken"] = hf_token
        if selected_formats:
            body["selectedFormats"] = list(selected_formats)
        if selected_variants:
            body["selectedVariants"] = list(selected_variants)
        if selected_filenames:
            body["selectedFilenames"] = list(selected_filenames)

        _client = client.get_instance()
        return _client._send_request(
            "POST",
            self._base_path(model_registry_id),
            headers={"content-type": "application/json"},
            data=json.dumps(body),
        )

    def get_status(self, model_registry_id: int, job_id: str) -> dict:
        """Poll the status of a running or finished HuggingFace import job."""
        _client = client.get_instance()
        return _client._send_request(
            "GET",
            self._base_path(model_registry_id) + ["status", job_id],
        )

    def cancel(
        self, model_registry_id: int, job_id: str, cleanup: bool = False
    ) -> None:
        """Cancel an import. If ``cleanup`` is true, the partial directory is removed."""
        _client = client.get_instance()
        query_params = {"cleanup": "true"} if cleanup else None
        _client._send_request(
            "DELETE",
            self._base_path(model_registry_id) + ["status", job_id],
            query_params=query_params,
        )
