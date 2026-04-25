#
#   Copyright 2021 Logical Clocks AB
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
from __future__ import annotations

import contextlib
import re
import time
import warnings
from typing import TYPE_CHECKING

import humps
from hopsworks_apigen import public
from hopsworks_common import usage, util
from hopsworks_common.client.exceptions import (
    HuggingFaceImportException,
    RestAPIError,
)
from hsml.core import huggingface_api, model_api
from hsml.llm import signature as llm_signature  # noqa: F401
from hsml.python import signature as python_signature  # noqa: F401
from hsml.sklearn import signature as sklearn_signature  # noqa: F401
from hsml.tensorflow import signature as tensorflow_signature  # noqa: F401
from hsml.torch import signature as torch_signature  # noqa: F401
from tqdm.auto import tqdm


if TYPE_CHECKING:
    from hsml import model


@public
class ModelRegistry:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        project_name,
        project_id,
        model_registry_id,
        shared_registry_project_name=None,
        **kwargs,
    ):
        self._project_name = project_name
        self._project_id = project_id

        self._shared_registry_project_name = shared_registry_project_name
        self._model_registry_id = model_registry_id

        self._model_api = model_api.ModelApi()
        self._huggingface_api = huggingface_api.HuggingFaceApi()

        self._tensorflow = tensorflow_signature
        self._python = python_signature
        self._sklearn = sklearn_signature
        self._torch = torch_signature
        self._llm = llm_signature

        tensorflow_signature._mr = self
        python_signature._mr = self
        sklearn_signature._mr = self
        torch_signature._mr = self
        llm_signature._mr = self

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @public
    @usage.method_logger
    def get_model(self, name: str, version: int | None = None) -> model.Model | None:
        """Get a model entity from the model registry.

        Getting a model from the Model Registry means getting its metadata handle so you can subsequently download the model directory.

        Parameters:
            name: Name of the model to get.
            version: Version of the model to retrieve, defaults to `None` and will return the `version=1`.

        Returns:
            The model metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve model from the model registry.
        """
        if version is None:
            warnings.warn(
                f"No version provided for getting model `{name}`, defaulting to `{self.DEFAULT_VERSION}`.",
                util.VersionWarning,
                stacklevel=1,
            )
            version = self.DEFAULT_VERSION

        return self._model_api.get(
            name,
            version,
            self.model_registry_id,
            shared_registry_project_name=self.shared_registry_project_name,
        )

    @public
    @usage.method_logger
    def get_models(self, name: str) -> list[model.Model]:
        """Get all model entities from the model registry for a specified name.

        Getting all models from the Model Registry for a given name returns a list of model entities, one for each version registered under the specified model name.

        Parameters:
            name: Name of the model to get.

        Returns:
            A list of model metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve model versions from the model registry.
        """
        return self._model_api.get_models(
            name,
            self.model_registry_id,
            shared_registry_project_name=self.shared_registry_project_name,
        )

    @public
    @usage.method_logger
    def get_best_model(
        self, name: str, metric: str, direction: str
    ) -> model.Model | None:
        """Get the best performing model entity from the model registry.

        Getting the best performing model from the Model Registry means specifying in addition to the name, also a metric name corresponding to one of the keys in the training_metrics dict of the model and a direction.
        For example, to get the model version with the highest accuracy, specify metric='accuracy' and direction='max'.

        Parameters:
            name: Name of the model to get.
            metric: Name of the key in the training metrics field to compare.
            direction: 'max' to get the model entity with the highest value of the set metric, or 'min' for the lowest.

        Returns:
            The model metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If unable to retrieve model from the model registry.
        """
        model = self._model_api.get_models(
            name,
            self.model_registry_id,
            shared_registry_project_name=self.shared_registry_project_name,
            metric=metric,
            direction=direction,
        )
        if isinstance(model, list) and len(model) > 0:
            return model[0]
        return None

    @public
    @property
    def project_name(self):
        """Name of the project the registry is connected to."""
        return self._project_name

    @public
    @property
    def project_path(self):
        """Path of the project the registry is connected to."""
        return f"/Projects/{self._project_name}"

    @public
    @property
    def project_id(self):
        """Id of the project the registry is connected to."""
        return self._project_id

    @public
    @property
    def shared_registry_project_name(self):
        """Name of the project the shared model registry originates from."""
        return self._shared_registry_project_name

    @public
    @property
    def model_registry_id(self):
        """Id of the model registry."""
        return self._model_registry_id

    @public
    @usage.method_logger
    def hf_download(
        self,
        hugging_face_model_id: str,
        hf_token: str | None = None,
        selected_formats: list[str] | None = None,
        selected_variants: list[str] | None = None,
        selected_filenames: list[str] | None = None,
        timeout: int = 3600,
        poll_interval: int = 5,
    ) -> model.Model:
        """Import a model from HuggingFace into this Model Registry.

        Kicks off the asynchronous server-side download, polls until the import
        reaches a terminal status, and returns the registered model on success.

        ```python
        import hopsworks

        project = hopsworks.login()
        mr = project.get_model_registry()

        # Public model — minimal call.
        m = mr.hf_download("Qwen/Qwen2.5-0.5B")

        # Gated / private — supply an HF access token.
        m = mr.hf_download("meta-llama/Llama-3.2-1B", hf_token="hf_xxx")

        # Multi-quant repo — pick exactly one variant.
        m = mr.hf_download(
            "unsloth/Kimi-K2.6-GGUF", selected_variants=["UD-Q4_K_XL"]
        )
        ```

        Parameters:
            hugging_face_model_id: HF repo id (``owner/repo``) or full HuggingFace URL.
            hf_token: Optional HuggingFace access token; required for gated/private repos.
            selected_formats: Weight-format keys to download (``safetensors``, ``pytorch``,
                ``tensorflow``, ``flax``, ``gguf``, ``onnx``, ``openvino``, ``coreml``,
                ``tflite``). When omitted, the backend picks one with the precedence
                safetensors > pytorch > tensorflow > flax > … so multi-format repos don't
                pull every copy of the same weights.
            selected_variants: GGUF / unsloth quantisation tags to keep
                (``Q4_K_M``, ``UD-Q4_K_XL``, ``BF16``, …). Acts as a sub-filter — files
                with a quant tag are only downloaded if their tag is in the list. Files
                without a tag (root README, config, …) are unaffected.
            selected_filenames: Explicit per-file allowlist. When non-empty this
                overrides ``selected_formats`` / ``selected_variants`` and the backend
                downloads exactly these paths.
            timeout: Maximum seconds to wait for the import to reach a terminal state.
            poll_interval: Seconds between status polls.

        Returns:
            The newly registered model entity.

        Raises:
            hopsworks.client.exceptions.HuggingFaceImportException: If the backend
                fails the import with a known terminal error. The exception's
                ``error_code`` field carries the stable code (``auth_required``,
                ``model_not_found``, ``not_found_or_auth_required``, ``no_disk_space``,
                ``fetch_failed``, etc.).
            hopsworks.client.exceptions.RestAPIError: For unexpected server errors
                that are not part of the HF import error set.
        """
        # Map known synchronous backend errors (returned by POST /huggingface and
        # /inspect on the resource) to the typed exception so the caller doesn't
        # need to introspect a raw RestAPIError.
        try:
            start = self._huggingface_api.start_import(
                self.model_registry_id,
                hugging_face_model_id,
                hf_token=hf_token,
                selected_formats=selected_formats,
                selected_variants=selected_variants,
                selected_filenames=selected_filenames,
            )
        except RestAPIError as e:
            self._raise_hf_exception_from_rest_error(e)
            raise  # pragma: no cover

        job_id = (start or {}).get("jobId") if isinstance(start, dict) else None
        if not job_id:
            raise HuggingFaceImportException(
                HuggingFaceImportException.FETCH_FAILED,
                "Backend did not return a jobId for the HuggingFace import.",
            )

        print(
            f"Importing '{hugging_face_model_id}' (job {job_id}). "
            f"Polling every {poll_interval}s for up to {timeout}s."
        )

        deadline = time.time() + timeout
        last_status: dict = {}
        pbar: tqdm | None = None
        last_completed = 0
        try:
            while time.time() < deadline:
                last_status = self._huggingface_api.get_status(
                    self.model_registry_id, job_id
                )
                status = (last_status or {}).get("status")
                total = int(last_status.get("totalFiles") or 0)
                done = int(last_status.get("completedFiles") or 0)
                current_file = last_status.get("currentFile")

                # Lazy-init the progress bar once the backend reports a file count.
                if pbar is None and total > 0:
                    pbar = tqdm(total=total, unit="file", desc="HF import")
                if pbar is not None:
                    if done > last_completed:
                        pbar.update(done - last_completed)
                        last_completed = done
                    if current_file:
                        pbar.set_postfix_str(current_file, refresh=False)

                if status == "COMPLETED":
                    if pbar is not None:
                        if total > last_completed:
                            pbar.update(total - last_completed)
                        pbar.close()
                    print(
                        f"✔ HuggingFace import succeeded: "
                        f"'{hugging_face_model_id}' downloaded "
                        f"({done}/{total} files)."
                    )
                    return self._resolve_imported_model(
                        hugging_face_model_id, last_status
                    )
                if status in ("FAILED", "CANCELLED"):
                    if pbar is not None:
                        pbar.close()
                    error = last_status.get("error") or "unknown_error"
                    raise self._build_hf_exception(error)
                time.sleep(poll_interval)
        finally:
            if pbar is not None:
                pbar.close()

        # Timed out — best-effort cancel so the partial dir doesn't linger.
        with contextlib.suppress(Exception):
            self._huggingface_api.cancel(self.model_registry_id, job_id, cleanup=True)
        raise HuggingFaceImportException(
            HuggingFaceImportException.FETCH_FAILED,
            f"HuggingFace import {hugging_face_model_id} did not finish within "
            f"{timeout}s. Last status: {last_status}",
        )

    @staticmethod
    def _raise_hf_exception_from_rest_error(err: RestAPIError) -> None:
        """If a RestAPIError carries one of the known HF backend codes, re-raise.

        Promotes the error to :class:`HuggingFaceImportException`. Returns
        without raising for unknown codes so the caller can re-raise the
        original RestAPIError untouched.
        """
        body = err.response.json() if hasattr(err, "response") else {}
        if not isinstance(body, dict):
            body = {}
        code = body.get("error") or body.get("errorCode")
        message = body.get("message") or body.get("usrMsg")
        known = {
            HuggingFaceImportException.AUTH_REQUIRED,
            HuggingFaceImportException.NOT_FOUND_OR_AUTH_REQUIRED,
            HuggingFaceImportException.MODEL_NOT_FOUND,
            HuggingFaceImportException.FETCH_FAILED,
        }
        if isinstance(code, str) and code in known:
            raise HuggingFaceImportException(code, message)

    @staticmethod
    def _build_hf_exception(error: str) -> HuggingFaceImportException:
        """Map a backend ``error`` field to a typed HuggingFaceImportException.

        Some codes carry a payload separated by ``": "`` (``download_failed: foo.bin``,
        ``invalid_filename: ../etc``, ``registration_failed: ...``). Strip that for
        the ``error_code`` so callers can match on the prefix, but keep the full
        message human-readable.
        """
        # download_failed: <file>, invalid_filename: <name>, registration_failed: <text>
        m = re.match(r"^([a-z_]+)(?:\s*:\s*(.*))?$", error or "")
        code = m.group(1) if m else (error or "unknown_error")
        return HuggingFaceImportException(code, error)

    def _resolve_imported_model(
        self, hugging_face_model_id: str, status: dict
    ) -> model.Model:
        """Look up the registered Model after a successful import.

        Mirrors the backend's name sanitiser (non-alphanumerics → ``_``) to
        find the right model entry, then returns the highest-version match.
        """
        # Mirror the backend's parseModelId: drop scheme + host, drop query and
        # fragment, then keep only the first two non-empty path segments
        # ("owner/repo"). HF UI URLs like .../tree/main, .../blob/main/README.md,
        # .../resolve/main/... need their trailing segments stripped before we
        # take the repo name as the basis for the sanitised model name.
        repo = hugging_face_model_id
        for prefix in ("https://", "http://", "//"):
            if repo.startswith(prefix):
                repo = repo[len(prefix) :]
                break
        for host in ("huggingface.co/", "www.huggingface.co/", "hf.co/"):
            if repo.startswith(host):
                repo = repo[len(host) :]
                break
        repo = repo.split("?", 1)[0].split("#", 1)[0]
        kept_segments: list[str] = []
        for seg in repo.split("/"):
            if not seg:
                continue
            kept_segments.append(seg)
            if len(kept_segments) == 2:
                break
        last = kept_segments[-1] if kept_segments else ""
        sanitised = re.sub(r"[^A-Za-z0-9]", "_", last)
        # The status payload doesn't include the registered version directly,
        # so look up the latest matching name. The HF importer always registers
        # at MAX(version)+1, so the most recent get_models() entry is ours.
        candidates = self._model_api.get_models(
            sanitised,
            self.model_registry_id,
            shared_registry_project_name=self.shared_registry_project_name,
        )
        if not candidates:
            raise HuggingFaceImportException(
                "registration_failed",
                f"Import for {hugging_face_model_id} reported COMPLETED but the "
                f"model {sanitised!r} is not visible in the registry.",
            )
        # Sorted ascending by version typically; pick the highest just in case.
        return max(candidates, key=lambda m: getattr(m, "version", 0))

    @public
    @property
    def tensorflow(self):
        """Module for exporting a TensorFlow model."""
        return tensorflow_signature

    @public
    @property
    def sklearn(self):
        """Module for exporting a sklearn model."""
        return sklearn_signature

    @public
    @property
    def torch(self):
        """Module for exporting a torch model."""
        return torch_signature

    @public
    @property
    def python(self):
        """Module for exporting a generic Python model."""
        return python_signature

    @public
    @property
    def llm(self):
        """Module for exporting a Large Language Model."""
        return llm_signature

    def __repr__(self):
        project_name = (
            self._shared_registry_project_name
            if self._shared_registry_project_name is not None
            else self._project_name
        )
        return f"ModelRegistry(project: {project_name!r})"
