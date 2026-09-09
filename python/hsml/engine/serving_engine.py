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
from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
import time
import uuid

from hopsworks_common import client
from hopsworks_common.client.exceptions import ModelServingException, RestAPIError
from hopsworks_common.client.istio.utils.infer_type import InferInput
from hopsworks_common.constants import (
    DEPLOYMENT,
    MODEL_REGISTRY,
    MODEL_SERVING,
    PREDICTOR,
    PREDICTOR_STATE,
)
from hopsworks_common.constants import INFERENCE_ENDPOINTS as IE
from hopsworks_common.core import dataset_api, inode
from hsml import default_predictor, deployable_component_logs, deployment_schema
from hsml.core import serving_api
from hsml.engine import local_engine
from hsml.utils.local_paths import _ensure_dataset_dir, _resolve_serving_file
from tqdm.auto import tqdm


def _structured_prediction_error(error: RestAPIError) -> dict | None:
    """The `detail` object of a default predictor error response, or `None` for any other body."""
    try:
        body = error.response.json()
    except Exception:  # noqa: BLE001 - not JSON, so not one of ours
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict) and "code" in detail:
        return detail
    return None


def _render_chunk(chunk) -> str:
    r"""Render a single log chunk's content with a trailing newline.

    Adding a newline here (rather than relying on the upstream pipeline) lets
    ``read_logs(...)`` produce output you can pipe straight to ``grep`` or
    ``awk`` without each block getting glued to the next one. We only add the
    separator when content does not already end in ``\n`` so we never
    double-space lines that already carry their own terminator.
    """
    content = chunk.content or ""
    if content and not content.endswith("\n"):
        content += "\n"
    return content


# RESTCodes.ServingErrorCode.SCHEMA_NOT_FOUND: the id is not a published schema
_SCHEMA_NOT_FOUND = 240037


class ServingEngine:
    START_STEPS = [
        PREDICTOR_STATE.CONDITION_TYPE_STOPPED,
        PREDICTOR_STATE.CONDITION_TYPE_SCHEDULED,
        PREDICTOR_STATE.CONDITION_TYPE_INITIALIZED,
        PREDICTOR_STATE.CONDITION_TYPE_STARTED,
        PREDICTOR_STATE.CONDITION_TYPE_READY,
    ]
    STOP_STEPS = [
        PREDICTOR_STATE.CONDITION_TYPE_SCHEDULED,
        PREDICTOR_STATE.CONDITION_TYPE_STOPPED,
    ]

    def __init__(self):
        self._serving_api = serving_api.ServingApi()
        self._dataset_api = dataset_api.DatasetApi()

        self._engine = local_engine.LocalEngine()

    def _set_tag(self, deployment_instance, name: str, value):
        """Attach a name/value tag to a deployment.

        Parameters:
            deployment_instance: the deployment to tag
            name: tag name
            value: tag value
        """
        self._serving_api._set_tag(deployment_instance, name, value)

    def _delete_tag(self, deployment_instance, name: str):
        """Remove a tag from a deployment.

        Parameters:
            deployment_instance: the deployment to remove the tag from
            name: tag name to remove
        """
        self._serving_api._delete_tag(deployment_instance, name)

    def _get_tag(self, deployment_instance, name: str):
        """Get tag with a certain name.

        Parameters:
            deployment_instance: the deployment to get the tag from
            name: tag name
        """
        return self._serving_api._get_tag(deployment_instance, name)

    def _get_tags(self, deployment_instance):
        """Get all tags for a deployment.

        Parameters:
            deployment_instance: the deployment to get tags from
        """
        return self._serving_api._get_tags(deployment_instance)

    def _poll_deployment_status(
        self, deployment_instance, status: str, await_status: int, update_progress=None
    ):
        if await_status > 0:
            sleep_seconds = 5
            for _ in range(int(await_status / sleep_seconds)):
                time.sleep(sleep_seconds)
                state = deployment_instance.get_state()
                num_instances = self._get_available_instances(state)
                if update_progress is not None:
                    update_progress(state, num_instances)
                if state.status == status:
                    return state  # deployment reached desired status
                if (
                    status == PREDICTOR_STATE.STATUS_RUNNING
                    and state.status == PREDICTOR_STATE.STATUS_FAILED
                ):
                    error_msg = state.condition.reason
                    if (
                        state.condition.type
                        == PREDICTOR_STATE.CONDITION_TYPE_INITIALIZED
                        or state.condition.type
                        == PREDICTOR_STATE.CONDITION_TYPE_STARTED
                    ):
                        component = (
                            "transformer"
                            if "transformer" in state.condition.reason
                            else "predictor"
                        )
                        error_msg += (
                            ". Please, check the server logs using `.get_logs(component='"
                            + component
                            + "')`"
                        )
                    raise ModelServingException(error_msg)
            raise ModelServingException(
                "Deployment has not reached the desired status within the expected awaiting time. Check the current status by using `.get_state()`, "
                "explore the server logs using `.get_logs()` or set a higher value for await_"
                + status.lower()
            )
        return None

    def _start(self, deployment_instance, await_status: int) -> bool:
        (done, state) = self._check_status(
            deployment_instance, PREDICTOR_STATE.STATUS_RUNNING
        )

        if not done:
            min_instances = self._get_min_starting_instances(deployment_instance)
            num_steps = (len(self.START_STEPS) - 1) + min_instances
            if deployment_instance._predictor._state.condition is None:
                num_steps = min_instances  # backward compatibility
            pbar = tqdm(total=num_steps)
            pbar.set_description("Creating deployment")

            # set progress function
            def update_progress(state, num_instances):
                (progress, desc) = self._get_starting_progress(
                    pbar.n, state, num_instances
                )
                pbar.update(progress)
                if desc is not None:
                    pbar.set_description(desc)

            try:
                update_progress(state, num_instances=0)

                if state.status == PREDICTOR_STATE.STATUS_CREATING:
                    state = self._poll_deployment_status(  # wait for preparation
                        deployment_instance,
                        PREDICTOR_STATE.STATUS_CREATED,
                        await_status,
                        update_progress,
                    )

                self._serving_api._post(
                    deployment_instance, DEPLOYMENT.ACTION_START
                )  # start deployment

                state = self._poll_deployment_status(  # wait for status
                    deployment_instance,
                    PREDICTOR_STATE.STATUS_RUNNING,
                    await_status,
                    update_progress,
                )
            except RestAPIError as re:
                self._stop(deployment_instance, await_status=0)
                raise re

        # await_running=0 returns before the state is polled again
        if state is not None and state.status == PREDICTOR_STATE.STATUS_RUNNING:
            if deployment_instance.model_server == PREDICTOR.MODEL_SERVER_VLLM:
                print("Start prompting using any OpenAI API-compatible client.")
            elif not deployment_instance.has_model:
                print("Start sending requests with your HTTP client of preference.")
            else:
                print("Start making predictions by using `.predict()`")

    def _stop(self, deployment_instance, await_status: int) -> bool:
        (done, state) = self._check_status(
            deployment_instance, PREDICTOR_STATE.STATUS_STOPPED
        )
        if not done:
            num_instances = self._get_available_instances(state)
            num_steps = len(self.STOP_STEPS) + (
                deployment_instance.requested_instances
                if deployment_instance.requested_instances >= num_instances
                else num_instances
            )
            if deployment_instance._predictor._state.condition is None:
                # backward compatibility
                num_steps = self._get_min_starting_instances(deployment_instance)
            pbar = tqdm(total=num_steps)
            pbar.set_description("Preparing to stop deployment")

            # set progress function
            def update_progress(state, num_instances):
                (progress, desc) = self._get_stopping_progress(
                    pbar.total, pbar.n, state, num_instances
                )
                pbar.update(progress)
                if desc is not None:
                    pbar.set_description(desc)

            update_progress(state, num_instances)
            self._serving_api._post(
                deployment_instance, DEPLOYMENT.ACTION_STOP
            )  # stop deployment

            _ = self._poll_deployment_status(  # wait for status
                deployment_instance,
                PREDICTOR_STATE.STATUS_STOPPED,
                await_status,
                update_progress,
            )

        # free grpc channel
        deployment_instance._grpc_channel = None

    def _check_status(self, deployment_instance, desired_status):
        state = deployment_instance.get_state()
        if state is None:
            return (True, None)

        # desired status: running
        if desired_status == PREDICTOR_STATE.STATUS_RUNNING:
            if (
                state.status == PREDICTOR_STATE.STATUS_RUNNING
                or state.status == PREDICTOR_STATE.STATUS_IDLE
            ):
                print("Deployment is already running")
                return (True, state)
            if state.status == PREDICTOR_STATE.STATUS_STARTING:
                print("Deployment is already starting")
                return (True, state)
            if state.status == PREDICTOR_STATE.STATUS_UPDATING:
                print("Deployments is already running and updating")
                return (True, state)
            if state.status == PREDICTOR_STATE.STATUS_FAILED:
                print("Deployment is in failed state. " + state.condition.reason)
                return (True, state)
            if state.status == PREDICTOR_STATE.STATUS_STOPPING:
                raise ModelServingException(
                    "Deployment is stopping, please wait until it completely stops"
                )

        # desired status: stopped
        if desired_status == PREDICTOR_STATE.STATUS_STOPPED:
            if (
                state.status == PREDICTOR_STATE.STATUS_CREATING
                or state.status == PREDICTOR_STATE.STATUS_CREATED
                or state.status == PREDICTOR_STATE.STATUS_STOPPED
            ):
                print("Deployment is already stopped")
                return (True, state)
            if state.status == PREDICTOR_STATE.STATUS_STOPPING:
                print("Deployment is already stopping")
                return (True, state)

        return (False, state)

    def _get_starting_progress(self, current_step, state, num_instances):
        if state.condition is None:  # backward compatibility
            progress = num_instances - current_step
            if state.status == PREDICTOR_STATE.STATUS_RUNNING:
                return (progress, "Deployment is ready")
            return (progress, None if current_step == 0 else "Deployment is starting")

        step = self.START_STEPS.index(state.condition.type)
        if (
            state.condition.type == PREDICTOR_STATE.CONDITION_TYPE_STARTED
            or state.condition.type == PREDICTOR_STATE.CONDITION_TYPE_READY
        ):
            step += num_instances
        progress = step - current_step
        desc = None
        if state.condition.type != PREDICTOR_STATE.CONDITION_TYPE_STOPPED:
            desc = (
                state.condition.reason
                if state.status != PREDICTOR_STATE.STATUS_FAILED
                else "Deployment failed to start"
            )
        return (progress, desc)

    def _get_stopping_progress(self, total_steps, current_step, state, num_instances):
        if state.condition is None:  # backward compatibility
            progress = (total_steps - num_instances) - current_step
            if state.status == PREDICTOR_STATE.STATUS_STOPPED:
                return (progress, "Deployment is stopped")
            return (
                progress,
                None if total_steps == current_step else "Deployment is stopping",
            )

        step = 0
        if state.condition.type == PREDICTOR_STATE.CONDITION_TYPE_SCHEDULED:
            step = 1 if state.condition.status is None else 0
        elif state.condition.type == PREDICTOR_STATE.CONDITION_TYPE_STOPPED:
            num_instances = (total_steps - 2) - num_instances  # num stopped instances
            step = (
                (2 + num_instances)
                if (state.condition.status is None or state.condition.status)
                else 0
            )
        progress = step - current_step
        desc = None
        if (
            state.condition.type != PREDICTOR_STATE.CONDITION_TYPE_READY
            and state.status != PREDICTOR_STATE.STATUS_FAILED
        ):
            desc = (
                "Deployment is stopped"
                if state.status == PREDICTOR_STATE.STATUS_STOPPED
                else state.condition.reason
            )

        return (progress, desc)

    def _get_min_starting_instances(self, deployment_instance):
        min_start_instances = 1  # predictor
        if deployment_instance.transformer is not None:
            min_start_instances += 1  # transformer
        return (
            deployment_instance.requested_instances
            if deployment_instance.requested_instances >= min_start_instances
            else min_start_instances
        )

    def _get_available_instances(self, state):
        if state.status == PREDICTOR_STATE.STATUS_CREATING:
            return 0
        num_instances = state.available_predictor_instances
        if state.available_transformer_instances is not None:
            num_instances += state.available_transformer_instances
        return num_instances

    def _get_stopped_instances(self, available_instances, requested_instances):
        num_instances = requested_instances - available_instances
        return num_instances if num_instances >= 0 else 0

    def _download_files_from_hopsfs_recursive(
        self,
        from_hdfs_path: str,
        to_local_path: str,
        update_download_progress,
        n_dirs,
        n_files,
    ):
        """Download model files from a model path in hdfs, recursively."""
        count, items = self._dataset_api._list_dataset_path(
            from_hdfs_path, inode.Inode, sort_by="NAME:desc"
        )
        for entry in items:
            basename = os.path.basename(entry.path)
            if entry.dir:
                # otherwise, make a recursive call for the folder
                if (
                    basename == MODEL_SERVING.ARTIFACTS_DIR_NAME
                ):  # NOTE: Keep for backward compatibility (<4.6). Existing models during upgrade contain Artifacts folder
                    continue  # skip Artifacts subfolder
                local_folder_path = os.path.join(to_local_path, basename)
                os.mkdir(local_folder_path)
                n_dirs, n_files = self._download_files_from_hopsfs_recursive(
                    from_hdfs_path=entry.path,
                    to_local_path=local_folder_path,
                    update_download_progress=update_download_progress,
                    n_dirs=n_dirs,
                    n_files=n_files,
                )
                n_dirs += 1
                update_download_progress(n_dirs=n_dirs, n_files=n_files)
            else:
                # if it's a file, download it
                local_file_path = os.path.join(to_local_path, basename)
                self._engine._download(entry.path, local_file_path)
                n_files += 1
                update_download_progress(n_dirs=n_dirs, n_files=n_files)

        return n_dirs, n_files

    def _download_files_from_hopsfs(
        self, from_hdfs_path: str, to_local_path: str, update_download_progress
    ):
        """Download files from a deployment path in hdfs."""
        n_dirs, n_files = self._download_files_from_hopsfs_recursive(
            from_hdfs_path=from_hdfs_path,
            to_local_path=to_local_path,
            update_download_progress=update_download_progress,
            n_dirs=0,
            n_files=0,
        )
        update_download_progress(n_dirs=n_dirs, n_files=n_files, done=True)

    def _download_artifact_files(self, deployment_instance, local_path=None):
        if deployment_instance.id is None:
            raise ModelServingException(
                "Deployment is not created yet. To create the deployment use `.save()`"
            )

        if local_path is None:
            local_path = os.path.join(
                tempfile.gettempdir(),
                str(uuid.uuid4()),
                deployment_instance.name,
                str(deployment_instance.version),
            )
        os.makedirs(local_path, exist_ok=True)

        def update_download_progress(n_dirs, n_files, done=False):
            print(
                "Downloading artifact files ({} dirs, {} files)... {}".format(
                    n_dirs, n_files, "DONE" if done else ""
                ),
                end="\r",
            )

        try:
            from_hdfs_path = deployment_instance.artifact_files_path
            if from_hdfs_path.startswith("hdfs:/"):
                projects_index = from_hdfs_path.find("/Projects", 0)
                from_hdfs_path = from_hdfs_path[projects_index:]

            # backward compatibility: running deployments during upgrade contain artifact files under /Models/name/version/Artifacts folder
            # if the deployment scales out, this method is used by storage-initializer to pull the files. Therefore, we need to pull files
            # from the legacy path if the deployment has not yet been migrated
            if deployment_instance.has_model and not self._dataset_api.path_exists(
                from_hdfs_path
            ):
                # legacy artifact version path under Models dataset
                legacy_from_hdfs_path = "{}/{}/{}/{}/{}/{}/{}".format(
                    "/Projects",
                    deployment_instance.project_name,
                    MODEL_REGISTRY.MODELS_DATASET,
                    deployment_instance.model_name,
                    str(deployment_instance.model_version),
                    MODEL_SERVING.ARTIFACTS_DIR_NAME,
                    str(deployment_instance.version),
                )
                if self._dataset_api.path_exists(legacy_from_hdfs_path):
                    from_hdfs_path = legacy_from_hdfs_path

            self._download_files_from_hopsfs(
                from_hdfs_path=from_hdfs_path,
                to_local_path=local_path,
                update_download_progress=update_download_progress,
            )
        except BaseException as be:
            raise be

        return local_path

    def _download_logs(self, deployment_instance, path=None, latest=False):
        """Download the HopsFS log archives of a deployment.

        Each instance archives its own output when it exits, restarts, or is
        stopped. Archives live in the project's ``Logs`` dataset, one file per
        instance run under ``Logs/Serving/<deployment_name>/`` named
        ``<UTC yyyyMMdd-HHmmss>_<pod>_<component>.log``.

        Parameters:
            deployment_instance: The deployment whose archived logs to download.
            path: Local directory to download into; the current working directory when unset.
            latest: Download only the most recent archives instead of all of them.

        Returns:
            The local paths of the downloaded archive files.

        Raises:
            hopsworks.client.exceptions.ModelServingException: If `path` does not exist or the deployment has no archived logs.
        """
        if path is not None and not os.path.exists(path):
            raise ModelServingException(f"Path {path} does not exist")
        if path is None:
            path = os.getcwd()

        archives_path = f"{MODEL_SERVING.LOGS_DATASET}/{MODEL_SERVING.ARCHIVED_LOGS_DIR}/{deployment_instance.name}"
        no_archives_msg = (
            f"No archived logs found for deployment '{deployment_instance.name}' "
            f"under {archives_path}. Instances archive their logs when they "
            "exit, restart, or are stopped."
        )
        if not self._dataset_api.exists(archives_path):
            raise ModelServingException(no_archives_msg)

        _, items = self._dataset_api._list_dataset_path(
            archives_path, inode.Inode, sort_by="NAME:desc"
        )
        # Inode paths are absolute (/Projects/<project>/...), and download() interpolates the path
        # straight into the request, so passing them through yields a //Projects/... segment. The
        # archives are flat files directly under archives_path, so rebuilding project-relative paths
        # from the basename avoids having to strip a project prefix.
        archive_paths = [
            f"{archives_path}/{os.path.basename(entry.path)}"
            for entry in items
            if not entry.dir
        ]
        if latest and archive_paths:
            # File names start with the UTC stop timestamp, so the
            # lexicographically greatest prefix is the most recent stop.
            latest_prefix = max(
                os.path.basename(p).split("_", 1)[0] for p in archive_paths
            )
            archive_paths = [
                p
                for p in archive_paths
                if os.path.basename(p).startswith(latest_prefix + "_")
            ]
        if not archive_paths:
            raise ModelServingException(no_archives_msg)

        download_dir = os.path.join(
            path,
            f"logs-deployment-{deployment_instance.name}_{str(uuid.uuid4())[:16]}",
        )
        os.makedirs(download_dir, exist_ok=True)
        return [
            self._dataset_api.download(p, download_dir, overwrite=True)
            for p in archive_paths
        ]

    def _create(self, deployment_instance):
        try:
            self._serving_api._put(deployment_instance)
            print("Deployment created, explore it at " + deployment_instance.get_url())
        except RestAPIError as re:
            raise_err = True
            if re.error_code == ModelServingException.ERROR_CODE_DUPLICATED_ENTRY:
                msg = "Deployment with the same name already exists"
                existing_deployment = self._serving_api._get(deployment_instance.name)
                if (
                    existing_deployment.model_name == deployment_instance.model_name
                    and existing_deployment.model_version
                    == deployment_instance.model_version
                ):  # if same name and model version, retrieve existing deployment
                    print(msg + ". Getting existing deployment...")
                    print("To create a new deployment choose a different name.")
                    deployment_instance.update_from_response_json(
                        existing_deployment.to_dict()
                    )
                    raise_err = False
                else:  # otherwise, raise an exception
                    print(msg + ", but it is serving a different model version.")
                    print("Please, choose a different name.")

            if raise_err:
                raise re

        if deployment_instance.is_stopped():
            print("Before making predictions, start the deployment by using `.start()`")

    def _update(self, deployment_instance, await_update):
        state = deployment_instance.get_state()
        if state is None:
            return

        if state.status == PREDICTOR_STATE.STATUS_STARTING:
            # if starting, it cannot be updated yet
            raise ModelServingException(
                "Deployment is starting, please wait until it is running before applying changes. \n"
                "Check the current status by using `.get_state()` or explore the server logs using `.get_logs()`"
            )
        if (
            state.status == PREDICTOR_STATE.STATUS_RUNNING
            or state.status == PREDICTOR_STATE.STATUS_IDLE
            or state.status == PREDICTOR_STATE.STATUS_FAILED
        ):
            # if running, it's fine
            self._serving_api._put(deployment_instance)
            print("Deployment updated, applying changes to running instances...")
            state = self._poll_deployment_status(  # wait for status
                deployment_instance, PREDICTOR_STATE.STATUS_RUNNING, await_update
            )
            if state is not None and state.status == PREDICTOR_STATE.STATUS_RUNNING:
                print("Running instances updated successfully")
            return
        if state.status == PREDICTOR_STATE.STATUS_UPDATING:
            # if updating, it cannot be updated yet
            raise ModelServingException(
                "Deployment is updating, please wait until it is running before applying changes. \n"
                "Check the current status by using `.get_state()` or explore the server logs using `.get_logs()`"
            )
        if state.status == PREDICTOR_STATE.STATUS_STOPPING:
            # if stopping, it cannot be updated yet
            raise ModelServingException(
                "Deployment is stopping, please wait until it is stopped before applying changes"
            )
        if (
            state.status == PREDICTOR_STATE.STATUS_CREATING
            or state.status == PREDICTOR_STATE.STATUS_CREATED
            or state.status == PREDICTOR_STATE.STATUS_STOPPED
        ):
            # if stopped, it's fine
            self._serving_api._put(deployment_instance)
            print("Deployment updated, explore it at " + deployment_instance.get_url())
            return

        raise ValueError("Unknown deployment status: " + state.status)

    def _save(self, deployment_instance, await_update: int):
        # Local paths on script_file / config_file are auto-uploaded under
        # /Projects/<p>/Deployments/<name>/resources/ and rewritten to
        # HopsFS paths in-memory. On update of a deployment fetched from the
        # backend, these fields hold backend-managed references (e.g. a bare
        # basename), which are left untouched; only newly-assigned local
        # paths are re-uploaded.
        self._upload_local_serving_files(deployment_instance)
        self._upload_default_predictor_stub(deployment_instance)
        self._publish_schema(deployment_instance)

        if deployment_instance.id is None:
            self._create(deployment_instance)
            return
        self._update(deployment_instance, await_update)

    def _upload_default_predictor_stub(self, deployment_instance):
        """Give a default-predictor deployment without a script the library stub as its predictor script."""
        predictor = deployment_instance._predictor
        if not predictor.default_predictor or predictor.script_file is not None:
            return
        with tempfile.TemporaryDirectory() as tmp_dir:
            stub_path = os.path.join(tmp_dir, MODEL_SERVING.DEFAULT_PREDICTOR_SCRIPT)
            with open(stub_path, "w", encoding="utf-8") as f:
                f.write(default_predictor.STUB_SCRIPT)
            predictor.script_file = _resolve_serving_file(
                self._engine,
                deployment_instance.name,
                stub_path,
                field_name="script_file",
                subdir="predictor",
                is_update=deployment_instance.id is not None,
            )

    def _schema_dir(self, deployment_name: str) -> str:
        return (
            f"{MODEL_SERVING.DEPLOYMENTS_DATASET}/{deployment_name}/"
            f"{MODEL_SERVING.DEPLOYMENT_RESOURCES_DIR}/{MODEL_SERVING.DEPLOYMENT_SCHEMA_DIR}"
        )

    def _publish_schema(self, deployment_instance):
        """Write the pending schema as immutable, content-addressed files and point the env vars at them.

        Both happen before the `PUT`, so the revision the backend creates
        already names files that exist and never change. The JSON Schema and
        OpenAPI renderings go next to the schema for the backend's discovery
        endpoint. The batch limit configured in the predictor's env vars is
        part of the schema, so changing it publishes a new id. A transformer
        gets the id too and is named the enforcer: it sees the client request
        first, and the predictor behind it defers.
        """
        predictor = deployment_instance._predictor
        # the property loads a fetched deployment's schema by id, so a save that
        # only attaches a transformer or changes the batch limit still publishes
        schema = predictor.schema
        schema_id = schema.schema_id if schema is not None else predictor.schema_id
        if schema_id is None:
            return
        if schema is not None:
            configured = deployment_schema._configured_batch_rows(predictor.env_vars)
            if configured is not None and configured != schema.max_batch_rows:
                schema = schema._with_max_batch_rows(configured)
                predictor._schema = schema
                schema_id = schema.schema_id
            self._write_schema_documents(deployment_instance.name, schema)
        transformer = predictor.transformer
        enforcer = (
            MODEL_SERVING.SCHEMA_ENFORCER_TRANSFORMER
            if transformer is not None
            else MODEL_SERVING.SCHEMA_ENFORCER_PREDICTOR
        )
        for component in [predictor] + ([transformer] if transformer else []):
            env_vars = dict(component.env_vars or {})
            env_vars[MODEL_SERVING.DEPLOYMENT_SCHEMA_ID_ENV_VAR] = schema_id
            # each pod validates, or defers, according to its own revision
            env_vars[MODEL_SERVING.SCHEMA_ENFORCER_ENV_VAR] = enforcer
            component.env_vars = env_vars

    def _write_schema_documents(self, deployment_name: str, schema) -> None:
        schema_dir = self._schema_dir(deployment_name)
        documents = {
            f"{schema.schema_id}.json": schema.json(),
            f"{schema.schema_id}{MODEL_SERVING.DEPLOYMENT_SCHEMA_JSON_SCHEMA_SUFFIX}": json.dumps(
                schema.to_json_schema()
            ),
            f"{schema.schema_id}{MODEL_SERVING.DEPLOYMENT_SCHEMA_OPENAPI_SUFFIX}": json.dumps(
                schema.to_openapi(deployment_name)
            ),
        }
        missing = {
            name: content
            for name, content in documents.items()
            if not self._engine._dataset_api.exists(f"{schema_dir}/{name}")
        }
        if not missing:
            return
        _ensure_dataset_dir(self._engine, schema_dir)
        with tempfile.TemporaryDirectory() as tmp_dir:
            for name, content in missing.items():
                local_path = os.path.join(tmp_dir, name)
                with open(local_path, "w", encoding="utf-8") as f:
                    f.write(content)
                self._engine._upload(local_path, schema_dir, overwrite=False)

    def _read_schema(self, predictor, schema_id: str):
        """Fetch the schema a revision names, or `None` when it is gone.

        The backend endpoint needs only the `SERVING` scope; a backend without
        it answers a bare 404, and the schema file is then downloaded
        directly, which needs dataset access. A 404 naming another error, such
        as an unknown deployment, propagates.
        """
        if predictor.id is not None:
            try:
                schema_json = self._serving_api._get_schema(predictor.id, schema_id)
                return deployment_schema.DeploymentSchema.from_response_json(
                    schema_json
                )
            except RestAPIError as e:
                if e.response.status_code != 404:
                    raise
                error_code = getattr(e, "error_code", None)
                if error_code == _SCHEMA_NOT_FOUND:
                    return None
                if error_code:
                    raise
        project_name = client._get_instance()._project_name
        path = f"/Projects/{project_name}/{self._schema_dir(predictor.name)}/{schema_id}.json"
        response = self._dataset_api.read_content(path)
        if response is None:
            return None
        content = getattr(response, "content", response)
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")
        return deployment_schema.DeploymentSchema.from_response_json(
            json.loads(content)
        )

    def _remove_schemas(self, deployment_name: str):
        schema_dir = self._schema_dir(deployment_name)
        if self._engine._dataset_api.exists(schema_dir):
            self._engine._dataset_api.remove(
                f"/Projects/{client._get_instance()._project_name}/{schema_dir}"
            )

    def _upload_local_serving_files(self, deployment_instance):
        """Upload local ``script_file`` / ``config_file`` paths.

        Rewrites the in-memory fields to HopsFS paths. HopsFS / ``None`` are
        left untouched. Each role uploads to its own subdirectory to avoid
        basename collisions. On update of a persisted deployment, fields that
        are not new local paths (e.g. backend-managed references returned by
        ``get_deployment``) are passed through unchanged.
        """
        predictor = deployment_instance._predictor
        deployment_name = deployment_instance.name
        is_update = deployment_instance.id is not None

        targets = [
            (predictor, "script_file", "predictor", "script_file"),
            (predictor, "config_file", "config", "config_file"),
        ]
        if predictor.transformer is not None:
            targets.append(
                (
                    predictor.transformer,
                    "script_file",
                    "transformer",
                    "transformer.script_file",
                ),
            )

        for obj, field, subdir, field_label in targets:
            resolved = _resolve_serving_file(
                self._engine,
                deployment_name,
                getattr(obj, field),
                field_name=field_label,
                subdir=subdir,
                is_update=is_update,
            )
            setattr(obj, field, resolved)

    def _delete(self, deployment_instance, force=False):
        state = deployment_instance.get_state()
        if state is None:
            return

        if (
            not force
            and state.status != PREDICTOR_STATE.STATUS_STOPPED
            and state.status != PREDICTOR_STATE.STATUS_CREATED
        ):
            raise ModelServingException(
                "Deployment not stopped, please stop it first by using `.stop()` or check its status with .get_state()"
            )

        self._serving_api._delete(deployment_instance)
        # The backend may already have removed the deployment folder.
        with contextlib.suppress(RestAPIError):
            self._remove_schemas(deployment_instance.name)
        print("Deployment deleted successfully")

    def _get_state(self, deployment_instance):
        try:
            state = self._serving_api._get_state(deployment_instance)
        except RestAPIError as re:
            if re.error_code == ModelServingException.ERROR_CODE_SERVING_NOT_FOUND:
                raise ModelServingException("Deployment not found") from re
            raise re
        deployment_instance._predictor._set_state(state)
        return state

    def _get_logs(self, deployment_instance, component, tail):
        state = self._get_state(deployment_instance)
        if state is None:
            return None

        if state.status == PREDICTOR_STATE.STATUS_STOPPING:
            print(
                "Deployment is stopping, explore historical logs at "
                + deployment_instance.get_url()
            )
            return None
        if state.status == PREDICTOR_STATE.STATUS_STOPPED:
            print(
                "Deployment not running, explore historical logs at "
                + deployment_instance.get_url()
            )
            return None
        if state.status == PREDICTOR_STATE.STATUS_STARTING:
            print("Deployment is starting, server logs might not be ready yet")

        # Kibana is gone with the OpenSearch pipeline: the deployment page is where the live
        # reader and the log-history archives are now.
        print(
            "Explore all the logs and filters on the deployment page at "
            + deployment_instance.get_url(),
            end="\n\n",
        )

        return self._serving_api._get_logs(deployment_instance, component, tail)

    # ----- Programmatic log APIs (read_logs / tail_logs) ---------------------
    # These never print and never short-circuit on deployment state, so a
    # deployment that is starting or stopping can still be read without the
    # legacy "deployment is stopping → return None" guard hiding data.

    def _read_logs(
        self,
        deployment_instance,
        component: str = "predictor",
        tail: int = 100,
        source: str = "kubernetes",
        since: str | None = None,
        until: str | None = None,
        pod: str | None = None,
    ) -> str:
        """Return deployment logs as a single plain-text string.

        Programmatic counterpart to :py:meth:`get_logs`. Never prints; the
        caller decides what to do with the returned value.

        Parameters:
            deployment_instance: The deployment whose logs to read.
            component: Which deployment component to read (``predictor``, ``transformer``).
            tail: Maximum number of recent log entries to fetch.
            source: Log source (``kubernetes`` or ``opensearch``).
            since: ISO-8601 lower bound for log timestamps, if any.
            until: ISO-8601 upper bound for log timestamps, if any.
            pod: Specific pod name to read logs for, if any.

        Returns:
            All matching log chunks concatenated into a single string.
        """
        chunks = self._serving_api._get_logs(
            deployment_instance,
            component,
            tail,
            source=source,
            since=since,
            until=until,
            pod=pod,
        )
        return self._format_log_chunks(chunks or [])

    def _tail_logs(
        self,
        deployment_instance,
        component: str = "predictor",
        interval: float = 2.0,
        source: str = "kubernetes",
        since: str | None = "now",
        timeout: float | None = None,
        stop_on_status=None,
        pod: str | None = None,
    ):
        """Yield only newly observed log chunks as plain text.

        v1 streaming is client-side polling: each tick fetches with a moving
        ``since`` cursor and yields the portion not already seen.
        Deduplication is by (timestamp, doc_id) on the OpenSearch path (old
        backends). On the Kubernetes path lines are requested with kubelet
        timestamps and a per-pod cursor advances past what was yielded, so a
        poll transfers only the news; against an old backend that ignores
        those params, the previous tail window is kept per pod and only the
        non-overlapping suffix is emitted. The generator stops when:

        - ``timeout`` (seconds, optional) elapses,
        - ``stop_on_status`` matches the current ``deployment.get_state().status``, or
        - the caller breaks out of the loop / closes the generator.

        Continuation is not lossless in one case, and says so when it happens.
        ``since`` filters by whole seconds, so a resume re-reads the second the
        cursor sits in; a replica writing more in that second than one read can
        return can never be resumed past it. After a few such reads the cursor
        for that replica is abandoned for a fresh tail and a line marking the
        skipped range is yielded in place of the lines that were lost.

        Parameters:
            deployment_instance: The deployment whose logs to tail.
            component: Which deployment component to tail (``predictor``, ``transformer``).
            interval: Seconds between successive polls.
            source: Log source (``kubernetes`` or ``opensearch``).
            since: ISO-8601 starting cursor, or ``"now"`` for new-only.
            timeout: Stop after this many seconds, if set.
            stop_on_status: Stop when the deployment status matches this value.

        Yields:
            Log text observed since the previous yield.
        """
        # OpenSearch documents are uniquely identified by ``doc_id`` and
        # ordered by ``timestamp``; this state suffices to dedupe across
        # successive overlapping windows.
        seen_doc_ids: set[str] = set()
        last_timestamp: str | None = since if (since and since != "now") else None
        # Kubernetes path: lines are requested with kubelet timestamps and a
        # per-pod cursor advances past what was already yielded, so each poll
        # transfers only what is new. ``since`` is applied by the kubelet at
        # second granularity, so the cursor comparison below is what actually
        # dedupes the overlap it re-sends. A cursor is (timestamp, ordinal):
        # the ordinal counts the lines already delivered bearing exactly that
        # timestamp, because coarse-clock runtimes can emit several lines per
        # timestamp and a timestamp-only cursor would drop the later ones.
        # Old backends ignore both params and return unprefixed tail windows;
        # the previous window is kept per pod (keyed by instance name) for
        # overlap-suffix dedup as a fallback.
        cursor_by_pod: dict[str, tuple[str, int]] = {}
        previous_lines_by_pod: dict[str, list[str]] = {}
        # Stall recovery. ``since`` filters by whole seconds, so a resume always
        # re-fetches the whole second the cursor sits in. When that second holds
        # more than the backend's per-read byte budget, every read returns the
        # same head, the cursor cannot advance, and without this the generator
        # polls that prefix for ever and never reaches the next second. Counted
        # per container instance, since only the busy replica is stuck.
        stalled_by_instance: dict[str, int] = {}
        reseeds_by_instance: dict[str, int] = {}
        pre_reseed_cursor: dict[str, tuple[str, int]] = {}
        # Set when a stalled instance abandons its cursor; makes the next read a
        # tail read instead of another since-bounded one.
        reseed_pending = False

        # ``since="now"`` is a UX shorthand: start streaming brand-new lines
        # only. Resolved here on the first call to a real ISO-8601 timestamp
        # so subsequent polls are time-bounded the same way.
        if since == "now":
            from datetime import datetime, timezone

            last_timestamp = datetime.now(tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        deadline = (time.monotonic() + timeout) if timeout else None

        while True:
            # since is an absolute instant, so one value covers every pod: the
            # earliest cursor lags without losing, and each pod trims its own
            # overlap.
            #
            # A reseed is the exception. Dropping the stalled instance's cursor
            # is not enough to reach the newest output: with any cursor left, or
            # any resolved ``since``, the next read is still bounded by that
            # instant and returns the same capped prefix. The gap notice would
            # then claim a jump that never happened. So a reseed clears every
            # cursor and drops the resolved ``since`` for exactly one read,
            # which is what makes it a tail read. That is why the reseed is
            # coordinated rather than per-instance: one request carries one
            # ``since``, so a fresh tail cannot be fetched for one replica
            # without releasing the others, and the gap notice covers them all.
            if reseed_pending:
                since_param = None
                reseed_pending = False
            elif cursor_by_pod:
                since_param = min(ts for ts, _ in cursor_by_pod.values())
            else:
                since_param = last_timestamp
            chunks = (
                self._serving_api._get_logs(
                    deployment_instance,
                    component,
                    # Bounded first fetch; dropped by the API layer on a
                    # resume, where a tail bound would discard exactly the
                    # lines being resumed.
                    tail=200,
                    source=source,
                    since=since_param,
                    until=None,
                    pod=pod,
                    timestamps=True,
                )
                or []
            )

            new_chunks = []
            pods_in_response: set[str] = set()
            saw_kubernetes_chunk = False
            for chunk in chunks:
                if chunk.doc_id is not None:
                    if chunk.doc_id in seen_doc_ids:
                        continue
                    seen_doc_ids.add(chunk.doc_id)
                    new_chunks.append(chunk)
                    if chunk.timestamp is not None and (
                        last_timestamp is None or chunk.timestamp > last_timestamp
                    ):
                        last_timestamp = chunk.timestamp
                    continue
                saw_kubernetes_chunk = True
                # Keyed by container instance, not by pod name: a restarted
                # container starts a new log at zero, and resuming it from the
                # dead instance's cursor would skip everything it printed.
                chunk_pod = self._instance_key(chunk)
                pods_in_response.add(chunk_pod)
                if chunk.skipped or chunk.read_failed:
                    # A note, not log lines. Surfacing it as content would seed
                    # a cursor from prose.
                    continue
                content = chunk.content or ""
                new_lines = content.splitlines()
                # splitlines() erases whether the last line ended. A byte-capped
                # response can stop mid-line, and emitting that fragment commits
                # its timestamp to the cursor, so the complete line is discarded
                # as already delivered on the next read and its tail is lost for
                # good. Hold the fragment back instead: the next read re-fetches
                # its second and delivers it whole. A line larger than the whole
                # budget never completes, which is what the stall counter below
                # is for.
                last_line_complete = content.endswith(("\n", "\r"))
                before = cursor_by_pod.get(chunk_pod)
                remainder = self._advance_pod_cursor(
                    cursor_by_pod, chunk_pod, new_lines, last_line_complete
                )
                if chunk.truncated and not remainder:
                    stalled_by_instance[chunk_pod] = (
                        stalled_by_instance.get(chunk_pod, 0) + 1
                    )
                    if (
                        stalled_by_instance[chunk_pod] >= self._MAX_STALLED_READS
                        and reseeds_by_instance.get(chunk_pod, 0)
                        < self._MAX_FRUITLESS_RESEEDS
                    ):
                        # Abandon the cursor and take a fresh tail. The jump is
                        # reported: this is a gap, not a continuation, and the
                        # caller must not be told otherwise.
                        pre_reseed_cursor[chunk_pod] = cursor_by_pod.pop(
                            chunk_pod, before
                        )
                        # Every cursor goes, not just this instance's: one
                        # request carries one ``since``, so a leftover cursor
                        # would keep the next read bounded and there would be no
                        # tail to jump to.
                        cursor_by_pod.clear()
                        reseed_pending = True
                        stalled_by_instance[chunk_pod] = 0
                        reseeds_by_instance[chunk_pod] = (
                            reseeds_by_instance.get(chunk_pod, 0) + 1
                        )
                        new_chunks.append(
                            deployable_component_logs.DeployableComponentLogs(
                                instance_name=chunk.instance_name,
                                content=self._GAP_NOTICE,
                            )
                        )
                elif remainder:
                    stalled_by_instance[chunk_pod] = 0
                    # Only progress past where the reseed restarted from counts.
                    # A reseed re-delivers lines already seen, and treating that
                    # as progress reset the budget and made the reseed cap
                    # unreachable.
                    resumed = pre_reseed_cursor.get(chunk_pod)
                    if resumed is None or cursor_by_pod.get(chunk_pod, ("", 0)) > resumed:
                        reseeds_by_instance[chunk_pod] = 0
                        pre_reseed_cursor.pop(chunk_pod, None)
                if remainder is None:
                    # No kubelet timestamps: an old backend that ignored the
                    # request param. Fall back to overlap-suffix dedup of the
                    # rolling tail windows.
                    previous_lines = previous_lines_by_pod.get(chunk_pod)
                    remainder = (
                        new_lines
                        if previous_lines is None
                        else self._overlap_remainder(previous_lines, new_lines)
                    )
                    previous_lines_by_pod[chunk_pod] = new_lines
                if remainder:
                    # ``content`` is read-only on the DTO, so build a
                    # new chunk holding only the unseen lines.
                    new_chunks.append(
                        deployable_component_logs.DeployableComponentLogs(
                            instance_name=chunk.instance_name,
                            content="\n".join(remainder),
                        )
                    )

            # A replaced or scaled-away pod must not keep pinning since to its
            # last position, or every later poll re-transfers a growing history
            # for the pods that are still alive.
            if saw_kubernetes_chunk:
                for known_pod in list(cursor_by_pod):
                    if known_pod not in pods_in_response:
                        del cursor_by_pod[known_pod]
                for tracked in (
                    stalled_by_instance,
                    reseeds_by_instance,
                    pre_reseed_cursor,
                ):
                    for known_pod in list(tracked):
                        if known_pod not in pods_in_response:
                            del tracked[known_pod]

            if new_chunks:
                yield self._format_log_chunks(new_chunks)

            if stop_on_status is not None:
                state = self._get_state(deployment_instance)
                if state is not None and state.status == stop_on_status:
                    return

            if deadline is not None and time.monotonic() >= deadline:
                return

            time.sleep(interval)

    # Truncated-but-empty reads tolerated before a cursor is abandoned, and
    # reseeds allowed without real progress in between. Both mirror the browser
    # reader so the two clients report the same gaps at the same points.
    _MAX_STALLED_READS = 2
    _MAX_FRUITLESS_RESEEDS = 2

    _GAP_NOTICE = (
        "-- lines skipped: this replica writes more in one second than a single read "
        "can return; jumped to the newest output --"
    )

    @staticmethod
    def _instance_key(chunk) -> str:
        return "|".join(
            str(part) if part is not None else ""
            for part in (chunk.instance_name, chunk.pod_uid, chunk.restart_count)
        )

    # Matches the kubelet's RFC 3339 line prefix requested via timestamps=true,
    # e.g. "2026-08-07T12:34:56.123456789Z log text".
    _K8S_TS_PREFIX = re.compile(
        r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})) ?"
    )

    @classmethod
    def _advance_pod_cursor(
        cls,
        cursor_by_pod: dict[str, tuple[str, int]],
        pod: str,
        lines: list[str],
        last_line_complete: bool = True,
    ) -> list[str] | None:
        """Strip kubelet timestamp prefixes and return the lines after this pod's cursor.

        Returns ``None`` when the lines carry no timestamps, signalling the
        caller to use the rolling-window fallback instead. Timestamps compare
        lexicographically: the kubelet emits them in one fixed UTC format.
        Lines sharing the cursor's exact timestamp are skipped only up to the
        cursor's ordinal, so a runtime emitting several lines per timestamp
        does not lose the later ones. A line without a prefix (a continuation
        of a long line) follows the keep-or-drop decision of the timestamped
        line before it.
        """
        if not any(cls._K8S_TS_PREFIX.match(line) for line in lines):
            return None
        if not last_line_complete and lines:
            # The trailing fragment is neither delivered nor recorded, so the
            # cursor stays behind it and the next read returns the whole line.
            lines = lines[:-1]
            if not lines:
                return []
        cursor_ts, cursor_ordinal = cursor_by_pod.get(pod, (None, 0))
        fresh: list[str] = []
        keep_continuation = cursor_ts is None
        max_ts = cursor_ts
        max_ts_count = 0
        equal_seen = 0
        for line in lines:
            match = cls._K8S_TS_PREFIX.match(line)
            if match is None:
                if keep_continuation:
                    fresh.append(line)
                continue
            ts = match.group(1)
            if cursor_ts is not None and ts < cursor_ts:
                keep_continuation = False
            elif ts == cursor_ts:
                equal_seen += 1
                keep_continuation = equal_seen > cursor_ordinal
            else:
                keep_continuation = True
            if keep_continuation:
                fresh.append(line[match.end() :])
            if max_ts is None or ts > max_ts:
                max_ts = ts
                max_ts_count = 1
            elif ts == max_ts:
                max_ts_count += 1
        if max_ts is not None:
            if max_ts == cursor_ts:
                # A since-bounded window re-sends the whole cursor second, so the
                # in-window count subsumes the previous ordinal; max() guards the
                # case where a byte cap cut the window short of it.
                cursor_by_pod[pod] = (max_ts, max(cursor_ordinal, equal_seen))
            else:
                cursor_by_pod[pod] = (max_ts, max_ts_count)
        return fresh

    @staticmethod
    def _overlap_remainder(
        previous_lines: list[str], new_lines: list[str]
    ) -> list[str]:
        """Return the lines of a new tail window not covered by the previous one.

        Finds the largest suffix of the previous window that is a prefix of
        the new window and returns the remaining new lines. When no overlap
        exists (the window rotated fully between polls), the whole new
        window is returned.

        Parameters:
            previous_lines: Lines of the previous poll's tail window.
            new_lines: Lines of the current poll's tail window.

        Returns:
            The lines of `new_lines` that were not already observed.
        """
        for overlap in range(min(len(previous_lines), len(new_lines)), 0, -1):
            if previous_lines[-overlap:] == new_lines[:overlap]:
                return new_lines[overlap:]
        return new_lines

    @staticmethod
    def _format_log_chunks(chunks) -> str:
        r"""Merge a list of ``DeployableComponentLogs`` into one plain string.

        When more than one distinct ``instance_name`` is present, prefix each
        block with ``==> <instance> <==\n`` (tail-style). Single-instance
        responses join contents directly so ``read_logs(...)`` round-trips
        cleanly through grep/awk.
        """
        if not chunks:
            return ""
        instance_names = {c.instance_name for c in chunks if c.instance_name}
        if len(instance_names) <= 1:
            return "".join(_render_chunk(c) for c in chunks)
        # Group chronologically per instance, then concatenate.
        by_instance: dict[str, list] = {}
        for c in chunks:
            by_instance.setdefault(c.instance_name or "", []).append(c)
        parts = []
        for instance, items in by_instance.items():
            parts.append(f"==> {instance} <==\n")
            for c in items:
                parts.append(_render_chunk(c))
        return "".join(parts)

    # Model inference

    def _predict(
        self,
        deployment_instance,
        data: dict | list[InferInput],
        inputs: dict | list[dict],
        validate: bool = True,
    ):
        # validate user-provided payload
        if deployment_instance.model_server == PREDICTOR.MODEL_SERVER_VLLM:
            raise ModelServingException(
                "Inference requests to LLM deployments are not supported by the `predict` method. Please, use any OpenAI API-compatible client instead."
            )

        self._validate_inference_payload(deployment_instance.api_protocol, data, inputs)

        # build inference payload based on API protocol
        payload = self._build_inference_payload(
            deployment_instance.api_protocol, data, inputs
        )
        if validate and deployment_instance.api_protocol == IE.API_PROTOCOL_REST:
            payload = self._validate_against_schema(deployment_instance, payload)

        # if not KServe, send request through Hopsworks
        serving_tool = deployment_instance.predictor.serving_tool
        through_hopsworks = serving_tool != PREDICTOR.SERVING_TOOL_KSERVE
        try:
            return self._serving_api._send_inference_request(
                deployment_instance, payload, through_hopsworks
            )
        except RestAPIError as re:
            # The default predictor answers 404 ENTITY_NOT_FOUND with a
            # structured body; only a bare 404 means the deployment is absent.
            if _structured_prediction_error(re) is None and (
                re.response.status_code == RestAPIError.STATUS_CODE_NOT_FOUND
                or re.error_code
                == ModelServingException.ERROR_CODE_DEPLOYMENT_NOT_RUNNING
            ):
                raise ModelServingException(
                    "Deployment not created or running. If it is already created, start it by using `.start()` or check its status with .get_state()"
                ) from re

            re.args = (
                re.args[0] + "\n\n Check the model server logs by using `.get_logs()`",
            )
            raise re

    def _validate_inference_payload(
        self,
        api_protocol,
        data: dict | list[InferInput],
        inputs: dict | list[dict],
    ):
        """Validates the user-provided inference payload. Either data or inputs parameter is expected, but both cannot be provided together."""
        # check null inputs
        if data is not None and inputs is not None:
            raise ModelServingException(
                "Inference data and inputs parameters cannot be provided together."
            )
        # check data or inputs
        if data is not None:
            self._validate_inference_data(api_protocol, data)
        else:
            self._validate_inference_inputs(api_protocol, inputs)

    def _validate_inference_data(self, api_protocol, data: dict | list[InferInput]):
        """Validates the inference payload when provided through the `data` parameter.

        The data parameter contains the raw payload to be sent
        in the inference request and should have the corresponding type and format depending on the API protocol.
        For the REST protocol, data should be a dictionary. For GRPC protocol, one or more InferInput objects is expected.
        """
        if api_protocol == IE.API_PROTOCOL_REST:  # REST protocol
            if isinstance(data, dict):
                if "instances" not in data and "inputs" not in data:
                    raise ModelServingException(
                        "Inference data is missing 'instances' key."
                    )

                payload = data["instances"] if "instances" in data else data["inputs"]
                if not isinstance(payload, list):
                    raise ModelServingException(
                        "Instances field should contain a 2-dim list."
                    )
                if len(payload) == 0:
                    raise ModelServingException(
                        "Inference data cannot contain an empty list."
                    )
                # KServe V1 instances may be lists (columnar predictors) or
                # objects/dicts (custom predictors that key on field names).
                # Accept both; only reject bare scalars and empty entries.
                first = payload[0]
                if isinstance(first, list):
                    if len(first) == 0:
                        raise ModelServingException(
                            "Inference data cannot contain an empty list."
                        )
                elif isinstance(first, dict):
                    if len(first) == 0:
                        raise ModelServingException(
                            "Inference data cannot contain an empty object."
                        )
                else:
                    raise ModelServingException(
                        "Instances field should contain a list of lists or a "
                        "list of objects."
                    )
            else:  # not Dict
                if isinstance(data, InferInput) or (
                    isinstance(data, list) and isinstance(data[0], InferInput)
                ):
                    raise ModelServingException(
                        "Inference data cannot contain `InferInput` for deployments with gRPC protocol disabled. Use a dictionary instead."
                    )
                raise ModelServingException(
                    "Inference data must be a dictionary. Otherwise, use the `inputs` parameter."
                )

        else:  # gRPC protocol
            if isinstance(data, dict):
                raise ModelServingException(
                    "Inference data cannot be a dictionary for deployments with gRPC protocol enabled. "
                    "Create a `InferInput` object or use the `inputs` parameter instead."
                )
            if isinstance(data, list):
                if len(data) == 0:
                    raise ModelServingException(
                        "Inference data cannot contain an empty list."
                    )
                if not isinstance(data[0], InferInput):
                    raise ModelServingException(
                        "Inference data must contain a list of `InferInput` objects. Otherwise, use the `inputs` parameter."
                    )
            else:
                raise ModelServingException(
                    "Inference data must contain a list of `InferInput` objects for deployments with gRPC protocol enabled."
                )

    def _validate_inference_inputs(
        self, api_protocol, inputs: dict | list[dict], recursive_call=False
    ):
        """Validates the inference payload when provided through the `inputs` parameter.

        The inputs parameter contains only the payload values, which will be parsed when building the request payload.
        It can be either a dictionary or a list.
        """
        if isinstance(inputs, list):
            if len(inputs) == 0:
                raise ModelServingException("Inference inputs cannot be an empty list.")
            self._validate_inference_inputs(
                api_protocol, inputs[0], recursive_call=True
            )
        elif isinstance(inputs, InferInput):
            raise ModelServingException(
                "Inference inputs cannot be of type `InferInput`. Use the `data` parameter instead."
            )
        elif isinstance(inputs, dict):
            required_keys = ("name", "shape", "datatype", "data")
            if api_protocol == IE.API_PROTOCOL_GRPC and not all(
                k in inputs for k in required_keys
            ):
                raise ModelServingException(
                    f"Inference inputs is missing one or more keys. Required keys are [{', '.join(required_keys)}]."
                )
        elif not recursive_call or (api_protocol == IE.API_PROTOCOL_GRPC):
            # if it is the first call to this method, inputs have an invalid type/format
            # if GRPC protocol is used, only Dict type is valid for the input values
            raise ModelServingException(
                "Inference inputs type is not valid. Supported types are dictionary and list."
            )

    def _build_inference_payload(
        self,
        api_protocol,
        data: dict | list[InferInput],
        inputs: dict | list[dict],
    ):
        """Build the inference payload for an inference request.

        If the 'data' parameter is provided, this method ensures it has the correct format depending on the API protocol.
        Otherwise, if the 'inputs' parameter is provided, this method builds the correct request payload depending on the API protocol.
        """
        if data is not None:
            # data contains the raw payload (dict or InferInput), nothing needs to be changed
            return data
        # parse inputs
        return self._parse_inference_inputs(api_protocol, inputs)

    def _validate_against_schema(self, deployment_instance, payload: dict) -> dict:
        """Encode and validate the rows of a REST payload against the deployment schema, when there is one."""
        schema = deployment_instance.schema
        if schema is None or not isinstance(payload, dict):
            return payload
        key = "instances" if "instances" in payload else "inputs"
        rows = deployment_schema._encode_instances(payload.get(key))
        schema._raise_if_invalid(rows, deployment_instance.name)
        return {**payload, key: rows}

    def _parse_inference_inputs(
        self, api_protocol, inputs: dict | list[dict], recursive_call=False
    ):
        if api_protocol == IE.API_PROTOCOL_REST:  # REST protocol
            if isinstance(inputs, dict):
                # A dict is one row keyed by field name; wrapping it twice
                # would hand the predictor a one-element list instead of the row.
                data = {"instances": [inputs]}
            elif not isinstance(inputs, list):
                data = {"instances": [[inputs]]}  # wrap inputs in a 2-dim list
            else:
                data = {"instances": inputs}  # use given inputs list by default
                # check depth of the list: at least two levels are required for batch inference
                # if the content is neither a list or dict, wrap it in an additional list
                for i in inputs:
                    if not isinstance(i, list) and not isinstance(i, dict):
                        # if there are no two levels, wrap inputs in a list
                        data = {"instances": [inputs]}
                        break
        else:  # gRPC protocol
            if isinstance(inputs, dict):  # dict
                data = InferInput(
                    name=inputs["name"],
                    shape=inputs["shape"],
                    datatype=inputs["datatype"],
                    data=inputs["data"],
                    parameters=(inputs.get("parameters", None)),
                )
                if not recursive_call:
                    # if inputs is of type dict, return a singleton
                    data = [data]

            else:  # list[dict]
                data = inputs
                for index, inputs_item in enumerate(inputs):
                    data[index] = self._parse_inference_inputs(
                        api_protocol, inputs_item, recursive_call=True
                    )

        return data
