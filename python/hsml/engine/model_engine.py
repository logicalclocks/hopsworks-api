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

import errno
import json
import logging
import os
import shutil
import tempfile
import time
from typing import TYPE_CHECKING

from hopsworks_common import client, constants, util
from hopsworks_common.client.exceptions import ModelRegistryException, RestAPIError
from hopsworks_common.core import dataset_api, inode
from hsml.core import model_api
from hsml.engine import local_engine
from tqdm.auto import tqdm


if TYPE_CHECKING:
    from hsml.core import explicit_provenance


_logger = logging.getLogger(__name__)


def model_cache_base_dirs():
    """Ordered list of base directories where downloaded models are cached.

    Downloads are attempted in this order, falling back to the next location
    when one is unusable (e.g. disk full or no write permission):

    1. The system temp area (``/tmp/hopsworks/models`` by default).
    2. A ``cache`` directory under the Hopsworks home directory (``~/.hopsworks``).
    3. A ``.hopsworks_cache`` directory under the current working directory.

    Returns:
        List of absolute base directory paths, de-duplicated and order-preserving.
    """
    home = os.path.expanduser("~")
    bases = [
        constants.MODEL_REGISTRY.MODEL_CACHE_DIR_DEFAULT,
        os.path.join(home, ".hopsworks", "cache", "models"),
        os.path.join(os.getcwd(), ".hopsworks_cache", "models"),
    ]

    # De-duplicate while preserving order; e.g. when CWD is itself under /tmp.
    seen = set()
    unique_bases = []
    for base in bases:
        abs_base = os.path.abspath(base)
        if abs_base not in seen:
            seen.add(abs_base)
            unique_bases.append(abs_base)
    return unique_bases


class ModelEngine:
    def __init__(self):
        self._model_api = model_api.ModelApi()
        self._dataset_api = dataset_api.DatasetApi()

        self._engine = local_engine.LocalEngine()

    def _poll_model_available(self, model_instance, await_registration):
        if await_registration > 0:
            model_registry_id = model_instance.model_registry_id
            sleep_seconds = 5
            for _ in range(int(await_registration / sleep_seconds)):
                try:
                    time.sleep(sleep_seconds)
                    model_meta = self._model_api.get(
                        model_instance.name,
                        model_instance.version,
                        model_registry_id,
                        model_instance.shared_registry_project_name,
                    )
                    if model_meta is not None:
                        return model_meta
                except RestAPIError as e:
                    if e.response.status_code != 404:
                        raise e
            print(
                "Model not available during polling, set a higher value for await_registration to wait longer."
            )
        return None

    def _upload_additional_resources(self, model_instance):
        # Stage these files in the system temp dir, not the CWD, so saving a
        # model never leaves input_example.json / model_schema.json behind in
        # the user's working directory.
        if (
            model_instance._input_example is not None
            or model_instance._model_schema is not None
        ):
            with tempfile.TemporaryDirectory() as tmp_dir:
                if model_instance._input_example is not None:
                    input_example_path = os.path.join(tmp_dir, "input_example.json")
                    input_example = util.input_example_to_json(
                        model_instance._input_example
                    )

                    with open(input_example_path, "w+") as out:
                        json.dump(input_example, out, cls=util.NumpyEncoder)

                    self._engine._upload(input_example_path, model_instance.version_path)
                    model_instance.input_example = None
                if model_instance._model_schema is not None:
                    model_schema_path = os.path.join(tmp_dir, "model_schema.json")
                    model_schema = model_instance._model_schema

                    with open(model_schema_path, "w+") as out:
                        out.write(model_schema.json())

                    self._engine._upload(model_schema_path, model_instance.version_path)
                    model_instance.model_schema = None
        return model_instance

    def _copy_or_move_hopsfs_model_item(
        self, from_path, to_model_files_path, keep_original_files
    ):
        """Copy or move model item from a hdfs path to the model version folder in the Models dataset. It works with files and folders."""
        to_hdfs_path = os.path.join(to_model_files_path, os.path.basename(from_path))
        if keep_original_files:
            self._engine._copy(from_path, to_hdfs_path)
        else:
            self._engine._move(from_path, to_hdfs_path)

    def _copy_or_move_hopsfs_model(
        self,
        from_hdfs_model_path,
        to_model_files_path,
        keep_original_files,
        update_upload_progress,
    ):
        """Copy or move model files from a hdfs path to the model version folder in the Models dataset."""
        # Strip hdfs prefix
        if from_hdfs_model_path.startswith("hdfs:/"):
            projects_index = from_hdfs_model_path.find("/Projects", 0)
            from_hdfs_model_path = from_hdfs_model_path[projects_index:]

        n_dirs, n_files = 0, 0

        model_path_resp = self._dataset_api.get(from_hdfs_model_path)
        model_path_attr = model_path_resp["attributes"]
        if (
            "datasetType" in model_path_resp
            and model_path_resp["datasetType"] == "DATASET"
        ):  # This is needed to avoid a user exporting for example "Resources" from wiping the dataset
            raise AssertionError(
                "It is disallowed to export a root dataset path."
                " Move the model to a sub-folder and try again."
            )
        if model_path_attr.get("dir", False):
            # if path is a directory, iterate of the directory content
            count, files = self._dataset_api._list_dataset_path(
                from_hdfs_model_path, inode.Inode, sort_by="NAME:desc"
            )
            for entry in files:
                self._copy_or_move_hopsfs_model_item(
                    entry.path, to_model_files_path, keep_original_files
                )
                if entry.dir:
                    n_dirs += 1
                else:
                    n_files += 1
                update_upload_progress(n_dirs=n_dirs, n_files=n_files)
        else:
            # if path is a file, copy/move it
            self._copy_or_move_hopsfs_model_item(
                model_path_attr["path"], to_model_files_path, keep_original_files
            )
            n_files += 1
            update_upload_progress(n_dirs=n_dirs, n_files=n_files)

    def _normalize_hopsfs_mount_path(self, model_path):
        if model_path.startswith(constants.MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX):
            return model_path.replace(
                constants.MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX, "", 1
            )
        # /mnt/hopsfs/ is rooted at /Projects/, so the on-disk path is
        # /mnt/hopsfs/<projectName>/<rest> — strip the project segment too
        # so the result is project-relative, matching the /hopsfs/ branch.
        base = constants.MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX_BASE
        if model_path.startswith(base + "/"):
            rest = model_path[len(base) + 1 :]
            first_slash = rest.find("/")
            if first_slash != -1 and first_slash + 1 < len(rest):
                return rest[first_slash + 1 :]
        return None

    def _download_model_from_hopsfs_recursive(
        self,
        from_hdfs_model_path: str,
        to_local_path: str,
        update_download_progress,
        n_dirs,
        n_files,
    ):
        """Download model files from a model path in hdfs, recursively."""
        count, files = self._dataset_api._list_dataset_path(
            from_hdfs_model_path, inode.Inode, sort_by="NAME:desc"
        )
        for entry in files:
            basename = os.path.basename(entry.path)
            if entry.dir:
                # otherwise, make a recursive call for the folder
                if (
                    basename == constants.MODEL_SERVING.ARTIFACTS_DIR_NAME
                ):  # NOTE: Keep for backward compatibility (<4.6). Existing models during upgrade contain Artifacts folder
                    continue  # skip Artifacts subfolder
                local_folder_path = os.path.join(to_local_path, basename)
                os.mkdir(local_folder_path)
                n_dirs, n_files = self._download_model_from_hopsfs_recursive(
                    from_hdfs_model_path=entry.path,
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

    def _download_model_from_hopsfs(
        self, from_hdfs_model_path: str, to_local_path: str, update_download_progress
    ):
        """Download model files from a model path in hdfs."""
        n_dirs, n_files = self._download_model_from_hopsfs_recursive(
            from_hdfs_model_path=from_hdfs_model_path,
            to_local_path=to_local_path,
            update_download_progress=update_download_progress,
            n_dirs=0,
            n_files=0,
        )
        update_download_progress(n_dirs=n_dirs, n_files=n_files, done=True)

    def _upload_local_model(
        self,
        from_local_model_path,
        to_model_files_path,
        update_upload_progress,
        upload_configuration=None,
    ):
        """Copy or upload model files from a local path to the model files folder in the Models dataset."""
        n_dirs, n_files = 0, 0
        if os.path.isdir(from_local_model_path):
            # if path is a dir, upload files and folders iteratively
            for root, dirs, files in os.walk(from_local_model_path):
                # os.walk(local_model_path), where local_model_path is expected to be an absolute path
                # - root is the absolute path of the directory being walked
                # - dirs is the list of directory names present in the root dir
                # - files is the list of file names present in the root dir
                # we need to replace the local path prefix with the hdfs path prefix (i.e., /srv/hops/....../root with /Projects/.../)
                remote_base_path = root.replace(
                    from_local_model_path, to_model_files_path
                ).replace(os.sep, "/")
                for d_name in dirs:
                    self._engine._mkdir(remote_base_path + "/" + d_name)
                    n_dirs += 1
                    update_upload_progress(n_dirs, n_files)
                for f_name in files:
                    self._engine._upload(
                        root + "/" + f_name,
                        remote_base_path,
                        upload_configuration=upload_configuration,
                    )
                    n_files += 1
                    update_upload_progress(n_dirs, n_files)
        else:
            # if path is a file, upload file
            self._engine._upload(
                from_local_model_path,
                to_model_files_path,
                upload_configuration=upload_configuration,
            )
            n_files += 1
            update_upload_progress(n_dirs, n_files)

    def _save_model_from_local_or_hopsfs_mount(
        self,
        model_instance,
        model_path,
        keep_original_files,
        update_upload_progress,
        upload_configuration=None,
    ):
        """Save model files from a local path. The local path can be on hopsfs mount."""
        # check hopsfs mount
        from_hdfs_model_path = self._normalize_hopsfs_mount_path(model_path)
        if from_hdfs_model_path is not None:
            self._copy_or_move_hopsfs_model(
                from_hdfs_model_path=from_hdfs_model_path,
                to_model_files_path=model_instance.model_files_path,
                keep_original_files=keep_original_files,
                update_upload_progress=update_upload_progress,
            )
        else:
            self._upload_local_model(
                from_local_model_path=model_path,
                to_model_files_path=model_instance.model_files_path,
                update_upload_progress=update_upload_progress,
                upload_configuration=upload_configuration,
            )

    def _set_model_version(
        self, model_instance, dataset_models_root_path, dataset_model_path
    ):
        # Set model version if not defined
        if model_instance._version is None:
            current_highest_version = 0
            files = []
            offset = 0
            limit = 1000
            count, items = self._dataset_api._list_dataset_path(
                dataset_model_path,
                inode.Inode,
                offset=offset,
                limit=limit,
                sort_by="NAME:desc",
            )
            while items:
                files = files + items
                offset += limit
                count, items = self._dataset_api._list_dataset_path(
                    dataset_model_path,
                    inode.Inode,
                    offset=offset,
                    limit=limit,
                    sort_by="NAME:desc",
                )
            for item in files:
                _, file_name = os.path.split(item.path)
                # Get highest version folder
                try:
                    try:
                        current_version = int(file_name)
                    except ValueError:
                        continue
                    if current_version > current_highest_version:
                        current_highest_version = current_version
                except RestAPIError:
                    pass

            current_highest_version += 1

            # Get highest available model metadata version
            # This makes sure we skip corrupt versions where the model folder is deleted manually but the backend metadata is still there
            model = self._model_api.get(
                model_instance._name,
                current_highest_version,
                model_instance.model_registry_id,
            )
            while model:
                current_highest_version += 1
                model = self._model_api.get(
                    model_instance._name,
                    current_highest_version,
                    model_instance.model_registry_id,
                )

            model_instance._version = current_highest_version
        else:
            model_backend_object_exists = (
                self._model_api.get(
                    model_instance._name,
                    model_instance._version,
                    model_instance.model_registry_id,
                )
                is not None
            )
            model_version_folder_exists = self._dataset_api.path_exists(
                dataset_models_root_path
                + "/"
                + model_instance._name
                + "/"
                + str(model_instance._version)
            )

            # Perform validations to handle possible inconsistency between db and filesystem
            if model_backend_object_exists and not model_version_folder_exists:
                raise ModelRegistryException(
                    f"Model with name {model_instance._name} and version {model_instance._version} looks to be corrupt as the version is registered but there is no Models/{model_instance._name}/{model_instance._version} folder in the filesystem. Delete this version using Model.delete() or the UI and try to export this version again."
                )
            if not model_backend_object_exists and model_version_folder_exists:
                raise ModelRegistryException(
                    f"Model with name {model_instance._name} and version {model_instance._version} looks to be corrupt as the version exists in the filesystem but it is not registered. To proceed, please delete the Models/{model_instance._name}/{model_instance._version} folder manually and try to save this model again."
                )
            if model_backend_object_exists and model_version_folder_exists:
                raise ModelRegistryException(
                    f"Model with name {model_instance._name} and version {model_instance._version} already exists, please select another version."
                )

        return model_instance

    def _build_resource_path(self, model_instance, artifact):
        return f"{model_instance.version_path}/{artifact}"

    def _save(
        self,
        model_instance,
        model_path,
        await_registration=480,
        keep_original_files=False,
        upload_configuration=None,
    ):
        _client = client.get_instance()

        is_shared_registry = model_instance.shared_registry_project_name is not None

        if is_shared_registry:
            dataset_models_root_path = f"{model_instance.shared_registry_project_name}::{constants.MODEL_REGISTRY.MODELS_DATASET}"
            model_instance._project_name = model_instance.shared_registry_project_name
        else:
            dataset_models_root_path = constants.MODEL_REGISTRY.MODELS_DATASET
            model_instance._project_name = _client._project_name

        util.validate_metrics(model_instance.training_metrics)
        util.validate_model_name(model_instance._name)

        if not self._dataset_api.path_exists(dataset_models_root_path):
            raise AssertionError(
                f"{dataset_models_root_path} dataset does not exist in this project. Please enable the Serving service or create it manually."
            )

        # Create /Models/{model_instance._name} folder
        dataset_model_name_path = dataset_models_root_path + "/" + model_instance._name
        if not self._dataset_api.path_exists(dataset_model_name_path):
            self._engine._mkdir(dataset_model_name_path)

        model_instance = self._set_model_version(
            model_instance, dataset_models_root_path, dataset_model_name_path
        )

        # Attach model summary xattr to /Models/{model_instance._name}/{model_instance._version}
        model_query_params = {}

        if "HOPSWORKS_JOB_NAME" in os.environ:
            model_query_params["jobName"] = os.environ["HOPSWORKS_JOB_NAME"]
        elif "HOPSWORKS_KERNEL_ID" in os.environ:
            model_query_params["kernelId"] = os.environ["HOPSWORKS_KERNEL_ID"]

        pbar = tqdm(
            [
                {"id": 0, "desc": "Creating model folder"},
                {"id": 1, "desc": "Uploading model files"},
                {"id": 2, "desc": "Uploading input_example and model_schema"},
                {"id": 3, "desc": "Registering model"},
                {"id": 4, "desc": "Waiting for model registration"},
                {"id": 5, "desc": "Model export complete"},
            ]
        )

        for step in pbar:
            try:
                pbar.set_description("{}".format(step["desc"]))
                if step["id"] == 0:
                    # Create folders
                    self._engine._mkdir(model_instance.version_path)
                    self._engine._mkdir(model_instance.model_files_path)
                if step["id"] == 1:
                    if not keep_original_files:
                        print(
                            f"Moving model files from '{model_path}' to the model registry... "
                            "This is the default behavior. Set keep_original_files=True to copy files instead."
                        )

                    def update_upload_progress(n_dirs=0, n_files=0, step=step):
                        pbar.set_description(
                            "{} ({} dirs, {} files)".format(
                                step["desc"], n_dirs, n_files
                            )
                        )

                    update_upload_progress(n_dirs=0, n_files=0)

                    # Upload Model files from local path to /Models/{model_instance._name}/{model_instance._version}/Files
                    # check local absolute
                    if os.path.isabs(model_path) and os.path.exists(model_path):
                        self._save_model_from_local_or_hopsfs_mount(
                            model_instance=model_instance,
                            model_path=model_path,
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                            upload_configuration=upload_configuration,
                        )
                    # check local relative
                    elif os.path.exists(
                        os.path.join(os.getcwd(), model_path)
                    ):  # check local relative
                        self._save_model_from_local_or_hopsfs_mount(
                            model_instance=model_instance,
                            model_path=os.path.join(os.getcwd(), model_path),
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                            upload_configuration=upload_configuration,
                        )
                    # check project relative
                    elif self._dataset_api.path_exists(
                        model_path
                    ):  # check hdfs relative and absolute
                        self._copy_or_move_hopsfs_model(
                            from_hdfs_model_path=model_path,
                            to_model_files_path=model_instance.model_files_path,
                            keep_original_files=keep_original_files,
                            update_upload_progress=update_upload_progress,
                        )
                    else:
                        raise OSError(
                            f"Could not find path {model_path} in the local filesystem or in Hopsworks File System"
                        )
                if step["id"] == 2:
                    model_instance = self._upload_additional_resources(model_instance)
                if step["id"] == 3:
                    model_instance = self._model_api.put(
                        model_instance, model_query_params
                    )
                if step["id"] == 4:
                    model_instance = self._poll_model_available(
                        model_instance, await_registration
                    )
                if step["id"] == 5:
                    pass
            except BaseException as be:
                self._dataset_api.rm(model_instance.version_path)
                raise be

        print("Model created, explore it at " + model_instance.get_url())

        return model_instance

    def _download(self, model_instance, local_path=None):
        # User provided an explicit path - honour it exactly, no cache fallback.
        if local_path is not None:
            try:
                self._prepare_download_dir(local_path)
                self._download_model_files(model_instance, local_path)
            except OSError as err:
                self._explain_dir_error(local_path, err, has_fallback=False)
                raise
            return local_path

        candidates = self._model_cache_paths(model_instance)

        # Reuse an already-complete download from any cache location. The cache
        # path is keyed by the backend model id, so a recreated model version
        # never matches a stale directory and is re-downloaded automatically.
        for cache_path in candidates:
            if self._is_cached_model_valid(cache_path):
                print(
                    f"Using cached model files at '{cache_path}'. "
                    f"Pass local_path or call Model.clear_cache(...) to force a "
                    f"fresh download."
                )
                return cache_path

        # Otherwise download into the first cache location that works, falling
        # back to the next one on disk-full or permission errors.
        last_error = None
        for index, cache_path in enumerate(candidates):
            has_fallback = index < len(candidates) - 1
            try:
                self._prepare_download_dir(
                    cache_path, clean_existing=True, restrict_perms=True
                )
                self._download_model_files(model_instance, cache_path)
                self._create_completion_marker(cache_path)
                # Drop cached copies of older ids for this same version to keep
                # the cache from growing every time the version is recreated.
                self._prune_other_model_ids(cache_path)
                return cache_path
            except OSError as err:
                last_error = err
                self._explain_dir_error(cache_path, err, has_fallback=has_fallback)
                # Remove the partial download so a stale dir is never mistaken
                # for a valid cache on a later run.
                self._cleanup_partial_download(cache_path)
                if not has_fallback:
                    raise

        # Defensive: only reachable if candidates is empty, which should not
        # happen because model_cache_base_dirs always returns at least one entry.
        if last_error is not None:
            raise last_error
        raise RuntimeError("No cache location available to download the model.")

    def _model_cache_paths(self, model_instance):
        """Per-model cache paths, one for each base in `model_cache_base_dirs`.

        The backend model `id` is the leaf segment so that a model version which
        is deleted and recreated (same name and version, new `id`) resolves to a
        different directory and is re-downloaded automatically, without the user
        having to invalidate the cache.

        Args:
            model_instance: Model instance with project_name, name, version, and id.

        Returns:
            List of ``{base}/{project}/{model}/{version}/{id}`` paths in fallback order.
        """
        project_name = model_instance.project_name
        if project_name is None:
            raise ValueError(
                "Cannot cache model without project_name. "
                "Please provide local_path explicitly."
            )
        if model_instance.id is None:
            raise ValueError(
                "Cannot cache model without id. Please provide local_path explicitly."
            )
        model_name = model_instance.name
        version = str(model_instance.version)
        model_id = str(model_instance.id)

        return [
            os.path.join(base, project_name, model_name, version, model_id)
            for base in model_cache_base_dirs()
        ]

    def _prepare_download_dir(self, path, clean_existing=False, restrict_perms=False):
        """Create the download directory and confirm it is writable.

        Args:
            path: Directory to download into.
            clean_existing: If True, remove any pre-existing directory first so
                stale or partial contents cannot masquerade as a complete
                download. Only safe for cache dirs we own, not user-supplied
                paths that may hold unrelated files.
            restrict_perms: If True, create the directory with owner-only (0o700)
                permissions so other local users cannot read or tamper with it.

        Raises:
            OSError: If the directory cannot be created or written to. The
                errno (e.g. ENOSPC, EACCES) lets the caller decide whether to
                fall back to another location.
        """
        if clean_existing and os.path.exists(path):
            # Refuse to touch a pre-existing cache dir owned by another user;
            # fall back to a location we control instead.
            if not self._is_path_owned_by_current_user(path):
                raise PermissionError(
                    errno.EACCES,
                    "Refusing to reuse cache directory owned by another user",
                    path,
                )
            shutil.rmtree(path)

        mode = 0o700 if restrict_perms else 0o777
        os.makedirs(path, mode=mode, exist_ok=True)
        if restrict_perms:
            # makedirs honours umask, so tighten the leaf explicitly.
            try:
                os.chmod(path, 0o700)
            except OSError as chmod_err:
                _logger.debug(
                    "Could not restrict permissions on %s: %s", path, chmod_err
                )

        if not os.access(path, os.W_OK):
            raise PermissionError(
                errno.EACCES, "Download directory is not writable", path
            )

    def _is_path_owned_by_current_user(self, path):
        """Whether `path` is owned by the current OS user.

        On platforms without POSIX ownership (e.g. Windows) this returns True,
        since there is no uid to compare against.
        """
        getuid = getattr(os, "getuid", None)
        if getuid is None:
            return True
        try:
            return os.stat(path).st_uid == getuid()
        except OSError:
            return False

    def _explain_dir_error(self, path, error, has_fallback):
        """Print an actionable message for a failed download into `path`.

        Args:
            path: Directory the download was attempted in.
            error: The OSError raised while preparing or writing the directory.
            has_fallback: Whether another cache location will be tried next.
        """
        errnum = getattr(error, "errno", None)
        next_step = (
            "Retrying in the next location..."
            if has_fallback
            else "No fallback locations remain."
        )

        if errnum == errno.ENOSPC:
            message = (
                f"Not enough disk space to download the model into '{path}'. "
                f"Free up space, or pass an explicit local_path with more room. "
                f"{next_step}"
            )
        elif errnum in (errno.EACCES, errno.EPERM) or isinstance(
            error, PermissionError
        ):
            owner_hint = self._dir_permission_hint(path)
            message = (
                f"Permission denied writing the model into '{path}'. {owner_hint} "
                f"Check directory ownership and permissions (e.g. 'ls -ld {path}'), "
                f"or pass an explicit local_path you can write to. {next_step}"
            )
        elif errnum == errno.EROFS:
            message = (
                f"Cannot write the model into '{path}': the filesystem is "
                f"read-only. Pass an explicit local_path on a writable "
                f"filesystem. {next_step}"
            )
        else:
            message = (
                f"Failed to download the model into '{path}': {error}. {next_step}"
            )

        _logger.warning(message)
        print(message)

    def _dir_permission_hint(self, path):
        """Best-effort hint about which existing parent directory blocks writes."""
        probe = path
        while probe and not os.path.exists(probe):
            parent = os.path.dirname(probe)
            if parent == probe:
                break
            probe = parent
        if probe and os.path.exists(probe) and not os.access(probe, os.W_OK):
            return f"The existing path '{probe}' is not writable by the current user."
        return ""

    def _cleanup_partial_download(self, path):
        """Remove a partially downloaded cache dir, ignoring cleanup failures."""
        if not os.path.exists(path):
            return
        # Never delete a directory owned by another user (e.g. a planted cache
        # path on a shared host); only clean up what we created.
        if not self._is_path_owned_by_current_user(path):
            return
        try:
            shutil.rmtree(path)
        except OSError as cleanup_err:
            _logger.debug(
                "Could not clean up partial download %s: %s", path, cleanup_err
            )

    def _prune_other_model_ids(self, cache_path):
        """Remove cached dirs for other ids of the same model version.

        `cache_path` is ``.../{version}/{id}``; this deletes sibling ``{id}``
        directories left over from earlier (recreated) versions of the same
        model version, keeping only the freshly downloaded one. Cleanup is
        best-effort and only touches directories owned by the current user.
        """
        version_dir = os.path.dirname(cache_path)
        keep = os.path.basename(cache_path)
        try:
            siblings = os.listdir(version_dir)
        except OSError:
            return
        for entry in siblings:
            if entry == keep:
                continue
            sibling_path = os.path.join(version_dir, entry)
            if os.path.isdir(sibling_path) and self._is_path_owned_by_current_user(
                sibling_path
            ):
                try:
                    shutil.rmtree(sibling_path)
                except OSError as prune_err:
                    _logger.debug(
                        "Could not prune stale cache dir %s: %s",
                        sibling_path,
                        prune_err,
                    )

    def _is_cached_model_valid(self, cache_path):
        """Check if a cached model is valid and complete.

        Args:
            cache_path: Path to the cached model directory

        Returns:
            bool: True if cache is valid, False otherwise
        """
        # Check if directory exists
        if not os.path.exists(cache_path):
            return False

        # On a shared host the cache path under /tmp is predictable, so another
        # local user could pre-create it with planted model files. Only trust a
        # cache directory owned by the current user.
        if not self._is_path_owned_by_current_user(cache_path):
            _logger.warning(
                "Ignoring cache directory '%s': not owned by the current user.",
                cache_path,
            )
            return False

        # Check if directory is not empty
        try:
            if not os.listdir(cache_path):
                return False
        except OSError:
            return False

        # Check for completion marker
        marker_path = os.path.join(cache_path, ".download_complete")
        return os.path.exists(marker_path)

    def _create_completion_marker(self, cache_path):
        """Create a marker file to indicate download completion.

        Args:
            cache_path: Path to the cached model directory
        """
        marker_path = os.path.join(cache_path, ".download_complete")
        with open(marker_path, "w") as f:
            f.write(f"Downloaded at: {time.time()}\n")

    def _download_model_files(self, model_instance, local_path):
        """Download model files from HDFS to local path.

        Args:
            model_instance: Model instance to download
            local_path: Local directory path where model files will be downloaded
        """

        def update_download_progress(n_dirs, n_files, done=False):
            print(
                "Downloading model artifact ({} dirs, {} files)... {}".format(
                    n_dirs, n_files, "DONE" if done else ""
                ),
                end="\r",
            )

        try:
            from_hdfs_model_path = model_instance.model_files_path
            if from_hdfs_model_path.startswith("hdfs:/"):
                projects_index = from_hdfs_model_path.find("/Projects", 0)
                from_hdfs_model_path = from_hdfs_model_path[projects_index:]

            if not self._dataset_api.path_exists(from_hdfs_model_path):
                # if Files directory doesn't exist, download files from the model version
                # directory for backwards compatibility with the old model file structure
                from_hdfs_model_path = model_instance.version_path

            self._download_model_from_hopsfs(
                from_hdfs_model_path=from_hdfs_model_path,
                to_local_path=local_path,
                update_download_progress=update_download_progress,
            )
        except BaseException as be:
            raise be

    def _read_file(self, model_instance, resource):
        hdfs_resource_path = self._build_resource_path(
            model_instance, os.path.basename(resource)
        )
        if self._dataset_api.path_exists(hdfs_resource_path):
            try:
                resource = os.path.basename(resource)
                tmp_dir = tempfile.TemporaryDirectory()
                local_resource_path = os.path.join(tmp_dir.name, resource)
                self._engine._download(
                    hdfs_resource_path,
                    local_resource_path,
                )
                with open(local_resource_path) as f:
                    return f.read()
            finally:
                if tmp_dir is not None and os.path.exists(tmp_dir.name):
                    tmp_dir.cleanup()
        return None

    def _read_json(self, model_instance, resource):
        hdfs_resource_path = self._build_resource_path(model_instance, resource)
        if self._dataset_api.path_exists(hdfs_resource_path):
            try:
                tmp_dir = tempfile.TemporaryDirectory()
                local_resource_path = os.path.join(tmp_dir.name, resource)
                self._engine._download(
                    hdfs_resource_path,
                    local_resource_path,
                )
                with open(local_resource_path, "rb") as f:
                    return json.loads(f.read())
            finally:
                if tmp_dir is not None and os.path.exists(tmp_dir.name):
                    tmp_dir.cleanup()
        return None

    def _delete(self, model_instance):
        self._engine._delete(model_instance)

    def _set_tag(self, model_instance, name, value):
        """Attach a name/value tag to a model.

        Parameters:
            model_instance: the model to tag
            name: tag name
            value: tag value
        """
        self._model_api.set_tag(model_instance, name, value)

    def _delete_tag(self, model_instance, name):
        """Remove a tag from a model.

        Parameters:
            model_instance: the model to remove the tag from
            name: tag name to remove
        """
        self._model_api.delete_tag(model_instance, name)

    def _get_tag(self, model_instance, name):
        """Get tag with a certain name.

        Parameters:
            model_instance: the model to get the tag from
            name: tag name
        """
        return self._model_api.get_tag(model_instance, name)

    def _get_tags(self, model_instance):
        """Get all tags for a model.

        Parameters:
            model_instance: the model to get tags from
        """
        return self._model_api.get_tags(model_instance)

    def _get_feature_view_provenance(
        self, model_instance
    ) -> explicit_provenance.Links | None:
        """Get the parent feature view of this model, based on explicit provenance.

        These feature views can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is
        returned.

        Parameters:
            model_instance: Metadata object of model.

        Returns:
            The feature view used to generate this model.
        """
        return self._model_api.get_feature_view_provenance(model_instance)

    def _get_training_dataset_provenance(
        self, model_instance
    ) -> explicit_provenance.Links | None:
        """Get the parent training dataset of this model, based on explicit provenance.

        These training datasets can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is
        returned.

        Parameters:
            model_instance: Metadata object of model.

        Returns:
            The training dataset used to generate this model.
        """
        return self._model_api.get_training_dataset_provenance(model_instance)
