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

import os

from hopsworks_common import client
from hopsworks_common.constants import MODEL_REGISTRY, MODEL_SERVING


def _normalize_hopsfs_mount_path(path: str) -> str | None:
    """Project-relative HopsFS path for a FUSE-mounted ``path``, or ``None``.

    Recognizes ``/hopsfs/<rest>`` (per-project mount in Jobs/Jupyter pods)
    and ``/mnt/hopsfs/<project>/<rest>`` (cluster-wide mount).
    """
    if path.startswith(MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX):
        return path.replace(MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX, "", 1)
    # /mnt/hopsfs/ is rooted at HopsFS root; strip the next path segment
    # (project name) too so the result is project-relative.
    base = MODEL_REGISTRY.HOPSFS_MOUNT_PREFIX_BASE
    if path.startswith(base + "/"):
        rest = path[len(base) + 1 :]
        first_slash = rest.find("/")
        if first_slash != -1 and first_slash + 1 < len(rest):
            return rest[first_slash + 1 :]
    return None


def _resolve_serving_file(
    local_engine,
    deployment_name: str,
    path: str | None,
    field_name: str,
    subdir: str,
    overwrite: bool = True,
    is_update: bool = False,
) -> str | None:
    """Resolve a user-supplied serving file path to an absolute HopsFS path.

    Mirrors the classification used by
    [`ModelEngine._save`][hsml.engine.model_engine.ModelEngine._save]:

    1. ``None`` → return unchanged.
    2. Absolute path that exists on disk → upload (or, if the path lives
       under a HopsFS FUSE mount, rewrite to ``/Projects/<p>/...`` without
       uploading).
    3. Relative path that exists when joined with cwd → resolve to the
       absolute form and treat as step 2.
    4. ``DatasetApi.exists(path)`` is true → it's a HopsFS path (absolute,
       project-relative, or ``hdfs://``); rewrite to the absolute
       ``/Projects/<p>/...`` form and return.
    5. Otherwise, on create → raise ``ValueError``; on update
       (``is_update``) → return unchanged. A deployment fetched from the
       backend reports ``script_file`` / ``config_file`` as backend-managed
       references (often a bare basename) that are neither local files nor
       resolvable via ``DatasetApi.exists``; re-resolving them on every save
       would spuriously fail, so they are passed through untouched.

    ``subdir`` is the per-role subfolder (``"predictor"``, ``"transformer"``,
    ``"config"``) under ``Deployments/<name>/resources/``; it prevents
    basename collisions across roles.

    Extension and content validation is the backend's responsibility.
    """
    if path is None:
        return path

    project_name = client._get_instance()._project_name

    # 1. Absolute local — may be a FUSE mount.
    if os.path.isabs(path) and os.path.exists(path):
        return _resolve_local_or_mount(
            local_engine, project_name, deployment_name, path, subdir, overwrite
        )

    # 2. Relative local — resolve via cwd, then run the same
    #    FUSE-mount-vs-upload decision on the absolute form. This catches
    #    `./predictor.py` from a Jobs/Jupyter pod whose cwd lives under
    #    /hopsfs/... or /mnt/hopsfs/<project>/...
    abs_path = os.path.join(os.getcwd(), path)
    if os.path.exists(abs_path):
        return _resolve_local_or_mount(
            local_engine, project_name, deployment_name, abs_path, subdir, overwrite
        )

    # 3. HopsFS — absolute /Projects/..., project-relative, or hdfs:// URI.
    if local_engine._dataset_api.exists(path):
        if path.startswith("hdfs:/"):
            idx = path.find("/Projects")
            return path[idx:] if idx != -1 else path
        if path.startswith("/Projects/"):
            return path
        return f"/Projects/{project_name}/{path.lstrip('/')}"

    if is_update:
        # Persisted deployment: the backend returns script_file / config_file
        # as backend-managed references (often a bare basename), not a
        # resolvable local or HopsFS path. Only genuinely new local paths
        # (handled above) get re-uploaded; leave everything else untouched.
        return path

    raise ValueError(
        f"Could not find {field_name}: '{path}' in the local filesystem "
        f"or in Hopsworks File System."
    )


def _ensure_dataset_dir(local_engine, remote_dir: str) -> None:
    """Recursively create every missing segment of ``remote_dir``.

    ``DatasetApi.mkdir`` does not create parents.
    The top-level dataset (first segment) is provisioned per project by the
    platform; it is never created here.
    """
    segments = [s for s in remote_dir.split("/") if s]
    if len(segments) < 2:
        return
    ds_api = local_engine._dataset_api
    for i in range(2, len(segments) + 1):
        sub_path = "/".join(segments[:i])
        if not ds_api.exists(sub_path):
            local_engine._mkdir(sub_path)


def _resolve_local_or_mount(
    local_engine, project_name, deployment_name, abs_path, subdir, overwrite
):
    """Rewrite or upload an absolute local path and return the HopsFS path.

    If ``abs_path`` is a FUSE mount, rewrite it to ``/Projects/<p>/...``.
    Otherwise upload it under
    ``Deployments/<deployment_name>/resources/<subdir>/``.
    """
    # Normalize away `./` and `../` segments that `os.path.join(cwd, path)`
    # leaves intact. The upload path stays in OS-native form (Windows uses
    # backslashes); the FUSE-mount check needs forward slashes, so use a
    # local variant for that comparison only.
    abs_path = os.path.normpath(abs_path)
    mount_relative = _normalize_hopsfs_mount_path(abs_path.replace(os.sep, "/"))
    if mount_relative is not None:
        return f"/Projects/{project_name}/{mount_relative}"

    remote_dir = (
        f"{MODEL_SERVING.DEPLOYMENTS_DATASET}/{deployment_name}/"
        f"{MODEL_SERVING.DEPLOYMENT_RESOURCES_DIR}/{subdir}"
    )
    _ensure_dataset_dir(local_engine, remote_dir)
    local_engine._upload(abs_path, remote_dir, overwrite=overwrite)
    return f"/Projects/{project_name}/{remote_dir}/{os.path.basename(abs_path)}"
