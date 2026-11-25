"""Large task content kept beside the database rather than in it.

A task's artifacts and history live in a row of the project's online
database.
A report, an image or an uploaded file inline in that row would be carried,
base64, by every read and write of the task, and enough of them outgrow what a
row should hold.
So when tasks are kept in the database, a ``raw`` part above a size is written
to the deployment's artifact storage as the task is saved, and the stored task
carries a ``url`` part in its place, which the agent serves back to whoever may
read the task.
A second cap bounds the whole task: when many parts that are each small enough
add up past it, the largest go the same way until the task fits.

A client that streams a task still receives the bytes inline in the events;
a client that reads the task afterwards receives the URL.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from a2a.server.cluster import VersionedTaskStore


if TYPE_CHECKING:
    from collections.abc import Callable

    from a2a.types import Part, Task


_logger = logging.getLogger(__name__)

#: A ``raw`` part larger than this is kept outside the task's row.
INLINE_BYTES_ENV = "HOPSWORKS_AGENT_ARTIFACT_INLINE_BYTES"
DEFAULT_INLINE_BYTES = 256 * 1024
#: The most a stored task may hold inline, all parts together.
TASK_MAX_BYTES_ENV = "HOPSWORKS_AGENT_TASK_MAX_BYTES"
DEFAULT_TASK_MAX_BYTES = 2 * 1024 * 1024
#: A directory every replica shares (a mounted project file system), when the
#: platform or a local run names one; otherwise the project's own dataset API.
ARTIFACT_DIR_ENV = "HOPSWORKS_AGENT_ARTIFACT_DIR"
#: Where spilled parts go in the project's file system, under the deployment.
DATASET_ROOT = "Resources/agents/a2a"
#: Under the whole-task cap, a part smaller than this is left inline: moving it buys nothing.
SMALLEST_SPILLED = 1024
#: Where a spilled part is served, under the app's base URL.
ARTIFACTS_PATH = "/a2a/artifacts"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TASK_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _limit(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name) or default)
    except ValueError:
        return default
    return value if value > 0 else default


class _DirectorySpill:
    """Spilled parts as files under a directory every replica sees."""

    def __init__(self, root: str):
        self._root = Path(root)

    def _path(self, task_id: str, digest: str) -> Path:
        return self._root / task_id / digest

    def _write(self, task_id: str, digest: str, data: bytes) -> None:
        path = self._path(task_id, digest)
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_suffix(".partial")
        partial.write_bytes(data)
        partial.replace(path)

    def _read(self, task_id: str, digest: str) -> bytes | None:
        path = self._path(task_id, digest)
        return path.read_bytes() if path.is_file() else None


class _DatasetSpill:
    """Spilled parts as files in the project's file system, through the deployment's own client."""

    def __init__(self, deployment_id: str):
        self._root = f"{DATASET_ROOT}/{deployment_id}"
        self._api: Any = None

    def _dataset_api(self) -> Any:
        if self._api is None:
            from hopsworks_common import client as hopsworks_client  # noqa: PLC0415
            from hopsworks_common.core import dataset_api  # noqa: PLC0415

            try:
                hopsworks_client._get_instance()
            except Exception:  # noqa: BLE001 -- nothing connected yet: connect as the deployment
                hopsworks_client._init("hopsworks")
            self._api = dataset_api.DatasetApi()
        return self._api

    def _write(self, task_id: str, digest: str, data: bytes) -> None:
        api = self._dataset_api()
        folder = f"{self._root}/{task_id}"
        if api.exists(f"{folder}/{digest}"):
            return
        if not api.exists(folder):
            api.mkdir(folder)
        with tempfile.TemporaryDirectory() as scratch:
            local = Path(scratch) / digest
            local.write_bytes(data)
            api.upload(str(local), folder, overwrite=True)

    def _read(self, task_id: str, digest: str) -> bytes | None:
        api = self._dataset_api()
        remote = f"{self._root}/{task_id}/{digest}"
        if not api.exists(remote):
            return None
        with tempfile.TemporaryDirectory() as scratch:
            local = api.download(remote, local_path=scratch, overwrite=True)
            return Path(local).read_bytes()


def _spill_backend() -> Any | None:
    """Where spilled parts go: a shared directory when one is named, the project's files on a deployment, nowhere otherwise."""
    directory = os.environ.get(ARTIFACT_DIR_ENV)
    if directory:
        return _DirectorySpill(directory)
    deployment = os.environ.get("DEPLOYMENT_ID")
    if deployment and os.environ.get("REST_ENDPOINT"):
        return _DatasetSpill(deployment)
    return None


def _spilled_digest(url: str) -> str | None:
    """The digest a spilled part's URL names, or None for any other URL."""
    marker = ARTIFACTS_PATH + "/"
    if marker not in url:
        return None
    digest = url.rsplit("/", 1)[-1]
    return digest if _DIGEST.match(digest) else None


class _SpillingStore(VersionedTaskStore):
    """The versioned task store, with large ``raw`` parts moved out of a task as it is saved."""

    def __init__(
        self,
        inner: VersionedTaskStore,
        backend: Any,
        base_url: Callable[[], str | None],
    ):
        self._inner = inner
        self._backend = backend
        self._base_url = base_url

    @property
    def as_task_store(self) -> Any:
        """The store's plain face, for reads that need no version."""
        return self._inner.as_task_store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def save(self, task: Task, **kwargs: Any) -> Any:
        try:
            await self._spill(task)
        except Exception:  # noqa: BLE001 -- the task is saved whole rather than not at all
            _logger.warning(
                "could not move the large parts of task %s out of its row; it is "
                "saved with them inline",
                task.id,
                exc_info=True,
            )
        return await self._inner.save(task, **kwargs)

    async def get(self, task_id: str, context: Any) -> Any:
        return await self._inner.get(task_id, context)

    async def list(self, params: Any, context: Any) -> Any:
        return await self._inner.list(params, context)

    async def delete(self, task_id: str, context: Any) -> None:
        await self._inner.delete(task_id, context)

    @staticmethod
    def _raw_parts(task: Task) -> list[Part]:
        parts: list[Part] = []
        for artifact in task.artifacts:
            parts.extend(p for p in artifact.parts if p.WhichOneof("content") == "raw")
        for message in task.history:
            parts.extend(p for p in message.parts if p.WhichOneof("content") == "raw")
        if task.status.HasField("message"):
            parts.extend(
                p for p in task.status.message.parts if p.WhichOneof("content") == "raw"
            )
        return parts

    async def _spill(self, task: Task) -> None:
        """Move out every part over the inline size, then the largest left while the task is over its cap."""
        inline = _limit(INLINE_BYTES_ENV, DEFAULT_INLINE_BYTES)
        cap = _limit(TASK_MAX_BYTES_ENV, DEFAULT_TASK_MAX_BYTES)
        parts = self._raw_parts(task)
        if not parts:
            return
        for part in parts:
            if len(part.raw) > inline:
                await self._move(task, part)
        if task.ByteSize() <= cap:
            return
        remaining = sorted(
            (p for p in parts if p.WhichOneof("content") == "raw"),
            key=lambda p: len(p.raw),
            reverse=True,
        )
        for part in remaining:
            if task.ByteSize() <= cap or len(part.raw) < SMALLEST_SPILLED:
                break
            await self._move(task, part)

    async def _move(self, task: Task, part: Part) -> None:
        if not _TASK_ID.match(task.id):
            return
        data = bytes(part.raw)
        digest = hashlib.sha256(data).hexdigest()
        await asyncio.to_thread(self._backend._write, task.id, digest, data)
        base = (self._base_url() or "").rstrip("/")
        # setting the URL clears the bytes: the two are one field's alternatives
        part.url = f"{base}{ARTIFACTS_PATH}/{task.id}/{digest}"
        part.metadata.update(
            {"hopsworks": {"spilled": True, "size": len(data), "sha256": digest}}
        )


async def _read_spilled(
    backend: Any, task: Task, digest: str
) -> tuple[bytes, str, str | None] | None:
    """A spilled part of a task the caller may read: its bytes, media type and file name.

    None when the task names no such part, so a digest is never a key to
    content the task does not carry.
    """
    if not _DIGEST.match(digest) or not _TASK_ID.match(task.id):
        return None
    found: Part | None = None
    candidates: list[Part] = []
    for artifact in task.artifacts:
        candidates.extend(artifact.parts)
    for message in task.history:
        candidates.extend(message.parts)
    if task.status.HasField("message"):
        candidates.extend(task.status.message.parts)
    for part in candidates:
        if part.WhichOneof("content") == "url" and _spilled_digest(part.url) == digest:
            found = part
            break
    if found is None:
        return None
    data = await asyncio.to_thread(backend._read, task.id, digest)
    if data is None:
        return None
    return (
        data,
        (found.media_type or "application/octet-stream"),
        found.filename or None,
    )
