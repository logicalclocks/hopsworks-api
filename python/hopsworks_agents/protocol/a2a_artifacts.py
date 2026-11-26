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
import json
import logging
import os
import re
import shutil
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from a2a.server.cluster import VersionedTaskStore

from .a2a_store import SPILL_TASK_ID


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
#: How many truncated tasks a replica remembers the note of.
TRUNCATED_LIMIT = 2000
#: Where a spilled part is served, under the app's base URL.
ARTIFACTS_PATH = "/a2a/artifacts"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _safe_task_id(task_id: str) -> bool:
    """Whether a task id can name a directory of its own: one path segment, not starting with a dot."""
    return bool(SPILL_TASK_ID.match(task_id))


def _limit(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name) or default)
    except ValueError:
        return default
    return value if value > 0 else default


class _DirectorySpill:
    """Spilled parts as files under a directory every replica sees.

    The directory is the author's or a local run's, which the platform does
    not know of: a deleted task's files are filed for removal as
    ``'directory'`` rows, which only a replica of the deployment retries.
    """

    #: Who can clean the files: a replica with this directory, never the platform.
    location = "directory"

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

    def _remove(self, task_id: str) -> None:
        """Remove a task's files; a task that has none is done already."""
        if not _safe_task_id(task_id):
            return
        folder = self._root / task_id
        if folder.exists():
            shutil.rmtree(folder)


class _DatasetSpill:
    """Spilled parts as files in the project's file system, through the deployment's own client.

    A deleted task's files are filed for removal in the database
    (``agent_a2a_spill_cleanup``) as ``'project'`` rows, so the platform
    removes them when no replica gets to it.
    """

    #: Who can clean the files: a replica, or the platform.
    location = "project"

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

    def _remove(self, task_id: str) -> None:
        """Remove a task's directory; a task that has none is done already."""
        if not _safe_task_id(task_id):
            return
        api = self._dataset_api()
        folder = f"{self._root}/{task_id}"
        if api.exists(folder):
            api.remove(folder)


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


def _json_size(value: Any) -> int:
    """The length of a value as the store writes it: ``json.dumps``, as SQLAlchemy's JSON type does."""
    return len(json.dumps(value))


def _stored_size(task: Task) -> int:
    """How much a task takes in its row: the JSON text of the columns the store writes."""
    from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

    size = _json_size(MessageToDict(task.status))
    size += _json_size([MessageToDict(a) for a in task.artifacts])
    size += _json_size([MessageToDict(m) for m in task.history])
    if task.metadata.fields:
        size += _json_size(MessageToDict(task.metadata))
    return size


def _part_size(part: Part) -> int:
    from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

    return _json_size(MessageToDict(part))


#: The kinds of part content that can be moved out or omitted.
_CONTENT = ("raw", "text", "data")
#: What a task holds when nothing of its content fits.
_EMPTIED_NOTE = (
    "[The content of this task was larger than a stored task may hold and was "
    "dropped; a caller that streamed the task received it.]"
)


class _SpillingStore(VersionedTaskStore):
    """The versioned task store, with a task made to fit its row as it is saved.

    Large ``raw`` parts are moved to the artifact storage, and the stored task
    carries their URL.
    The cap is measured on what the store writes: the JSON text of the task's
    columns, the note of what was dropped included.
    A task that is still over its cap gives up, in this order, what costs the
    reader least: its largest remaining parts, text and ``data`` included,
    moved out the same way; then its oldest history messages; then the
    content of its largest parts, replaced by a line that says so; then the
    metadata beyond the ``hopsworks`` key, on the task, its messages, its
    artifacts and their parts.
    What was dropped rather than moved is said in the task's
    ``metadata.hopsworks.truncated``.
    A task that still does not fit is stored with its artifacts and history
    emptied and its status message replaced by a short note; if even that
    does not fit, the save fails.
    The cap therefore holds for the task as stored, whatever its content and
    whether or not the artifact storage could be reached.
    """

    def __init__(
        self,
        inner: VersionedTaskStore,
        backend: Any,
        base_url: Callable[[], str | None],
        after_save: Callable[[Task], Any] | None = None,
        cleanups: Any = None,
    ):
        self._inner = inner
        self._backend = backend
        self._base_url = base_url
        self._after_save = after_save
        #: Where a deleted task's files are filed for removal, when they need a row.
        self._cleanups = cleanups
        # What was dropped from each task so far, by task id. A later status
        # update replaces the task's ``hopsworks`` metadata with its own
        # facts, so the note is put back at every save of the task.
        self._truncated: OrderedDict[str, dict[str, Any]] = OrderedDict()

    @property
    def as_task_store(self) -> Any:
        """The store's plain face, for reads that need no version."""
        return self._inner.as_task_store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def save(self, task: Task, **kwargs: Any) -> Any:
        await self._fit(task)
        version = await self._inner.save(task, **kwargs)
        if self._after_save is not None:
            # what the state now in the store means for who holds the task
            await self._after_save(task)
        return version

    async def get(self, task_id: str, context: Any) -> Any:
        return await self._inner.get(task_id, context)

    async def list(self, params: Any, context: Any) -> Any:
        return await self._inner.list(params, context)

    async def delete(self, task_id: str, context: Any) -> None:
        """Delete the task, and its spilled files with it.

        Only a task the caller may delete has its files touched; the store
        says whether it had one.
        """
        deleted = await self._inner.delete(task_id, context)
        if deleted is False or self._backend is None:
            return
        await self._clean(task_id)

    async def _clean(self, task_id: str) -> None:
        """Remove a deleted task's files; a removal that fails is left to the sweep, which retries the row filed for it."""
        try:
            await asyncio.to_thread(self._backend._remove, task_id)
        except Exception:  # noqa: BLE001 -- the sweep tries again
            _logger.warning(
                "could not remove the files of deleted task %s%s",
                task_id,
                "; they are removed later" if self._cleanups is not None else "",
                exc_info=True,
            )
            if self._cleanups is not None:
                try:
                    await self._cleanups._failed(task_id)
                except Exception:  # noqa: BLE001 -- the row is there all the same
                    _logger.debug("could not count the failure", exc_info=True)
            return
        if self._cleanups is not None:
            try:
                await self._cleanups._done(task_id)
            except Exception:  # noqa: BLE001 -- the sweep finds the files gone
                _logger.debug(
                    "could not clear the cleanup of %s", task_id, exc_info=True
                )

    @staticmethod
    def _content_parts(task: Task, kinds: tuple[str, ...]) -> list[Part]:
        """The parts of the task's artifacts and history that hold content of these kinds."""
        parts: list[Part] = []
        for artifact in task.artifacts:
            parts.extend(p for p in artifact.parts if p.WhichOneof("content") in kinds)
        for message in task.history:
            parts.extend(p for p in message.parts if p.WhichOneof("content") in kinds)
        return parts

    @staticmethod
    def _data_bytes(part: Part) -> bytes:
        from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

        return json.dumps(MessageToDict(part.data)).encode()

    @staticmethod
    def _size(part: Part) -> int:
        kind = part.WhichOneof("content")
        if kind == "raw":
            return len(part.raw)
        if kind == "text":
            return len(part.text.encode())
        if kind == "data":
            return len(_SpillingStore._data_bytes(part))
        return 0

    async def _fit(self, task: Task) -> None:
        """Bring the task under its cap: move what can be moved, then drop what must be dropped.

        Raises:
            a2a.utils.errors.InternalError: Not even the task without its content fits.
        """
        try:
            await self._fit_once(task)
        finally:
            self._note(task)

    def _note(self, task: Task) -> None:
        """Say on the task what was dropped from it, also after a status update replaced its metadata."""
        from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

        dropped = self._truncated.get(task.id)
        if dropped is None:
            return
        existing = MessageToDict(task.metadata).get("hopsworks")
        facts = dict(existing) if isinstance(existing, dict) else {}
        facts.update(dropped)
        task.metadata.update({"hopsworks": facts})

    async def _fit_once(self, task: Task) -> None:
        inline = _limit(INLINE_BYTES_ENV, DEFAULT_INLINE_BYTES)
        cap = _limit(TASK_MAX_BYTES_ENV, DEFAULT_TASK_MAX_BYTES)
        # what an earlier save dropped is said again, and counted in the size
        self._note(task)
        storage = self._backend is not None and _safe_task_id(task.id)
        if storage:
            # a file is kept beside the row whenever it is large, cap or no cap
            for part in self._content_parts(task, ("raw",)):
                if len(part.raw) > inline:
                    storage = await self._move(task, part)
                    if not storage:
                        break
        if _stored_size(task) <= cap:
            return
        if storage:
            movable = sorted(
                self._content_parts(task, _CONTENT), key=self._size, reverse=True
            )
            size = _stored_size(task)
            for part in movable:
                if size <= cap or self._size(part) < SMALLEST_SPILLED:
                    break
                before = _part_size(part)
                if not await self._move(task, part):
                    break
                size -= before - _part_size(part)
                if size <= cap:
                    size = _stored_size(task)
        if _stored_size(task) <= cap:
            return
        dropped = self._truncate(task, cap)
        _logger.warning(
            "task %s is larger than a stored task may be (%d bytes): %s",
            task.id,
            cap,
            dropped,
        )

    async def _move(self, task: Task, part: Part) -> bool:
        """Write a part's content to the artifact storage and leave its URL; False when the storage failed."""
        kind = part.WhichOneof("content")
        if kind == "raw":
            data = bytes(part.raw)
        elif kind == "data":
            data = self._data_bytes(part)
        else:
            data = part.text.encode()
        digest = hashlib.sha256(data).hexdigest()
        try:
            await asyncio.to_thread(self._backend._write, task.id, digest, data)
        except Exception:  # noqa: BLE001 -- the cap is then kept by dropping instead
            _logger.warning(
                "could not move a part of task %s to the artifact storage",
                task.id,
                exc_info=True,
            )
            return False
        base = (self._base_url() or "").rstrip("/")
        if kind == "text" and not part.media_type:
            part.media_type = "text/plain; charset=utf-8"
        elif kind == "data" and not part.media_type:
            part.media_type = "application/json"
        # setting the URL clears the content: they are one field's alternatives
        part.url = f"{base}{ARTIFACTS_PATH}/{task.id}/{digest}"
        part.metadata.update(
            {"hopsworks": {"spilled": True, "size": len(data), "sha256": digest}}
        )
        return True

    @staticmethod
    def _omit(part: Part) -> None:
        size = _SpillingStore._size(part)
        part.text = f"[omitted: {size} bytes, more than a stored task may hold]"
        part.metadata.update({"hopsworks": {"omitted": True, "size": size}})

    @staticmethod
    def _structs(task: Task) -> list[Any]:
        """Every metadata Struct of the task: its own, its messages', its artifacts' and their parts'."""
        messages = list(task.history)
        if task.status.HasField("message"):
            messages.append(task.status.message)
        structs = [task.metadata]
        for artifact in task.artifacts:
            structs.append(artifact.metadata)
            structs.extend(part.metadata for part in artifact.parts)
        for message in messages:
            structs.append(message.metadata)
            structs.extend(part.metadata for part in message.parts)
        return structs

    def _measure(self, task: Task, base: dict[str, Any], counts: dict[str, Any]) -> int:
        """The task's stored size with the note of what was dropped so far on it."""
        dropped = dict(base)
        for key, value in counts.items():
            if isinstance(value, bool):
                dropped[key] = value or bool(dropped.get(key))
            else:
                dropped[key] = int(dropped.get(key) or 0) + value
        self._truncated[task.id] = dropped
        self._truncated.move_to_end(task.id)
        while len(self._truncated) > TRUNCATED_LIMIT:
            self._truncated.popitem(last=False)
        self._note(task)
        return _stored_size(task)

    def _truncate(self, task: Task, cap: int) -> str:
        """Drop content until the task fits, oldest and least needed first; what was dropped, in words.

        Raises:
            a2a.utils.errors.InternalError: Not even the task without its content fits.
        """
        from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

        base = self._truncated.pop(task.id, None) or {
            "truncated": True,
            "dropped_history_messages": 0,
            "omitted_parts": 0,
        }
        counts: dict[str, Any] = {
            "dropped_history_messages": 0,
            "omitted_parts": 0,
        }
        size = self._measure(task, base, counts)
        # 1. the oldest history; the last message is what the turn was asked,
        # kept while anything else can go
        if size > cap and len(task.history) > 1:
            excess = size - cap
            saved = drop = 0
            for message in task.history[:-1]:
                if saved >= excess:
                    break
                # the message and the separator before the next one
                saved += _json_size(MessageToDict(message)) + 2
                drop += 1
            del task.history[:drop]
            counts["dropped_history_messages"] = drop
            size = self._measure(task, base, counts)
        # 2. the content of the largest parts, the answer itself last
        if size > cap:
            candidates = sorted(
                self._content_parts(task, _CONTENT), key=self._size, reverse=True
            )
            if task.status.HasField("message"):
                candidates.extend(
                    sorted(
                        (
                            p
                            for p in task.status.message.parts
                            if p.WhichOneof("content") in _CONTENT
                        ),
                        key=self._size,
                        reverse=True,
                    )
                )
            for part in candidates:
                if size <= cap:
                    break
                if self._size(part) < 128:
                    # omitting it would save nothing
                    continue
                before = _part_size(part)
                self._omit(part)
                counts["omitted_parts"] += 1
                size -= before - _part_size(part)
                if size <= cap:
                    size = self._measure(task, base, counts)
            size = self._measure(task, base, counts)
        # 3. metadata beyond the platform's own key, largest first
        if size > cap:
            structs = sorted(
                self._structs(task),
                key=lambda struct: _json_size(MessageToDict(struct)),
                reverse=True,
            )
            for struct in structs:
                if size <= cap:
                    break
                extra = [key for key in struct.fields if key != "hopsworks"]
                if not extra:
                    continue
                for key in extra:
                    del struct[key]
                counts["dropped_metadata"] = counts.get("dropped_metadata", 0) + 1
                size = self._measure(task, base, counts)
        # 4. nothing of the content fits: the task is kept, without it
        if size > cap:
            self._empty(task)
            counts["emptied"] = True
            size = self._measure(task, base, counts)
            if size > cap:
                from a2a.utils.errors import InternalError  # noqa: PLC0415

                raise InternalError(
                    message=f"Task {task.id} does not fit in a stored task "
                    f"({cap} bytes) even without its content."
                )
        return (
            f"{counts['dropped_history_messages']} history message(s) dropped, "
            f"{counts['omitted_parts']} part(s) omitted, "
            f"{counts.get('dropped_metadata', 0)} metadata dropped"
            + (", content emptied" if counts.get("emptied") else "")
        )

    @staticmethod
    def _empty(task: Task) -> None:
        """Keep the task, its state and its ``hopsworks`` facts, and nothing of its content."""
        from a2a.types import Message, Role  # noqa: PLC0415

        del task.artifacts[:]
        del task.history[:]
        for key in [key for key in task.metadata.fields if key != "hopsworks"]:
            del task.metadata[key]
        if task.status.HasField("message"):
            old = task.status.message
            note = Message(
                message_id=old.message_id,
                role=old.role or Role.ROLE_AGENT,
                task_id=old.task_id,
                context_id=old.context_id,
            )
            note.parts.add(text=_EMPTIED_NOTE)
            task.status.message.CopyFrom(note)


async def _read_spilled(
    backend: Any, task: Task, digest: str
) -> tuple[bytes, str, str | None] | None:
    """A spilled part of a task the caller may read: its bytes, media type and file name.

    None when the task names no such part, so a digest is never a key to
    content the task does not carry.
    """
    if not _DIGEST.match(digest) or not _safe_task_id(task.id):
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
