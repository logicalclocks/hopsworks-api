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
"""One line per change made in Hopsworks, for a host that shows changes as they happen.

The client appends a JSON line to the file `HOPS_EVENTS_FILE` names; without it nothing is
written. Writing never fails or slows the operation it reports.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import json
import os
import re
import secrets
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


ENV_VAR = "HOPS_EVENTS_FILE"
MAX_LINE_BYTES = 4096
_MAX_STRING = 200
_MAX_DETAIL_BYTES = 1024
_PROGRESS_INTERVAL_S = 1.0
# A source preview file, as the host reads it: at most this many bytes, rows and columns.
PREVIEW_MAX_BYTES = 65536
_PREVIEW_ROWS = 20
_PREVIEW_COLUMNS = 200
# Previews kept beside the events file; the host reads each one once, right after its line.
_PREVIEWS_KEPT = 20

_last_progress: dict[tuple, float] = {}


def emit(
    kind: str,
    op: str,
    phase: str,
    *,
    name: str | None = None,
    version: int | None = None,
    path: str | None = None,
    progress: dict[str, Any] | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Append one event line, or do nothing when no events file is configured.

    Parameters:
        kind: What changed: `file`, `fg`, `fv`, `td`, `model`, `deployment`, `job`, `execution`, `schedule`, `dashboard`, `chart`, `app`, `notebook` or `datasource`.
        op: `create`, `update` or `delete`; `read` for a `datasource` (`source_preview` writes it).
        phase: `started`, `progress`, `done` or `failed`.
        name: The asset's name.
        version: The asset's version.
        path: A project-relative path, for a file or a notebook.
        progress: `{done, total?, unit}` for a `progress` line.
        detail: A flat object of short values; `error` on a `failed` line.
    """
    target = os.environ.get(ENV_VAR)
    if not target:
        return
    try:
        key = {
            k: v
            for k, v in (
                ("name", _text(name)),
                ("version", version),
                ("path", _text(_relative(path))),
            )
            if v is not None
        }
        if phase == "progress":
            now = time.monotonic()
            throttle = (kind, op, tuple(sorted(key.items())))
            if now - _last_progress.get(throttle, 0.0) < _PROGRESS_INTERVAL_S:
                return
            _last_progress[throttle] = now
        line = {
            "v": 1,
            "id": secrets.token_hex(8),
            "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "kind": kind,
            "key": key,
            "op": op,
            "phase": phase,
        }
        if progress is not None:
            line["progress"] = progress
        if detail:
            line["detail"] = _flat(detail)
        data = (json.dumps(line, separators=(",", ":")) + "\n").encode("utf-8")
        if len(data) > MAX_LINE_BYTES:
            line.pop("detail", None)
            data = (json.dumps(line, separators=(",", ":")) + "\n").encode("utf-8")
            if len(data) > MAX_LINE_BYTES:
                return
        os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
        # One write of a whole line under 4 KB on an O_APPEND descriptor is atomic, so
        # lines from parallel processes never interleave.
        fd = os.open(target, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
    except Exception:  # noqa: BLE001 - reporting a change never fails the change
        return


def source_preview(
    name: str, table: str, head: list[str], rows: list[list[Any]]
) -> None:
    """Write the rows a data source preview printed, then the `read` line that names them.

    The rows go to `<events dir>/previews/<32 hex>.json` as `{"head": [...], "rows": [[...]]}`:
    at most 20 rows and 200 columns, every cell text of at most 200 characters or null, the
    file at most 64 KB (rows past it are left out). The file name is random, never from input.
    Nothing is written without an events file; writing never fails the preview.

    Parameters:
        name: The data source.
        table: The table, sheet or path read.
        head: The column names, as printed.
        rows: The cells, as printed; None for a null.
    """
    target = os.environ.get(ENV_VAR)
    if not target:
        return
    try:
        head = [_cell(h) or "" for h in head[:_PREVIEW_COLUMNS]]
        rows = [[_cell(c) for c in row[: len(head)]] for row in rows[:_PREVIEW_ROWS]]
        while True:
            data = json.dumps(
                {"head": head, "rows": rows}, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if len(data) <= PREVIEW_MAX_BYTES:
                break
            if not rows:
                return
            rows.pop()
        folder = os.path.join(os.path.dirname(os.path.abspath(target)), "previews")
        os.makedirs(folder, mode=0o700, exist_ok=True)
        preview_id = secrets.token_hex(16)
        fd = os.open(
            os.path.join(folder, preview_id + ".json"),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        _prune(folder)
        emit(
            "datasource",
            "read",
            "done",
            name=name,
            detail={"table": table, "preview": preview_id},
        )
    except Exception:  # noqa: BLE001 - relaying a preview never fails the preview
        return


def _cell(value: Any) -> str | None:
    """A cell as text of at most 200 characters as JavaScript counts them (UTF-16 units), or None."""
    if value is None:
        return None
    text = str(value)[:_MAX_STRING]
    while len(text.encode("utf-16-le")) > 2 * _MAX_STRING:
        text = text[:-1]
    return text


def _prune(folder: str) -> None:
    """Keep the newest previews only."""
    files = [
        e
        for e in os.scandir(folder)
        if e.name.endswith(".json") and e.is_file(follow_symlinks=False)
    ]
    # More than 20 previews written before the host reads them drop the oldest.
    files.sort(key=lambda e: e.stat(follow_symlinks=False).st_mtime_ns)
    for e in files[:-_PREVIEWS_KEPT]:
        with contextlib.suppress(OSError):
            os.unlink(e.path)


class Change:
    """A change in progress, as `tracked` yields it."""

    def __init__(self, kind: str, op: str, key: dict[str, Any]):
        self.kind, self.op, self.key = kind, op, key
        self.detail: dict[str, Any] = {}

    def progress(self, done: float, total: float | None = None, unit: str = "") -> None:
        """Report progress; written at most once a second for this change."""
        value: dict[str, Any] = {"done": done, "unit": unit[:16]}
        if total is not None:
            value["total"] = total
        emit(self.kind, self.op, "progress", progress=value, **self.key)


_current: contextvars.ContextVar[Change | None] = contextvars.ContextVar(
    "hops_event_change", default=None
)


def progress(done: float, total: float | None = None, unit: str = "") -> None:
    """Report progress on the innermost change `tracked` reports, if there is one."""
    change = _current.get()
    if change is not None:
        change.progress(done, total, unit)


def note(**detail: Any) -> None:
    """Add to the `detail` of the innermost change `tracked` reports, if there is one."""
    change = _current.get()
    if change is not None:
        change.detail.update(detail)


@contextmanager
def tracked(
    kind: str,
    op: str,
    *,
    long: bool = False,
    name: str | None = None,
    version: int | None = None,
    path: str | None = None,
) -> Iterator[Change]:
    """Report the change the block makes: `done` when it returns, `failed` with the error when it raises.

    A long change also reports `started` on entry. Set `change.key` inside the block when the
    name or version is only known once the change is made.
    """
    change = Change(kind, op, {"name": name, "version": version, "path": path})
    if not os.environ.get(ENV_VAR):
        yield change
        return
    if long:
        emit(kind, op, "started", **change.key)
    token = _current.set(change)
    try:
        yield change
    except BaseException as e:
        error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        detail = {**change.detail, "error": error.splitlines()[0]}
        emit(kind, op, "failed", detail=detail, **change.key)
        raise
    finally:
        _current.reset(token)
    emit(kind, op, "done", detail=change.detail or None, **change.key)


def tracking(
    kind: str,
    op: str,
    *,
    long: bool = False,
    key: Callable[..., dict[str, Any]] = lambda *args, **kwargs: {},
) -> Callable:
    """Decorate a method whose call is one change; `key` maps its arguments to `name`, `version` or `path`.

    The key is read before the call and again after it, so a version the server assigns
    reaches the `done` line.
    """

    def decorate(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not os.environ.get(ENV_VAR):
                return fn(*args, **kwargs)
            with tracked(kind, op, long=long, **_key(key, args, kwargs)) as change:
                result = fn(*args, **kwargs)
                change.key = _key(key, args, kwargs)
                return result

        return wrapper

    return decorate


def _key(key: Callable, args: tuple, kwargs: dict) -> dict[str, Any]:
    try:
        found = key(*args, **kwargs) or {}
    except Exception:  # noqa: BLE001 - a key that cannot be read reports without it
        found = {}
    return {k: found.get(k) for k in ("name", "version", "path")}


def _relative(path: str | None) -> str | None:
    """A path as the CLI prints it: relative to the project, without scheme or `/Projects/<project>/`."""
    if not path:
        return path
    path = re.sub(r"/{2,}", "/", re.sub(r"^hopsfs://[^/]*", "", str(path)))
    from hopsworks_common import client

    try:
        project = client._get_instance()._project_name
    except Exception:  # noqa: BLE001 - no client yet: keep the path as given
        project = None
    if project:
        prefix = f"/Projects/{project}/"
        if path.startswith(prefix):
            return path[len(prefix) :]
    return path


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = "".join(c for c in str(value) if c.isprintable())
    return text[:_MAX_STRING]


def _flat(detail: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in detail.items():
        if not (isinstance(v, (bool, int, float)) or v is None):
            v = _text(v)
        out[_text(k)[:64]] = v
        if len(json.dumps(out)) > _MAX_DETAIL_BYTES:
            out.pop(_text(k)[:64])
            break
    return out
