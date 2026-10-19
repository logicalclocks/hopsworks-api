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
        op: `create`, `update` or `delete`.
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
