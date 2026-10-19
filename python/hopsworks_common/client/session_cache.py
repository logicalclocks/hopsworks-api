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
"""What a login fetches, kept on disk so the next process with the same host, key and project fetches nothing.

On only when `HOPS_SESSION_CACHE=1`, which the Hopsable sandbox sets: a sandbox is one user's machine.
Never set it on a host other users share: the files hold that user's certificates.

A session is valid for an hour, or until its client certificate expires if that comes first. A call refused
because the session went stale (an unknown key, a project that is gone, a certificate the server rejects)
drops it; the client then logs in again and repeats the call once.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any


ENV_VAR = "HOPS_SESSION_CACHE"
DIR_ENV_VAR = "HOPS_SESSION_DIR"
TTL_S = 3600
# A session's certificate folders outlive it by this much, so a process still using one keeps it.
_CERTS_GRACE_S = 2 * TTL_S

# One session per process: the client binds it at login. Plain data, nothing that holds a resource.
_key: str | None = None
_data: dict[str, Any] = {}
_used = False
# What the host answers the same to every user, such as its versions, before any project is chosen.
_host_key: str | None = None


def enabled() -> bool:
    return os.environ.get(ENV_VAR) == "1"


def active() -> bool:
    """Whether this process has a session bound: the cache is on and the project is known."""
    return _key is not None


def directory() -> Path:
    return Path(os.environ.get(DIR_ENV_VAR) or Path.home() / ".hops" / "session")


def bind(host: str, port: Any, api_key: str, project: str | None) -> None:
    """Load the session for this host, key and project, or start an empty one."""
    global _key, _data, _used
    _used = False
    _data = {}
    if not enabled() or not project:
        _key = None
        return
    material = f"{host}:{port}\n{api_key}\n{project}".encode()
    _key = hashlib.sha256(material).hexdigest()
    try:
        data = json.loads(_file().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    now = time.time()
    if data.get("v") == 1 and now < data.get("expires_at", 0):
        _data = data


def bind_host(host: str, port: Any) -> None:
    global _host_key
    _host_key = (
        hashlib.sha256(f"{host}:{port}".encode()).hexdigest() if enabled() else None
    )


def get_host(name: str) -> Any:
    """A value the host gave an earlier process, or None."""
    if _host_key is None:
        return None
    try:
        data = json.loads(
            (directory() / f"host-{_host_key}.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return None
    if time.time() >= data.get("expires_at", 0):
        return None
    return data.get(name)


def put_host(name: str, value: Any) -> None:
    if _host_key is None:
        return
    _write_json(
        directory() / f"host-{_host_key}.json",
        {"v": 1, "expires_at": time.time() + TTL_S, name: value},
    )


def get(name: str) -> Any:
    """A value the session holds, or None."""
    global _used
    if _key is None:
        return None
    value = _data.get(name)
    if value is not None:
        _used = True
    return value


def put(name: str, value: Any, expires_at: float | None = None) -> None:
    """Keep a value; `expires_at` can shorten the session, never lengthen it."""
    if _key is None:
        return
    if "expires_at" not in _data:
        _data["v"] = 1
        _data["expires_at"] = time.time() + TTL_S
    if expires_at is not None:
        _data["expires_at"] = min(_data["expires_at"], expires_at)
    _data[name] = value
    _write()


def used() -> bool:
    """Whether this process took anything from a session written by an earlier one."""
    return _used


def invalidate() -> None:
    """Forget the session, on disk too: the next login fetches everything again."""
    global _data, _used
    _data = {}
    _used = False
    if _key is not None:
        with contextlib.suppress(OSError):
            _file().unlink()


def new_certs_folder() -> str:
    """A fresh folder for the session's certificates, readable by this user only."""
    root = directory()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _remove_old_certs_folders(root)
    return tempfile.mkdtemp(prefix=f"certs-{_key[:16]}-", dir=root)


def owns(folder: str | None) -> bool:
    """Whether the folder holds a session's certificates, which outlive the process."""
    if not folder:
        return False
    return Path(folder).parent == directory() and Path(folder).name.startswith("certs-")


def _file() -> Path:
    return directory() / f"{_key}.json"


def _write() -> None:
    _write_json(_file(), _data)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    root = directory()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Parallel commands log in at once: each writes its own file and renames it into place.
    fd, tmp = tempfile.mkstemp(dir=root, prefix=".session-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp)


def _remove_old_certs_folders(root: Path) -> None:
    cutoff = time.time() - _CERTS_GRACE_S
    for folder in root.glob("certs-*"):
        with contextlib.suppress(OSError):
            if folder.stat().st_mtime < cutoff:
                shutil.rmtree(folder)
