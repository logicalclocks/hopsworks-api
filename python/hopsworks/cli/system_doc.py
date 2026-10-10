"""A system's ``system.yaml``: every change to it, and the lease on its build.

Every writer (the CLI, the build's ``set.py``, the Hopsworks UI through
``hops factory system write-doc``) changes the file the same way: under a
short write lock, it reads the file and keeps its sha256, changes the
document, validates it, writes a temporary file in the same directory and
fsyncs it, then re-reads the file and replaces it only when its sha256 is
still the one read. A writer that bypassed the lock is caught by that
re-read: a programmatic change is retried on the new document, a whole
document from an editor is refused as a conflict.

The build lease, ``.hops.lock`` in the system directory, records which
invocation builds the system: ``{"owner", "token", "acquired", "expires"}``
as JSON, times in UTC ISO 8601. ``hops factory run`` takes it before it
starts Claude Code and hands the token on as ``HOPS_LEASE_TOKEN``; the build
renews it and releases it at the end. A lease past its expiry is taken over.
"""

from __future__ import annotations

import contextlib
import getpass
import hashlib
import json
import os
import secrets
import socket
import tempfile
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import click
import yaml


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


FILE = "system.yaml"
WRITE_LOCK = ".system.yaml.lock"
LEASE = ".hops.lock"
TOKEN_ENV = "HOPS_LEASE_TOKEN"
LEASE_TTL = timedelta(hours=4)
# A write lock is held for milliseconds; one this old was left by a dead writer.
STALE_WRITE_LOCK = 30.0


class Conflict(click.ClickException):
    """The document changed since it was read."""

    exit_code = 3


class Invalid(click.ClickException):
    """The new document is not a valid system.yaml."""

    exit_code = 2


class LeaseHeld(click.ClickException):
    """Another invocation holds the build lease, or this one lost it."""

    exit_code = 4


def sha256(text: str) -> str:
    """The sha256 hex digest of `text` as UTF-8.

    Args:
        text: The text.

    Returns:
        The digest.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def dump(doc: dict) -> str:
    """The document as every writer serializes it.

    Args:
        doc: The document.

    Returns:
        The YAML.
    """
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100)


def problems(text: str) -> list[str]:
    """What makes `text` no system.yaml at all: unparseable, not a mapping, or without a `system` or `layer` block.

    Args:
        text: The YAML.

    Returns:
        The problems.
    """
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return [f"not valid YAML: {exc}"]
    if not isinstance(doc, dict):
        return ["the document is not a mapping"]
    found = []
    if not isinstance(doc.get("system"), dict) and not isinstance(
        doc.get("layer"), dict
    ):
        found.append("it has no `system` (or analytics `layer`) mapping")
    version = doc.get("schema_version")
    if version is not None and (not isinstance(version, int) or version < 1):
        found.append("`schema_version` must be a positive integer")
    return found


def read(directory: Path) -> tuple[dict, str | None]:
    """The parsed system.yaml in `directory` and its sha256, `({}, None)` when there is none.

    Args:
        directory: The system's directory.

    Returns:
        The document and its sha256.
    """
    text = _text(directory / FILE)
    return (
        (yaml.safe_load(text) or {}, sha256(text)) if text is not None else ({}, None)
    )


def update(
    directory: Path,
    change: Callable[[dict], Any],
    validate: Callable[[dict], list[str]] | None = None,
    attempts: int = 5,
) -> dict:
    """Apply `change` to the system.yaml in `directory` in place, and return the document written.

    `change` mutates the document it is given; it is called again on the newer
    document when another writer replaced the file in between.
    `validate` adds rules of the system's kind to the basic ones.

    Args:
        directory: The system's directory.
        change: Mutates the document it is given.
        validate: Further rules; returns the problems.
        attempts: How often to try.

    Returns:
        The document written.

    Raises:
        Invalid: the changed document is not valid.
        Conflict: the file kept changing under every attempt.
    """
    path = directory / FILE
    for attempt in range(attempts):
        with _write_lock(directory):
            text = _text(path)
            doc = (yaml.safe_load(text) if text is not None else None) or {}
            change(doc)
            new = dump(doc)
            _check(new, doc, validate)
            try:
                _commit(path, sha256(text) if text is not None else None, new)
                return doc
            except Conflict:
                if attempt == attempts - 1:
                    raise
    raise AssertionError("unreachable")


def replace(
    directory: Path,
    text: str,
    expected_sha256: str | None,
    validate: Callable[[dict], list[str]] | None = None,
) -> str:
    """Replace the system.yaml in `directory` with `text`, which was edited from the version of sha256 `expected_sha256`.

    `expected_sha256` None creates the file, which must not exist yet.
    Returns the sha256 of the new file.

    Args:
        directory: The system's directory.
        text: The new document.
        expected_sha256: The sha256 of the version the edit started from.
        validate: Further rules; returns the problems.

    Returns:
        The sha256 of the new file.

    Raises:
        Invalid: `text` is not a valid system.yaml.
        Conflict: the file is not the version the edit started from.
    """
    _check(text, None, validate)
    path = directory / FILE
    with _write_lock(directory):
        current = _text(path)
        if (sha256(current) if current is not None else None) != expected_sha256:
            raise Conflict(
                f"{path} changed since it was read; reload it and edit again"
            )
        _commit(path, expected_sha256, text)
    return sha256(text)


def create(directory: Path, doc: dict) -> None:
    """Write a new system's system.yaml; refused when the directory already holds one.

    Args:
        directory: The system's directory.
        doc: The document.
    """
    replace(directory, dump(doc), None)


def _check(
    text: str, doc: dict | None, validate: Callable[[dict], list[str]] | None
) -> None:
    found = problems(text)
    if not found and validate is not None:
        found = validate(doc if doc is not None else yaml.safe_load(text))
    if found:
        raise Invalid("system.yaml would be invalid:\n  " + "\n  ".join(found))


def _text(path: Path) -> str | None:
    # Bytes, not text mode: the sha256 every writer compares is of the file's bytes,
    # which Windows text mode would change by translating line endings.
    try:
        return path.read_bytes().decode("utf-8")
    except FileNotFoundError:
        return None


def _commit(path: Path, expected: str | None, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        current = _text(path)
        if (sha256(current) if current is not None else None) != expected:
            raise Conflict(f"{path} changed while it was being written")
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise


@contextlib.contextmanager
def _write_lock(directory: Path, wait: float = 15.0) -> Iterator[None]:
    """Hold the directory's write lock, created exclusively, which every filesystem (HopsFS too) makes atomic."""
    path = directory / WRITE_LOCK
    deadline = time.monotonic() + wait
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            with contextlib.suppress(FileNotFoundError):
                if time.time() - path.stat().st_mtime > STALE_WRITE_LOCK:
                    path.unlink()
                    continue
            if time.monotonic() > deadline:
                raise Conflict(f"{path} is held by another writer; try again") from None
            time.sleep(0.05)
            continue
        os.write(fd, _owner().encode("utf-8"))
        os.close(fd)
        break
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


# region The build lease


def _owner() -> str:
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - no user name in a bare container
        user = "?"
    return f"{user}@{socket.gethostname()}:{os.getpid()}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H:%M:%SZ")


def lease(directory: Path) -> dict | None:
    """The build lease of the system in `directory`, None when nobody holds one.

    Args:
        directory: The system's directory.

    Returns:
        The lease.
    """
    path = directory / LEASE
    held = lease_at(path)
    if held is None or "expires" not in held:
        # A half-written lease, or the old agent-written lock: it expires a TTL after it was written.
        try:
            written = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        except FileNotFoundError:
            return None
        held = {"owner": "unknown", "token": "", "expires": _stamp(written + LEASE_TTL)}
    return held


def expired(held: dict) -> bool:
    """Whether the lease `held` is past its expiry.

    Args:
        held: The lease.

    Returns:
        Whether it expired.
    """
    try:
        expires = datetime.strptime(held["expires"], "%Y-%m-%dT%H:%M:%SZ")
    except (KeyError, TypeError, ValueError):
        return True
    return expires.replace(tzinfo=timezone.utc) <= _now()


def check(directory: Path) -> None:
    """Refuse to start on a system another invocation is building.

    Args:
        directory: The system's directory.

    Raises:
        LeaseHeld: another invocation holds a lease that has not expired.
    """
    held = lease(directory)
    if (
        held is not None
        and not expired(held)
        and held.get("token") != os.environ.get(TOKEN_ENV)
    ):
        raise LeaseHeld(_held_message(directory, held))


def acquire(
    directory: Path, token: str | None = None, ttl: timedelta = LEASE_TTL
) -> dict:
    """Take the build lease of the system in `directory`, and return it.

    `token` (default: `HOPS_LEASE_TOKEN`) adopts a lease this invocation's caller already holds, renewing it.
    An expired lease is taken over.

    Args:
        directory: The system's directory.
        token: A token of a lease already held, to adopt.
        ttl: How long the lease lasts without a renewal.

    Returns:
        The lease.

    Raises:
        LeaseHeld: another invocation holds a lease that has not expired.
    """
    token = token or os.environ.get(TOKEN_ENV)
    path = directory / LEASE
    for _ in range(5):
        held = lease(directory)
        if held is not None and token and held.get("token") == token:
            return renew(directory, token, ttl)
        if held is not None and not expired(held):
            raise LeaseHeld(_held_message(directory, held))
        if held is not None and not _clear_expired(directory, held):
            continue
        record = {
            "owner": _owner(),
            "token": secrets.token_hex(16),
            "acquired": _stamp(_now()),
            "expires": _stamp(_now() + ttl),
        }
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            json.dump(record, out)
            out.flush()
            os.fsync(out.fileno())
        return record
    raise LeaseHeld(f"could not take the build lease {path}; try again")


def _clear_expired(directory: Path, held: dict) -> bool:
    """Move the expired lease `held` aside; False when another invocation changed it first."""
    path = directory / LEASE
    aside = directory / f"{LEASE}.expired-{secrets.token_hex(4)}"
    try:
        os.rename(path, aside)
    except FileNotFoundError:
        return False
    moved = lease_at(aside)
    if moved is not None and moved.get("token") != held.get("token"):
        # Renewed or taken over between the read and the rename: put it back.
        if not path.exists():
            os.rename(aside, path)
        return False
    with contextlib.suppress(FileNotFoundError):
        aside.unlink()
    return True


def lease_at(path: Path) -> dict | None:
    """The lease in the file `path`, None when it is gone or unreadable.

    Args:
        path: The lease file.

    Returns:
        The lease.
    """
    try:
        held = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None
    return held if isinstance(held, dict) else None


def renew(directory: Path, token: str, ttl: timedelta = LEASE_TTL) -> dict:
    """Extend the build lease `token` holds; returns it.

    Args:
        directory: The system's directory.
        token: The lease's token.
        ttl: How long the lease lasts from now.

    Returns:
        The lease.

    Raises:
        LeaseHeld: the lease is not `token`'s, or it expired (take it again with `acquire`).
    """
    held = lease(directory)
    if held is None or held.get("token") != token:
        raise LeaseHeld(
            f"the build lease of {directory.name} is not yours"
            + (f"; {_held_message(directory, held)}" if held else "")
        )
    if expired(held):
        raise LeaseHeld(
            f"the build lease of {directory.name} expired at {held['expires']}; take it again"
        )
    held = {**held, "expires": _stamp(_now() + ttl)}
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=f"{LEASE}.", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        json.dump(held, out)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, directory / LEASE)
    return held


def release(directory: Path, token: str | None = None) -> bool:
    """Give up the build lease `token` (default: `HOPS_LEASE_TOKEN`) holds; False when it is not held with that token.

    Args:
        directory: The system's directory.
        token: The lease's token.

    Returns:
        Whether it was released.
    """
    token = token or os.environ.get(TOKEN_ENV)
    held = lease(directory)
    if not token or held is None or held.get("token") != token:
        return False
    with contextlib.suppress(FileNotFoundError):
        (directory / LEASE).unlink()
    return True


def _held_message(directory: Path, held: dict) -> str:
    return (
        f"{directory.name} is being built by {held.get('owner', 'unknown')} "
        f"(lease until {held.get('expires')}); wait for it, or for the lease to expire"
    )


# endregion
