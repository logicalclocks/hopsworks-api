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
"""Sync job of Gmail data sources.

Runs as a PYTHON job that the backend creates for a Gmail data source, with `--config` naming the job's JSON configuration.
It upserts the mailbox's messages into a Delta feature group partitioned by the `day` of their Gmail receive time in UTC.
New messages are inserted without rewriting the partition's other files, so an hourly job adds about one Parquet file to the current day every hour.
Once a day has ended, the next run compacts that day's files into files of up to 192 MiB.
Attachments are written as files under the job's attachments directory, `<path>/day=YYYY-MM-DD/<message_id>/<filename>`, and the feature group's `attachments` column holds their full paths.
It writes through the HopsFS mount of the job's pod and reads Gmail with short-lived access tokens that the backend mints for the data source, so it never holds the data source's credentials.

`FULL_LOAD` overwrites the feature group and rebuilds the attachments directory in a staging directory that it then swaps in.
`INCREMENTAL_TIMESTAMP` upserts the messages received after the cursor recorded by the previous run.
Upserts are keyed by message id and the cursor moves only after a batch is committed, so a retry, or a run that overlaps the previous one, never duplicates a message.

This module also runs outside a job, for tests, so the client library is imported inside `_main` only.
"""

from __future__ import annotations

import argparse
import base64
import collections
import email
import email.policy
import email.utils
import html
import json
import logging
import os
import random
import re
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import Self


_logger = logging.getLogger(__name__)

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/"
STATE_DIR = "_gmail_sync_state"
STATE_SCHEMA_VERSION = 2
STAGING_SUFFIX = ".__staging__"
PREVIOUS_SUFFIX = ".__previous__"
LEASE_SUFFIX = ".__lease__"
STRATEGIES = ("FULL_LOAD", "INCREMENTAL_TIMESTAMP")
# Messages received in the last minute are left to the next run: Gmail can still be indexing them.
RUN_HORIZON_MS = 60_000
# A token is renewed this long before it expires, so no request goes out with one about to lapse.
TOKEN_RENEWAL_MS = 5 * 60_000
# Gmail rate-limits per user; a few parallel fetches stay well under it and the client backs off when it does not.
FETCH_WORKERS = 4
# Messages fetched per batch; bounds how many raw messages, attachments included, are in memory at once.
FETCH_BATCH = 64
# Messages per feature group upsert, and so per cursor checkpoint.
UPSERT_BATCH = 5000
# A run reads its range in windows this long, oldest first, so a backfill upserts as it goes.
WINDOW_MS = 7 * 24 * 3600_000
# Rate-limit retries are logged at most this often.
THROTTLE_LOG_INTERVAL_S = 60.0
# Longest wait honoured from a Retry-After header.
MAX_RETRY_AFTER_S = 300.0
MAX_ATTEMPTS = 8
# A lease whose holder cannot be checked is taken over after this long; no sync runs for a day.
UNVERIFIABLE_LEASE_MS = 24 * 3600_000
MAX_FILENAME_LENGTH = 200

# The feature group's columns, in order, with their feature types.
# `from`, `to` and `date` are SQL keywords, hence the suffixed names.
COLUMNS = (
    ("message_id", "string"),
    ("thread_id", "string"),
    ("history_id", "string"),
    ("internal_date", "timestamp"),
    ("day", "string"),
    ("hour", "string"),
    ("date_header", "string"),
    ("from_address", "string"),
    ("to_addresses", "array<string>"),
    ("cc_addresses", "array<string>"),
    ("bcc_addresses", "array<string>"),
    ("subject", "string"),
    ("labels", "array<string>"),
    ("snippet", "string"),
    ("size_estimate", "bigint"),
    ("body_text", "string"),
    ("attachments", "array<string>"),
)
PRIMARY_KEY = ["message_id"]
PARTITION_KEY = ["day"]
# Compacted files aim for 100 to 250 MB; delta-rs packs a day's files into files of at most this size.
COMPACTION_TARGET_BYTES = 192 * 1024 * 1024
EVENT_TIME = "internal_date"


class _GmailSyncError(Exception):
    """A sync run that cannot continue; its message is shown in the job log."""


class _LeaseHeld(_GmailSyncError):
    """Another execution is syncing the same attachments directory."""


# region Gmail client


class _GmailClient:
    """Read-only Gmail REST client that renews its token and backs off on rate limits.

    `token_provider` returns `(access_token, expires_at_ms)`.
    """

    def __init__(
        self,
        mailbox: str,
        token_provider: Callable[[], tuple[str, int | None]],
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ):
        if session is None:
            import requests

            session = requests.Session()
        self._mailbox = mailbox
        self._token_provider = token_provider
        self._session = session
        self._sleep = sleep
        self._clock = clock
        self._token: str | None = None
        self._expires_at_ms: int | None = None
        self._throttled = 0
        self._throttle_logged_at: float | None = None

    def _access_token(self, renew: bool = False) -> str:
        now_ms = int(self._clock() * 1000)
        if (
            renew
            or self._token is None
            or (
                self._expires_at_ms is not None
                and self._expires_at_ms - now_ms < TOKEN_RENEWAL_MS
            )
        ):
            self._token, self._expires_at_ms = self._token_provider()
        return self._token

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict | None:
        """GET a Gmail resource, or None when Gmail answers 404."""
        url = GMAIL_API + self._mailbox + path
        renewed = False
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = self._session.get(
                    url,
                    params=params,
                    headers={"Authorization": "Bearer " + self._access_token()},
                    timeout=(10, 60),
                )
            except OSError as e:
                if attempt == MAX_ATTEMPTS - 1:
                    raise _GmailSyncError(
                        f"Gmail unreachable: {type(e).__name__}"
                    ) from e
                self._backoff(attempt, None)
                continue
            status = response.status_code
            if status == 200:
                return response.json()
            if status == 404:
                return None
            if status == 401 and not renewed:
                self._access_token(renew=True)
                renewed = True
                continue
            retry_after = _retry_after(response)
            if _is_retryable(status, response) and attempt < MAX_ATTEMPTS - 1:
                self._log_throttle(status, response)
                self._backoff(attempt, retry_after)
                continue
            raise _GmailSyncError(
                f"Gmail returned HTTP {status}: {_error_reason(response)}"
            )
        raise _GmailSyncError("Gmail kept refusing the request after retries")

    def _log_throttle(self, status: int, response: Any) -> None:
        # Unlocked across fetch workers: a lost count or an extra line is harmless.
        self._throttled += 1
        now = self._clock()
        if (
            self._throttle_logged_at is None
            or now - self._throttle_logged_at >= THROTTLE_LOG_INTERVAL_S
        ):
            _logger.info(
                "Gmail is throttling the sync (HTTP %d %s); %d retries so far",
                status,
                _error_reason(response),
                self._throttled,
            )
            self._throttle_logged_at = now

    def _backoff(self, attempt: int, retry_after: float | None) -> None:
        delay = (
            min(retry_after, MAX_RETRY_AFTER_S)
            if retry_after is not None
            else min(64.0, 2**attempt) + random.uniform(0, 1)
        )
        self._sleep(delay)

    def _list_message_ids(self, query: str, label_ids: list[str]) -> Iterator[str]:
        """Ids of the messages matching `query` and every label in `label_ids`, newest first."""
        page_token = None
        while True:
            params: dict[str, Any] = {
                "q": query,
                "maxResults": 500,
                "includeSpamTrash": "false",
            }
            if label_ids:
                params["labelIds"] = label_ids
            if page_token:
                params["pageToken"] = page_token
            page = self._get("/messages", params) or {}
            for message in page.get("messages", []):
                yield message["id"]
            page_token = page.get("nextPageToken")
            if not page_token:
                return

    def _get_raw_message(self, message_id: str) -> dict | None:
        """The message in `raw` format, or None when it was deleted after it was listed."""
        return self._get(f"/messages/{message_id}", {"format": "raw"})


def _is_retryable(status: int, response: Any) -> bool:
    if status == 429 or status >= 500:
        return True
    # Gmail reports per-user and per-project rate limits as 403 with these reasons.
    return status == 403 and _error_reason(response) in (
        "rateLimitExceeded",
        "userRateLimitExceeded",
    )


def _error_reason(response: Any) -> str:
    try:
        error = response.json().get("error", {})
        errors = error.get("errors") or [{}]
        return errors[0].get("reason") or error.get("status") or str(error.get("code"))
    except (ValueError, AttributeError):
        return "unknown"


def _retry_after(response: Any) -> float | None:
    value = (getattr(response, "headers", None) or {}).get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


# endregion

# region Message parsing


def _parse_message(
    message: dict, attachments_root: Path | None, attachments_prefix: str
) -> dict:
    """One feature group row from a Gmail message in `raw` format.

    Each attachment is written under `attachments_root/day=.../<message_id>/`, and the row lists it as `attachments_prefix` plus the same relative path.
    With `attachments_root` None, attachments are listed but not written.
    `internal_date` stays epoch milliseconds here; the writer converts it.
    """
    raw = base64.urlsafe_b64decode(message["raw"] + "=" * (-len(message["raw"]) % 4))
    parsed = email.message_from_bytes(raw, policy=email.policy.default)
    internal = int(message["internalDate"])
    day = _day_of(internal)
    relative_dir = f"day={day}/{_safe_name(message['id'])}"
    attachments = []
    taken: set[str] = set()
    for part in parsed.iter_attachments():
        name = _unique(_safe_name(part.get_filename() or "attachment"), taken)
        if attachments_root is not None:
            _atomic_write(attachments_root / relative_dir / name, _payload(part))
        attachments.append(f"{attachments_prefix}/{relative_dir}/{name}")
    return {
        "message_id": message["id"],
        "thread_id": message.get("threadId"),
        "history_id": message.get("historyId"),
        "internal_date": internal,
        "day": day,
        "hour": _hour_of(internal),
        "date_header": _header(parsed, "Date"),
        "from_address": _header(parsed, "From"),
        "to_addresses": _addresses(parsed, "To"),
        "cc_addresses": _addresses(parsed, "Cc"),
        "bcc_addresses": _addresses(parsed, "Bcc"),
        "subject": _header(parsed, "Subject"),
        "labels": list(message.get("labelIds") or []),
        "snippet": html.unescape(message.get("snippet") or ""),
        "size_estimate": message.get("sizeEstimate"),
        "body_text": _body_text(parsed),
        "attachments": attachments,
    }


def _header(parsed: email.message.Message, name: str) -> str | None:
    try:
        value = parsed.get(name)
    except (ValueError, IndexError):
        # A header malformed beyond what the default policy can parse.
        return None
    return None if value is None else str(value)


def _addresses(parsed: email.message.Message, name: str) -> list[str]:
    try:
        values = [str(v) for v in parsed.get_all(name, [])]
    except (ValueError, IndexError):
        return []
    return [
        email.utils.formataddr(pair) if pair[0] else pair[1]
        for pair in email.utils.getaddresses(values)
        if pair[1]
    ]


_TAG = re.compile(r"<[^>]+>")
_SCRIPT = re.compile(r"<(script|style)\b.*?</\1>", re.IGNORECASE | re.DOTALL)
_BLANK_LINES = re.compile(r"\n\s*\n\s*\n+")
_UNSAFE_NAME = re.compile(r"[\x00-\x1f\x7f/\\:*?\"<>|]+")


def _body_text(parsed: email.message.EmailMessage) -> str:
    part = parsed.get_body(preferencelist=("plain", "html"))
    if part is None:
        return ""
    text = _decoded(part)
    if part.get_content_subtype() == "html":
        text = _SCRIPT.sub("", text)
        text = re.sub(r"<br\s*/?>|</p>|</div>", "\n", text, flags=re.IGNORECASE)
        text = html.unescape(_TAG.sub("", text))
        text = _BLANK_LINES.sub("\n\n", text)
    return text.strip()


def _decoded(part: email.message.EmailMessage) -> str:
    try:
        return part.get_content()
    except (LookupError, UnicodeDecodeError, AssertionError):
        # An unknown or wrong charset; keep what decodes.
        payload = part.get_payload(decode=True) or b""
        return payload.decode("utf-8", errors="replace")


def _payload(part: email.message.EmailMessage) -> bytes:
    payload = part.get_payload(decode=True)
    if payload is None:
        # A message/rfc822 attachment: keep the attached message as it was sent.
        payload = part.as_bytes()
    return payload


def _safe_name(name: str) -> str:
    """A file name that stays inside its directory, whatever the sender called the attachment."""
    name = _UNSAFE_NAME.sub("_", name).strip(" .")
    if not name:
        name = "attachment"
    if len(name) > MAX_FILENAME_LENGTH:
        stem, dot, ext = name.rpartition(".")
        if dot and len(ext) < 16:
            name = stem[: MAX_FILENAME_LENGTH - len(ext) - 1] + "." + ext
        else:
            name = name[:MAX_FILENAME_LENGTH]
    return name


def _unique(name: str, taken: set[str]) -> str:
    """`name`, or `name-2`, `name-3`... before its extension when an earlier attachment took it."""
    candidate, n = name, 1
    stem, dot, ext = name.rpartition(".")
    while candidate in taken:
        n += 1
        candidate = f"{stem}-{n}.{ext}" if dot and stem else f"{name}-{n}"
    taken.add(candidate)
    return candidate


# endregion

# region Files


def _day_of(epoch_ms: int) -> str:
    """The `day` partition value of a Gmail receive time: its UTC date."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _hour_of(epoch_ms: int) -> str:
    """The `hour` partition value of a Gmail receive time: its UTC hour, `00` to `23`."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%H")


def _iso_utc(epoch_ms: int) -> str:
    return (
        datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(content)
    _replace(tmp, path)


def _replace(source: Path, target: Path) -> None:
    try:
        os.replace(source, target)
    except OSError:
        # A FUSE mount that cannot rename over an existing file: remove it first.
        # Only a retry of the same run reaches this, and it rewrites the same content.
        if target.exists():
            target.unlink()
        os.replace(source, target)


# endregion

# region Feature group


class _FeatureGroupWriter:
    """Upserts rows into the mailbox's feature group; `overwrite` replaces its contents first."""

    def __init__(self, feature_group):
        self._fg = feature_group

    def _upsert(self, rows: list[dict], overwrite: bool) -> None:
        import pandas as pd

        frame = pd.DataFrame(rows, columns=[name for name, _ in COLUMNS])
        frame["internal_date"] = pd.to_datetime(
            frame["internal_date"], unit="ms", utc=True
        )
        frame["size_estimate"] = frame["size_estimate"].astype("Int64")
        self._fg.insert(frame, overwrite=overwrite, wait=True)

    def _compact(self, day: str) -> None:
        """Rewrite one day's files into as few files of up to COMPACTION_TARGET_BYTES as fit.

        A Delta optimize commit: readers see the day before or after it, never in between.
        """
        from deltalake import DeltaTable
        from hsfs.core import delta_engine

        engine = delta_engine.DeltaEngine(
            self._fg.feature_store_id, self._fg.feature_store_name, self._fg, None, None
        )
        table = DeltaTable(
            engine._get_delta_rs_location(),
            storage_options=engine._get_delta_rs_storage_options(),
        )
        table.optimize.compact(
            partition_filters=[("day", "=", day)],
            target_size=COMPACTION_TARGET_BYTES,
            max_concurrent_tasks=1,
        )


def _feature_group(fs, name: str, mailbox: str):
    """The mailbox's Delta feature group, created with its schema on the first run."""
    from hsfs.feature import Feature

    fg = fs.get_or_create_feature_group(
        name=name,
        version=1,
        description=f"Emails of {mailbox}, copied by its Gmail sync job",
        primary_key=PRIMARY_KEY,
        partition_key=PARTITION_KEY,
        event_time=EVENT_TIME,
        time_travel_format="DELTA",
        online_enabled=False,
        statistics_config=False,
    )
    if fg.id is None:
        fg.save(features=[Feature(n, type=t) for n, t in COLUMNS])
    return fg


# endregion

# region State


@dataclass
class _SyncState:
    """What a run leaves behind for the next one: the configuration it ran with and its cursor."""

    mailbox: str
    connector_id: int
    feature_group: str
    query: str
    labels: list[str]
    cursor_ms: int | None = None
    cursor_ids: list[str] = field(default_factory=list)
    # Days written to since they were last compacted; each is compacted once it has ended.
    uncompacted_days: list[str] = field(default_factory=list)
    schema_version: int = STATE_SCHEMA_VERSION

    def _identity(self) -> tuple:
        return (
            self.mailbox,
            self.connector_id,
            self.feature_group,
            self.query,
            tuple(sorted(self.labels)),
            self.schema_version,
        )


def _read_state(directory: Path) -> tuple[_SyncState | None, int]:
    """The newest readable state and its sequence number; (None, 0) when there is none.

    Raises:
        _GmailSyncError: When state versions exist but none can be read.
            Starting over would copy the whole mailbox again.
    """
    state_dir = directory / STATE_DIR
    if not state_dir.is_dir():
        return None, 0
    versions = sorted(
        (int(p.stem), p)
        for p in state_dir.iterdir()
        if p.suffix == ".json" and p.stem.isdigit()
    )
    for seq, path in reversed(versions):
        try:
            return _SyncState(**json.loads(path.read_text())), seq
        except (ValueError, TypeError):
            _logger.warning("Skipping unreadable sync state %s", path.name)
    if versions:
        raise _GmailSyncError(
            f"The sync state in {state_dir} cannot be read; run a full load to rebuild it"
        )
    return None, 0


def _write_state(directory: Path, state: _SyncState, seq: int) -> None:
    """Record `state` as version `seq`; earlier versions are pruned only after it is in place."""
    state_dir = directory / STATE_DIR
    _atomic_write(state_dir / f"{seq}.json", json.dumps(state.__dict__).encode())
    for path in state_dir.iterdir():
        if path.suffix == ".json" and path.stem.isdigit() and int(path.stem) < seq - 1:
            path.unlink(missing_ok=True)


# endregion

# region Run


@dataclass
class _SyncConfig:
    """The job configuration the backend writes next to the job's program."""

    job_name: str
    connector_id: int
    connector_name: str
    feature_group_name: str
    path: str
    query: str = ""
    labels: list[str] = field(default_factory=list)
    loading_strategy: str = "INCREMENTAL_TIMESTAMP"
    initial_value: int | None = None

    @classmethod
    def from_json(cls, config: dict) -> _SyncConfig:
        if config.get("schemaVersion") != 2:
            raise _GmailSyncError(
                f"Unsupported sync configuration version {config.get('schemaVersion')}; save the sync job again"
            )
        sync_config = cls(
            job_name=config["jobName"],
            connector_id=int(config["connectorId"]),
            connector_name=config["connectorName"],
            feature_group_name=config["featureGroupName"],
            path=config["path"].strip("/"),
            query=config.get("query") or "",
            labels=list(config.get("labels") or []),
            loading_strategy=config.get("loadingStrategy") or "INCREMENTAL_TIMESTAMP",
            initial_value=config.get("initialValue"),
        )
        if sync_config.loading_strategy not in STRATEGIES:
            raise _GmailSyncError(
                f"Unknown loading strategy {sync_config.loading_strategy}"
            )
        return sync_config


def _gmail_query(user_query: str, lower_ms: int | None, upper_ms: int) -> str:
    """The Gmail search for a run: the user's query, without drafts, widened by a second on each side.

    Gmail's `after:` and `before:` take whole seconds, so the run filters the exact bounds itself.
    """
    parts = []
    if user_query.strip():
        parts.append(f"({user_query.strip()})")
    parts.append("-in:drafts")
    if lower_ms is not None:
        parts.append(f"after:{lower_ms // 1000 - 1}")
    parts.append(f"before:{upper_ms // 1000 + 1}")
    return " ".join(parts)


class _Lease:
    """Exclusive use of an attachments directory by one execution, recoverable after that execution dies.

    `is_running(holder)` reports whether the holder recorded in an existing lease still runs, or None when it cannot tell.
    """

    def __init__(
        self,
        target: Path,
        holder: str,
        is_running: Callable[[str], bool | None],
        clock: Callable[[], float] = time.time,
    ):
        self._path = target.with_name(target.name + LEASE_SUFFIX)
        self._holder = holder
        self._is_running = is_running
        self._clock = clock

    def __enter__(self) -> Self:
        for _ in range(2):
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if self._take_over_stale():
                    continue
                raise _LeaseHeld(
                    f"Another sync of this mailbox is running; remove {self._path} only if it is not"
                ) from None
            with os.fdopen(fd, "w") as f:
                json.dump(
                    {"holder": self._holder, "since": int(self._clock() * 1000)}, f
                )
            return self
        raise _GmailSyncError(f"Could not take the lease {self._path}")

    def _take_over_stale(self) -> bool:
        try:
            lease = json.loads(self._path.read_text())
        except (OSError, ValueError):
            lease = {}
        holder = lease.get("holder")
        running = self._is_running(holder) if holder else None
        age_ms = int(self._clock() * 1000) - int(lease.get("since") or 0)
        if running or (running is None and age_ms < UNVERIFIABLE_LEASE_MS):
            return False
        _logger.info("Taking over the lease of %s, which no longer runs", holder)
        self._path.unlink(missing_ok=True)
        return True

    def __exit__(self, *exc: object) -> None:
        self._path.unlink(missing_ok=True)


@dataclass
class _RunResult:
    copied: int
    days: list[str]
    skipped_deleted: int
    cursor_ms: int | None


class _SyncRun:
    """One execution of a sync job.

    `client` is a `_GmailClient` for the data source's mailbox, `writer` upserts into its feature group, and the attachments directory is `fs_root / config.path`, listed in rows as `project_root / config.path`.
    """

    def __init__(
        self,
        config: _SyncConfig,
        mailbox: str,
        client: _GmailClient,
        writer: _FeatureGroupWriter,
        fs_root: Path,
        project_root: str,
        execution_id: str,
        is_running: Callable[[str], bool | None],
        clock: Callable[[], float] = time.time,
        scratch: Path | None = None,
    ):
        self._config = config
        self._mailbox = mailbox
        self._client = client
        self._writer = writer
        self._target = fs_root / config.path
        self._dataset = fs_root / config.path.split("/", 1)[0]
        self._prefix = f"{project_root.rstrip('/')}/{config.path}"
        self._execution_id = execution_id
        self._is_running = is_running
        self._clock = clock
        self._scratch = scratch

    def _run(self) -> _RunResult:
        # A missing dataset means the HopsFS mount is absent or the path is wrong; directories below it are ours.
        if not self._dataset.is_dir():
            raise _GmailSyncError(f"The dataset {self._dataset.name} does not exist")
        self._target.parent.mkdir(parents=True, exist_ok=True)
        with _Lease(self._target, self._execution_id, self._is_running, self._clock):
            self._recover_publication()
            if self._config.loading_strategy == "FULL_LOAD":
                return self._full_load()
            return self._incremental()

    def _expected_state(self) -> _SyncState:
        return _SyncState(
            mailbox=self._mailbox,
            connector_id=self._config.connector_id,
            feature_group=self._config.feature_group_name,
            query=self._config.query,
            labels=sorted(self._config.labels),
        )

    def _incremental(self) -> _RunResult:
        expected = self._expected_state()
        state, seq = _read_state(self._target)
        if state is not None and state._identity() != expected._identity():
            raise _GmailSyncError(
                f"The feature group {self._config.feature_group_name} was filled for a different mailbox, "
                "query or labels. Run a full load to rebuild it, or sync into a new feature group."
            )
        if state is None:
            state = expected
        if state.cursor_ms is not None:
            lower, inclusive, seen = state.cursor_ms, False, set(state.cursor_ids)
        else:
            lower, inclusive, seen = self._config.initial_value, True, set()
        result = _RunResult(0, [], 0, state.cursor_ms)
        for batch in self._windows(lower, inclusive, seen, self._target, result):
            self._writer._upsert(batch, overwrite=False)
            _advance(state, batch)
            seq += 1
            _write_state(self._target, state, seq)
            self._log_progress(result, state)
        self._compact_ended_days(state, seq)
        result.cursor_ms = state.cursor_ms
        return result

    def _full_load(self) -> _RunResult:
        state = self._expected_state()
        staging = self._staging_root() / self._execution_id
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        result = _RunResult(0, [], 0, None)
        batches = self._windows(
            self._config.initial_value, True, set(), staging, result
        )
        for i, batch in enumerate(batches):
            self._writer._upsert(batch, overwrite=i == 0)
            _advance(state, batch)
            self._log_progress(result, state)
        if not result.copied:
            _logger.info("No messages matched; the feature group is left as it was")
        _write_state(staging, state, 1)
        self._publish(staging)
        self._compact_ended_days(state, 1)
        result.cursor_ms = state.cursor_ms
        return result

    def _windows(
        self,
        lower_ms: int | None,
        inclusive: bool,
        seen: set[str],
        attachments_root: Path,
        result: _RunResult,
    ) -> Iterator[list[dict]]:
        """Upsert batches, oldest first, of the messages from `lower_ms` up to the run horizon.

        The range is read in `WINDOW_MS` windows and a batch is only cut from fully read windows, so every batch ends at a point the cursor can safely move to.
        Without a lower bound the first window reaches back to the oldest message Gmail lists.
        """
        upper_ms = int(self._clock() * 1000) - RUN_HORIZON_MS
        start = lower_ms if lower_ms is not None else self._oldest(upper_ms)
        edges = (
            list(range(start + WINDOW_MS, upper_ms, WINDOW_MS))
            if start is not None
            else []
        )
        pending: list[dict] = []
        window_lower = lower_ms
        for window_upper in [*edges, upper_ms]:
            rows, skipped = self._fetch(
                window_lower, inclusive, seen, attachments_root, window_upper
            )
            result.skipped_deleted += skipped
            for day in sorted(rows):
                pending.extend(rows[day])
                if day not in result.days:
                    result.days.append(day)
            while len(pending) >= UPSERT_BATCH:
                batch, pending = pending[:UPSERT_BATCH], pending[UPSERT_BATCH:]
                result.copied += len(batch)
                yield batch
            # A message on the edge belongs to this window; the next one skips it as a cursor would.
            seen = {
                row["message_id"]
                for day_rows in rows.values()
                for row in day_rows
                if row["internal_date"] == window_upper
            }
            window_lower, inclusive = window_upper, False
        if pending:
            result.copied += len(pending)
            yield pending
        result.days.sort()

    def _oldest(self, upper_ms: int) -> int | None:
        """The receive time of the last message Gmail lists, which lists newest first, or None for no messages."""
        ids = self._client._list_message_ids(
            _gmail_query(self._config.query, None, upper_ms), self._config.labels
        )
        last = collections.deque(ids, maxlen=1)
        message = self._client._get_raw_message(last[0]) if last else None
        return int(message["internalDate"]) if message else None

    def _log_progress(self, result: _RunResult, state: _SyncState) -> None:
        _logger.info(
            "Upserted %d messages so far, through %s",
            result.copied,
            _iso_utc(state.cursor_ms),
        )

    def _compact_ended_days(self, state: _SyncState, seq: int) -> None:
        """Compact every written day that has ended in UTC, and checkpoint each one done.

        An hourly job leaves a file per run in the current day; the first run after midnight compacts the day before.
        A day still under way is left alone, so its files are rewritten once, not every hour.
        """
        today = _day_of(int(self._clock() * 1000))
        for day in sorted(set(state.uncompacted_days)):
            if day >= today:
                continue
            self._writer._compact(day)
            state.uncompacted_days = [d for d in state.uncompacted_days if d != day]
            seq += 1
            _write_state(self._target, state, seq)
            _logger.info("Compacted %s", day)

    def _staging_root(self) -> Path:
        return self._target.with_name(self._target.name + STAGING_SUFFIX)

    def _previous_root(self) -> Path:
        return self._target.with_name(self._target.name + PREVIOUS_SUFFIX)

    def _publish(self, staging: Path) -> None:
        """Swap the new attachments directory in; the old one is kept until the swap is done."""
        previous = self._previous_root() / self._execution_id
        if self._target.exists():
            previous.parent.mkdir(parents=True, exist_ok=True)
            os.replace(self._target, previous)
        os.replace(staging, self._target)
        shutil.rmtree(self._previous_root(), ignore_errors=True)
        shutil.rmtree(self._staging_root(), ignore_errors=True)

    def _recover_publication(self) -> None:
        """Undo what an execution that died during a full load left behind."""
        previous_root = self._previous_root()
        if not self._target.exists() and previous_root.is_dir():
            generations = sorted(
                previous_root.iterdir(), key=lambda p: p.stat().st_mtime
            )
            if generations:
                _logger.info(
                    "Restoring %s, interrupted while a full load replaced it",
                    self._config.path,
                )
                os.replace(generations[-1], self._target)
        shutil.rmtree(previous_root, ignore_errors=True)
        shutil.rmtree(self._staging_root(), ignore_errors=True)

    def _fetch(
        self,
        lower_ms: int | None,
        inclusive: bool,
        seen: set[str],
        attachments_root: Path,
        upper_ms: int,
    ) -> tuple[dict[str, list[dict]], int]:
        """The messages in (lower_ms, upper_ms], or [lower_ms, upper_ms] when inclusive, grouped by day and sorted.

        Attachments are written under `attachments_root` as messages arrive, and rows are spilled to scratch files per day, so memory holds one message per worker.
        """
        query = _gmail_query(self._config.query, lower_ms, upper_ms)
        ids = list(self._client._list_message_ids(query, self._config.labels))
        if ids:
            _logger.info(
                "Gmail listed %d candidate messages up to %s",
                len(ids),
                _iso_utc(upper_ms),
            )
        scratch = (
            Path(self._scratch)
            if self._scratch
            else Path(os.environ.get("TMPDIR", "/tmp"))
        )
        spill_dir = scratch / f"gmail_sync_{uuid.uuid4().hex}"
        spill_dir.mkdir(parents=True)
        skipped = 0
        try:
            with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
                for start in range(0, len(ids), FETCH_BATCH):
                    batch = ids[start : start + FETCH_BATCH]
                    for message in pool.map(self._client._get_raw_message, batch):
                        if message is None:
                            skipped += 1
                            continue
                        if not _in_window(message, lower_ms, inclusive, seen, upper_ms):
                            continue
                        row = _parse_message(message, attachments_root, self._prefix)
                        with open(
                            spill_dir / f"{row['day']}.jsonl", "a", encoding="utf-8"
                        ) as f:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            rows: dict[str, list[dict]] = {}
            for spill in spill_dir.iterdir():
                with open(spill, encoding="utf-8") as f:
                    day_rows = [json.loads(line) for line in f]
                day_rows.sort(key=lambda r: (r["internal_date"], r["message_id"]))
                rows[spill.stem] = day_rows
            return rows, skipped
        finally:
            shutil.rmtree(spill_dir, ignore_errors=True)


def _advance(state: _SyncState, batch: list[dict]) -> None:
    """Move the cursor to the newest message of `batch`, which is sorted oldest first, and note the days it wrote."""
    state.uncompacted_days = sorted(
        set(state.uncompacted_days) | {r["day"] for r in batch}
    )
    last = batch[-1]["internal_date"]
    ids_at_last = [r["message_id"] for r in batch if r["internal_date"] == last]
    if last == state.cursor_ms:
        ids_at_last = sorted(set(ids_at_last) | set(state.cursor_ids))
    state.cursor_ms, state.cursor_ids = last, ids_at_last


def _in_window(
    message: dict, lower_ms: int | None, inclusive: bool, seen: set[str], upper_ms: int
) -> bool:
    internal = int(message["internalDate"])
    if internal > upper_ms:
        return False
    if lower_ms is None or internal > lower_ms:
        return True
    if internal < lower_ms:
        return False
    # At the cursor's millisecond: the previous run copied exactly the ids it recorded there.
    return inclusive or message["id"] not in seen


# endregion

# region Job entry point


def _project_relative(path: str) -> str:
    return re.sub(r"^(hdfs://[^/]*)?/Projects/[^/]+/", "", path).strip("/")


def _token_provider(connector) -> Callable[[], tuple[str, int | None]]:
    def provide() -> tuple[str, int | None]:
        token = connector.get_access_token()
        return token.token, token.expires_at

    return provide


def _execution_liveness(project, job_name: str) -> Callable[[str], bool | None]:
    def is_running(holder: str) -> bool | None:
        if not holder.isdigit():
            return None
        try:
            job = project.get_job_api().get_job(job_name)
            for execution in job.get_executions():
                if str(execution.id) == holder:
                    return execution.success is None
            return False
        except Exception:
            # Unknown, not stale: the lease then falls back to its age.
            return None

    return is_running


def _main(argv: list[str] | None = None) -> None:
    """Run the sync job named by `--config`; the scheduler's `-start_time` argument is ignored."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(
        description="Copy a Gmail mailbox into a feature group"
    )
    parser.add_argument("--config", required=True)
    args, _ = parser.parse_known_args(argv)

    fs_root = Path(os.environ.get("HOPSFS_MOUNT", "/hopsfs"))
    if not fs_root.is_dir():
        raise _GmailSyncError(
            f"The HopsFS mount {fs_root} is missing; Gmail sync jobs need mount_hopsfs_in_python_job enabled"
        )
    config = _SyncConfig.from_json(
        json.loads((fs_root / _project_relative(args.config)).read_text())
    )

    import hopsworks

    project = hopsworks.login()
    fs = project.get_feature_store()
    connector = fs.get_data_source(config.connector_name).storage_connector
    if connector.id != config.connector_id or connector.type != "GMAIL":
        raise _GmailSyncError(
            f"Data source {config.connector_name} is no longer the one this job was created for; "
            "recreate the sync job"
        )
    writer = _FeatureGroupWriter(
        _feature_group(fs, config.feature_group_name, connector.mailbox)
    )
    execution_id = (
        os.environ.get("HOPSWORKS_JOB_EXECUTION_ID") or f"local-{uuid.uuid4().hex}"
    )
    try:
        result = _SyncRun(
            config,
            connector.mailbox,
            _GmailClient(connector.mailbox, _token_provider(connector)),
            writer,
            fs_root,
            f"/Projects/{project.name}",
            execution_id,
            _execution_liveness(project, config.job_name),
        )._run()
    except _LeaseHeld as e:
        # A scheduled run that fires during a long backfill has nothing to do; it is not a failure.
        _logger.info("%s; this run does nothing", e)
        return
    _logger.info(
        "Upserted %d messages into %s across %d days (%d deleted while listed); cursor %s",
        result.copied,
        config.feature_group_name,
        len(result.days),
        result.skipped_deleted,
        _iso_utc(result.cursor_ms) if result.cursor_ms is not None else "none",
    )


if __name__ == "__main__":
    _main()

# endregion
