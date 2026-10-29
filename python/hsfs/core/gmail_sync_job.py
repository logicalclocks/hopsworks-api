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

Reads the mailbox through the Gmail API for `mail_sync_job`, which describes what a sync run writes.
The `day` partition is the UTC date of Gmail's `internalDate`, which is the sender's `Date` for imported mail.
Windows select on when Gmail received a message, so an incremental run copies what arrived since the previous one whatever its `internalDate`.
"""

from __future__ import annotations

import base64
import collections
import html
from typing import TYPE_CHECKING, Any

from hsfs.core import mail_sync_job as m


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/"
STATE_DIR = "_gmail_sync_state"


class _GmailClient(m._RestClient):
    """Read-only Gmail REST client for one mailbox."""

    service = "Gmail"

    def __init__(
        self,
        mailbox: str,
        token_provider: Callable[[], tuple[str, int | None]],
        session: Any = None,
        sleep: Callable[[float], None] = m.time.sleep,
        clock: Callable[[], float] = m.time.time,
    ):
        super().__init__(GMAIL_API + mailbox, token_provider, session, sleep, clock)

    def _is_rate_limit(self, status: int, response: Any) -> bool:
        if status == 429:
            return True
        # Gmail reports per-user and per-project rate limits as 403 with these reasons.
        return status == 403 and self._error_reason(response) in (
            "rateLimitExceeded",
            "userRateLimitExceeded",
        )

    def _error_reason(self, response: Any) -> str:
        try:
            error = response.json().get("error", {})
            errors = error.get("errors") or [{}]
            return (
                errors[0].get("reason") or error.get("status") or str(error.get("code"))
            )
        except (ValueError, AttributeError):
            return "unknown"

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


def _parse_message(
    message: dict, attachments_root: Path | None, attachments_prefix: str
) -> dict:
    """One feature group row from a Gmail message in `raw` format, writing its attachments as `mail_sync_job._parse_mime` does."""
    raw = base64.urlsafe_b64decode(message["raw"] + "=" * (-len(message["raw"]) % 4))
    row = m._parse_mime(
        raw,
        message["id"],
        int(message["internalDate"]),
        attachments_root,
        attachments_prefix,
    )
    row.update(
        {
            "thread_id": message.get("threadId"),
            "history_id": message.get("historyId"),
            "labels": list(message.get("labelIds") or []),
            "snippet": html.unescape(message.get("snippet") or ""),
            "size_estimate": message.get("sizeEstimate"),
        }
    )
    return {name: row.get(name) for name, _ in m.COLUMNS}


def _gmail_query(user_query: str, lower_ms: int | None, upper_ms: int) -> str:
    """The Gmail search for a window: the user's query, without drafts, widened by a second on each side.

    Gmail's `after:` and `before:` take whole seconds; the run skips the ids the previous window listed.
    """
    parts = []
    if user_query.strip():
        parts.append(f"({user_query.strip()})")
    parts.append("-in:drafts")
    if lower_ms is not None:
        parts.append(f"after:{lower_ms // 1000 - 1}")
    parts.append(f"before:{upper_ms // 1000 + 1}")
    return " ".join(parts)


class _GmailSource:
    """The messages of a Gmail mailbox that a sync job's query and labels select."""

    service = "Gmail"
    state_dir = STATE_DIR

    def __init__(self, client: _GmailClient, query: str, labels: list[str]):
        self._client = client
        self._query = query
        self._labels = labels

    def _label_sets(self) -> list[list[str]]:
        # Gmail's labelIds selects the messages carrying every label given, so each label is listed on its own.
        return [[label] for label in self._labels] or [[]]

    def _list(self, lower_ms: int | None, upper_ms: int) -> list[str]:
        """Ids of the messages Gmail received in the window that carry any of the configured labels, or of all of them without labels.

        Gmail's `after:` and `before:` search on when Gmail received a message, which can be days away from its `internalDate`.
        """
        query = _gmail_query(self._query, lower_ms, upper_ms)
        return list(
            dict.fromkeys(
                message_id
                for labels in self._label_sets()
                for message_id in self._client._list_message_ids(query, labels)
            )
        )

    def _oldest(self, upper_ms: int) -> int | None:
        """The `internalDate` of the last message each label lists, or None for no messages.

        Gmail lists newest first, so this is about the oldest message; the first window is open below, so a message before it is still copied.
        """
        query = _gmail_query(self._query, None, upper_ms)
        oldest = None
        for labels in self._label_sets():
            last = collections.deque(
                self._client._list_message_ids(query, labels), maxlen=1
            )
            message = self._client._get_raw_message(last[0]) if last else None
            if message is not None:
                received = int(message["internalDate"])
                oldest = received if oldest is None else min(oldest, received)
        return oldest

    def _get(self, message_id: str) -> dict | None:
        return self._client._get_raw_message(message_id)

    def _row(self, message: dict, attachments_root: Path | None, prefix: str) -> dict:
        return _parse_message(message, attachments_root, prefix)


def _source(connector, config: m._SyncConfig) -> _GmailSource:
    return _GmailSource(
        _GmailClient(connector.mailbox, m._token_provider(connector)),
        config.query,
        config.labels,
    )


def _main(argv: list[str] | None = None) -> None:
    """Run the Gmail sync job named by `--config`."""
    m._main(argv, "GMAIL", "Gmail", _source)


if __name__ == "__main__":
    _main()
