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
"""Sync job of Outlook data sources.

Reads the mailbox through Microsoft Graph for `mail_sync_job`, which describes what a sync run writes.
Each message is read as its MIME source, so the columns parsed from it match a Gmail feature group's.
Messages are keyed by their immutable Graph id, which survives a move to another folder.
Windows select on `receivedDateTime`, when the message reached the mailbox, which is also the `day` partition.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hsfs.core import mail_sync_job as m


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


GRAPH_API = "https://graph.microsoft.com/v1.0/"
STATE_DIR = "_outlook_sync_state"
# Without chosen folders, every folder is copied but these; Graph accepts their well-known names in place of ids.
EXCLUDED_FOLDERS = ("deleteditems", "junkemail", "drafts", "outbox")
# Graph serves at most 1000 messages per page.
PAGE_SIZE = 500
LISTED_FIELDS = (
    "id,conversationId,changeKey,receivedDateTime,categories,parentFolderId,bodyPreview"
)


class _GraphClient(m._RestClient):
    """Read-only Microsoft Graph client for one mailbox.

    `user_path` is `users/<mailbox>`, for delegated and app-only tokens alike, so a token for another account fails instead of reading that account.
    Every request asks for immutable ids, so the ids listed, fetched and stored agree.
    """

    service = "Outlook"

    def __init__(
        self,
        user_path: str,
        token_provider: Callable[[], tuple[str, int | None]],
        session: Any = None,
        sleep: Callable[[float], None] = m.time.sleep,
        clock: Callable[[], float] = m.time.time,
    ):
        super().__init__(
            GRAPH_API + user_path,
            token_provider,
            session,
            sleep,
            clock,
            headers={"Prefer": 'IdType="ImmutableId"'},
            api_root=GRAPH_API,
        )

    def _is_rate_limit(self, status: int, response: Any) -> bool:
        # Graph throttles with 429, and an overloaded mailbox server answers 503 with a Retry-After.
        return status == 429 or (status == 503 and m._retry_after(response) is not None)

    def _pages(self, path: str, params: dict[str, Any]) -> Iterator[dict]:
        """The items of a collection, following `@odata.nextLink`, which carries the query on."""
        page = self._collection(path, params)
        while True:
            yield from page.get("value", [])
            next_link = page.get("@odata.nextLink")
            if not next_link:
                return
            page = self._collection(next_link)

    def _collection(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """A page of a collection.

        Raises:
            _MailSyncError: When the collection is gone, such as a selected folder deleted in Outlook; listing it as empty would move the cursor past mail never copied.
        """
        page = self._get(path, params)
        if page is None:
            raise m._MailSyncError(
                f"Outlook has no {path.split('?', 1)[0]} in this mailbox; "
                "if a selected folder was deleted or moved, edit the sync job's folders"
            )
        return page


def _graph_time(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms // 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _epoch_ms(graph_time: str) -> int:
    parsed = datetime.fromisoformat(graph_time.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _quote(graph_id: str) -> str:
    return urllib.parse.quote(graph_id, safe="")


class _OutlookSource:
    """The messages of an Outlook mailbox in a sync job's folders, or in every folder but the excluded ones.

    Listing returns each message's metadata, which is kept until the message is fetched, so a message costs one request for its MIME source.
    """

    service = "Outlook"
    state_dir = STATE_DIR

    def __init__(
        self, client: _GraphClient, folder_ids: list[str], scratch: Path | None = None
    ):
        self._client = client
        self._folder_ids = folder_ids
        self._listed: dict[str, dict] = {}
        self._excluded: set[str] | None = None
        self._folder_paths: dict[str, str] | None = None
        self._folder_parents: dict[str, str | None] = {}
        # Messages are streamed here, so fetching a batch holds paths rather than whole messages in memory.
        self._downloads = Path(
            tempfile.mkdtemp(prefix="outlook_messages_", dir=scratch)
        )

    def _collections(self) -> list[str]:
        if self._folder_ids:
            return [f"/mailFolders/{_quote(f)}/messages" for f in self._folder_ids]
        return ["/messages"]

    def _list(self, lower_ms: int | None, upper_ms: int) -> list[str]:
        """Ids of the messages received in the window, widened by a second on each side; the run skips the ids the previous window listed."""
        clauses = []
        if lower_ms is not None:
            clauses.append(f"receivedDateTime ge {_graph_time(lower_ms - 1000)}")
        clauses.append(f"receivedDateTime le {_graph_time(upper_ms + 1000)}")
        clauses.append("isDraft eq false")
        params = {
            "$filter": " and ".join(clauses),
            "$select": LISTED_FIELDS,
            "$top": PAGE_SIZE,
        }
        excluded = set() if self._folder_ids else self._excluded_folders()
        # The previous window's messages are fetched by now; its metadata is no longer needed.
        self._listed = {}
        for path in self._collections():
            for message in self._client._pages(path, params):
                if message.get("parentFolderId") not in excluded:
                    self._listed[message["id"]] = message
        return list(self._listed)

    def _oldest(self, upper_ms: int) -> int | None:
        """The `receivedDateTime` of the oldest message in the folders, or None for no messages.

        Excluded folders are not filtered out here: an earlier start only adds empty windows.
        """
        oldest = None
        for path in self._collections():
            page = self._client._collection(
                path,
                {
                    "$orderby": "receivedDateTime asc",
                    "$top": 1,
                    "$select": "receivedDateTime",
                },
            )
            for message in page.get("value", []):
                received = _epoch_ms(message["receivedDateTime"])
                if received <= upper_ms:
                    oldest = received if oldest is None else min(oldest, received)
        return oldest

    def _excluded_folders(self) -> set[str]:
        """The excluded folders and every folder below them, such as a folder deleted in Outlook, which moves under Deleted Items."""
        if self._excluded is None:
            roots = set()
            for name in EXCLUDED_FOLDERS:
                folder = self._client._get(f"/mailFolders/{name}", {"$select": "id"})
                # A mailbox can lack a well-known folder; Graph answers 404 for it.
                if folder is not None:
                    roots.add(folder["id"])
            self._paths()
            self._excluded = {
                folder_id
                for folder_id in self._folder_parents
                if self._below(folder_id, roots)
            } | roots
        return self._excluded

    def _below(self, folder_id: str | None, roots: set[str]) -> bool:
        while folder_id is not None:
            if folder_id in roots:
                return True
            folder_id = self._folder_parents.get(folder_id)
        return False

    def _paths(self) -> dict[str, str]:
        """Folder paths by id, such as `Inbox/Projects`, read once per run."""
        if self._folder_paths is None:
            self._folder_paths = {}
            pending: list[tuple[str, str, str | None]] = [("/mailFolders", "", None)]
            while pending:
                path, parent, parent_id = pending.pop()
                for folder in self._client._pages(
                    path,
                    {
                        "$top": 100,
                        "$select": "id,displayName,childFolderCount",
                    },
                ):
                    name = f"{parent}/{folder['displayName']}".lstrip("/")
                    self._folder_paths[folder["id"]] = name
                    self._folder_parents[folder["id"]] = parent_id
                    if folder.get("childFolderCount"):
                        pending.append(
                            (
                                f"/mailFolders/{_quote(folder['id'])}/childFolders",
                                name,
                                folder["id"],
                            )
                        )
        return self._folder_paths

    def _get(self, message_id: str) -> tuple[dict, Path] | None:
        """The message's listed metadata and the file its MIME source was streamed to; `_row` removes the file."""
        download = self._downloads / f"{os.urandom(8).hex()}.eml"
        if not self._client._get(
            f"/messages/{_quote(message_id)}/$value", to_file=download
        ):
            download.unlink(missing_ok=True)
            return None
        return self._listed.get(message_id, {"id": message_id}), download

    def _row(
        self, message: tuple[dict, Path], attachments_root: Path | None, prefix: str
    ) -> dict:
        metadata, download = message
        try:
            raw = download.read_bytes()
        finally:
            download.unlink(missing_ok=True)
        return _parse_message(metadata, raw, self._paths(), attachments_root, prefix)

    def _close(self) -> None:
        shutil.rmtree(self._downloads, ignore_errors=True)


def _parse_message(
    metadata: dict,
    raw: bytes,
    folder_paths: dict[str, str],
    attachments_root: Path | None,
    attachments_prefix: str,
) -> dict:
    """One feature group row from a message's listed metadata and MIME source, writing its attachments as `mail_sync_job._parse_mime` does.

    `labels` holds the path of the message's folder, then its categories.
    """
    row = m._parse_mime(
        raw,
        metadata["id"],
        _epoch_ms(metadata["receivedDateTime"]),
        attachments_root,
        attachments_prefix,
    )
    folder = folder_paths.get(metadata.get("parentFolderId") or "")
    row.update(
        {
            "thread_id": metadata.get("conversationId"),
            "history_id": metadata.get("changeKey"),
            "labels": ([folder] if folder else [])
            + list(metadata.get("categories") or []),
            "snippet": metadata.get("bodyPreview") or "",
            "size_estimate": len(raw),
        }
    )
    return {name: row.get(name) for name, _ in m.COLUMNS}


def _user_path(connector) -> str:
    """The Graph path of the data source's mailbox.

    A delegated token is sent to the mailbox's path too, not `me`, so a token of another account fails with 403 or 404 instead of copying that account's mail; it also reaches a shared mailbox the account may read.
    """
    return "users/" + _quote(connector.mailbox)


def _source(connector, config: m._SyncConfig) -> _OutlookSource:
    return _OutlookSource(
        _GraphClient(_user_path(connector), m._token_provider(connector)),
        config.labels,
    )


def _main(argv: list[str] | None = None) -> None:
    """Run the Outlook sync job named by `--config`."""
    m._main(argv, "OUTLOOK", "Outlook", _source)


if __name__ == "__main__":
    _main()
