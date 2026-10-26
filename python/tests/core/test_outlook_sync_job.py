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

import re
from datetime import datetime, timezone
from email.message import EmailMessage

from hsfs.core import mail_sync_job as m
from hsfs.core import outlook_sync_job as o


NOW_MS = int(datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
HOUR = 3_600_000
BASE = o.GRAPH_API + "me"
PROJECT_ROOT = "/Projects/mail"
FOLDERS = {
    "id-inbox": ("Inbox", None, "inbox"),
    "id-projects": ("Projects", "id-inbox", None),
    "id-sent": ("Sent Items", None, "sentitems"),
    "id-deleted": ("Deleted Items", None, "deleteditems"),
    "id-junk": ("Junk Email", None, "junkemail"),
}


def _mime(subject="Hello", attachments=()):
    message = EmailMessage()
    message["From"] = "Bob <bob@example.com>"
    message["To"] = "alice@contoso.com"
    message["Subject"] = subject
    message["Date"] = "Tue, 20 Oct 2026 23:59:59 +0000"
    message.set_content("Body text")
    for name, content in attachments:
        message.add_attachment(
            content, maintype="application", subtype="octet-stream", filename=name
        )
    return message.as_bytes()


def _iso(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


class Response:
    def __init__(self, status, body=None, content=b"", headers=None):
        self.status_code = status
        self._body = body
        self.content = content
        self.headers = headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("no JSON")
        return self._body


class FakeGraph:
    """The Graph calls a sync run makes, over an in-memory mailbox, as a requests session."""

    def __init__(self, page_size=2):
        self.messages = {}
        self.page_size = page_size
        self.requests = []
        self.throttle = []

    def add(self, message_id, received_ms, folder="id-inbox", draft=False, **kwargs):
        self.messages[message_id] = {
            "id": message_id,
            "conversationId": "c-" + message_id,
            "changeKey": "k-" + message_id,
            "receivedDateTime": _iso(received_ms),
            "categories": kwargs.pop("categories", []),
            "parentFolderId": folder,
            "bodyPreview": "Preview of " + message_id,
            "isDraft": draft,
            "_mime": _mime(**kwargs),
        }

    def get(self, url, params=None, headers=None, timeout=None):
        self.requests.append((url, dict(params or {}), dict(headers or {})))
        if self.throttle:
            return self.throttle.pop(0)
        path = url[len(BASE) :]
        match = re.fullmatch(r"/messages/([^/]+)/\$value", path)
        if match:
            message = self.messages.get(match.group(1))
            return (
                Response(404, {})
                if message is None
                else Response(200, content=message["_mime"])
            )
        match = re.fullmatch(r"/mailFolders/([a-z]+)", path)
        if match:
            for folder_id, (_, _, well_known) in FOLDERS.items():
                if well_known == match.group(1):
                    return Response(200, {"id": folder_id})
            return Response(404, {"error": {"code": "ErrorFolderNotFound"}})
        if path == "/mailFolders" or path.endswith("/childFolders"):
            parent = None if path == "/mailFolders" else path.split("/")[2]
            return Response(
                200,
                {
                    "value": [
                        {
                            "id": folder_id,
                            "displayName": name,
                            "childFolderCount": sum(
                                1 for f in FOLDERS.values() if f[1] == folder_id
                            ),
                        }
                        for folder_id, (name, p, _) in FOLDERS.items()
                        if p == parent
                    ]
                },
            )
        match = re.fullmatch(r"(?:/mailFolders/([^/]+))?/messages", path)
        if match:
            return self._list(match.group(1), params)
        match = re.fullmatch(r"https://next/(\d+)/(.*)", url)
        if match:
            return self._page(self._pending[match.group(2)], int(match.group(1)))
        raise AssertionError("unexpected request " + url)

    def _list(self, folder, params):
        selected = [
            msg
            for msg in self.messages.values()
            if folder is None or msg["parentFolderId"] == folder
        ]
        if "$orderby" in params:
            selected.sort(key=lambda msg: msg["receivedDateTime"])
            return Response(200, {"value": selected[:1]})
        expression = params["$filter"]
        for op, value in re.findall(r"receivedDateTime (ge|le) (\S+)", expression):
            if op == "ge":
                selected = [msg for msg in selected if msg["receivedDateTime"] >= value]
            else:
                selected = [msg for msg in selected if msg["receivedDateTime"] <= value]
        if "isDraft eq false" in expression:
            selected = [msg for msg in selected if not msg["isDraft"]]
        listed = [
            {k: v for k, v in msg.items() if not k.startswith("_") and k != "isDraft"}
            for msg in selected
        ]
        key = str(len(self.requests))
        self._pending = getattr(self, "_pending", {})
        self._pending[key] = listed
        return self._page(listed, 0, key)

    def _page(self, listed, start, key=None):
        if key is None:
            key = next(k for k, v in self._pending.items() if v is listed)
        body = {"value": listed[start : start + self.page_size]}
        if start + self.page_size < len(listed):
            body["@odata.nextLink"] = f"https://next/{start + self.page_size}/{key}"
        return Response(200, body)


class FakeWriter:
    def __init__(self):
        self.table = {}
        self.compacted = []

    def _compact(self, days):
        self.compacted.extend(days)

    def _upsert(self, rows, overwrite):
        if overwrite:
            self.table = {}
        for row in rows:
            self.table[row["message_id"]] = row


def _config(**kwargs):
    values = {
        "schemaVersion": 2,
        "jobName": "outlook_sync",
        "connectorId": 7,
        "connectorName": "outlook",
        "featureGroupName": "alice_contosos",
        "path": "Resources/outlook",
        "loadingStrategy": "INCREMENTAL_TIMESTAMP",
    }
    values.update(kwargs)
    return m._SyncConfig.from_json(values)


def _run(tmp_path, graph, writer, config=None, now_ms=NOW_MS, sleeps=None):
    (tmp_path / "Resources").mkdir(exist_ok=True)
    config = config or _config()
    clock = [now_ms / 1000]

    def sleep(seconds):
        if sleeps is not None:
            sleeps.append(seconds)
        clock[0] += seconds

    client = o._GraphClient(
        "me",
        lambda: ("token", None),
        session=graph,
        sleep=sleep,
        clock=lambda: clock[0],
    )
    return m._SyncRun(
        config,
        "alice@contoso.com",
        o._OutlookSource(client, config.labels),
        writer,
        tmp_path,
        PROJECT_ROOT,
        "1",
        lambda h: False,
        clock=lambda: now_ms / 1000,
        scratch=tmp_path / "scratch",
    )._run()


def test_a_backfill_copies_every_folder_but_the_excluded_ones(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 400 * 24 * HOUR, categories=["Red"])
    graph.add("b", NOW_MS - 3 * HOUR, folder="id-projects")
    graph.add("c", NOW_MS - 2 * HOUR, folder="id-sent")
    graph.add("trash", NOW_MS - 2 * HOUR, folder="id-deleted")
    graph.add("spam", NOW_MS - 2 * HOUR, folder="id-junk")
    graph.add("draft", NOW_MS - 2 * HOUR, draft=True)
    writer = FakeWriter()
    result = _run(tmp_path, graph, writer)
    assert result.copied == 3
    assert sorted(writer.table) == ["a", "b", "c"]
    assert writer.table["a"]["labels"] == ["Inbox", "Red"]
    assert writer.table["b"]["labels"] == ["Inbox/Projects"]
    assert writer.table["c"]["labels"] == ["Sent Items"]


def test_a_row_has_the_columns_of_a_gmail_row(tmp_path):
    graph = FakeGraph()
    received = NOW_MS - 3 * HOUR
    graph.add("a", received, attachments=[("report.pdf", b"%PDF")])
    writer = FakeWriter()
    _run(tmp_path, graph, writer)
    row = writer.table["a"]
    assert list(row) == [name for name, _ in m.COLUMNS]
    assert row["internal_date"] == received // 1000 * 1000
    assert (row["day"], row["hour"]) == ("2026-10-25", "09")
    assert row["thread_id"] == "c-a"
    assert row["history_id"] == "k-a"
    assert row["snippet"] == "Preview of a"
    assert row["subject"] == "Hello"
    assert row["from_address"] == "Bob <bob@example.com>"
    assert row["body_text"] == "Body text"
    assert row["size_estimate"] == len(graph.messages["a"]["_mime"])
    assert row["attachments"] == [
        "/Projects/mail/Resources/outlook/day=2026-10-25/a/report.pdf"
    ]
    assert (
        tmp_path / "Resources/outlook/day=2026-10-25/a/report.pdf"
    ).read_bytes() == b"%PDF"


def test_chosen_folders_are_ored(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 3 * HOUR)
    graph.add("b", NOW_MS - 3 * HOUR, folder="id-sent")
    graph.add("c", NOW_MS - 3 * HOUR, folder="id-projects")
    writer = FakeWriter()
    _run(tmp_path, graph, writer, _config(labels=["id-inbox", "id-sent"]))
    assert sorted(writer.table) == ["a", "b"]


def test_an_incremental_run_copies_only_new_mail_and_a_moved_message_once(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 3 * HOUR)
    writer = FakeWriter()
    _run(tmp_path, graph, writer)
    graph.messages["a"]["parentFolderId"] = "id-projects"
    graph.add("b", NOW_MS + HOUR)
    second = _run(tmp_path, graph, writer, now_ms=NOW_MS + 2 * HOUR)
    assert second.copied == 1
    assert sorted(writer.table) == ["a", "b"]


def test_every_request_asks_for_immutable_ids_and_pages_are_followed(tmp_path):
    graph = FakeGraph(page_size=1)
    for i in range(3):
        graph.add(str(i), NOW_MS - (i + 2) * HOUR)
    writer = FakeWriter()
    _run(tmp_path, graph, writer)
    assert sorted(writer.table) == ["0", "1", "2"]
    assert all(h["Prefer"] == 'IdType="ImmutableId"' for _, _, h in graph.requests)
    assert any(url.startswith("https://next/") for url, _, _ in graph.requests)


def test_throttled_requests_are_retried_until_served(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 3 * HOUR)
    graph.throttle = [
        Response(
            429,
            {"error": {"code": "ApplicationThrottled"}},
            headers={"Retry-After": "7"},
        )
    ] * 50 + [
        Response(503, {"error": {"code": "ServerBusy"}}, headers={"Retry-After": "3"})
    ]
    sleeps = []
    writer = FakeWriter()
    _run(tmp_path, graph, writer, sleeps=sleeps)
    assert list(writer.table) == ["a"]
    assert sleeps.count(7.0) == 50
    assert 3.0 in sleeps


def test_a_message_deleted_after_listing_is_skipped(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 3 * HOUR)
    graph.add("b", NOW_MS - 3 * HOUR)
    source_get = o._OutlookSource._get

    def delete_b_first(self, message_id):
        graph.messages.pop("b", None)
        return source_get(self, message_id)

    o._OutlookSource._get = delete_b_first
    try:
        result = _run(tmp_path, graph, FakeWriter())
    finally:
        o._OutlookSource._get = source_get
    assert (result.copied, result.skipped_deleted) == (1, 1)


def test_the_cursor_lives_in_its_own_state_directory(tmp_path):
    graph = FakeGraph()
    graph.add("a", NOW_MS - 3 * HOUR)
    _run(tmp_path, graph, FakeWriter())
    state, _ = m._read_state(tmp_path / "Resources" / "outlook", o.STATE_DIR)
    assert state.cursor_ms == NOW_MS - m.RUN_HORIZON_MS


def test_the_mailbox_path_follows_the_auth_type():
    delegated = type("C", (), {"auth_type": "OAUTH", "mailbox": "a@contoso.com"})()
    app = type(
        "C", (), {"auth_type": "CLIENT_CREDENTIALS", "mailbox": "a@contoso.com"}
    )()
    assert o._user_path(delegated) == "me"
    assert o._user_path(app) == "users/a%40contoso.com"
