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

import base64
import json
import logging
from datetime import datetime, timezone
from email.message import EmailMessage

import pytest
from hsfs.core import gmail_sync_job as g


NOW_MS = int(datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
DAY1 = int(
    datetime(2026, 10, 20, 23, 59, 59, 999000, tzinfo=timezone.utc).timestamp() * 1000
)
DAY2 = DAY1 + 1
HOUR = 3_600_000
PROJECT_ROOT = "/Projects/mail"


def _raw(
    subject="Hello",
    body="Body text",
    sender="Bob <bob@example.com>",
    to="alice@example.com",
    html=None,
    attachments=(),
    charset=None,
):
    message = EmailMessage()
    message["From"] = sender
    message["To"] = to
    message["Subject"] = subject
    message["Date"] = "Tue, 20 Oct 2026 23:59:59 +0000"
    if html is not None:
        message.set_content(html, subtype="html")
    elif charset is not None:
        message.set_content(
            body.encode(charset),
            maintype="text",
            subtype="plain",
            cte="8bit",
            params={"charset": charset},
        )
    else:
        message.set_content(body)
    for name, content in attachments:
        message.add_attachment(
            content, maintype="application", subtype="octet-stream", filename=name
        )
    return base64.urlsafe_b64encode(message.as_bytes()).decode().rstrip("=")


def _message(message_id, internal_ms, labels=("INBOX",), **kwargs):
    return {
        "id": message_id,
        "threadId": "t-" + message_id,
        "historyId": "100",
        "internalDate": str(internal_ms),
        "labelIds": list(labels),
        "snippet": "Snippet &amp; more",
        "sizeEstimate": 1234,
        "raw": _raw(**kwargs),
    }


class FakeGmail:
    """The two calls a sync run makes, over an in-memory mailbox."""

    def __init__(self, messages=()):
        self.messages = {m["id"]: m for m in messages}
        self.queries = []
        self.fail_ids = set()

    def add(self, *messages):
        for m in messages:
            self.messages[m["id"]] = m

    def _list_message_ids(self, query, label_ids):
        self.queries.append((query, list(label_ids)))
        return sorted(
            self.messages, key=lambda i: -int(self.messages[i]["internalDate"])
        )

    def _get_raw_message(self, message_id):
        if message_id in self.fail_ids:
            raise g._GmailSyncError("Gmail returned HTTP 400: failedPrecondition")
        return self.messages.get(message_id)


class FakeWriter:
    """A feature group that upserts by message id, as the Delta merge does."""

    def __init__(self):
        self.table = {}
        self.calls = []
        self.compacted = []

    def _compact(self, day):
        self.compacted.append(day)

    def _upsert(self, rows, overwrite):
        self.calls.append((len(rows), overwrite))
        if overwrite:
            self.table = {}
        for row in rows:
            self.table[row["message_id"]] = row


def _config(strategy="INCREMENTAL_TIMESTAMP", **kwargs):
    values = {
        "schemaVersion": 2,
        "jobName": "inbox_sync",
        "connectorId": 42,
        "connectorName": "inbox",
        "featureGroupName": "alice_examples",
        "path": "Resources/mail",
        "loadingStrategy": strategy,
    }
    values.update(kwargs)
    return g._SyncConfig.from_json(values)


def _run(
    tmp_path,
    client,
    config,
    writer,
    execution_id="7",
    is_running=lambda h: False,
    now_ms=NOW_MS,
):
    (tmp_path / "Resources").mkdir(exist_ok=True)
    return g._SyncRun(
        config,
        "alice@example.com",
        client,
        writer,
        tmp_path,
        PROJECT_ROOT,
        execution_id,
        is_running,
        clock=lambda: now_ms / 1000,
        scratch=tmp_path / "scratch",
    )._run()


class TestParseMessage:
    def test_columns(self):
        row = g._parse_message(
            _message("m1", DAY1, to="Alice <alice@example.com>, carol@example.com"),
            None,
            PROJECT_ROOT + "/Resources/mail",
        )
        assert [name for name, _ in g.COLUMNS] == list(row)
        assert row["subject"] == "Hello"
        assert row["from_address"] == "Bob <bob@example.com>"
        assert row["to_addresses"] == ["Alice <alice@example.com>", "carol@example.com"]
        assert row["date_header"] == "Tue, 20 Oct 2026 23:59:59 +0000"
        assert row["body_text"] == "Body text"
        assert row["snippet"] == "Snippet & more"
        assert (row["day"], row["hour"]) == ("2026-10-20", "23")

    def test_attachments_are_written_and_listed_by_full_path(self, tmp_path):
        root = tmp_path / "attachments"
        row = g._parse_message(
            _message(
                "m1",
                DAY1,
                attachments=[("report.pdf", b"%PDF"), ("report.pdf", b"%PDF-2")],
            ),
            root,
            PROJECT_ROOT + "/Resources/mail",
        )
        assert row["attachments"] == [
            "/Projects/mail/Resources/mail/day=2026-10-20/m1/report.pdf",
            "/Projects/mail/Resources/mail/day=2026-10-20/m1/report-2.pdf",
        ]
        assert (root / "day=2026-10-20" / "m1" / "report.pdf").read_bytes() == b"%PDF"
        assert (
            root / "day=2026-10-20" / "m1" / "report-2.pdf"
        ).read_bytes() == b"%PDF-2"

    @pytest.mark.parametrize(
        ("sent", "stored"),
        [
            ("../../etc/passwd", "_.._etc_passwd"),
            ("a/b\\c.txt", "a_b_c.txt"),
            ("..", "attachment"),
            ("x" * 300 + ".pdf", "x" * 196 + ".pdf"),
        ],
    )
    def test_attachment_names_stay_inside_their_directory(self, sent, stored):
        assert g._safe_name(sent) == stored

    def test_html_only_message_is_reduced_to_text(self):
        html = "<html><style>p{}</style><p>Hi <b>there</b></p><p>Second &amp; last</p></html>"
        row = g._parse_message(_message("m1", DAY1, html=html), None, PROJECT_ROOT)
        assert row["body_text"] == "Hi there\nSecond & last"

    def test_latin1_message_is_decoded(self):
        row = g._parse_message(
            _message("m1", DAY1, body="Grüße", charset="latin-1"), None, PROJECT_ROOT
        )
        assert row["body_text"] == "Grüße"

    def test_unknown_charset_keeps_what_decodes(self):
        message = _message("m1", DAY1)
        raw = base64.urlsafe_b64decode(message["raw"] + "==").replace(
            b'charset="utf-8"', b'charset="x-unknown"'
        )
        message["raw"] = base64.urlsafe_b64encode(raw).decode()
        assert "Body text" in g._parse_message(message, None, PROJECT_ROOT)["body_text"]


class TestIncremental:
    @pytest.mark.parametrize("strategy", ["INCREMENTAL_TIMESTAMP", "FULL_LOAD"])
    def test_directories_below_the_dataset_are_created(self, tmp_path, strategy):
        config = _config(strategy, path="Resources/gmail/alice_examples")
        _run(tmp_path, FakeGmail([_message("a", DAY1)]), config, FakeWriter())
        assert (tmp_path / "Resources" / "gmail" / "alice_examples").is_dir()

    def test_a_missing_dataset_is_refused(self, tmp_path):
        with pytest.raises(g._GmailSyncError, match="dataset Mail does not exist"):
            _run(
                tmp_path,
                FakeGmail([_message("a", DAY1)]),
                _config(path="Mail/alice"),
                FakeWriter(),
            )
        assert not (tmp_path / "Mail").exists()

    def test_later_runs_upsert_only_new_messages(self, tmp_path):
        client, writer = (
            FakeGmail([_message("a", DAY1), _message("b", DAY2)]),
            FakeWriter(),
        )
        assert _run(tmp_path, client, _config(), writer).copied == 2
        client.add(_message("c", DAY2 + 5_000))
        second = _run(tmp_path, client, _config(), writer, execution_id="8")
        assert second.copied == 1
        assert second.cursor_ms == DAY2 + 5_000
        assert _run(tmp_path, client, _config(), writer, execution_id="9").copied == 0
        assert sorted(writer.table) == ["a", "b", "c"]
        assert all(not overwrite for _, overwrite in writer.calls)

    def test_rows_carry_day_and_hour_partitions(self, tmp_path):
        client = FakeGmail([_message("a", DAY2), _message("b", DAY2 + HOUR)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        assert {(r["day"], r["hour"]) for r in writer.table.values()} == {
            ("2026-10-21", "00"),
            ("2026-10-21", "01"),
        }

    def test_query_excludes_drafts_and_overlaps_the_cursor_by_a_second(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(query="from:bob", labels=["INBOX"]), writer)
        _run(
            tmp_path,
            client,
            _config(query="from:bob", labels=["INBOX"]),
            writer,
            execution_id="8",
        )
        query, labels = client.queries[-1]
        assert query.startswith("(from:bob) -in:drafts after:")
        assert f"after:{DAY1 // 1000 - 1}" in query
        assert labels == ["INBOX"]

    def test_messages_at_the_cursors_millisecond_are_neither_lost_nor_duplicated(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(g, "UPSERT_BATCH", 500)
        client = FakeGmail([_message(f"m{i:04d}", DAY1) for i in range(1200)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        client.add(_message("late", DAY1))
        upserted_before = sum(n for n, _ in writer.calls)
        assert _run(tmp_path, client, _config(), writer, execution_id="8").copied == 1
        assert sum(n for n, _ in writer.calls) == upserted_before + 1
        assert len(writer.table) == 1201

    def test_initial_value_is_an_inclusive_lower_bound(self, tmp_path):
        client = FakeGmail(
            [_message("old", DAY1 - 1), _message("edge", DAY1), _message("new", DAY2)]
        )
        writer = FakeWriter()
        _run(tmp_path, client, _config(initialValue=DAY1), writer)
        assert sorted(writer.table) == ["edge", "new"]

    def test_messages_inside_the_run_horizon_wait_for_the_next_run(self, tmp_path):
        client, writer = FakeGmail([_message("a", NOW_MS - 10_000)]), FakeWriter()
        assert _run(tmp_path, client, _config(), writer).copied == 0
        later = _run(
            tmp_path,
            client,
            _config(),
            writer,
            execution_id="8",
            now_ms=NOW_MS + 120_000,
        )
        assert later.copied == 1

    def test_the_cursor_is_checkpointed_after_every_batch(self, tmp_path, monkeypatch):
        monkeypatch.setattr(g, "UPSERT_BATCH", 2)
        client = FakeGmail([_message(str(i), DAY1 + i * HOUR) for i in range(5)])
        writer = FakeWriter()
        real_write_state = g._write_state

        def crash_after_second_checkpoint(directory, state, seq):
            real_write_state(directory, state, seq)
            if seq == 2:
                raise OSError("pod killed")

        monkeypatch.setattr(g, "_write_state", crash_after_second_checkpoint)
        with pytest.raises(OSError):
            _run(tmp_path, client, _config(), writer)
        monkeypatch.setattr(g, "_write_state", real_write_state)
        upserts_before = len(writer.calls)
        retry = _run(tmp_path, client, _config(), writer, execution_id="8")
        assert retry.copied == 1
        assert len(writer.calls) == upserts_before + 1
        assert sorted(writer.table) == ["0", "1", "2", "3", "4"]

    def test_a_crash_after_an_upsert_is_repaired_by_the_retrys_upsert(
        self, tmp_path, monkeypatch
    ):
        client = FakeGmail([_message("a", DAY1), _message("b", DAY2)])
        writer = FakeWriter()
        monkeypatch.setattr(
            g, "_write_state", lambda *a: (_ for _ in ()).throw(OSError("killed"))
        )
        with pytest.raises(OSError):
            _run(tmp_path, client, _config(), writer)
        monkeypatch.undo()
        del client.messages["a"]
        client.add(_message("c", DAY2 + 1))
        _run(tmp_path, client, _config(), writer, execution_id="8")
        assert sorted(writer.table) == ["a", "b", "c"]

    def test_attachments_land_next_to_the_cursor_state(self, tmp_path):
        client = FakeGmail([_message("a", DAY1, attachments=[("x.bin", b"1")])])
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        mail = tmp_path / "Resources" / "mail"
        assert (mail / "day=2026-10-20" / "a" / "x.bin").read_bytes() == b"1"
        assert writer.table["a"]["attachments"] == [
            "/Projects/mail/Resources/mail/day=2026-10-20/a/x.bin"
        ]
        assert (mail / g.STATE_DIR).is_dir()

    def test_a_failed_fetch_commits_nothing(self, tmp_path):
        client = FakeGmail([_message("a", DAY1), _message("b", DAY2)])
        client.fail_ids.add("b")
        writer = FakeWriter()
        with pytest.raises(g._GmailSyncError):
            _run(tmp_path, client, _config(), writer)
        assert writer.calls == []
        assert not (tmp_path / "Resources" / "mail" / g.STATE_DIR).exists()

    def test_a_message_deleted_after_listing_is_skipped(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        original = client._get_raw_message
        client._get_raw_message = lambda i: None if i == "a" else original(i)
        assert _run(tmp_path, client, _config(), writer).skipped_deleted == 1

    @pytest.mark.parametrize(
        "changed", [{"labels": ["INBOX", "IMPORTANT"]}, {"featureGroupName": "other"}]
    )
    def test_a_changed_selection_or_feature_group_refuses_the_old_cursor(
        self, tmp_path, changed
    ):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(labels=["INBOX"]), writer)
        with pytest.raises(g._GmailSyncError, match="full load"):
            _run(
                tmp_path,
                client,
                _config(**{"labels": ["INBOX"], **changed}),
                writer,
                execution_id="8",
            )

    def test_an_unreadable_state_refuses_to_start_over(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(), writer)
        for state in (tmp_path / "Resources" / "mail" / g.STATE_DIR).iterdir():
            state.write_text("{not json")
        with pytest.raises(g._GmailSyncError, match="cannot be read"):
            _run(tmp_path, client, _config(), writer, execution_id="8")

    def test_checkpoints_keep_the_previous_version(self, tmp_path, monkeypatch):
        monkeypatch.setattr(g, "UPSERT_BATCH", 1)
        client = FakeGmail([_message(str(i), DAY1 + i * 86_400_000) for i in range(4)])
        _run(tmp_path, client, _config(), FakeWriter())
        names = sorted(
            p.name for p in (tmp_path / "Resources" / "mail" / g.STATE_DIR).iterdir()
        )
        # Four upsert checkpoints, then one per compacted day.
        assert names == ["7.json", "8.json"]


class TestCompaction:
    def test_a_day_is_compacted_by_the_first_run_after_it_ends(self, tmp_path):
        today = NOW_MS - 2 * HOUR
        client = FakeGmail([_message("a", today - 3 * HOUR), _message("b", today)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        assert writer.compacted == []
        client.add(_message("c", today + HOUR))
        _run(
            tmp_path,
            client,
            _config(),
            writer,
            execution_id="8",
            now_ms=NOW_MS + 2 * HOUR,
        )
        assert writer.compacted == []
        next_day = NOW_MS + 13 * HOUR
        _run(tmp_path, client, _config(), writer, execution_id="9", now_ms=next_day)
        assert writer.compacted == ["2026-10-25"]
        _run(
            tmp_path,
            client,
            _config(),
            writer,
            execution_id="10",
            now_ms=next_day + HOUR,
        )
        assert writer.compacted == ["2026-10-25"]

    def test_a_backfill_compacts_every_ended_day_but_today(self, tmp_path):
        client = FakeGmail(
            [_message("a", DAY1), _message("b", DAY2), _message("c", NOW_MS - 2 * HOUR)]
        )
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        assert writer.compacted == ["2026-10-20", "2026-10-21"]

    def test_a_failed_compaction_is_retried_by_the_next_run(self, tmp_path):
        client = FakeGmail([_message("a", DAY1)])
        writer = FakeWriter()

        def fail(day):
            raise OSError("namenode unavailable")

        writer._compact = fail
        with pytest.raises(OSError):
            _run(tmp_path, client, _config(), writer)
        writer._compact = writer.compacted.append
        _run(tmp_path, client, _config(), writer, execution_id="8")
        assert writer.compacted == ["2026-10-20"]
        assert sorted(writer.table) == ["a"]

    def test_a_full_load_compacts_its_ended_days(self, tmp_path):
        client = FakeGmail([_message("a", DAY1), _message("b", DAY2)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        assert writer.compacted == ["2026-10-20", "2026-10-21"]


class TestFullLoad:
    def test_a_full_load_overwrites_the_feature_group_and_the_attachments(
        self, tmp_path
    ):
        client = FakeGmail(
            [_message("a", DAY1, attachments=[("x.bin", b"1")]), _message("b", DAY2)]
        )
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        del client.messages["a"]
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer, execution_id="8")
        assert sorted(writer.table) == ["b"]
        assert writer.calls[-1][1] is True
        assert not (tmp_path / "Resources" / "mail" / "day=2026-10-20").exists()
        assert sorted(p.name for p in (tmp_path / "Resources").iterdir()) == ["mail"]

    def test_only_the_first_batch_overwrites(self, tmp_path, monkeypatch):
        monkeypatch.setattr(g, "UPSERT_BATCH", 2)
        client = FakeGmail([_message(str(i), DAY1 + i) for i in range(5)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        assert writer.calls == [(2, True), (2, False), (1, False)]
        assert len(writer.table) == 5

    def test_incremental_after_a_full_load_continues_from_its_cursor(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        client.add(_message("b", DAY2))
        assert _run(tmp_path, client, _config(), writer, execution_id="8").copied == 1
        assert sorted(writer.table) == ["a", "b"]

    def test_a_publication_interrupted_between_renames_is_restored(self, tmp_path):
        client = FakeGmail([_message("a", DAY1, attachments=[("x.bin", b"1")])])
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        mail = tmp_path / "Resources" / "mail"
        previous = tmp_path / "Resources" / "mail.__previous__" / "7"
        previous.parent.mkdir()
        mail.rename(previous)
        (tmp_path / "Resources" / "mail.__staging__" / "7").mkdir(parents=True)
        client.add(_message("b", DAY2))
        _run(tmp_path, client, _config(), writer, execution_id="8")
        assert sorted(writer.table) == ["a", "b"]
        assert (mail / "day=2026-10-20" / "a" / "x.bin").exists()
        assert sorted(p.name for p in (tmp_path / "Resources").iterdir()) == ["mail"]


class TestLease:
    def _lease(self, tmp_path, holder, since):
        (tmp_path / "Resources").mkdir()
        lease = tmp_path / "Resources" / "mail.__lease__"
        lease.write_text(json.dumps({"holder": holder, "since": since}))
        return lease

    def test_a_running_holder_keeps_the_mailbox(self, tmp_path):
        self._lease(tmp_path, "5", NOW_MS)
        with pytest.raises(g._GmailSyncError, match="Another sync"):
            _run(
                tmp_path,
                FakeGmail([_message("a", DAY1)]),
                _config(),
                FakeWriter(),
                is_running=lambda h: h == "5",
            )

    def test_a_finished_holder_is_taken_over(self, tmp_path):
        lease = self._lease(tmp_path, "5", NOW_MS)
        result = _run(
            tmp_path, FakeGmail([_message("a", DAY1)]), _config(), FakeWriter()
        )
        assert result.copied == 1
        assert not lease.exists()

    def test_an_unverifiable_holder_is_taken_over_only_when_old(self, tmp_path):
        lease = self._lease(tmp_path, "local-x", NOW_MS - 1000)
        client = FakeGmail([_message("a", DAY1)])
        with pytest.raises(g._GmailSyncError):
            _run(tmp_path, client, _config(), FakeWriter(), is_running=lambda h: None)
        lease.write_text(
            json.dumps(
                {"holder": "local-x", "since": NOW_MS - g.UNVERIFIABLE_LEASE_MS - 1}
            )
        )
        result = _run(
            tmp_path, client, _config(), FakeWriter(), is_running=lambda h: None
        )
        assert result.copied == 1


class _FakeFeatureGroup:
    def __init__(self, id=None):
        self.id = id
        self.inserts = []
        self.saved = None

    def insert(self, frame, overwrite, wait):
        self.inserts.append((frame, overwrite, wait))

    def save(self, features):
        self.saved = features


class TestFeatureGroup:
    def test_the_writer_sends_typed_columns(self):
        fg = _FakeFeatureGroup(1)
        row = g._parse_message(_message("a", DAY1), None, PROJECT_ROOT)
        g._FeatureGroupWriter(fg)._upsert([row], overwrite=True)
        frame, overwrite, wait = fg.inserts[0]
        assert list(frame.columns) == [name for name, _ in g.COLUMNS]
        assert str(frame["internal_date"].dtype).endswith(", UTC]")
        assert frame["internal_date"][0].year == 2026
        assert str(frame["size_estimate"].dtype) == "Int64"
        assert frame["to_addresses"][0] == ["alice@example.com"]
        assert (overwrite, wait) == (True, True)

    def test_a_new_feature_group_is_created_with_the_full_schema(self, mocker):
        fg = _FakeFeatureGroup()
        fs = mocker.MagicMock()
        fs.get_or_create_feature_group.return_value = fg
        assert g._feature_group(fs, "alice_examples", "alice@example.com") is fg
        kwargs = fs.get_or_create_feature_group.call_args.kwargs
        assert kwargs["name"] == "alice_examples"
        assert kwargs["primary_key"] == ["message_id"]
        assert kwargs["partition_key"] == ["day"]
        assert kwargs["event_time"] == "internal_date"
        assert kwargs["time_travel_format"] == "DELTA"
        assert kwargs["online_enabled"] is False
        assert [(f.name, f.type) for f in fg.saved] == list(g.COLUMNS)

    def test_an_existing_feature_group_is_reused(self, mocker):
        fg = _FakeFeatureGroup(5)
        fs = mocker.MagicMock()
        fs.get_or_create_feature_group.return_value = fg
        g._feature_group(fs, "alice_examples", "alice@example.com")
        assert fg.saved is None


class _Response:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.auth_headers = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.auth_headers.append(headers["Authorization"])
        return self.responses.pop(0)


def _rate_limited(reason):
    return _Response(403, {"error": {"code": 403, "errors": [{"reason": reason}]}})


class TestGmailClient:
    def _client(self, responses, tokens=("t1", "t2")):
        issued = list(tokens)
        sleeps = []
        session = _Session(responses)
        client = g._GmailClient(
            "alice@example.com",
            lambda: (issued.pop(0), NOW_MS + 3_600_000),
            session=session,
            sleep=sleeps.append,
            clock=lambda: NOW_MS / 1000,
        )
        return client, session, sleeps

    def test_rate_limits_are_retried_and_retry_after_is_honoured(self):
        client, _, sleeps = self._client(
            [
                _rate_limited("userRateLimitExceeded"),
                _Response(429, headers={"Retry-After": "7"}),
                _Response(503),
                _Response(200, {"id": "a"}),
            ]
        )
        assert client._get_raw_message("a") == {"id": "a"}
        assert sleeps[1] == 7.0
        assert len(sleeps) == 3

    def test_a_forbidden_mailbox_is_not_retried(self):
        client, _, sleeps = self._client([_rate_limited("insufficientPermissions")])
        with pytest.raises(g._GmailSyncError, match="403: insufficientPermissions"):
            client._get_raw_message("a")
        assert sleeps == []

    def test_an_expired_token_is_renewed_once(self):
        client, session, _ = self._client([_Response(401), _Response(200, {"id": "a"})])
        assert client._get_raw_message("a") == {"id": "a"}
        assert session.auth_headers == ["Bearer t1", "Bearer t2"]

    def test_listing_follows_pages(self):
        client, _, _ = self._client(
            [
                _Response(200, {"messages": [{"id": "a"}], "nextPageToken": "p2"}),
                _Response(200, {"messages": [{"id": "b"}]}),
            ]
        )
        assert list(client._list_message_ids("q", [])) == ["a", "b"]

    def test_a_deleted_message_is_none(self):
        client, _, _ = self._client([_Response(404)])
        assert client._get_raw_message("a") is None


def test_logs_carry_no_message_content_or_credentials(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    client = FakeGmail(
        [_message("a", DAY1, subject="Secret subject", body="Secret body")]
    )
    _run(tmp_path, client, _config(), FakeWriter())
    assert "Secret" not in caplog.text
    assert "bob@example.com" not in caplog.text


def test_the_scheduler_argument_is_ignored(tmp_path, monkeypatch):
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path / "missing"))
    with pytest.raises(g._GmailSyncError, match="HopsFS mount"):
        g._main(
            [
                "--config",
                "Resources/jobs/x/gmail_sync.json",
                "-start_time",
                "2026-10-25T12:00:00Z",
            ]
        )


def test_configs_from_before_the_feature_group_are_refused():
    with pytest.raises(g._GmailSyncError, match="save the sync job again"):
        g._SyncConfig.from_json({"schemaVersion": 1})
