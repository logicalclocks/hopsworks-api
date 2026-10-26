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
import re
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
# A run two hours after NOW_MS, by when new mail has arrived.
LATER = NOW_MS + 2 * HOUR
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


def _message(message_id, internal_ms, labels=("INBOX",), received_ms=None, **kwargs):
    """A message in `raw` format; `received_ms`, when Gmail got it, defaults to `internal_ms`."""
    return {
        "_received_ms": internal_ms if received_ms is None else received_ms,
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
        # Like Gmail, after: and before: search on when a message was received, and labelIds selects the messages carrying every label given.
        after = re.search(r"after:(\d+)", query)
        before = re.search(r"before:(\d+)", query)

        def selected(m):
            received_s = m["_received_ms"] // 1000
            return (
                set(label_ids) <= set(m["labelIds"])
                and (after is None or received_s > int(after.group(1)))
                and (before is None or received_s < int(before.group(1)))
            )

        return sorted(
            (i for i, m in self.messages.items() if selected(m)),
            key=lambda i: -int(self.messages[i]["internalDate"]),
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
        self.compactions = 0

    def _compact(self, days):
        self.compacted.extend(days)
        self.compactions += 1

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

    def test_non_ascii_addresses_stay_readable(self):
        row = g._parse_message(
            _message(
                "m1",
                DAY1,
                to='=?utf-8?q?J=C3=B6ran?= <j\u00f6ran@example.se>, "Doe, John" <john@example.com>',
            ),
            None,
            PROJECT_ROOT + "/Resources/mail",
        )
        assert row["to_addresses"] == [
            "J\u00f6ran <j\u00f6ran@example.se>",
            '"Doe, John" <john@example.com>',
        ]

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
            ("Fo\u0308rening.pdf", "F\u00f6rening.pdf"),
            ("\u6587\u4ef6 report \U0001f600.pdf", "_ report _.pdf"),
            ("\u00f6" * 150 + ".pdf", "\u00f6" * 98 + ".pdf"),
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
        client.add(_message("c", NOW_MS + HOUR))
        second = _run(
            tmp_path, client, _config(), writer, execution_id="8", now_ms=LATER
        )
        assert second.copied == 1
        assert second.cursor_ms == LATER - g.RUN_HORIZON_MS
        assert (
            _run(
                tmp_path, client, _config(), writer, execution_id="9", now_ms=LATER
            ).copied
            == 0
        )
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
        assert f"after:{(NOW_MS - g.RUN_HORIZON_MS) // 1000 - 1}" in query
        assert labels == ["INBOX"]

    @pytest.mark.parametrize("strategy", ["INCREMENTAL_TIMESTAMP", "FULL_LOAD"])
    def test_labels_select_the_messages_carrying_any_of_them(self, tmp_path, strategy):
        client = FakeGmail(
            [
                _message("sent", DAY1 - 9 * g.WINDOW_MS, labels=("SENT",)),
                _message("inbox", DAY1, labels=("INBOX",)),
                _message("both", DAY1 + 1000, labels=("INBOX", "SENT")),
                _message("other", DAY1 + 2000, labels=("Label_1",)),
            ]
        )
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy, labels=["INBOX", "SENT"]), writer)
        # The SENT-only message is the oldest, so the backfill must start from it.
        assert sorted(writer.table) == ["both", "inbox", "sent"]

    def test_messages_in_the_overlap_with_the_cursor_are_neither_lost_nor_duplicated(
        self, tmp_path
    ):
        cursor = NOW_MS - g.RUN_HORIZON_MS
        # Received in the cursor's second: one before the cursor, one after it but indexed only after the run.
        client = FakeGmail([_message("before", DAY1, received_ms=cursor - 200)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        client.add(_message("late", DAY1, received_ms=cursor + 300))
        upserted_before = sum(n for n, _ in writer.calls)
        second = _run(
            tmp_path, client, _config(), writer, execution_id="8", now_ms=LATER
        )
        assert second.copied == 1
        assert sum(n for n, _ in writer.calls) == upserted_before + 1
        assert sorted(writer.table) == ["before", "late"]

    def test_a_message_is_copied_whenever_gmail_received_it(self, tmp_path):
        # Gmail searches on when it received a message; internalDate can be the sender's Date, days away.
        client = FakeGmail(
            [
                _message("dated_later", DAY1, received_ms=DAY1 - 9 * 24 * HOUR),
                _message("dated_earlier", DAY1 - 3 * g.WINDOW_MS, received_ms=DAY1),
                _message("old_date", 0, received_ms=DAY1 - 2 * g.WINDOW_MS),
                _message("anchor", DAY1 - 4 * g.WINDOW_MS),
            ]
        )
        writer = FakeWriter()
        _run(tmp_path, client, _config(), writer)
        assert sorted(writer.table) == [
            "anchor",
            "dated_earlier",
            "dated_later",
            "old_date",
        ]
        client.add(_message("delayed", DAY1, received_ms=NOW_MS + HOUR))
        _run(tmp_path, client, _config(), writer, execution_id="8", now_ms=LATER)
        assert "delayed" in writer.table

    def test_a_cursor_from_an_earlier_version_is_refused(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(), writer)
        state_dir = tmp_path / "Resources" / "mail" / g.STATE_DIR
        for path in state_dir.iterdir():
            state = json.loads(path.read_text())
            path.write_text(json.dumps({**state, "schema_version": 2}))
        with pytest.raises(g._GmailSyncError, match="earlier version"):
            _run(tmp_path, client, _config(), writer, execution_id="8")

    def test_initial_value_is_an_inclusive_lower_bound(self, tmp_path):
        client = FakeGmail(
            [
                _message("old", DAY1 - 5_000),
                _message("edge", DAY1),
                _message("new", DAY2),
            ]
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
        client = FakeGmail(
            [_message(str(i), NOW_MS - (5 - i) * g.WINDOW_MS) for i in range(5)]
        )
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

    def test_a_backfill_upserts_as_it_reads_its_windows(self, tmp_path, monkeypatch):
        monkeypatch.setattr(g, "UPSERT_BATCH", 2)
        weeks_ago = [5, 4, 3, 2, 1]
        client = FakeGmail(
            [_message(str(w), NOW_MS - w * g.WINDOW_MS) for w in weeks_ago]
        )
        writer = FakeWriter()
        result = _run(tmp_path, client, _config(), writer)
        assert result.copied == 5
        assert writer.calls == [(2, False), (2, False), (1, False)]
        assert list(writer.table) == ["5", "4", "3", "2", "1"]
        state, _ = g._read_state(tmp_path / "Resources" / "mail")
        assert state.cursor_ms == NOW_MS - g.RUN_HORIZON_MS
        assert writer.compacted == result.days

    def test_large_messages_are_upserted_in_smaller_batches(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(g, "UPSERT_BATCH_BYTES", 30_000)
        client = FakeGmail(
            [
                _message(str(i), NOW_MS - (5 - i) * g.WINDOW_MS, body="x" * 20_000)
                for i in range(5)
            ]
        )
        writer = FakeWriter()
        result = _run(tmp_path, client, _config(), writer)
        assert result.copied == 5
        # Each window adds about 20 kB, so a batch is cut after every second window.
        assert writer.calls == [(2, False), (2, False), (1, False)]

    def test_without_a_start_date_the_oldest_message_is_copied(self, tmp_path):
        years_ago = NOW_MS - 2 * 365 * 24 * HOUR
        client = FakeGmail([_message("old", years_ago), _message("new", DAY1)])
        writer = FakeWriter()
        assert _run(tmp_path, client, _config(), writer).copied == 2
        assert list(writer.table) == ["old", "new"]

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

    def test_an_unparseable_message_is_left_out(self, tmp_path, monkeypatch, caplog):
        client = FakeGmail([_message("a", DAY1), _message("b", DAY1 + 1000)])
        writer = FakeWriter()
        parse = g._parse_message

        def fail_on_a(message, *args):
            if message["id"] == "a":
                raise ValueError("unusual header")
            return parse(message, *args)

        monkeypatch.setattr(g, "_parse_message", fail_on_a)
        result = _run(tmp_path, client, _config(), writer)
        assert (result.copied, result.skipped_unreadable) == (1, 1)
        assert "Leaving out message a" in caplog.text

    def test_a_filesystem_error_fails_the_run(self, tmp_path, monkeypatch):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()

        def eio(*args):
            raise OSError(5, "Input/output error")

        monkeypatch.setattr(g, "_parse_message", eio)
        with pytest.raises(OSError):
            _run(tmp_path, client, _config(), writer)
        assert writer.calls == []

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
        client = FakeGmail([_message(str(i), DAY1 - i * g.WINDOW_MS) for i in range(4)])
        _run(tmp_path, client, _config(), FakeWriter())
        names = sorted(
            p.name for p in (tmp_path / "Resources" / "mail" / g.STATE_DIR).iterdir()
        )
        # One upsert checkpoint per weekly window, then one for the compaction.
        assert names == ["4.json", "5.json"]


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
        assert writer.compactions == 1

    def test_a_failed_compaction_is_retried_by_the_next_run(self, tmp_path):
        client = FakeGmail([_message("a", DAY1)])
        writer = FakeWriter()

        def fail(days):
            raise OSError("namenode unavailable")

        writer._compact = fail
        with pytest.raises(OSError):
            _run(tmp_path, client, _config(), writer)
        writer._compact = writer.compacted.extend
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
        client = FakeGmail([_message(str(i), DAY1 - i * g.WINDOW_MS) for i in range(5)])
        writer = FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        assert writer.calls == [(2, True), (2, False), (1, False)]
        assert len(writer.table) == 5

    def test_incremental_after_a_full_load_continues_from_its_cursor(self, tmp_path):
        client, writer = FakeGmail([_message("a", DAY1)]), FakeWriter()
        _run(tmp_path, client, _config(strategy="FULL_LOAD"), writer)
        client.add(_message("b", NOW_MS + HOUR))
        assert (
            _run(
                tmp_path, client, _config(), writer, execution_id="8", now_ms=LATER
            ).copied
            == 1
        )
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
        client.add(_message("b", NOW_MS + HOUR))
        _run(tmp_path, client, _config(), writer, execution_id="8", now_ms=LATER)
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

    def test_a_batch_without_any_cc_or_bcc_keeps_the_feature_group_types(self):
        from hsfs.engine import python

        fg = _FakeFeatureGroup(1)
        rows = [g._parse_message(_message(i, DAY1), None, PROJECT_ROOT) for i in "ab"]
        assert all(r["bcc_addresses"] == [] for r in rows)
        for row in rows:
            row["date_header"] = None
        g._FeatureGroupWriter(fg)._upsert(rows, overwrite=False)
        features = python.Engine._parse_schema_feature_group(
            None, fg.inserts[0][0], "DELTA"
        )
        assert {f.name: f.type for f in features} == dict(g.COLUMNS)

    def test_an_isolated_upsert_runs_in_a_child_process(self, tmp_path, mocker):
        fg = _FakeFeatureGroup(1)
        fg.name, fg.version = "alice_examples", 1
        rows = [g._parse_message(_message(i, DAY1), None, PROJECT_ROOT) for i in "ab"]
        sent = {}

        def upsert_from_file(name, version, path, overwrite):
            # The child reads the rows the parent wrote, before the parent removes them.
            with open(path) as f:
                ids = [json.loads(line)["message_id"] for line in f]
            sent.update(name=name, version=version, overwrite=overwrite, ids=ids)

        def child(argv):
            exec(argv[2], {})
            return mocker.MagicMock(returncode=0)

        mocker.patch.object(g, "_upsert_from_file", side_effect=upsert_from_file)
        run = mocker.patch.object(g.subprocess, "run", side_effect=child)
        g._FeatureGroupWriter(fg, isolated=True, scratch=tmp_path)._upsert(
            rows, overwrite=True
        )
        assert run.call_args.args[0][0] == g.sys.executable
        assert sent == {
            "name": "alice_examples",
            "version": 1,
            "overwrite": True,
            "ids": ["a", "b"],
        }
        assert fg.inserts == [] and list(tmp_path.iterdir()) == []

    def test_a_child_killed_for_memory_fails_the_run(self, tmp_path, mocker):
        fg = _FakeFeatureGroup(1)
        fg.name, fg.version = "alice_examples", 1
        mocker.patch.object(
            g.subprocess, "run", return_value=mocker.MagicMock(returncode=-9)
        )
        row = g._parse_message(_message("a", DAY1), None, PROJECT_ROOT)
        with pytest.raises(g._GmailSyncError, match="running out of memory"):
            g._FeatureGroupWriter(fg, isolated=True, scratch=tmp_path)._upsert(
                [row], overwrite=False
            )

    def test_the_child_upserts_the_rows_of_its_file(self, tmp_path, mocker):
        import hopsworks

        fg = _FakeFeatureGroup(1)
        project = mocker.MagicMock()
        project.get_feature_store.return_value.get_feature_group.return_value = fg
        mocker.patch.object(hopsworks, "login", return_value=project)
        rows_path = tmp_path / "rows.jsonl"
        row = g._parse_message(_message("a", DAY1), None, PROJECT_ROOT)
        rows_path.write_text(json.dumps(row) + "\n")
        g._upsert_from_file("alice_examples", 1, str(rows_path), False)
        frame, overwrite, wait = fg.inserts[0]
        assert list(frame["message_id"]) == ["a"] and (overwrite, wait) == (False, True)
        project.get_feature_store.return_value.get_feature_group.assert_called_once_with(
            "alice_examples", version=1
        )

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
        client, sleeps = self._clocked_client(
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

    def _clocked_client(self, responses):
        sleeps = []
        client = g._GmailClient(
            "alice@example.com",
            lambda: ("t", None),
            session=_Session(responses),
            sleep=sleeps.append,
            clock=lambda: NOW_MS / 1000 + sum(sleeps),
        )
        return client, sleeps

    def test_rate_limits_are_retried_until_gmail_serves_the_request(self):
        # 200 throttles at the 64s cap is over three hours of backoff.
        throttled = [_rate_limited("rateLimitExceeded")] * 200
        client, sleeps = self._clocked_client([*throttled, _Response(200, {"id": "a"})])
        assert client._get_raw_message("a") == {"id": "a"}
        assert len(sleeps) == 200
        assert max(sleeps) <= g.MAX_RATE_LIMIT_BACKOFF_S + 1
        assert sum(sleeps) > 3 * 3600

    def test_a_throttled_request_holds_back_a_concurrent_one(self):
        clock = [NOW_MS / 1000]
        sleeps = []
        client = g._GmailClient(
            "alice@example.com",
            lambda: ("t", None),
            session=_Session([_Response(200, {"id": "b"})]),
            sleep=sleeps.append,
            clock=lambda: clock[0],
        )
        client._pause(3, None)
        client._get_raw_message("b")
        assert len(sleeps) == 1
        assert 8 <= sleeps[0] <= 9

    def test_server_errors_keep_the_attempt_count(self):
        client, sleeps = self._clocked_client([_Response(503)] * 100)
        with pytest.raises(g._GmailSyncError, match="HTTP 503"):
            client._get_raw_message("a")
        assert len(sleeps) == g.MAX_ATTEMPTS - 1

    def test_throttling_is_logged_once_per_interval(self, caplog):
        caplog.set_level(logging.INFO)
        client, _, _ = self._client(
            [_rate_limited("rateLimitExceeded")] * 3 + [_Response(200, {"id": "a"})]
        )
        client._get_raw_message("a")
        lines = [
            r.getMessage() for r in caplog.records if "throttling" in r.getMessage()
        ]
        assert lines == [
            "Gmail is throttling the sync (HTTP 403 rateLimitExceeded); 1 retries so far"
        ]

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


def test_a_run_that_finds_another_one_syncing_does_nothing(tmp_path, monkeypatch):
    import hopsworks

    config = tmp_path / "Resources" / "jobs" / "x" / "gmail_sync.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "jobName": "x",
                "connectorId": 42,
                "connectorName": "inbox",
                "featureGroupName": "alice_examples",
                "path": "Resources/mail",
            }
        )
    )
    monkeypatch.setenv("HOPSFS_MOUNT", str(tmp_path))
    connector = type("C", (), {"id": 42, "type": "GMAIL", "mailbox": "a@b.c"})()
    data_source = type("D", (), {"storage_connector": connector})()
    fs = type("F", (), {"get_data_source": lambda self, name: data_source})()
    project = type("P", (), {"name": "mail", "get_feature_store": lambda self: fs})()
    monkeypatch.setattr(hopsworks, "login", lambda: project)
    monkeypatch.setattr(g, "_feature_group", lambda *a: None)

    class HeldRun:
        def __init__(self, *a):
            pass

        def _run(self):
            raise g._LeaseHeld("Another sync of this mailbox is running")

    monkeypatch.setattr(g, "_SyncRun", HeldRun)
    g._main(["--config", "Resources/jobs/x/gmail_sync.json"])


def test_configs_from_before_the_feature_group_are_refused():
    with pytest.raises(g._GmailSyncError, match="save the sync job again"):
        g._SyncConfig.from_json({"schemaVersion": 1})
