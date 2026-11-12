"""``hops fg embed``: the pure decisions of a run and the command surface.

Chunking, the input limits, the rewrite set from versions and unsettled
epochs, the convergence check, the fence barrier and the refusal messages are
plain functions tested here. The run against the stores, the claim, the fence
and the deletion execution are tested on a cluster in
tests/live/test_fg_embed_fence.py.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest import mock

import pyarrow as pa
import pytest
import requests
from click.testing import CliRunner
from hopsworks.cli import embed_run
from hopsworks.cli.embed_run import Doc, Rewrite, converged, fence_lag, rewrite_set
from hopsworks.cli.main import cli


# --- chunking ----------------------------------------------------------------


def test_chunks_are_a_pure_function_of_the_text():
    text = "".join(chr(97 + i % 26) for i in range(5000))
    assert embed_run.chunks(text, 2000, 200) == embed_run.chunks(text, 2000, 200)
    parts = embed_run.chunks(text, 2000, 200)
    assert [len(p) for p in parts] == [2000, 2000, 1400]
    assert parts[1][:200] == parts[0][-200:]
    assert "".join([parts[0], *(p[200:] for p in parts[1:])]) == text


def test_appending_text_keeps_the_earlier_chunks_and_their_numbers():
    text = "x" * 1000 + "y" * 3000
    before = embed_run.chunks(text, 2000, 200)
    after = embed_run.chunks(text + "z" * 500, 2000, 200)
    assert after[: len(before) - 1] == before[:-1]


@pytest.mark.parametrize("value", [None, "", "   ", float("nan")])
def test_empty_text_has_no_chunks(value):
    assert embed_run.chunks(value) == []


def test_a_composite_key_is_one_stable_id():
    assert embed_run.doc_id([7]) == "7"
    assert embed_run.doc_id(["a", 1]) == '["a", "1"]'


# --- input limits --------------------------------------------------------------


def _batches(rows, size=2):
    for start in range(0, len(rows), size):
        part = rows[start : start + size]
        yield pa.RecordBatch.from_pydict(
            {"id": [r[0] for r in part], "body": [r[1] for r in part]}
        )


def _chunk(rows, max_chunks=50, max_bytes=10_000, batches=None):
    return embed_run.chunk_rows(
        batches if batches is not None else _batches(rows),
        "notes",
        ["id"],
        "body",
        10,
        2,
        max_chunks,
        max_bytes,
    )


def test_each_row_becomes_its_chunks_keyed_by_doc_and_index():
    assert _chunk([[1, "abcdefghijkl"], [2, "xy"], [3, None]]) == {
        ("1", 0): "abcdefghij",
        ("1", 1): "ijkl",
        ("2", 0): "xy",
    }


def test_a_source_over_the_chunk_maximum_is_refused():
    with pytest.raises(embed_run.EmbedError, match="over the supported input of 2"):
        _chunk([[1, "abcdefghijkl"], [2, "xy"]], max_chunks=2)


def test_a_source_at_the_maximum_is_read():
    assert len(_chunk([[1, "abcdefghijkl"]], max_chunks=2, max_bytes=12)) == 2


def test_bytes_are_utf8_bytes_of_the_text_not_characters():
    with pytest.raises(embed_run.EmbedError, match="bytes of text"):
        _chunk([[1, "ééééé"]], max_bytes=9)


def test_rows_count_toward_the_maximum_even_with_no_text():
    with pytest.raises(embed_run.EmbedError, match="rows count too"):
        _chunk([[1, ""], [2, " "], [3, None]], max_chunks=2)


def test_the_read_stops_at_the_first_batch_past_the_maximum():
    pulled = []

    def stream():
        for batch in _batches([[i, "x"] for i in range(20)]):
            pulled.append(batch.column(0)[0].as_py())
            yield batch

    with pytest.raises(embed_run.EmbedError, match="supported input"):
        _chunk(None, max_chunks=3, batches=stream())
    assert pulled == [0, 2]


@pytest.mark.parametrize(("raw", "value"), [("80000", 80_000), (" 25 ", 25), (3, 3)])
def test_an_admin_grants_a_larger_input_through_a_variable(raw, value):
    assert embed_run.positive_int(embed_run.VAR_MAX_CHUNKS, raw) == value


@pytest.mark.parametrize("bad", ["abc", "0", "-5", ""])
def test_a_variable_that_is_not_a_positive_integer_stops_the_run(bad):
    with pytest.raises(embed_run.EmbedError, match="not a positive integer"):
        embed_run.positive_int(embed_run.VAR_MAX_CHUNKS, bad)


# --- the rewrite set -------------------------------------------------------------


A, B, C = ("a", 0), ("a", 1), ("b", 0)
WANT = {A: "alpha", B: "beta"}
HELD = {A: embed_run.sha("alpha"), B: embed_run.sha("beta")}


def _docs(version=3, deleted=(), overrides=None):
    docs = {
        k: Doc(embed_run.sha(text), version, k in deleted) for k, text in WANT.items()
    }
    docs.update(overrides or {})
    return docs


def test_a_converged_group_with_no_failed_run_rewrites_nothing():
    assert rewrite_set(WANT, HELD, _docs(), []) == Rewrite([], [], [])


def test_a_changed_chunk_is_embedded_and_written_to_both_stores():
    held = {**HELD, B: embed_run.sha("old beta")}
    assert rewrite_set(WANT, held, _docs(), []) == Rewrite([B], [], [])


def test_a_new_chunk_is_changed_even_when_a_document_exists_online():
    assert rewrite_set(WANT, {A: HELD[A]}, _docs(), []).changed == [B]


@pytest.mark.parametrize(
    "doc",
    [None, Doc(embed_run.sha("beta"), 3, True), Doc(embed_run.sha("other"), 3, False)],
    ids=["missing", "tombstone", "other content"],
)
def test_an_unchanged_chunk_that_is_wrong_online_is_rewritten_online(doc):
    docs = _docs()
    docs.pop(B)
    if doc is not None:
        docs[B] = doc
    assert rewrite_set(WANT, HELD, docs, []) == Rewrite([], [B], [])


def test_with_unsettled_epochs_every_document_below_the_highest_is_rewritten():
    docs = _docs(version=3, overrides={B: Doc(embed_run.sha("beta"), 6, False)})
    assert rewrite_set(WANT, HELD, docs, [2, 5]) == Rewrite([], [A], [])
    assert rewrite_set(WANT, HELD, docs, [2]) == Rewrite([], [], [])


def test_offline_keys_the_source_lacks_are_deleted():
    held = {**HELD, C: embed_run.sha("gone")}
    assert rewrite_set(WANT, held, _docs(), []).delete == [C]


# --- convergence -------------------------------------------------------------------


def test_a_written_chunk_converges_at_this_epoch_with_its_content():
    check = converged(WANT, Rewrite([A], [B], []), 7, [])
    assert check(A, Doc(embed_run.sha("alpha"), 7, False))
    assert check(B, Doc(embed_run.sha("beta"), 7, False))
    assert not check(A, Doc(embed_run.sha("alpha"), 6, False))
    assert not check(A, Doc(embed_run.sha("alpha"), 8, False))
    assert not check(A, Doc(embed_run.sha("alpha"), 7, True))
    assert not check(A, None)


def test_a_deleted_key_converges_as_a_tombstone_at_this_epoch():
    check = converged(WANT, Rewrite([], [], [C]), 7, [])
    assert check(C, Doc(None, 7, True))
    assert not check(C, Doc(None, 6, True))
    assert not check(C, Doc(embed_run.sha("gone"), 7, False))
    assert not check(C, None)


def test_an_untouched_chunk_converges_at_its_content_above_the_unsettled_epochs():
    check = converged(WANT, Rewrite(), 9, [4])
    assert check(A, Doc(embed_run.sha("alpha"), 4, False))
    assert not check(A, Doc(embed_run.sha("alpha"), 3, False))
    assert not check(A, Doc(embed_run.sha("other"), 5, False))


# --- the fence barrier ---------------------------------------------------------------


def _progress(group, fences):
    return [
        {"consumerGroup": group, "partition": p, "offset": 10, "fenceEpoch": e}
        for p, e in fences.items()
    ]


def test_the_barrier_waits_for_every_partition_of_the_vectordb_group():
    rows = _progress("VECTORDB", {0: 5, 1: 4}) + _progress("ONLINE", {0: None})
    assert fence_lag(rows, [0, 1], 5) == [1]
    assert fence_lag(_progress("VECTORDB", {0: 5, 1: 6}), [0, 1], 5) == []


def test_a_partition_with_no_saved_offset_lags():
    assert fence_lag(_progress("VECTORDB", {0: 5}), [0, 1], 5) == [1]


def test_every_namespaced_vectordb_group_must_pass_the_fence():
    rows = _progress("VECTORDB@a", {0: 5}) + _progress("VECTORDB@b", {0: None})
    assert fence_lag(rows, [0], 5) == [0]


def test_with_no_vectordb_group_every_partition_lags():
    assert fence_lag(_progress("ONLINE", {0: 9, 1: 9}), [0, 1], 5) == [0, 1]


def test_a_barrier_past_its_deadline_names_the_partitions_left():
    writer = SimpleNamespace(progress=lambda: _progress("VECTORDB", {0: 5, 1: 2}))
    now = iter([0.0, 0.0, 61.0])
    with pytest.raises(
        embed_run.EmbedError,
        match="^fence not yet consumed on partition 1; run again$",
    ):
        embed_run.wait_fenced(writer, [0, 1], 5, 60, lambda: next(now), lambda s: None)


# --- refusals ---------------------------------------------------------------------------


def _refused(status, error_code=None, usr_msg="", retry_after=None):
    from hopsworks_common.client.exceptions import RestAPIError

    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(
        {"errorCode": error_code, "usrMsg": usr_msg}
    ).encode()
    if retry_after:
        response.headers["Retry-After"] = retry_after
    return RestAPIError("http://x/executions", response)


def test_a_start_refused_for_capacity_names_the_retry_time():
    from hopsworks.cli.commands.embed import start_refusal

    assert start_refusal(_refused(429, retry_after="37"), "g") == (
        "embed jobs are at capacity; nothing was started; retry after 37 s"
    )
    assert "retry after a while" in start_refusal(_refused(429), "g")


def test_a_start_refused_for_a_running_build_or_a_deletion_says_which():
    from hopsworks.cli.commands.embed import start_refusal

    assert (
        start_refusal(_refused(409, 130095), "g") == "a build of g is already running"
    )
    assert start_refusal(_refused(409, 130102), "g") == (
        "g is being deleted; nothing was started; run hops fg embed delete g again to finish it"
    )
    assert start_refusal(_refused(500), "g") is None


def test_a_claim_refused_for_a_newer_run_stops_without_writing():
    message = str(
        embed_run.claim_refused(
            "g",
            1,
            "Feature group 9, execution 41: a newer run of the writer job holds the group "
            "(epoch 12 above 7)",
        )
    )
    assert message.startswith("a newer run of this embed job holds the group (")
    assert "epoch 12 above 7" in message
    assert message.endswith("); this run stopped without writing")


def test_a_claim_refused_for_another_reason_says_the_reason():
    message = str(
        embed_run.claim_refused(
            "g", 1, "Feature group 9, execution 41: the writer job is being deleted"
        )
    )
    assert "the writer job is being deleted" in message
    assert "stopped without writing" in message


def test_only_a_409_is_a_refused_claim():
    assert embed_run._refusal(_refused(409, 270374, "x")).user_message == "x"
    assert embed_run._refusal(_refused(500)) is None
    assert embed_run._refusal(RuntimeError("down")) is None


def test_a_build_outside_a_job_execution_writes_nothing(monkeypatch):
    monkeypatch.delenv(embed_run.EXECUTION_ENV, raising=False)
    with pytest.raises(embed_run.EmbedError, match="Nothing was written"):
        embed_run.execution_id()
    monkeypatch.setenv(embed_run.EXECUTION_ENV, "41")
    assert embed_run.execution_id() == 41


def test_only_a_fenced_group_of_this_job_can_be_claimed():
    def fg(fenced, writer):
        return SimpleNamespace(
            embedding_index=SimpleNamespace(fenced=fenced), writer_job_id=writer
        )

    assert embed_run.fenced_by(fg(True, 5), 5)
    assert not embed_run.fenced_by(fg(True, 6), 5)
    assert not embed_run.fenced_by(fg(False, 5), 5)
    assert not embed_run.fenced_by(SimpleNamespace(embedding_index=None), 5)


# --- schedule --------------------------------------------------------------------------


def test_the_default_schedule_is_daily_at_a_minute_and_hour_hashed_from_project_and_group():
    import hashlib

    from hopsworks.cli.commands.embed import schedule_cron

    d = hashlib.sha256(b"119/notes_body_emb").digest()
    assert (
        schedule_cron("daily", 119, "notes_body_emb")
        == f"0 {d[0] % 60} {d[1] % 24} * * ?"
    )
    assert schedule_cron("@daily", 119, "notes_body_emb") == schedule_cron(
        "1440", 119, "notes_body_emb"
    )


def test_groups_and_projects_spread_over_the_day():
    from hopsworks.cli.commands.embed import schedule_cron

    slots = {
        schedule_cron("daily", pid, f"g{i}_emb")
        for pid in range(100, 110)
        for i in range(20)
    }
    assert len(slots) > 150
    assert schedule_cron("daily", 1, "a") != schedule_cron("daily", 2, "a")


@pytest.mark.parametrize("bad", ["weekly", "7", "0", "90", "*/5 * * * * ?"])
def test_a_schedule_that_is_not_daily_hourly_or_an_even_interval_is_a_usage_error(bad):
    import click
    from hopsworks.cli.commands.embed import schedule_cron

    with pytest.raises(click.BadParameter):
        schedule_cron(bad, 1, "g")


# --- command surface ---------------------------------------------------------


def test_embed_help_names_the_command_without_the_hidden_verb():
    result = CliRunner().invoke(cli, ["fg", "embed", "-h"])
    assert result.exit_code == 0
    assert result.output.startswith("Usage: hops fg embed [OPTIONS] SOURCE")
    assert "HOPSABLE_EMBEDDING_PROJECT" in result.output
    assert "lease" not in result.output.lower()
    delete = CliRunner().invoke(cli, ["fg", "embed", "delete", "-h"])
    assert delete.output.startswith("Usage: hops fg embed delete [OPTIONS] GROUP")
    assert "lease" not in delete.output.lower()


def test_the_lease_options_are_gone():
    for args in (
        ["fg", "embed", "notes", "--text", "b"],
        ["fg", "embed", "delete", "g"],
    ):
        result = CliRunner().invoke(cli, [*args, "--lease-timeout", "5"])
        assert result.exit_code == 2 and "No such option" in result.output


def test_the_job_script_runs_the_cli_with_the_job_arguments():
    from hopsworks.cli.commands import embed

    assert "cli.main(args=sys.argv[1:]" in embed._RUNNER_CODE


def test_the_upsert_frame_matches_the_group_schema():
    from datetime import datetime, timezone

    from hsfs.engine import python

    row = {
        "doc_id": "1",
        "chunk_ix": 0,
        "text": "a",
        "embedding": [0.1, 0.2],
        "content_sha": "x",
        "embedded_at": datetime.now(timezone.utc),
    }
    features = python.Engine()._parse_schema_feature_group(
        embed_run.upsert_frame([row]), "HUDI"
    )
    assert {f.name: f.type for f in features} == {
        "doc_id": "string",
        "chunk_ix": "bigint",
        "text": "string",
        "embedding": "array<float>",
        "content_sha": "string",
        "embedded_at": "timestamp",
    }


def test_create_with_an_embedding_builds_the_index(mock_project):
    fs = mock_project.get_feature_store.return_value
    result = CliRunner().invoke(
        cli,
        ["fg", "create", "docs", "--primary-key", "id", "--features", "id:bigint",
         "--embedding", "vec:3"],
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    index = fs.create_feature_group.call_args.kwargs["embedding_index"]
    (vec,) = list(index.get_embeddings())
    assert (vec.name, vec.dimension, vec.similarity_function_type) == (
        "vec",
        3,
        "cosine",
    )


# --- the embedding client ----------------------------------------------------


def test_the_client_pairs_each_vector_with_its_text():
    from hopsworks_agents import embed

    answer = {"predictions": [{"data": [
        {"index": 1, "embedding": [2.0]}, {"index": 0, "embedding": [1.0]},
    ]}]}  # fmt: skip
    answer["predictions"][0]["model"] = "m"
    assert embed._vectors(answer, 2, "m") == [[1.0], [2.0]]
    with pytest.raises(ValueError, match="1 vectors for 2 texts"):
        embed._vectors(
            {"predictions": [{"model": "m", "data": [{"embedding": [1.0]}]}]}, 2, "m"
        )
    with pytest.raises(ValueError, match="reports model 'other', not 'm'"):
        embed._vectors({"predictions": [{"model": "other", "data": []}]}, 0, "m")


def test_the_client_needs_the_project_and_the_model(monkeypatch):
    from hopsworks_agents import embed

    monkeypatch.setenv("HOPSABLE_EMBEDDING_PROJECT", "hopsable_embeddings")
    for name in ("DEPLOYMENT", "BUILD_DEPLOYMENT", "MODEL"):
        monkeypatch.delenv(f"HOPSABLE_EMBEDDING_{name}", raising=False)
    with pytest.raises(ValueError, match="HOPSABLE_EMBEDDING_MODEL"):
        embed.endpoint()
    assert embed.endpoint(model="m") == ("hopsable_embeddings", "embedder", "m")
    assert embed.endpoint(model="m", build=True) == (
        "hopsable_embeddings",
        "embedderbuild",
        "m",
    )


def test_the_client_posts_to_the_serving_project_by_its_backing_name(monkeypatch):
    from hopsworks_agents import embed

    monkeypatch.setattr(embed, "_routes", {})
    rest, istio = mock.MagicMock(), mock.MagicMock()
    rest._send_request.return_value = {
        "projectId": 21623, "projectName": "hopsable_embeddings", "backingName": "5okprgv34044",
    }  # fmt: skip
    istio._send_request.return_value = {
        "predictions": [{"model": "bge", "data": [{"embedding": [0.5]}]}]
    }
    with (
        mock.patch("hopsworks_common.client._get_instance", return_value=rest),
        mock.patch("hopsworks_common.client.istio._get_instance", return_value=istio),
    ):
        vectors = embed.embed_texts(
            ["hi"],
            project="hopsable_embeddings",
            deployment="embedderbuild",
            model="bge",
        )
    assert vectors == [[0.5]]
    call = istio._send_request.call_args
    assert call.args[1] == [
        "v1",
        "5okprgv34044",
        "embedderbuild",
        "v1",
        "models",
        "embedderbuild:predict",
    ]
    assert json.loads(call.kwargs["data"]) == {
        "instances": [{"model": "bge", "input": ["hi"]}]
    }
    assert call.kwargs["with_base_path_params"] is False


def test_every_shipped_text_awaits_the_embed_build_with_job_wait():
    """`hops job info` shows a job's config and no run state: an agent told to read an embed build there polls it."""
    import re
    from pathlib import Path

    from hopsworks.cli.commands import embed

    root = Path(__file__).resolve().parents[3]
    texts = {
        "embed --help": embed.fg_embed.callback.__doc__,
        **{
            str(p.relative_to(root)): p.read_text()
            for pattern in (
                "python/hopsworks/skills/**/*.md",
                "python/hopsworks/cli/templates/*.md",
                ".claude/**/*.md",
            )
            for p in root.glob(pattern)
        },
    }
    stale = {
        name
        for name, text in texts.items()
        if re.search(r"job info[^\n|]{0,40}_embed", text)
    }
    assert not stale
    naming = {
        name
        for name, text in texts.items()
        if "hops fg embed" in text and "_embed" in text
    }
    assert naming, "no shipped text describes the embed build"
    assert all("hops job wait" in texts[name] for name in naming), sorted(
        n for n in naming if "hops job wait" not in texts[n]
    )
