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
"""`hops fg embed` under the writer fence, against a real project (HABLE-239).

Each test makes a source feature group, builds its embedding group with the
command (the build runs as the group's job), checks the stores and deletes
everything with `hops fg embed delete`. A stale writer is played by records
produced to the group's topic with a lower writer epoch, the shape of a delayed
Kafka append, and by Delta commits of a lower epoch.

Runs only with HOPS_LIVE=1, a cluster login of a test user in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT, the embedding settings
(HOPSABLE_EMBEDDING_PROJECT, HOPSABLE_EMBEDDING_MODEL) and a backend, OnlineFS
and SDK with the HABLE-239 fence. Each run calls the build embedding deployment.
HOPS_EMBED_ENV, required, names the Python environment the build job runs in:
the cluster default carries an unfenced client.

Not automated here, run by hand on the dev cluster under its claim: OnlineFS
restart between a fence and a stale record (r5 a, r10), two OnlineFS replicas
with split partitions (r5 e, r6), a paused former partition owner (r7), a
request held at a proxy in front of OpenSearch and the A/B/C held-write
sequence (r7, r8, r9), a failed tombstone through an index write block (r7),
auto-create denial as the real OnlineFS identity (H5).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from urllib.parse import urlparse

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("HOPS_LIVE") != "1", reason="needs HOPS_LIVE=1 and a cluster login"
)

if os.environ.get("HOPS_LIVE") == "1" and not os.environ.get("HOPS_EMBED_ENV"):
    # On the cluster default the build runs the unfenced pod client: a result there proves nothing.
    raise pytest.UsageError(
        "HOPS_EMBED_ENV must name an environment with the fenced client"
    )

ENV_ARGS = (
    ("--env", os.environ["HOPS_EMBED_ENV"]) if os.environ.get("HOPS_EMBED_ENV") else ()
)

#: A build pod takes minutes to start; a run of a few rows ends well inside this.
RUN_TIMEOUT_S = 900
ROWS = {1: "alpha " * 50, 2: "beta " * 50, 3: "gamma " * 50}


def _hops(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hopsworks.cli", *args], capture_output=True, text=True
    )


def _hops_json(*args: str) -> dict:
    result = _hops("--json", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.fixture(scope="module")
def project():
    import hopsworks

    return hopsworks.login()


@pytest.fixture
def source(project):
    import pandas as pd

    fs = project.get_feature_store()
    name = f"fence_src_{uuid.uuid4().hex[:8]}"
    fg = fs.create_feature_group(
        name,
        version=1,
        primary_key=["id"],
        online_enabled=False,
        statistics_config=False,
        description="test_fg_embed_fence fixture, deleted by the test",
    )
    fg.insert(pd.DataFrame({"id": list(ROWS), "body": list(ROWS.values())}), wait=True)
    yield fg
    fs.get_feature_group(name, version=1).delete()


@pytest.fixture
def group(project, source):
    """The embedding group of `source`, built once; deleted with `hops fg embed delete` after the test."""
    name = f"{source.name}_body_emb"
    built = _build(project, source.name, name)
    yield {"name": name, **built}
    if _job(project, name) is not None or _versions(project, name):
        deleted = _hops("fg", "embed", "delete", name, "--yes")
        assert deleted.returncode == 0, deleted.stdout + deleted.stderr


def _job(project, name):
    try:
        return project.get_job_api().get_job(f"{name}_embed")
    except Exception:  # noqa: BLE001
        return None


def _versions(project, name):
    from hopsworks.cli import embed_run

    return embed_run.versions_of(project.get_feature_store(), name)


def _wait(project, job_name: str, execution_id: int) -> object:
    from hopsworks_common.core.execution_api import ExecutionApi

    job = project.get_job_api().get_job(job_name)
    deadline = time.monotonic() + RUN_TIMEOUT_S
    while True:
        execution = ExecutionApi()._get(job, execution_id)
        if str(execution.final_status or "UNDEFINED") != "UNDEFINED":
            return execution
        assert time.monotonic() < deadline, f"execution {execution_id} still running"
        time.sleep(5)


#: A refused start says "retry after N s" (HTTP 429): the platform class's start rate, and each instance
#: process's own bucket, which is empty when that process starts.
RATE_RETRIES = 3


def _build(project, source_name: str, name: str) -> dict:
    args = ("fg", "embed", source_name, "--text", "body", "--to", name, *ENV_ARGS)
    for attempt in range(RATE_RETRIES):
        result = _hops("--json", *args)
        wait = re.search(r"retry after (\d+) s", result.stderr)
        if result.returncode == 0 or wait is None or attempt == RATE_RETRIES - 1:
            break
        time.sleep(int(wait.group(1)) + 1)
    assert result.returncode == 0, result.stdout + result.stderr
    started = json.loads(result.stdout)
    execution = _wait(project, started["job"], started["execution"])
    assert execution.final_status == "SUCCEEDED", execution
    return started


def _fg(project, name, version=1):
    return project.get_feature_store().get_feature_group(name, version=version)


def _docs(fg, keys):
    from hopsworks.cli.embed_run import Stores

    return Stores(fg).online(sorted(keys))


def _all_keys():
    from hopsworks.cli import embed_run

    return {
        (str(doc), ix)
        for doc, text in ROWS.items()
        for ix in range(len(embed_run.chunks(text)))
    }


def _epoch(fg):
    """The epoch every document of the group carries; one value after a run that wrote them all."""
    versions = {d.version for d in _docs(fg, _all_keys()).values()}
    assert len(versions) == 1, versions
    return versions.pop()


def _writer(fg, epoch: int, entries: int) -> tuple:
    """A writer's Kafka options and resources for `entries` records of the group at `epoch`."""
    from hsfs.core import kafka_engine

    options = {kafka_engine.WRITER_EPOCH_OPTION: epoch}
    return options, kafka_engine._get_kafka_resources(
        fg, options, num_entries=entries, storage="online"
    )


def _inject(
    fg,
    epoch: int,
    rows: list[dict],
    delete: bool = False,
    resources: tuple | None = None,
) -> None:
    """Records of the group at `epoch` on its topic, as a stale writer's delayed append would arrive."""
    from hsfs.core import kafka_engine
    from hsfs.engine import python

    options, (producer, headers, writers, writer) = resources or _writer(
        fg, epoch, len(rows)
    )
    if delete:
        headers = {**headers, "operation": b"delete"}
    fill = kafka_engine._online_delete_fill_values(fg) if delete else {}
    for row in rows:
        python.Engine._produce_row_kafka(
            fg, producer, {**fill, **row}, headers, writers, writer, None, options
        )
    producer.flush()


def _row(doc, ix, text, dimension):
    from hopsworks.cli import embed_run

    return {
        "doc_id": doc,
        "chunk_ix": ix,
        "text": text,
        "embedding": [0.5] * dimension,
        "content_sha": embed_run.sha(text),
        "embedded_at": None,
    }


def _settle_wait():
    # Longer than the build's read-back deadline: a stale record has had every chance to apply.
    time.sleep(30)


def _inject_per_partition(fg, epoch: int, prefix: str, dimension: int) -> list[int]:
    """One record of key `<prefix><p>` at `epoch` on each partition p of the group's topic; the partitions.

    Each record names its partition, as the fence records do, so every partition gets one whatever the keys.
    """
    from hsfs.core import kafka_engine

    topic = fg._online_topic_name
    partitions = sorted(
        kafka_engine._init_kafka_producer(fg.feature_store_id, {})
        .list_topics(topic, timeout=30)
        .topics[topic]
        .partitions
    )
    _, (producer, headers, writers, writer) = _writer(fg, epoch, len(partitions))
    failed = []

    def acked(err, msg):
        if err is not None:
            failed.append(f"partition {msg.partition()}: {err}")

    for p in partitions:
        row = _row(f"{prefix}{p}", 0, f"{prefix} on partition {p}", dimension)
        producer.produce(
            topic=topic,
            partition=p,
            key="".join(str(row[k]) for k in sorted(fg.primary_key)),
            value=kafka_engine._encode_row(writers, writer, row),
            headers=headers,
            on_delivery=acked,
        )
    assert producer.flush(30) == 0 and not failed, failed
    return partitions


#: Execution states before RUNNING (JobState.getQueuedStates in the backend). A state never moves back
#: to one of these, so one read after a claim's answer shows the state the claim was taken in.
BEFORE_RUNNING = {
    "INITIALIZING",
    "NEW",
    "NEW_SAVING",
    "GENERATING_SECURITY_MATERIAL",
    "SUBMITTED",
    "ACCEPTED",
    "STARTING_APP_MASTER",
    "CONVERTING_NOTEBOOK",
    "QUEUED",
    "PENDING",
    "PENDING_RERUN",
}


def _start(job):
    """A new execution of the job; a start refused for the start rate (429) is retried after its Retry-After."""
    from hopsworks_common.client.exceptions import RestAPIError
    from hopsworks_common.core.execution_api import ExecutionApi

    for _ in range(RATE_RETRIES - 1):
        try:
            return ExecutionApi()._start(job)
        except RestAPIError as exc:
            after = (exc.response.headers.get("Retry-After") or "").strip()
            if exc.response.status_code != 429 or not after.isdigit():
                raise
            time.sleep(int(after) + 1)
    return ExecutionApi()._start(job)


def _claim_before_running(project, fg, job) -> tuple[int, dict, str]:
    """Claim the group from a new execution of its job, read its state, then stop it.

    (execution id, the claim's answer, the execution's state read right after the answer). The build pod
    takes minutes to start, so the execution's own code never runs: the claim stays unsettled.
    """
    from hopsworks.cli import embed_run
    from hopsworks_common.core.execution_api import ExecutionApi

    execution = _start(job)
    answer = embed_run.Writer(fg, execution.id)._send("POST", "writer")
    state = ExecutionApi()._get(job, execution.id).state
    ExecutionApi()._stop(job.name, execution.id)
    _wait(project, job.name, execution.id)
    return execution.id, answer, state


# --- runs ------------------------------------------------------------------------


def test_a_first_run_writes_every_chunk_at_one_epoch_in_both_stores(project, group):
    fg = _fg(project, group["name"])
    assert fg.embedding_index.fenced is True
    assert fg.writer_job_id == _job(project, group["name"]).id
    docs = _docs(fg, _all_keys())
    assert set(docs) == _all_keys()
    assert all(not d.deleted for d in docs.values())
    assert _epoch(fg) >= 1
    offline = fg.read()
    assert len(offline) == len(_all_keys())


def test_an_unchanged_rerun_rewrites_nothing(project, source, group):
    fg = _fg(project, group["name"])
    first = _epoch(fg)
    _build(project, source.name, group["name"])
    assert _epoch(fg) == first


def test_a_rerun_after_a_source_change_writes_and_deletes_at_its_epoch(
    project, source, group
):
    import pandas as pd

    fg = _fg(project, group["name"])
    first = _epoch(fg)
    source.insert(pd.DataFrame({"id": [1], "body": ["changed alpha"]}), wait=True)
    source.remove_rows(pd.DataFrame({"id": [3]}))
    _build(project, source.name, group["name"])
    docs = _docs(fg, _all_keys())
    changed = docs[("1", 0)]
    assert changed.version > first and not changed.deleted
    gone = [k for k in _all_keys() if k[0] == "3"]
    assert all(docs[k].deleted and docs[k].version == changed.version for k in gone)
    assert not set(fg.read()["doc_id"].astype(str)) & {"3"}


# --- stale online records (r5 a, r6) -----------------------------------------------


def test_a_stale_record_after_a_run_is_dropped_for_a_key_present_or_absent(
    project, group
):
    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    _inject(
        fg,
        epoch - 1,
        [
            _row("1", 0, "stale value", dimension),
            _row("absent", 0, "stale key", dimension),
        ],
    )
    _settle_wait()
    docs = _docs(fg, {("1", 0), ("absent", 0)})
    stale = _row("1", 0, "stale value", dimension)["content_sha"]
    assert docs[("1", 0)].version == epoch
    assert docs[("1", 0)].sha != stale
    assert ("absent", 0) not in docs


def test_a_record_without_an_epoch_is_dropped_on_a_fenced_group(project, group):
    import numpy as np
    import pandas as pd

    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    row = _row("noepoch", 0, "unfenced", dimension)
    # The schema's array<float>: a list of Python floats derives array<double>, which the insert refuses.
    row["embedding"] = np.full(dimension, 0.5, dtype=np.float32)
    fg.insert(pd.DataFrame([row]), storage="online")
    _settle_wait()
    assert ("noepoch", 0) not in _docs(fg, {("noepoch", 0)})
    assert _epoch(fg) == epoch


def test_an_unchanged_run_still_fences_so_an_earlier_epoch_is_dropped(
    project, source, group
):
    fg = _fg(project, group["name"])
    first = _epoch(fg)
    _build(project, source.name, group["name"])
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    # The second run wrote nothing but fenced at a higher epoch: the first run's epoch is now stale.
    _inject(fg, first, [_row("absent", 0, "stale key", dimension)])
    _settle_wait()
    assert ("absent", 0) not in _docs(fg, {("absent", 0)})


def test_two_fenced_groups_on_one_topic_fence_only_themselves(project, source, group):
    other = f"{source.name}_body2_emb"
    try:
        _build(project, source.name, other)
        x, y = _fg(project, group["name"]), _fg(project, other)
        assert x._online_topic_name == y._online_topic_name
        dimension = x.embedding_index.get_embedding("embedding").dimension
        _inject(x, _epoch(x) - 1, [_row("absent", 0, "stale x", dimension)])
        _inject(y, _epoch(y), [_row("fresh", 0, "y at its own epoch", dimension)])
        _settle_wait()
        assert ("absent", 0) not in _docs(x, {("absent", 0)})
        assert ("fresh", 0) in _docs(y, {("fresh", 0)})
    finally:
        _hops("fg", "embed", "delete", other, "--yes")


def test_stale_records_on_every_partition_are_dropped(project, group):
    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    # Keys the source lacks: a stale record of an existing key is refused by the version check alone.
    partitions = _inject_per_partition(fg, epoch - 1, "stale", dimension)
    assert len(partitions) >= 2, partitions
    # The same records at the fence epoch: every partition is consumed, so the stale ones were read and dropped.
    _inject_per_partition(fg, epoch, "fresh", dimension)
    _settle_wait()
    stale = {(f"stale{p}", 0) for p in partitions}
    fresh = {(f"fresh{p}", 0) for p in partitions}
    docs = _docs(fg, stale | fresh)
    assert not stale & set(docs), sorted(stale & set(docs))
    assert fresh <= set(docs), sorted(fresh - set(docs))


# --- the delete leg order (r6, r7) -----------------------------------------------------


def test_a_crash_between_the_delete_legs_is_finished_by_the_next_run(
    project, source, group
):
    """The state a run killed after its online tombstone leaves: gone online, still offline."""
    import pandas as pd

    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    source.remove_rows(pd.DataFrame({"id": [2]}))
    keys = [k for k in _all_keys() if k[0] == "2"]
    _inject(fg, epoch, [{"doc_id": d, "chunk_ix": i} for d, i in keys], delete=True)
    _settle_wait()
    assert all(_docs(fg, keys)[k].deleted for k in keys)
    assert set(fg.read()["doc_id"].astype(str)) >= {"2"}
    _build(project, source.name, group["name"])
    docs = _docs(fg, keys)
    assert all(docs[k].deleted and docs[k].version > epoch for k in keys)
    assert "2" not in set(fg.read()["doc_id"].astype(str))


# --- recovery of unsettled epochs (r9) ----------------------------------------------------


def test_a_crash_during_recovery_before_settlement_is_finished_by_the_next_run(
    project, source, group
):
    """Run A claimed and fenced, one held write landed, never settled; recovery R rewrote one key and stopped."""
    from hopsworks.cli import embed_run
    from hopsworks.cli.embed_run import Stores

    fg = _fg(project, group["name"])
    job = _job(project, group["name"])
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    _, a, state = _claim_before_running(project, fg, job)
    assert state in BEFORE_RUNNING, state
    a_epoch = int(a["epoch"])
    Stores(fg, a_epoch).fence()
    _inject(fg, a_epoch, [_row("1", 0, "held write of A", dimension)])
    _, r, state = _claim_before_running(project, fg, job)
    assert state in BEFORE_RUNNING, state
    r_epoch = int(r["epoch"])
    assert r_epoch > a_epoch and a_epoch in r["unsettledEpochs"], r
    Stores(fg, r_epoch).fence()
    _inject(fg, r_epoch, [_row("2", 0, ROWS[2], dimension)])
    _settle_wait()
    crashed = _docs(fg, _all_keys())
    assert crashed[("1", 0)].version == a_epoch
    assert crashed[("2", 0)].version == r_epoch

    _build(project, source.name, group["name"])
    docs = _docs(fg, _all_keys())
    want = {
        (str(doc), ix): embed_run.sha(chunk)
        for doc, text in ROWS.items()
        for ix, chunk in enumerate(embed_run.chunks(text))
    }
    assert all(not docs[k].deleted and docs[k].sha == want[k] for k in want)
    assert all(docs[k].version >= r_epoch for k in want)
    # A's held value sat below R's epoch: rewritten at the next run's epoch.
    assert docs[("1", 0)].version > r_epoch
    _, later, _ = _claim_before_running(project, fg, job)
    assert not later.get("unsettledEpochs"), later


# --- stale Delta commits (r5 b, r6) -------------------------------------------------------


def test_a_lower_epoch_delta_commit_is_refused_and_a_racing_one_fails(project, group):
    import pandas as pd
    from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
    from deltalake.exceptions import CommitFailedError
    from hsfs.core import delta_engine

    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    engine = delta_engine.DeltaEngine(
        fg.feature_store_id, fg.feature_store_name, fg, None, None
    )
    location = engine._get_delta_rs_location()
    before = DeltaTable(location).version()
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    with pytest.raises(delta_engine.StaleWriterError):
        engine._write_delta_rs_dataset(
            pd.DataFrame([_row("1", 0, "stale", dimension)]),
            write_options={"writer_epoch": epoch - 1},
        )
    assert DeltaTable(location).version() == before

    # Two writers racing from the same snapshot with the same app id: the second fails at commit.
    first, second = DeltaTable(location), DeltaTable(location)
    import pyarrow as pa

    empty = pa.schema(first.schema().to_arrow()).empty_table()

    def txn(v):
        return CommitProperties(
            app_transactions=[Transaction(app_id="hops-writer", version=v)]
        )

    write_deltalake(first, empty, mode="append", commit_properties=txn(epoch))
    with pytest.raises(CommitFailedError):
        write_deltalake(second, empty, mode="append", commit_properties=txn(epoch))


# --- claims (r5 c, r10) ----------------------------------------------------------------


def test_a_claim_from_an_ended_execution_is_refused(project, group):
    from hopsworks.cli import embed_run
    from hopsworks_common.client.exceptions import RestAPIError

    fg = _fg(project, group["name"])
    with pytest.raises(RestAPIError) as refused:
        embed_run.Writer(fg, group["execution"])._send("POST", "writer")
    assert refused.value.response.status_code == 409
    assert "ended" in refused.value.user_message


def test_a_claim_from_an_execution_before_running_is_accepted_and_refused_once_it_ended(
    project, group
):
    """r10: a claim accepts any non-final execution of the writer job, RUNNING or not yet."""
    from hopsworks.cli import embed_run
    from hopsworks_common.client.exceptions import RestAPIError

    fg = _fg(project, group["name"])
    epoch = _epoch(fg)
    execution, answer, state = _claim_before_running(
        project, fg, _job(project, group["name"])
    )
    assert state in BEFORE_RUNNING, state
    assert int(answer["epoch"]) > epoch, answer
    with pytest.raises(RestAPIError) as refused:
        embed_run.Writer(fg, execution)._send("POST", "writer")
    assert refused.value.response.status_code == 409
    assert "ended" in refused.value.user_message


def test_a_rerun_of_a_stopped_build_gets_an_epoch_above_the_last(
    project, source, group
):
    from hopsworks_common.core.execution_api import ExecutionApi

    fg = _fg(project, group["name"])
    job = _job(project, group["name"])
    stopped = job.run(await_termination=False)
    ExecutionApi()._stop(job.name, stopped.id)
    _wait(project, job.name, stopped.id)
    middle = _build(project, source.name, group["name"])
    assert middle["execution"] != stopped.id
    import pandas as pd

    source.insert(pd.DataFrame({"id": [1], "body": ["rerun alpha"]}), wait=True)
    before = _docs(fg, {("1", 0)})[("1", 0)].version
    ExecutionApi()._rerun(job.name, stopped.id)
    rerun = _wait(project, job.name, stopped.id)
    assert rerun.final_status == "SUCCEEDED", rerun
    assert _docs(fg, {("1", 0)})[("1", 0)].version > before


# --- delete (r5 d, r10) ------------------------------------------------------------------


def test_delete_runs_the_deletion_execution_and_removes_group_job_and_index(
    project, group
):
    fg = _fg(project, group["name"])
    dimension = fg.embedding_index.get_embedding("embedding").dimension
    epoch = _epoch(fg)
    # A stale writer set up its Kafka resources before the delete.
    stale = _writer(fg, epoch, 1)
    deleted = _hops_json("fg", "embed", "delete", group["name"], "--yes")
    assert deleted["deletion_execution"]
    assert _job(project, group["name"]) is None
    assert _versions(project, group["name"]) == []
    # A stale record for the deleted group targets a deleted group and index: nothing comes back.
    _inject(fg, epoch, [_row("late", 0, "after delete", dimension)], resources=stale)
    _settle_wait()
    assert _versions(project, group["name"]) == []


def test_a_held_record_of_a_deleted_group_does_not_reach_its_recreated_group(
    project, source, group
):
    from hopsworks_common.core.opensearch import OpenSearchClientSingleton
    from opensearchpy.exceptions import AuthorizationException

    old = _fg(project, group["name"])
    dimension = old.embedding_index.get_embedding("embedding").dimension
    old_index = old.embedding_index.index_name
    # Far above any epoch the new incarnation takes here, so only the group the record names keeps it out.
    held_epoch = _epoch(old) + 1_000_000
    held = _writer(old, held_epoch, 2)
    _hops_json("fg", "embed", "delete", group["name"], "--yes")
    _build(project, source.name, group["name"])
    new = _fg(project, group["name"])
    assert new.id != old.id
    assert new.embedding_index.index_name != old_index
    before = _docs(new, _all_keys())
    epoch = _epoch(new)
    _inject(
        old,
        held_epoch,
        [
            _row("1", 0, "held value of incarnation 1", dimension),
            _row("held", 0, "held key of incarnation 1", dimension),
        ],
        resources=held,
    )
    _settle_wait()
    after = _docs(new, _all_keys() | {("held", 0)})
    assert ("held", 0) not in after
    assert {k: after[k] for k in _all_keys()} == before
    assert _epoch(new) == epoch
    try:
        exists = (
            OpenSearchClientSingleton(feature_store_id=new.feature_store_id)
            ._get_opensearch_client()
            .indices.exists(index=old_index)
        )
    except AuthorizationException:
        print(
            f"HEAD {old_index} refused to the test user: the operator step reads it as admin"
        )
    else:
        assert exists is False, old_index


def test_a_start_while_the_job_is_being_deleted_is_refused(project, source, group):
    from hopsworks_common.core.execution_api import ExecutionApi

    job = _job(project, group["name"])
    deletion = ExecutionApi()._start(
        job,
        args=f"fg embed delete {group['name']} --in-job",
        deletion=True,
    )
    started = _hops(
        "fg", "embed", source.name, "--text", "body", "--to", group["name"], *ENV_ARGS
    )
    assert started.returncode == 1
    assert "is being deleted; nothing was started" in started.stdout + started.stderr
    _wait(project, job.name, deletion.id)


def test_a_delete_interrupted_after_its_deletion_execution_finishes_on_a_rerun(
    project, group
):
    from hopsworks_common.core.execution_api import ExecutionApi

    job = _job(project, group["name"])
    deletion = ExecutionApi()._start(
        job,
        args=f"fg embed delete {group['name']} --in-job",
        deletion=True,
    )
    assert _wait(project, job.name, deletion.id).final_status == "SUCCEEDED"
    assert _versions(project, group["name"]) == []
    assert _job(project, group["name"]) is not None
    finished = _hops_json("fg", "embed", "delete", group["name"], "--yes")
    assert finished["deletion_execution"] == deletion.id
    assert _job(project, group["name"]) is None


@pytest.mark.xfail(
    strict=True,
    reason="H3 is blocked: a stale Delta append after the table directory is removed recreates it "
    "(h3-out.txt, TODO line 13)",
)
def test_a_stale_delta_append_after_delete_does_not_recreate_the_table(project, group):
    import pyarrow as pa
    from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
    from hopsworks_common.client.exceptions import RestAPIError
    from hsfs.core import delta_engine

    fg = _fg(project, group["name"])
    location = delta_engine.DeltaEngine(
        fg.feature_store_id, fg.feature_store_name, fg, None, None
    )._get_delta_rs_location()
    stale = DeltaTable(location)
    empty = pa.schema(stale.schema().to_arrow()).empty_table()
    _hops_json("fg", "embed", "delete", group["name"], "--yes")
    # A refused append passes too: the directory decides.
    with contextlib.suppress(Exception):
        write_deltalake(
            stale,
            empty,
            mode="append",
            commit_properties=CommitProperties(
                app_transactions=[Transaction(app_id="hops-writer", version=1)]
            ),
        )
    with pytest.raises(RestAPIError) as gone:
        project.get_dataset_api()._get(urlparse(fg.location).path)
    assert gone.value.response.status_code == 404
