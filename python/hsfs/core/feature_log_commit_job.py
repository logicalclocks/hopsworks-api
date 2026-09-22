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
"""Commit job of the `job` feature logging transport.

Runs as a PYTHON job with the feature view's name and version as arguments.
It claims the chunks that deployments staged under the feature view's `pending/` directory, adds a `log_id` derived from the chunk and the row's position, and appends the rows to the logging feature group in bounded Delta commits.
The file is uploaded as the job's script when logging is enabled with the `job` transport, so it depends on nothing outside the released client surface.

A claim is a manifest first and moved files second, so an execution that dies at any point leaves either files still in `pending/` or a manifest that names them; the next execution finishes the move and the commit.
Every commit carries one Delta application transaction per chunk it holds, and a chunk whose transaction the table already records is committed by nobody again, whether it comes back under a retried claim or as a second copy under a later one.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _positive_env(name: str, default: int) -> int:
    """A positive integer setting, from the environment or the default.

    Defined here rather than imported: this file is uploaded as the commit job's script
    and keeps to what it can carry on its own.

    The name carries no `HOPSWORKS_` prefix on purpose. That prefix is reserved
    (`ReservedEnvVars.RESERVED_PREFIXES`), so a job cannot be given a variable that
    carries it, and the limits below would be settable by nobody. These names reach a
    manual run through `job.run(env_vars=...)`; a scheduled run has no environment of its
    own, so it takes the values from the job's arguments instead, which is what
    `_maintenance_limits` reads.
    """
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


STAGING_ROOT = "Resources/feature_logging"
CHUNK_SUFFIX = ".arrow"
LOG_ID_NAMESPACE = uuid.UUID("8c4a9d5e-3f0b-4c6e-9a1d-3b2c1d0e9f8a")
MAX_CHUNKS_PER_CLAIM = 256
MAX_PART_BYTES = 512 * 1024 * 1024
MAX_CLAIMS_PER_EXECUTION = 10
CLAIM_RECLAIM_SECONDS = 7200
# Active data files at which an execution compacts instead of waiting for midnight.
# One append commit writes one file, and the commits here carry roughly a megabyte to a
# few, so a hundred files is the low hundreds of megabytes: around one compacted file at
# the engine's own target, and small enough that the rewrite fits beside the writes.
COMPACT_FILE_THRESHOLD = _positive_env("FEATURE_LOG_COMPACT_FILE_THRESHOLD", 100)
# The rewrite runs in this job's process, beside the commits, so it does not get the
# pod's whole CPU budget.
COMPACT_CONCURRENT_TASKS = _positive_env("FEATURE_LOG_COMPACT_CONCURRENT_TASKS", 1)
# A vacuum deletes files an in-flight query may still be reading, so this has to stay
# comfortably longer than the longest query that runs against a logging group. It is
# also the time travel window: a version whose files have been vacuumed cannot be read.
COMPACT_VACUUM_RETENTION_HOURS = _positive_env("FEATURE_LOG_VACUUM_RETENTION_HOURS", 24)
# Days before the last compaction that an incremental one reopens, so a row that arrived
# late still reaches a partition the rewrite covers.
COMPACT_LOOKBACK_DAYS = _positive_env("FEATURE_LOG_COMPACT_LOOKBACK_DAYS", 1)
# Older than any upload still in progress: a chunk this old in `uploading/` is whole
# and its pod is gone.
UPLOAD_STALE_SECONDS = 3600


def _read_chunk(path):
    """Read one chunk batch by batch; a truncated tail loses only its last batch.

    Returns:
        The table of complete batches, or `None` when not one batch was readable, and whether the tail was truncated.
    """
    import pyarrow as pa

    batches = []
    truncated = False
    with open(path, "rb") as stream:
        try:
            reader = pa.ipc.open_stream(stream)
            for batch in reader:
                batches.append(batch)
        except (pa.ArrowInvalid, pa.ArrowIOError, OSError, ValueError):
            truncated = True
            if not batches:
                return None, truncated
        schema = reader.schema
    return pa.Table.from_batches(batches, schema=schema), truncated


def _chunk_id_of(table, path) -> str:
    metadata = table.schema.metadata or {}
    chunk_id = metadata.get(b"chunk_id")
    return chunk_id.decode() if chunk_id else Path(path).stem


def _with_log_ids(table, chunk_id: str):
    """Add `log_id` values that are the same on every run over the same chunk."""
    import pyarrow as pa

    ids = [
        str(uuid.uuid5(LOG_ID_NAMESPACE, f"{chunk_id}:{ordinal}"))
        for ordinal in range(table.num_rows)
    ]
    return table.append_column("log_id", pa.array(ids, pa.string()))


def _frame(table):
    """The table as a pandas frame whose columns keep their Arrow types.

    Arrow-backed columns hand the engine the chunk's own schema, nested types and all,
    where a conversion to NumPy-backed columns would have the engine infer types from
    the values again: an integer list with a null became doubles, a decimal took the
    precision of the values it happened to hold, an all-null date became a timestamp.
    """
    import pandas as pd

    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _columns(feature_group):
    return getattr(feature_group, "columns", None) or feature_group.features


def _chunk_app_id(feature_group, chunk_id: str) -> str:
    return f"hopsworks_feature_log_{feature_group.id}/chunk/{chunk_id}"


class _DeltaTransactions:
    """The Delta application transactions that record which chunks the table holds.

    One transaction per chunk, in the same commit as the chunk's rows, so the record and
    the rows land or fail together. A chunk is recognised by its own identity rather
    than by the claim that carried it: the same chunk reaches `pending/` twice when an
    upload whose acknowledgement was lost is retried, or when a stale upload is adopted
    while its writer is still retrying, and the second copy arrives under a later claim.
    """

    def __init__(self, feature_group):
        self._feature_group = feature_group
        self._table = None
        self.available = False
        try:
            from deltalake import DeltaTable
            from deltalake.exceptions import TableNotFoundError
            from hsfs.core import delta_engine

            engine = delta_engine.DeltaEngine(
                feature_store_id=feature_group.feature_store_id,
                feature_store_name=feature_group.feature_store_name,
                feature_group=feature_group,
                spark_context=None,
                spark_session=None,
            )
            self._location = engine._get_delta_rs_location()
            self._storage_options = engine._get_delta_rs_storage_options()
            self._DeltaTable = DeltaTable
            self._not_found = TableNotFoundError
            self.available = True
        except (ImportError, AttributeError):
            # An older client: retries fall back on the derived log ids alone.
            self.available = False

    def _committed(self, chunk_id: str) -> bool:
        """Whether this chunk's rows are in the table; a missing table means they are not.

        The log is read once per claim, which is enough: a claim holds each chunk name once.
        Any other failure to read the log is raised: guessing here is what turns a retry into a duplicate or a loss.
        """
        if not self.available:
            return False
        if self._table is None:
            try:
                self._table = self._DeltaTable(
                    self._location, storage_options=self._storage_options
                )
            except self._not_found:
                return False
        app_id = _chunk_app_id(self._feature_group, chunk_id)
        return self._table.transaction_version(app_id) is not None


def _absolute(path: str) -> str:
    """Dataset reads take the project path with its leading slash; listings return it that way."""
    return path if path.startswith("/") else "/" + path


def _modified_at(attributes: dict) -> float:
    """The Dataset API's modification time as epoch seconds; it arrives as ISO text or epoch milliseconds."""
    value = attributes.get("modificationTime", 0)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    return float(value) / 1000.0


def _manifest_content(content) -> str:
    if content is None:
        return ""
    if isinstance(content, dict):
        return content.get("content", "") or ""
    text = getattr(content, "text", None)
    if text is not None:
        return text
    raw = getattr(content, "content", b"")
    return raw.decode() if isinstance(raw, bytes) else str(raw)


class _Staging:
    """The feature view's staging directory in HopsFS through the Dataset API."""

    def __init__(self, dataset_api, feature_view_name: str, feature_view_version: int):
        self._api = dataset_api
        self.root = f"{STAGING_ROOT}/{feature_view_name}_{feature_view_version}"
        for name in ("pending", "claimed", "failed"):
            self._mkdirs(f"{self.root}/{name}")

    def _mkdirs(self, path: str) -> None:
        # The Dataset API creates one level at a time.
        parts = [p for p in path.split("/") if p]
        for depth in range(1, len(parts) + 1):
            current = "/".join(parts[:depth])
            if not self._api.exists(current):
                self._api.mkdir(current)

    def _list(self, path: str) -> list[str]:
        entries = []
        offset = 0
        while True:
            page = self._api.list(path, offset=offset, limit=1000)
            entries.extend(page)
            if len(page) < 1000:
                return entries
            offset += len(page)

    def _adopt_stale_uploads(self) -> list[str]:
        """Complete chunks a pod left in `uploading/` when it was killed before the rename into `pending/`.

        An upload in progress keeps a `.temp` companion and is younger than `UPLOAD_STALE_SECONDS`; anything else there is whole.
        """
        uploading = f"{self.root}/uploading"
        if not self._api.exists(uploading):
            return []
        entries = set(self._list(uploading))
        adopted = []
        for path in sorted(entries):
            if not path.endswith(CHUNK_SUFFIX) or path + ".temp" in entries:
                continue
            attributes = self._api._get(_absolute(path)).get("attributes", {})
            if time.time() - _modified_at(attributes) < UPLOAD_STALE_SECONDS:
                continue
            name = path.rsplit("/", 1)[-1]
            self._api.move(path, f"{self.root}/pending/{name}", overwrite=True)
            adopted.append(name)
        return adopted

    def _pending(self) -> list[str]:
        return sorted(
            p for p in self._list(f"{self.root}/pending") if p.endswith(CHUNK_SUFFIX)
        )

    def _claim_dir(self, claim_id: str) -> str:
        return f"{self.root}/claimed/{claim_id}"

    def _write_manifest(self, claim: dict) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "claim.json"
            local.write_text(json.dumps(claim))
            self._api.upload(
                str(local), self._claim_dir(claim["claim_id"]), overwrite=True
            )

    def _claim(self, claim_id: str, files: list[str]) -> dict:
        """Reserve `files` for this claim: the manifest first, then the moves it names."""
        directory = self._claim_dir(claim_id)
        self._mkdirs(directory)
        claim = {
            "claim_id": claim_id,
            "chunks": sorted(path.rsplit("/", 1)[-1] for path in files),
            "claimed_at": time.time(),
        }
        self._write_manifest(claim)
        self._move_into(claim)
        return claim

    def _move_into(self, claim: dict) -> None:
        """Finish a claim's moves: whatever it names that is still pending comes over."""
        directory = self._claim_dir(claim["claim_id"])
        for name in claim["chunks"]:
            source = f"{self.root}/pending/{name}"
            if self._api.exists(source):
                self._api.move(source, f"{directory}/{name}", overwrite=True)

    def _read_manifest(self, claim_id: str) -> dict | None:
        manifest = f"{self._claim_dir(claim_id)}/claim.json"
        if not self._api.exists(manifest):
            return None
        try:
            claim = json.loads(
                _manifest_content(self._api.read_content(_absolute(manifest)))
            )
        except ValueError:
            return None
        if "chunks" not in claim:
            return None
        claim["claim_id"] = claim_id
        # Chunks are names; a manifest that recorded paths names the same files.
        claim["chunks"] = [c.rsplit("/", 1)[-1] for c in claim["chunks"]]
        return claim

    def _recoverable_claims(self) -> list[dict]:
        """Claims left by executions that died, old enough that none is still working on them.

        A directory without a manifest never received a file, so it is only removed.
        """
        claims = []
        for directory in self._list(f"{self.root}/claimed"):
            claim_id = directory.rsplit("/", 1)[-1]
            claim = self._read_manifest(claim_id)
            if claim is None:
                if not [p for p in self._list(directory) if p.endswith(CHUNK_SUFFIX)]:
                    self._api.remove(directory)
                continue
            if time.time() - float(claim.get("claimed_at", 0)) < CLAIM_RECLAIM_SECONDS:
                continue
            self._move_into(claim)
            claims.append(claim)
        return claims

    def _chunks(self, claim: dict) -> list[str]:
        """The claim's chunk paths that reached its directory, in stable order."""
        directory = self._claim_dir(claim["claim_id"])
        present = {p.rsplit("/", 1)[-1] for p in self._list(directory)}
        return [f"{directory}/{name}" for name in claim["chunks"] if name in present]

    def _download(self, path: str, local_dir: str) -> str:
        return self._api.download(_absolute(path), local_path=local_dir, overwrite=True)

    def _fail(self, path: str, claim_id: str) -> None:
        directory = f"{self.root}/failed/{claim_id}"
        self._mkdirs(directory)
        self._api.move(path, f"{directory}/{path.rsplit('/', 1)[-1]}", overwrite=True)

    def _release(self, claim_id: str) -> None:
        self._api.remove(self._claim_dir(claim_id))


def _unify(tables):
    """One table from several chunks; a chunk whose types differ is cast to the first's."""
    import pyarrow as pa

    schema = tables[0].schema
    aligned = [
        table.select(schema.names).cast(schema)
        if not table.schema.equals(schema, check_metadata=False)
        else table
        for table in tables
    ]
    return pa.concat_tables(aligned)


def _commit_part(feature_group, staging, claim_id, tables, chunks, summary):
    """Append one part of a claim in one commit that also records each of its chunks.

    A part the group's schema rejects is parked under `failed/` rather than raised: the
    rejection would be the same on every retry, and raising it would hold every chunk
    behind it in `pending/` for as long as the parked one stayed in the way.
    """
    from hsfs.client.exceptions import FeatureStoreException

    table = _unify(tables)
    try:
        feature_group.insert(
            _frame(table),
            storage="offline",
            write_options={
                "mode": "append",
                "wait_for_job": True,
                "commit_properties": {
                    "transactions": [
                        {"app_id": _chunk_app_id(feature_group, chunk_id), "version": 1}
                        for chunk_id, _remote in chunks
                    ]
                },
            },
        )
    except FeatureStoreException as error:
        summary["chunks_rejected"] += len(chunks)
        for _chunk_id, remote in chunks:
            staging._fail(remote, claim_id)
        print(f"FEATURE_LOG_COMMIT part rejected: {error}", flush=True)
        return
    summary["rows_committed"] += table.num_rows
    summary["commits"] += 1


def _process_claim(
    feature_group, staging: _Staging, claim: dict, summary: dict
) -> None:
    """Commit a claim in parts of at most MAX_PART_BYTES of Arrow, each its own commit.

    A chunk the table already records is skipped, so a retry of the claim and a second copy of a chunk under a later claim both commit nothing for it.
    """
    expected = {f.name for f in _columns(feature_group) if f.name != "log_id"}
    transactions = _DeltaTransactions(feature_group)
    claim_id = claim["claim_id"]
    tables, part_chunks, part_bytes = [], [], 0
    chunks = staging._chunks(claim)
    with tempfile.TemporaryDirectory() as tmp:
        for remote in chunks:
            local = staging._download(remote, tmp)
            table, truncated = _read_chunk(local)
            summary["chunks_truncated"] += int(truncated)
            if table is None:
                # Not one complete batch: keep the chunk under `failed/` rather
                # than letting the claim's release take the only copy of it.
                summary["chunks_rejected"] += 1
                staging._fail(remote, claim_id)
                continue
            if set(table.column_names) != expected:
                summary["chunks_rejected"] += 1
                staging._fail(remote, claim_id)
                continue
            chunk_id = _chunk_id_of(table, local)
            if transactions._committed(chunk_id):
                summary["chunks_already_applied"] += 1
                os.remove(local)
                continue
            try:
                table = _with_log_ids(table, chunk_id)
                if tables:
                    _unify([tables[0], table])
            except Exception:  # noqa: BLE001 - one bad chunk must not stop the claim
                summary["chunks_rejected"] += 1
                staging._fail(remote, claim_id)
                continue
            if tables and part_bytes + table.nbytes > MAX_PART_BYTES:
                _commit_part(
                    feature_group, staging, claim_id, tables, part_chunks, summary
                )
                tables, part_chunks, part_bytes = [], [], 0
            tables.append(table)
            part_chunks.append((chunk_id, remote))
            part_bytes += table.nbytes
            os.remove(local)
    if tables:
        _commit_part(feature_group, staging, claim_id, tables, part_chunks, summary)
    summary["chunks_claimed"] += len(chunks)
    staging._release(claim_id)


def _trigger(project, name: str, running_execution: str | None = None) -> bool:
    """Start another run of the job unless one other than `running_execution` is already running."""
    job = project.get_job_api().get_job(name)
    if job is None or any(
        e.success is None and str(e.id) != str(running_execution)
        for e in job.get_executions() or []
    ):
        return False
    job.run(await_termination=False)
    return True


def _checkpoint(feature_group, summary: dict) -> None:
    """Checkpoint the logging table once per execution that committed something.

    Every append adds a commit to the Delta log, and a reader replays the log from the
    last checkpoint. Nothing writes one on its own under delta-rs, so on a table that is
    only ever appended to the replay grows with the number of commits, and this job pays
    it twice per part: once to read the application transaction that makes a retry
    idempotent, and once to read back the commit it just made.

    One checkpoint per execution rather than one per commit: the runs are scheduled, so
    this leaves the log at most a single execution's commits ahead of the checkpoint,
    and a checkpoint costs one write of the table's file list.

    A failure here is logged and not raised: the rows are already committed, and the
    next execution checkpoints again.
    """
    if not summary["commits"]:
        return
    try:
        summary["checkpoint"] = feature_group.delta_checkpoint()
    except Exception as error:  # noqa: BLE001 - the commit already landed
        summary["checkpoint_error"] = type(error).__name__
        print(
            f"FEATURE_LOG_COMMIT checkpoint failed: {type(error).__name__}: {error}",
            flush=True,
        )


def _should_compact(state: dict, now: float) -> str | None:
    """Why this execution should compact, or None.

    Two triggers, because they cover different traffic. A busy view reaches the file
    threshold within a day and should not wait for midnight; a quiet one never reaches
    it and would otherwise never compact at all.

    The daily trigger reads the table's own history rather than any state this job
    keeps, so it stays correct across executions and across writers.
    """
    if state["active_files"] >= COMPACT_FILE_THRESHOLD:
        return f"{state['active_files']} files at or above the {COMPACT_FILE_THRESHOLD} threshold"
    last = state["last_optimize_at"]
    midnight = datetime.fromtimestamp(now, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if last is None or last < midnight.timestamp():
        return "first run of the day"
    return None


def _compaction_scope(state: dict) -> str | None:
    """The earliest ingest date this compaction needs to rewrite, or None for all of it.

    Only files written since the last compaction need rewriting, and on a group
    partitioned by a date those files are in partitions at or after that date. Bounding
    the rewrite that way is what keeps a daily compaction's cost flat: without it every
    run rewrites the whole table, including everything earlier runs already compacted,
    and the cost grows with the log forever.

    `COMPACT_LOOKBACK_DAYS` of slack before that date covers a row that arrived late and
    landed in a partition earlier than the one it was logged in.

    None, meaning the whole table, in the two cases where nothing narrower is sound: a
    group with no date partition column, where only a partition column can select files
    without reading them, and a table that has never been compacted, which has no
    earlier point to start from.

    On the logging group this job creates, that first case is the one that applies: the
    backend partitions it by `model_name` and `model_version`, both strings, so
    `_date_partition_column` finds nothing and every run takes the whole-table branch.
    The narrowing is reachable today only on a group the user partitioned by a date
    themselves, through `delta_optimize(after_ingest_date=...)`. Making it bound this
    job's own compaction needs a date partition on the logging group, a `log_date`
    derived from `log_time`, which is a backend change and not made here.
    """
    last = state.get("last_optimize_at")
    if last is None or not state.get("date_partition"):
        return None
    start = datetime.fromtimestamp(last, tz=timezone.utc) - timedelta(
        days=COMPACT_LOOKBACK_DAYS
    )
    return start.date().isoformat()


def _maintain(feature_group, summary: dict, now: float | None = None) -> None:
    """Compact, checkpoint, then expire the log and the files it orphaned.

    Compaction replaces many small files with few large ones and leaves the old ones
    on disk, still referenced by older versions. The checkpoint goes next, so the
    smaller file list is recorded before anything is deleted. Only then the two
    deletions: the log entries the checkpoint now covers, and the data files the
    compaction orphaned.

    What protects a reader is the retention, not the position: a vacuum deletes files
    an in-flight query may still be reading, so COMPACT_VACUUM_RETENTION_HOURS has to
    stay comfortably longer than the longest query that runs against this group. The
    effect is that a run reclaims what earlier runs orphaned rather than its own
    rewrite, whose files are seconds old.

    Skipped entirely on an execution that committed nothing, so an idle view costs its
    schedule and nothing else. Every step is best effort: the rows are committed by the
    time this runs and the next execution tries again.
    """
    if not summary["commits"]:
        return
    now = time.time() if now is None else now
    try:
        state = feature_group._feature_group_engine._delta_maintenance_state(
            feature_group
        )
    except Exception as error:  # noqa: BLE001 - maintenance never fails a commit
        summary["maintenance_error"] = type(error).__name__
        print(
            f"FEATURE_LOG_COMMIT maintenance state failed: {type(error).__name__}: {error}",
            flush=True,
        )
        _checkpoint(feature_group, summary)
        return
    if state is None:
        _checkpoint(feature_group, summary)
        return
    summary["active_files"] = state["active_files"]
    reason = _should_compact(state, now)
    if reason is None:
        _checkpoint(feature_group, summary)
        return
    summary["compact_reason"] = reason
    after = _compaction_scope(state)
    if after is not None:
        summary["compact_after"] = after
    try:
        summary["compaction"] = feature_group.delta_optimize(
            after_ingest_date=after,
            max_concurrent_tasks=COMPACT_CONCURRENT_TASKS,
        )
    except Exception as error:  # noqa: BLE001 - maintenance never fails a commit
        summary["maintenance_error"] = type(error).__name__
        print(
            f"FEATURE_LOG_COMMIT compaction failed: {type(error).__name__}: {error}",
            flush=True,
        )
    _checkpoint(feature_group, summary)
    # Both deletions go last, and only once the checkpoint above describes the compacted
    # state: the log entries it covers, then the data files it orphaned.
    try:
        summary["log_pruned_to"] = feature_group.delta_cleanup_metadata()
    except Exception as error:  # noqa: BLE001 - maintenance never fails a commit
        summary["cleanup_error"] = type(error).__name__
        print(
            f"FEATURE_LOG_COMMIT log cleanup failed: {type(error).__name__}: {error}",
            flush=True,
        )
    try:
        summary["vacuum_deleted"] = len(
            feature_group.delta_vacuum(retention_hours=COMPACT_VACUUM_RETENTION_HOURS)
            or []
        )
    except Exception as error:  # noqa: BLE001 - maintenance never fails a commit
        summary["vacuum_error"] = type(error).__name__
        print(
            f"FEATURE_LOG_COMMIT vacuum failed: {type(error).__name__}: {error}",
            flush=True,
        )


def _run(feature_view_name: str, feature_view_version: int) -> dict:
    import hopsworks

    project = hopsworks.login()
    feature_store = project.get_feature_store()
    feature_group = feature_store.get_feature_group(
        f"{feature_view_name}_{feature_view_version}_log", 1
    )
    # The group is resolved by name and version, so a view switched to the
    # realtime transport since this job was scheduled resolves to the stream
    # group that replaced the job one. Appending there would put rows in the
    # offline table behind Kafka's back, with no online copy, and break the
    # one-transport-per-view invariant. A chunk left over from before the
    # switch belongs to the group that is gone, so this refuses rather than
    # commits, and the run leaves the chunks where they are.
    if getattr(feature_group, "stream", False):
        raise RuntimeError(
            f"Feature view {feature_view_name} v{feature_view_version} now logs "
            "through the 'realtime' transport, so its logging feature group is a "
            "stream group that Kafka writes. This job only commits chunks staged "
            "by the 'job' transport; unschedule it, or switch the view back with "
            "delete_log(transport='job')."
        )
    # A statistics Spark job per commit would queue this job's own runs behind
    # Spark on a busy deployment; the group is created with statistics off, and
    # this covers a group created before that was so.
    feature_group.statistics_config.enabled = False
    staging = _Staging(
        project.get_dataset_api(), feature_view_name, feature_view_version
    )
    summary = {
        "feature_group": feature_group.name,
        "claims": 0,
        "claims_recovered": 0,
        "chunks_already_applied": 0,
        "chunks_claimed": 0,
        "chunks_truncated": 0,
        "chunks_rejected": 0,
        "rows_committed": 0,
        "commits": 0,
        "pending_after": 0,
        "retriggered": False,
    }
    summary["uploads_adopted"] = len(staging._adopt_stale_uploads())
    for claim in staging._recoverable_claims():
        summary["claims"] += 1
        summary["claims_recovered"] += 1
        _process_claim(feature_group, staging, claim, summary)
    execution_id = os.environ.get("HOPSWORKS_JOB_EXECUTION_ID") or uuid.uuid4().hex
    for iteration in range(MAX_CLAIMS_PER_EXECUTION):
        files = staging._pending()[:MAX_CHUNKS_PER_CLAIM]
        if not files:
            break
        claim = staging._claim(f"{execution_id}-{iteration}", files)
        summary["claims"] += 1
        _process_claim(feature_group, staging, claim, summary)
    _maintain(feature_group, summary)
    summary["pending_after"] = len(staging._pending())
    if summary["pending_after"]:
        # A backlog larger than one execution drains through another run now
        # rather than at the next scheduled one.
        summary["retriggered"] = _trigger(
            project,
            f"{feature_view_name}_{feature_view_version}_log_feature_log_commit",
            os.environ.get("HOPSWORKS_JOB_EXECUTION_ID"),
        )
    print("FEATURE_LOG_COMMIT " + json.dumps(summary), flush=True)
    return summary


def _maintenance_limits(arguments) -> None:
    """Take the maintenance limits from the job's arguments, where a scheduled run can carry them.

    A PYTHON job holds no environment variables of its own: `JobController` refuses any
    name under a reserved prefix, and nothing sets the rest, so a scheduled execution
    runs with the platform's environment and not the operator's. Its arguments are the
    one thing the operator does control, through `defaultArgs` on the job, and the
    schedule passes them on every run. An argument left out keeps the environment value,
    and then the default.
    """
    global \
        COMPACT_FILE_THRESHOLD, \
        COMPACT_CONCURRENT_TASKS, \
        COMPACT_VACUUM_RETENTION_HOURS, \
        COMPACT_LOOKBACK_DAYS
    if arguments.compact_file_threshold is not None:
        COMPACT_FILE_THRESHOLD = max(1, arguments.compact_file_threshold)
    if arguments.compact_concurrent_tasks is not None:
        COMPACT_CONCURRENT_TASKS = max(1, arguments.compact_concurrent_tasks)
    if arguments.vacuum_retention_hours is not None:
        COMPACT_VACUUM_RETENTION_HOURS = max(1, arguments.vacuum_retention_hours)
    if arguments.compact_lookback_days is not None:
        COMPACT_LOOKBACK_DAYS = max(1, arguments.compact_lookback_days)


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-view", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--compact-file-threshold", type=int)
    parser.add_argument("--compact-concurrent-tasks", type=int)
    parser.add_argument("--vacuum-retention-hours", type=int)
    parser.add_argument("--compact-lookback-days", type=int)
    # A scheduled run also receives the scheduler's -start_time.
    arguments, _ = parser.parse_known_args()
    _maintenance_limits(arguments)
    _run(arguments.feature_view, arguments.version)


if __name__ == "__main__":
    _main()
