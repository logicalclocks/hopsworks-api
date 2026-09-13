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
Every commit carries a Delta application transaction named after its claim and part, and a retry commits nothing for a part whose transaction the table already records.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path


STAGING_ROOT = "Resources/feature_logging"
CHUNK_SUFFIX = ".arrow"
LOG_ID_NAMESPACE = uuid.UUID("8c4a9d5e-3f0b-4c6e-9a1d-3b2c1d0e9f8a")
MAX_CHUNKS_PER_CLAIM = 256
MAX_PART_BYTES = 512 * 1024 * 1024
MAX_CLAIMS_PER_EXECUTION = 10
CLAIM_RECLAIM_SECONDS = 7200
# Older than any upload still in progress: a chunk this old in `uploading/` is whole
# and its pod is gone.
UPLOAD_STALE_SECONDS = 3600


def _read_chunk(path):
    """Read one chunk batch by batch; a truncated tail loses only its last batch.

    Returns:
        The table of complete batches, or `None` for an empty chunk, and whether the tail was truncated.
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


def _frame(table, feature_group):
    """The table as a frame that keeps its types; an all-null pandas column would reach Delta as `null`."""
    try:
        import polars as pl

        return pl.from_arrow(table)
    except ImportError:
        import pandas as pd
        import pyarrow as pa

        # Nullable pandas dtypes keep integers with nulls integral; a plain
        # to_pandas() would make them floats and hand Delta the wrong type.
        nullable = {
            pa.int8(): pd.Int8Dtype(),
            pa.int16(): pd.Int16Dtype(),
            pa.int32(): pd.Int32Dtype(),
            pa.int64(): pd.Int64Dtype(),
            pa.bool_(): pd.BooleanDtype(),
            pa.string(): pd.StringDtype(),
            pa.large_string(): pd.StringDtype(),
        }
        frame = table.to_pandas(types_mapper=nullable.get)
        types = {feature.name: feature.type for feature in _columns(feature_group)}
        for name in frame.columns:
            dtype = _pandas_dtype(types.get(name))
            if frame[name].isna().all() and dtype is not None:
                frame[name] = frame[name].astype(dtype)
        return frame


_PANDAS_DTYPES = {
    "string": "string",
    "bigint": "Int64",
    "int": "Int32",
    "smallint": "Int16",
    "tinyint": "Int8",
    "boolean": "boolean",
    "double": "float64",
    "float": "float32",
    "timestamp": "datetime64[ns]",
    "date": "datetime64[ns]",
}


def _pandas_dtype(offline_type) -> str | None:
    """The nullable pandas dtype for a logging group column type, `None` for types left as they are."""
    return _PANDAS_DTYPES.get(str(offline_type).lower()) if offline_type else None


def _columns(feature_group):
    return getattr(feature_group, "columns", None) or feature_group.features


def _part_app_id(feature_group, claim_id: str, part: int) -> str:
    return f"hopsworks_feature_log_{feature_group.id}/{claim_id}/{part}"


class _DeltaTransactions:
    """The Delta application transactions that make a retried part commit nothing."""

    def __init__(self, feature_group):
        self._feature_group = feature_group
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

    def _committed(self, app_id: str) -> bool:
        """Whether a commit for this part has landed; a missing table means it has not.

        Any other failure to read the log is raised: guessing here is what turns a retry into a duplicate or a loss.
        """
        if not self.available:
            return False
        try:
            table = self._DeltaTable(
                self._location, storage_options=self._storage_options
            )
        except self._not_found:
            return False
        return table.transaction_version(app_id) is not None


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


def _commit_part(feature_group, transactions, claim_id, part, tables, summary):
    app_id = _part_app_id(feature_group, claim_id, part)
    if transactions._committed(app_id):
        summary["parts_already_applied"] += 1
        return
    table = _unify(tables)
    feature_group.insert(
        _frame(table, feature_group),
        storage="offline",
        write_options={
            "mode": "append",
            "wait_for_job": True,
            "commit_properties": {"app_id": app_id, "version": 1},
        },
    )
    summary["rows_committed"] += table.num_rows
    summary["commits"] += 1


def _process_claim(
    feature_group, staging: _Staging, claim: dict, summary: dict
) -> None:
    """Commit a claim in parts of at most MAX_PART_BYTES of Arrow, each its own transaction.

    Parts are cut in chunk order by size, so a retry forms the same parts and skips the ones that landed.
    """
    expected = {f.name for f in _columns(feature_group) if f.name != "log_id"}
    transactions = _DeltaTransactions(feature_group)
    claim_id = claim["claim_id"]
    part, tables, part_bytes = 0, [], 0
    chunks = staging._chunks(claim)
    with tempfile.TemporaryDirectory() as tmp:
        for remote in chunks:
            local = staging._download(remote, tmp)
            table, truncated = _read_chunk(local)
            summary["chunks_truncated"] += int(truncated)
            if table is None:
                continue
            if set(table.column_names) != expected:
                summary["chunks_rejected"] += 1
                staging._fail(remote, claim_id)
                continue
            try:
                table = _with_log_ids(table, _chunk_id_of(table, local))
                if tables:
                    _unify([tables[0], table])
            except Exception:  # noqa: BLE001 - one bad chunk must not stop the claim
                summary["chunks_rejected"] += 1
                staging._fail(remote, claim_id)
                continue
            if tables and part_bytes + table.nbytes > MAX_PART_BYTES:
                _commit_part(
                    feature_group, transactions, claim_id, part, tables, summary
                )
                part, tables, part_bytes = part + 1, [], 0
            tables.append(table)
            part_bytes += table.nbytes
            os.remove(local)
    if tables:
        _commit_part(feature_group, transactions, claim_id, part, tables, summary)
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


def _run(feature_view_name: str, feature_view_version: int) -> dict:
    import hopsworks

    project = hopsworks.login()
    feature_store = project.get_feature_store()
    feature_group = feature_store.get_feature_group(
        f"{feature_view_name}_{feature_view_version}_log", 1
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
        "parts_already_applied": 0,
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


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-view", required=True)
    parser.add_argument("--version", required=True, type=int)
    # A scheduled run also receives the scheduler's -start_time.
    arguments, _ = parser.parse_known_args()
    _run(arguments.feature_view, arguments.version)


if __name__ == "__main__":
    _main()
