"""One run of ``hops fg embed``, in the group's job: chunk a text column, embed it, converge the group.

The group is keyed by ``(doc_id, chunk_ix)``; ``doc_id`` is the source row's
primary key. ``hops fg embed`` makes the group, fenced, with this job as its
writer: a run never creates a group, and stops with nothing written when its
group is missing. A run converges the group onto the source in this order:

1. Inputs. The source is read once from a pinned commit and the read stops
   past the input maximum. One chunk is embedded to check the model's
   dimension against the group's. Nothing is written yet.
2. Claim. The backend gives this execution its epoch e and the epochs below e
   whose runs did not settle. A newer run holding the group stops this one.
3. Fence. An empty Delta commit recording e, then one fence record per
   partition of the group's topic. From here every store refuses the writes
   of an older run.
4. Barrier. online-progress is polled every 2 s until OnlineFS saved a fence
   at e on every partition: every record sent before the fence is applied.
5. Reconcile. The offline rows are read, then the online documents of their
   keys and of the source's, with versions, tombstones included. Written at
   e: changed chunks (embedded, offline then online), chunks missing, stale or
   tombstoned online, and with unsettled epochs every document below the
   highest one. Keys the source no longer has are deleted online, read back as
   tombstones at e, and only then deleted offline: the offline row is the
   delete intent until the online delete is verified.
6. Every key is verified at its value and version, then the claim is settled:
   the run's last act. A run that stops before leaves its claim unsettled, and
   the next run rewrites what it may have left behind.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


CHUNK_CHARS = 2000
CHUNK_OVERLAP = 200
#: Seconds a run waits for the fence barrier and for each read-back:
#: 30x the worst online lag seen on the dev cluster (1.8 s, 2026-11-04, light load).
READBACK_S = 60
#: Seconds between two polls of online-progress or of a read-back.
POLL_S = 2
#: Keys per online read request.
READ_BATCH = 200
#: The supported input of one group, unless the Hopsworks variables of
#: embed_limits() grant more: chunks (also the most source rows and held
#: vectors a run reads) and bytes of source text.
MAX_CHUNKS = 50_000
MAX_BYTES = 100_000_000
#: Source rows per Arrow batch of the source read.
SOURCE_BATCH = 1_000
#: Hopsworks variables an admin sets; each is a positive integer, absent for the default.
VAR_MAX_CHUNKS = "platform_jobs_embed_max_chunks"
VAR_MAX_BYTES = "platform_jobs_embed_max_source_bytes"
VAR_MIN_INTERVAL = "platform_jobs_embed_min_interval_minutes"
#: The id of the job execution running this process, set by Hopsworks in the job's container.
EXECUTION_ENV = "HOPSWORKS_JOB_EXECUTION_ID"
#: The OnlineFS consumer group writing the vector database, the only one that keeps writer fences;
#: a namespaced deployment adds "@<namespace>".
VECTORDB_GROUP = "VECTORDB"

STATE_DIR = "Resources/hopsable-embed"
OWNER = "hopsable.embed.owner="
SEARCH = "hopsable.embed.search="
KEYS = ["doc_id", "chunk_ix"]

_DESCRIPTION = re.compile(
    r"\((?P<model>[^,()]+), (?P<dimension>\d+)d, cosine\) "
    + re.escape(OWNER)
    + r"(?P<owner>\S+)$"
)


class EmbedError(Exception):
    """The run stopped; the message says what was and was not changed."""


def variable_int(name: str, default: int) -> int:
    """A positive-integer Hopsworks variable; `default` when it is not set."""
    from hopsworks.cli import versions  # noqa: PLC0415
    from hopsworks_common.core.variable_api import VariableApi  # noqa: PLC0415

    try:
        raw = VariableApi()._get_variable(name)
    except Exception as exc:  # noqa: BLE001
        if versions.not_found(exc):
            return default
        raise EmbedError(
            f"Could not read the Hopsworks variable {name}: {exc}"
        ) from exc
    return positive_int(name, raw)


def positive_int(name: str, raw: Any) -> int:
    """The value of Hopsworks variable `name`, which must be a positive integer."""
    try:
        value = int(str(raw).strip())
    except ValueError:
        value = 0
    if value < 1:
        raise EmbedError(
            f"The Hopsworks variable {name} is '{raw}', not a positive integer."
        )
    return value


def embed_limits() -> tuple[int, int]:
    """(maximum chunks, maximum source bytes) of one group on this cluster."""
    return variable_int(VAR_MAX_CHUNKS, MAX_CHUNKS), variable_int(
        VAR_MAX_BYTES, MAX_BYTES
    )


def chunks(
    text: Any, size: int = CHUNK_CHARS, overlap: int = CHUNK_OVERLAP
) -> list[str]:
    """Windows of `size` characters, each starting `size - overlap` after the last.

    A pure function of the text, so unchanged text keeps its chunk numbers.
    Empty or missing text has no chunks.
    """
    if not 0 <= overlap < size:
        raise ValueError("The overlap must be at least 0 and less than the chunk size.")
    if text is None or (isinstance(text, float) and text != text):
        return []
    text = str(text)
    if not text.strip():
        return []
    step = size - overlap
    out = []
    for start in range(0, len(text), step):
        out.append(text[start : start + size])
        if start + size >= len(text):
            break
    return out


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def doc_id(values: list[Any]) -> str:
    """The primary key as one string; a composite key joins its parts as a JSON list."""
    if len(values) == 1:
        return str(values[0])
    return json.dumps([str(v) for v in values])


def read_source(
    source: Any,
    primary_key: list[str],
    text: str,
    size: int,
    overlap: int,
    max_chunks: int,
    max_bytes: int,
) -> dict:
    """(doc_id, chunk_ix) -> chunk text, for every row of the source, read once as a stream of Arrow batches."""
    columns = [*primary_key, text]
    with source.select(columns).read_batches(batch_size=SOURCE_BATCH) as batches:
        return chunk_rows(
            batches,
            source.name,
            primary_key,
            text,
            size,
            overlap,
            max_chunks,
            max_bytes,
        )


def chunk_rows(
    batches: Any,
    name: str,
    primary_key: list[str],
    text: str,
    size: int,
    overlap: int,
    max_chunks: int,
    max_bytes: int,
) -> dict:
    """(doc_id, chunk_ix) -> chunk text of the rows of `batches`, Arrow record batches of feature group `name`.

    Stops at the first row that takes the input past `max_chunks` (rows count
    too: each is a document) or `max_bytes` of UTF-8 text, so what is held is
    never more than the maximum and no later batch is pulled.
    """
    out: dict = {}
    rows = 0
    total = 0
    for batch in batches:
        for row in batch.to_pylist():
            rows += 1
            value = row[text]
            if value is not None:
                total += len(str(value).encode("utf-8"))
            if rows > max_chunks or total > max_bytes:
                raise _over(name, max_chunks, max_bytes)
            doc = doc_id([row[k] for k in primary_key])
            for ix, chunk in enumerate(chunks(value, size, overlap)):
                out[(doc, ix)] = chunk
            if len(out) > max_chunks:
                raise _over(name, max_chunks, max_bytes)
    return out


def _over(name: str, max_chunks: int, max_bytes: int) -> EmbedError:
    return EmbedError(
        f"Feature group '{name}' is over the supported input of {max_chunks} chunks "
        f"(rows count too) or {max_bytes} bytes of text. Nothing was written or deleted."
    )


def _pin(source: Any) -> str | None:
    """The source's latest commit id, or None when it keeps no commits."""
    if str(getattr(source, "time_travel_format", None)).upper() not in (
        "HUDI",
        "DELTA",
        "ICEBERG",
    ):
        return None
    try:
        commits = source.commit_details(limit=1)
    except Exception as exc:  # noqa: BLE001 - an unreadable timeline is not an unchanged one
        raise EmbedError(
            f"Could not read the commits of feature group '{source.name}': {exc}. Nothing was written."
        ) from exc
    return str(max(commits)) if commits else ""


def description(
    source: str, version: int, column: str, model: str, dimension: int, group: str
) -> str:
    return f"Embeddings of {source} v{version} {column} ({model}, {dimension}d, cosine) {OWNER}{group}"


def described(fg: Any) -> dict | None:
    """Model, dimension and owner from a group `hops fg embed` made; None for any other group."""
    found = _DESCRIPTION.search((getattr(fg, "description", "") or "").strip())
    if not found:
        return None
    return {
        "model": found.group("model"),
        "dimension": int(found.group("dimension")),
        "owner": found.group("owner"),
    }


def fenced_by(fg: Any, job_id: Any) -> bool:
    """Whether `fg` is fenced with the job `job_id` as its writer: the only groups a run of that job can claim."""
    index = getattr(fg, "embedding_index", None)
    return (
        getattr(index, "fenced", False) is True
        and job_id is not None
        and getattr(fg, "writer_job_id", None) == job_id
    )


# --- the decisions of a run, pure --------------------------------------------


@dataclass(frozen=True)
class Doc:
    """One online document as the reconcile reads it: its content hash, version (writer epoch) and tombstone mark."""

    sha: str | None
    version: int | None
    deleted: bool


@dataclass(frozen=True)
class Rewrite:
    """What a run writes at its epoch.

    `changed` chunks are embedded and written offline then online; `online`
    chunks keep their offline vector and are written online only; `delete`
    keys are tombstoned online, then deleted offline.
    """

    changed: list = field(default_factory=list)
    online: list = field(default_factory=list)
    delete: list = field(default_factory=list)


def rewrite_set(want: dict, held: dict, online: dict, unsettled: list[int]) -> Rewrite:
    """The writes that converge the group, from the source chunks, the offline hashes and the online documents.

    `want` maps a key to its chunk text, `held` a key to the content hash the
    offline table holds, `online` a key to its `Doc`. A chunk whose offline
    hash differs is changed. An unchanged chunk is written online when its
    document is missing, a tombstone, of another content, or, with unsettled
    epochs, below the highest of them: a held write of an unsettled run would
    otherwise still be accepted over it. Offline keys the source lacks are
    deleted.
    """
    floor = max(unsettled, default=None)
    changed, stale = [], []
    for key, chunk in want.items():
        digest = sha(chunk)
        if held.get(key) != digest:
            changed.append(key)
            continue
        doc = online.get(key)
        if (
            doc is None
            or doc.deleted
            or doc.sha != digest
            or (floor is not None and (doc.version or 0) < floor)
        ):
            stale.append(key)
    return Rewrite(sorted(changed), sorted(stale), sorted(set(held) - set(want)))


def converged(
    want: dict, rewrite: Rewrite, epoch: int, unsettled: list[int]
) -> Callable[[Any, Doc | None], bool]:
    """Whether a key's online document is what the run settles on.

    A written chunk is live with its content at `epoch`; a deleted key is a
    tombstone at `epoch`; any other chunk is live with its content, at or above
    the highest unsettled epoch.
    """
    floor = max(unsettled, default=0)
    written = set(rewrite.changed) | set(rewrite.online)
    deleted = set(rewrite.delete)

    def check(key: Any, doc: Doc | None) -> bool:
        if doc is None:
            return False
        if key in deleted:
            return doc.deleted and doc.version == epoch
        if doc.deleted or doc.sha != sha(want[key]):
            return False
        if key in written:
            return doc.version == epoch
        return (doc.version or 0) >= floor

    return check


def fence_lag(progress: list[dict], partitions: list[int], epoch: int) -> list[int]:
    """The partitions on which OnlineFS has not yet saved a fence of the group at `epoch`.

    Only the VECTORDB consumer groups keep fences, and each of them must have
    passed the fence on every partition. Every partition lags while none of
    them reports.
    """
    fences: dict[str, dict[int, Any]] = {}
    for row in progress:
        group = str(row.get("consumerGroup") or "")
        if group == VECTORDB_GROUP or group.startswith(VECTORDB_GROUP + "@"):
            fences.setdefault(group, {})[int(row["partition"])] = row.get("fenceEpoch")
    if not fences:
        return sorted(partitions)
    return sorted(
        {
            p
            for saved in fences.values()
            for p in partitions
            if saved.get(p) is None or int(saved[p]) < epoch
        }
    )


@dataclass
class Plan:
    """What a run did, for its report."""

    group: str = ""
    version: int | None = None
    epoch: int | None = None
    unsettled: list = field(default_factory=list)
    desired: int = 0
    embedded: int = 0
    rewritten: int = 0
    removed: int = 0


# --- the stores and the backend ------------------------------------------------


def _refusal(exc: BaseException) -> Any:
    """The RestAPIError behind `exc` when the backend answered 409; None otherwise."""
    from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

    cause: BaseException | None = exc
    while cause is not None and not isinstance(cause, RestAPIError):
        cause = cause.__cause__
    if cause is None or cause.response.status_code != RestAPIError.STATUS_CODE_CONFLICT:
        return None
    return cause


def claim_refused(name: str, version: int, reason: str) -> EmbedError:
    """The stop of a run whose claim the backend refused (409) for `reason`."""
    if "newer run" in reason:
        return EmbedError(
            f"a newer run of this embed job holds the group ({reason}); "
            "this run stopped without writing"
        )
    return EmbedError(
        f"The claim of {name} v{version} was refused: {reason}. "
        "This run stopped without writing."
    )


class Writer:
    """This execution's claim of one fenced group, through the backend."""

    def __init__(self, fg: Any, execution_id: int):
        self.fg = fg
        self.execution_id = execution_id

    def _send(self, method: str, *path: str) -> Any:
        from hopsworks_common import client  # noqa: PLC0415

        _client = client._get_instance()
        return _client._send_request(
            method,
            [
                "project",
                _client._project_id,
                "featurestores",
                self.fg.feature_store_id,
                "featuregroups",
                self.fg.id,
                *path,
            ],
            query_params={"executionId": self.execution_id}
            if method == "POST"
            else None,
        )

    def claim(self) -> tuple[int, list[int]]:
        """(this execution's epoch, the unsettled epochs below it); a refused claim stops the run before it writes."""
        try:
            answer = self._send("POST", "writer")
        except Exception as exc:  # noqa: BLE001
            refused = _refusal(exc)
            if refused is None:
                raise EmbedError(
                    f"Could not claim {self.fg.name} v{self.fg.version} for writing: {exc}. "
                    "This run stopped without writing."
                ) from exc
            raise claim_refused(
                self.fg.name, self.fg.version, refused.user_message or str(exc)
            ) from exc
        return int(answer["epoch"]), sorted(
            int(e) for e in answer.get("unsettledEpochs") or []
        )

    def settle(self) -> None:
        try:
            self._send("POST", "writer", "settle")
        except Exception as exc:  # noqa: BLE001
            raise EmbedError(
                f"The writes of {self.fg.name} v{self.fg.version} are verified but the claim did not settle: "
                f"{exc}. The next run checks them again."
            ) from exc

    def progress(self) -> list[dict]:
        return self._send("GET", "online-progress") or []


class Stores:
    """The offline and online legs of one embedding group, written at `epoch` when it is set."""

    def __init__(self, fg: Any, epoch: int | None = None):
        self.fg = fg
        self.epoch = epoch

    def _options(self) -> dict:
        from hsfs.core import kafka_engine  # noqa: PLC0415

        return {kafka_engine.WRITER_EPOCH_OPTION: self.epoch}

    def fence(self) -> list[int]:
        """The Delta fence commit, then one fence record per partition; the partitions fenced."""
        from hsfs.core import delta_engine, kafka_engine  # noqa: PLC0415

        delta_engine.DeltaEngine(
            self.fg.feature_store_id, self.fg.feature_store_name, self.fg, None, None
        )._fence(self.epoch)
        return kafka_engine._produce_fence(self.fg, self.epoch, {})

    def offline(self, limit: int | None = None) -> dict:
        """(doc_id, chunk_ix) -> (content_sha, embedding, embedded_at) of every offline row.

        Streamed; with `limit`, the read stops once the group holds more rows than that.
        """
        import numpy as np  # noqa: PLC0415

        if not self.fg.commit_details(limit=1):
            return {}
        out = {}
        columns = [*KEYS, "content_sha", "embedding", "embedded_at"]
        with self.fg.select(columns).read_batches() as batches:
            for batch in batches:
                for row in batch.to_pylist():
                    out[(str(row["doc_id"]), int(row["chunk_ix"]))] = (
                        row["content_sha"],
                        np.asarray(row["embedding"], dtype=np.float32),
                        row["embedded_at"],
                    )
                if limit is not None and len(out) > limit:
                    raise EmbedError(
                        f"The group '{self.fg.name}' holds more than {limit} vectors, "
                        "over the supported input. No row was written or deleted."
                    )
        return out

    def online(self, keys: list) -> dict:
        """Key -> `Doc` of every key with a document online, tombstones included."""
        from hsfs.core.vector_db_client import (  # noqa: PLC0415
            DELETED_FIELD,
            VERSION_FIELD,
            VectorDbClient,
        )

        client = VectorDbClient(self.fg.select_all())
        found = {}
        for start in range(0, len(keys), READ_BATCH):
            batch = keys[start : start + READ_BATCH]
            results = client._read_many(
                self.fg.id,
                self.fg.features,
                [{"doc_id": d, "chunk_ix": i} for d, i in batch],
                versions=True,
            )
            for key, rows in zip(batch, results, strict=True):
                # A match query can match more than the exact key; compare the values.
                for r in rows:
                    if (str(r.get("doc_id")), int(r.get("chunk_ix"))) == key:
                        found[key] = Doc(
                            r.get("content_sha"),
                            r.get(VERSION_FIELD),
                            bool(r.get(DELETED_FIELD)),
                        )
        return found

    def upsert(self, rows: list[dict], storage: str | None = None) -> None:
        self.fg.insert(
            upsert_frame(rows), storage=storage, write_options=self._options()
        )

    def remove(self, keys: list, storage: str) -> None:
        import pandas as pd  # noqa: PLC0415

        self.fg.remove_rows(
            pd.DataFrame(keys, columns=KEYS),
            storage=storage,
            write_options=self._options(),
        )


def upsert_frame(rows: list[dict]) -> Any:
    """The rows as a frame whose embeddings infer as array<float>, the group's type.

    A list of Python floats infers as array<double>, which the insert refuses.
    """
    import numpy as np  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415

    frame = pd.DataFrame(rows)
    frame["embedding"] = [np.asarray(v, dtype=np.float32) for v in frame["embedding"]]
    return frame


def wait_fenced(
    writer: Writer,
    partitions: list[int],
    epoch: int,
    timeout_s: int,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Poll online-progress until every partition saved the fence at `epoch`, or stop past `timeout_s`."""
    deadline = clock() + timeout_s
    while True:
        lag = fence_lag(writer.progress(), partitions, epoch)
        if not lag:
            return
        if clock() >= deadline:
            raise EmbedError(
                f"fence not yet consumed on partition {', '.join(map(str, lag))}; run again"
            )
        sleep(POLL_S)


def wait_online(
    stores: Stores,
    keys: list,
    check: Callable[[Any, Doc | None], bool],
    timeout_s: int,
    what: str,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Read `keys` back online until each passes `check`, or stop past `timeout_s` naming what is left."""
    deadline = clock() + timeout_s
    pending = list(keys)
    while pending:
        docs = stores.online(pending)
        pending = [k for k in pending if not check(k, docs.get(k))]
        if not pending:
            return
        if clock() >= deadline:
            raise EmbedError(
                f"After {timeout_s} s, {len(pending)} keys are not yet {what} (first: {pending[0]}). "
                "The claim stays unsettled; the next run finishes the work."
            )
        sleep(POLL_S)


def execution_id() -> int:
    """This job execution's id; a run outside a job execution has none and cannot claim a group."""
    raw = os.environ.get(EXECUTION_ENV, "")
    if not raw.isdigit():
        raise EmbedError(
            f"{EXECUTION_ENV} is not set: the build runs only in the group's job. Nothing was written."
        )
    return int(raw)


def _group(fs: Any, group: str, version: int) -> Any:
    from hopsworks.cli import versions  # noqa: PLC0415

    try:
        return fs.get_feature_group(group, version=version)
    except Exception as exc:  # noqa: BLE001
        if versions.not_found(exc):
            raise EmbedError(
                f"Feature group '{group}' v{version} does not exist: run hops fg embed again to make it. "
                "Nothing was written."
            ) from exc
        raise EmbedError(
            f"Could not read feature group '{group}' v{version}: {exc}. Nothing was written."
        ) from exc


def _dimension(fg: Any) -> int:
    return int(fg.embedding_index.get_embedding("embedding").dimension)


def run(
    *,
    fs: Any,
    source: Any,
    text: str,
    group: str,
    version: int,
    embed: Callable[[list[str]], list[list[float]]],
    execution: int,
    size: int = CHUNK_CHARS,
    overlap: int = CHUNK_OVERLAP,
    readback_s: int = READBACK_S,
    max_chunks: int = MAX_CHUNKS,
    max_bytes: int = MAX_BYTES,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = lambda _: None,
) -> Plan:
    plan = Plan(group=group, version=version)
    fg = _group(fs, group, version)
    info = described(fg)
    if info is None or info["owner"] != group:
        raise EmbedError(
            f"Feature group '{group}' v{version} was not made by hops fg embed. Nothing was written."
        )
    dimension = _dimension(fg)

    # 1. Inputs, read before anything is written.
    primary_key = list(getattr(source, "primary_key", []) or [])
    if not primary_key:
        raise EmbedError(
            f"Feature group '{source.name}' has no primary key, so its rows have no stable id. Nothing was written."
        )
    pin = _pin(source)
    try:
        want = read_source(
            source, primary_key, text, size, overlap, max_chunks, max_bytes
        )
    except EmbedError:
        raise
    except Exception as exc:  # noqa: BLE001 - any failed read is not an empty source
        raise EmbedError(
            f"Could not read feature group '{source.name}': {exc}. Nothing was written or deleted."
        ) from exc
    if _pin(source) != pin:
        raise EmbedError(
            f"Feature group '{source.name}' was committed to while it was read. "
            "Nothing was written or deleted; run again."
        )
    plan.desired = len(want)
    first = next(iter(want.values()), None)
    probe = embed([first]) if first is not None else []
    if probe and len(probe[0]) != dimension:
        raise EmbedError(
            f"The model answers {len(probe[0])}-dimension vectors and {group} v{version} holds {dimension}. "
            "Run hops fg embed again to make a version for this model. Nothing was written."
        )

    # 2. Claim, 3. fence, 4. barrier.
    writer = Writer(fg, execution)
    epoch, unsettled = writer.claim()
    plan.epoch, plan.unsettled = epoch, unsettled
    log(
        f"Claimed {group} v{version} at epoch {epoch}"
        + (f"; unsettled epochs below it: {unsettled}" if unsettled else "")
    )
    stores = Stores(fg, epoch)
    partitions = stores.fence()
    wait_fenced(writer, partitions, epoch, readback_s, clock, sleep)

    # 5. Reconcile against what both stores hold after the barrier.
    held = stores.offline(max_chunks)
    keys = sorted(set(held) | set(want))
    rewrite = rewrite_set(
        want, {k: v[0] for k, v in held.items()}, stores.online(keys), unsettled
    )
    vectors = dict(
        zip(
            rewrite.changed,
            _embed_all(embed, [want[k] for k in rewrite.changed], first, probe),
            strict=True,
        )
    )
    stamp = datetime.now(timezone.utc)
    if rewrite.changed:
        stores.upsert([_row(k, want[k], vectors[k], stamp) for k in rewrite.changed])
    if rewrite.online:
        stores.upsert(
            [_row(k, want[k], held[k][1], held[k][2]) for k in rewrite.online],
            storage="online",
        )
    plan.embedded, plan.rewritten = len(rewrite.changed), len(rewrite.online)
    check = converged(want, rewrite, epoch, unsettled)
    if rewrite.delete:
        stores.remove(rewrite.delete, "online")
        wait_online(
            stores,
            rewrite.delete,
            check,
            readback_s,
            f"tombstones at epoch {epoch} online, so their offline rows are kept",
            clock,
            sleep,
        )
        stores.remove(rewrite.delete, "offline")
    plan.removed = len(rewrite.delete)

    # 6. Verified, then settled.
    wait_online(
        stores,
        sorted(want),
        check,
        readback_s,
        f"at their content and epoch online (epoch {epoch})",
        clock,
        sleep,
    )
    writer.settle()
    return plan


def _row(key: Any, chunk: str, vector: Any, at: Any) -> dict:
    return {
        "doc_id": key[0],
        "chunk_ix": key[1],
        "text": chunk,
        "embedding": vector,
        "content_sha": sha(chunk),
        "embedded_at": at,
    }


def _embed_all(
    embed, texts: list[str], first: str | None, probe: list[list[float]]
) -> list[list[float]]:
    if texts and texts[0] == first:
        return probe + (embed(texts[1:]) if len(texts) > 1 else [])
    return embed(texts) if texts else []


def versions_of(fs: Any, group: str) -> list[Any]:
    """Every version of `group`, oldest first; none when it does not exist."""
    from hopsworks.cli import versions  # noqa: PLC0415

    try:
        found = fs.get_feature_groups(group) or []
    except Exception as exc:  # noqa: BLE001
        if versions.not_found(exc):
            return []
        raise EmbedError(f"Could not read feature group '{group}': {exc}") from exc
    return sorted(found, key=lambda fg: fg.version)


def delete_run(
    *,
    fs: Any,
    job: Any,
    group: str,
    execution: int,
    log: Callable[[str], None] = lambda _: None,
) -> list[int]:
    """The deletion execution D: claim and fence each group, unschedule the job, delete the groups; the versions deleted.

    Fenced first, so no older run writes any of them again; the backend then
    deletes each group with its index. The job itself is deleted by the caller
    once D has succeeded.
    """
    owned = [
        fg
        for fg in versions_of(fs, group)
        if (described(fg) or {}).get("owner") == group
    ]
    for fg in owned:
        if fenced_by(fg, job.id):
            epoch, _ = Writer(fg, execution).claim()
            Stores(fg, epoch).fence()
            log(f"Fenced {group} v{fg.version} at epoch {epoch}")
    if getattr(job, "job_schedule", None):
        job.unschedule()
    for fg in owned:
        fg.delete()
        log(f"Deleted {group} v{fg.version}")
    return [fg.version for fg in owned]
