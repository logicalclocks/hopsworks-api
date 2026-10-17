"""Semantic memory search over a Hopsworks embedding feature group.

This is not new infrastructure: it is the same vector-DB mechanism Hopsworks
already exposes through ``EmbeddingIndex`` + ``find_neighbors``, pointed at a
per-deployment feature group of the agent's own messages.

**Why this is a separate tier from the MySQL buffer.** A feature-group insert
reaches the online store and vector index asynchronously, so a message written
this turn is not immediately searchable. That is not a defect to work around —
it is the reason the tiers split cleanly:

- the **MySQL buffer + summary own recency** ("what did I just say"), and are
  synchronous;
- the **vector store owns old relevant context** ("what did we discuss weeks
  ago").

So never reach for :meth:`search` to recover the current turn — the synchronous
tier already has it, and the vector store may not for a while.

``VectorStore`` is the seam. :class:`InMemoryVectorStore` implements it exactly
and is what the tests run against; :class:`HopsworksVectorStore` is the
production backend.
"""

from __future__ import annotations

import logging
import math
import threading
from abc import ABC, abstractmethod
from datetime import timedelta


log = logging.getLogger(__name__)

#: Columns carried alongside the vector: enough to filter on and to render a
#: result without a second lookup in MySQL.
FEATURES = (
    "item_id",
    "conversation_id",
    "subject",
    "memory_type",
    "role",
    "content",
    "created_at",
    "embedding",
)


class VectorPurgeError(RuntimeError):
    """A purge could not be completed, so the content may still be searchable.

    Distinct from "nothing matched": erasure is the one operation where an
    unreported failure is worse than an error, because the caller has told a
    user their data is gone.
    """


class VectorStore(ABC):
    """Where embedded memories live and are searched."""

    @abstractmethod
    def ingest(self, rows: list[dict]) -> None:
        """Add embedded rows. Not guaranteed to be searchable immediately."""

    @abstractmethod
    def search(
        self,
        vector: list[float],
        *,
        k: int = 5,
        subject: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict]:
        """Nearest rows, each with a ``score``.

        ``subject`` is load-bearing rather than an optimization: filtering on it
        is what gives per-user isolation, so a user only ever searches their own
        past. Callers must pass it whenever one exists.
        """

    @abstractmethod
    def purge(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        """Delete rows for a conversation or subject. Returns rows removed.

        Deletion is a two-store problem: this holds a *copy* of the content, so
        deleting from MySQL alone leaves it searchable here.

        Raises :class:`VectorPurgeError` if the rows could not be removed. A
        return of ``0`` therefore means "nothing matched", never "it failed".
        """

    def healthcheck(self) -> bool:
        return True


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if not na or not nb:
        return 0.0
    return dot / (na * nb)


class InMemoryVectorStore(VectorStore):
    """Process-local reference implementation.

    Exact rather than approximate search, so it is a correctness oracle for the
    filtering and ranking semantics the Hopsworks backend has to match. Lost on
    restart and not shared between replicas — development and tests only.
    """

    def __init__(self) -> None:
        self._rows: list[dict] = []
        self._lock = threading.Lock()

    def ingest(self, rows: list[dict]) -> None:
        with self._lock:
            existing = {r["item_id"] for r in self._rows}
            self._rows.extend(r for r in rows if r["item_id"] not in existing)

    def search(
        self,
        vector: list[float],
        *,
        k: int = 5,
        subject: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict]:
        with self._lock:
            candidates = [
                r
                for r in self._rows
                if (subject is None or r.get("subject") == subject)
                and (
                    conversation_id is None
                    or r.get("conversation_id") == conversation_id
                )
            ]
        scored = [dict(r, score=_cosine(vector, r["embedding"])) for r in candidates]
        scored.sort(key=lambda r: r["score"], reverse=True)
        return [{key: r[key] for key in r if key != "embedding"} for r in scored[:k]]

    def purge(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        with self._lock:
            before = len(self._rows)
            self._rows = [
                r
                for r in self._rows
                if not (
                    (
                        conversation_id is not None
                        and r.get("conversation_id") == conversation_id
                    )
                    or (subject is not None and r.get("subject") == subject)
                )
            ]
            return before - len(self._rows)


class HopsworksVectorStore(VectorStore):
    """Per-deployment embedding feature group, created lazily once.

    ``dimension`` is fixed when the feature group is created, and vectors from a
    different embedding model are not comparable even at the same dimension — a
    swapped embedder returns plausible-looking nonsense rather than an error. So
    the model's identity is recorded in the feature group description and
    checked on connect; a mismatch disables search rather than serving garbage,
    and the fix is a new feature-group version plus a re-ingest.
    """

    def __init__(
        self,
        deployment_id: str,
        dimension: int,
        *,
        embedder_id: str = "unknown",
        version: int = 1,
        ttl_days: int | None = 90,
        feature_store=None,
    ) -> None:
        self._name = f"agent_memory_{deployment_id}"
        self._dimension = dimension
        self._embedder_id = embedder_id
        self._version = version
        self._ttl_days = ttl_days
        self._fs = feature_store
        self._fg = None
        self._features: list[str] = []
        self._failed = False
        self._lock = threading.Lock()

    def _description(self) -> str:
        return (
            f"Hopsworks agent memory ({self._embedder_id}, {self._dimension}d, cosine)"
        )

    def _ensure_fg(self):
        if self._fg is not None or self._failed:
            return self._fg
        with self._lock:
            if self._fg is not None or self._failed:
                return self._fg
            try:
                from hsfs.embedding import EmbeddingIndex, SimilarityFunctionType

                fs = self._fs
                if fs is None:
                    import hopsworks

                    fs = hopsworks.login().get_feature_store()

                index = EmbeddingIndex()
                index.add_embedding(
                    name="embedding",
                    dimension=self._dimension,
                    similarity_function_type=SimilarityFunctionType.COSINE,
                )
                kwargs = {
                    "name": self._name,
                    "version": self._version,
                    "description": self._description(),
                    "primary_key": ["item_id"],
                    "event_time": "created_at",
                    "online_enabled": True,
                    # memory rows are many and cold; keep them off RonDB's
                    # in-memory tier
                    "online_disk": True,
                    "embedding_index": index,
                }
                if self._ttl_days is not None:
                    # native row expiry, measured from event_time — the vector
                    # tier's answer to retention, since there is no row-level
                    # delete available from a Python pod (see purge)
                    kwargs["ttl"] = timedelta(days=self._ttl_days)
                try:
                    fg = fs.get_or_create_feature_group(**kwargs)
                except TypeError:
                    # older hsfs without ttl / online_disk
                    for opt in ("ttl", "online_disk"):
                        kwargs.pop(opt, None)
                    fg = fs.get_or_create_feature_group(**kwargs)
                existing = (getattr(fg, "description", "") or "").strip()
                if existing and existing != self._description():
                    # a different embedding model wrote this feature group;
                    # its vectors are not comparable to ours
                    log.error(
                        "Feature group %s v%d was built by a different embedder "
                        "(%r, ours is %r). Disabling semantic search — bump the "
                        "feature group version and re-ingest to change embedder.",
                        self._name,
                        self._version,
                        existing,
                        self._description(),
                    )
                    self._failed = True
                    return None
                self._fg = fg
                # NB: empty until the feature group has a schema, i.e. until
                # the first insert. Re-read lazily in search() rather than
                # trusting this snapshot.
                self._features = [f.name for f in fg.features]
                log.info("Agent memory vector store ready (%s)", self._name)
            except Exception:  # noqa: BLE001 — search is optional; never crash
                self._failed = True
                log.exception(
                    "Could not open the agent memory feature group; semantic "
                    "search is disabled"
                )
            return self._fg

    def ingest(self, rows: list[dict]) -> None:
        fg = self._ensure_fg()
        if fg is None or not rows:
            return
        try:
            import pandas as pd

            frame = pd.DataFrame(rows, columns=list(FEATURES))
            # storage="online" skips the offline Delta write, which needs
            # HopsFS/Spark and fails outright from an agent pod. Memory is an
            # online-only concern anyway: it is read by vector lookup, never
            # by a training query.
            fg.insert(frame, storage="online", write_options={"wait_for_job": False})
        except Exception:  # noqa: BLE001
            log.exception("Vector ingest failed; memories stay searchable only in SQL")

    def search(
        self,
        vector: list[float],
        *,
        k: int = 5,
        subject: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict]:
        fg = self._ensure_fg()
        if fg is None:
            return []

        try:
            condition = None
            for column, value in (
                ("subject", subject),
                ("conversation_id", conversation_id),
            ):
                if value is None:
                    continue
                clause = self._feature_filter(fg, column, value)
                condition = clause if condition is None else (condition & clause)
        except Exception:  # noqa: BLE001 — never fall through to an unfiltered
            # search: that is a cross-user read, not a degraded result
            log.exception(
                "Could not build the %r filter; refusing to search unfiltered",
                "subject" if subject is not None else "conversation_id",
            )
            return []

        # The schema only exists after the first insert, so a snapshot taken at
        # connect time can be empty; refresh before zipping values to names.
        if not self._features:
            self._features = [f.name for f in fg.features]

        try:
            results = self._find_neighbors(fg, vector, k, condition)
        except Exception:  # noqa: BLE001
            log.exception("Vector search failed; returning no hits")
            return []
        hits = []
        for score, values in results or []:
            row = dict(zip(self._features, values, strict=False))
            row.pop("embedding", None)
            row["score"] = score
            hits.append(row)
        return hits

    @staticmethod
    def _feature_filter(fg, column: str, value):
        """A filter on one feature, via ``get_feature`` rather than attribute access.

        ``getattr(fg, "subject")`` does **not** give you the feature: ``subject``
        is a real FeatureGroup attribute (the Avro schema-registry subject), so
        it shadows the column and ``fg.subject == "alice"`` evaluates to a plain
        ``False``. hsfs then translates that to *no filter clauses* and returns
        unfiltered neighbours — a silent cross-user read, on the one field the
        whole isolation story rests on. Any feature whose name collides with a
        FeatureGroup attribute fails the same way, so never use attribute access
        to build these.
        """
        from hsfs.constructor.filter import Filter

        condition = fg.get_feature(column) == value
        if not isinstance(condition, Filter):
            raise TypeError(
                f"filter on {column!r} produced {type(condition).__name__}, "
                "not a Filter"
            )
        return condition

    def _find_neighbors(self, fg, vector, k, condition):
        """``find_neighbors`` with a guard for its max-k discovery probe.

        On a project-shared OpenSearch index (the default), asking for more
        neighbours than exist makes hsfs re-query with ``k = 2**31 - 1`` to
        discover the index limit from the resulting error. When that error text
        is not in the shape hsfs parses, it propagates instead. A per-subject
        filter almost always matches fewer rows than ``k``, so this is the
        common case here, not an edge case: retry with progressively smaller k
        and return what we can rather than nothing.
        """
        attempt = k
        while attempt >= 1:
            try:
                if condition is not None:
                    return fg.find_neighbors(
                        vector, col="embedding", k=attempt, filter=condition
                    )
                return fg.find_neighbors(vector, col="embedding", k=attempt)
            except Exception as err:  # noqa: BLE001
                if "requires k to be in the range" not in str(err) or attempt == 1:
                    raise
                attempt //= 2
                log.debug("kNN probe tripped; retrying with k=%d", attempt)
        return []

    def purge(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        fg = self._ensure_fg()
        if fg is None:
            return 0
        probe = {}
        if conversation_id is not None:
            probe["conversation_id"] = conversation_id
        if subject is not None:
            probe["subject"] = subject
        if not probe:
            return 0

        # NOT `fg.delete(...)`: on the FeatureGroup API that name drops the
        # entire feature group, so guessing here risks destroying every user's
        # memories instead of one conversation's. Only a row-level deletion
        # method is acceptable, and if this hsfs version does not expose one we
        # say so loudly rather than silently reporting success.
        delete_rows = getattr(fg, "delete_from_online_store", None)
        if not callable(delete_rows):
            log.error(
                "This hsfs version exposes no row-level delete on feature group "
                "%s, so %s could not be purged from the vector index. The rows "
                "remain searchable until they are removed out of band — do not "
                "treat this deletion as complete.",
                self._name,
                probe,
            )
            return 0
        try:
            # `online=True` is not optional: ingest writes with
            # storage="online" only, so the offline Delta table this defaults to
            # is always empty. Worse, a serving pod cannot reach HopsFS at all,
            # so the default read raises there — and the old blanket `except`
            # turned that into "purged 0 rows", reporting a deletion that never
            # happened. The filter stays client-side because the online store is
            # a key-value lookup, not a query engine; there is no predicate to
            # push down.
            frame = fg.read(online=True)
            keys = [
                row["item_id"]
                for _, row in frame.iterrows()
                if all(row.get(col) == val for col, val in probe.items())
            ]
            for key in keys:
                delete_rows({"item_id": key})
            return len(keys)
        except Exception as err:  # noqa: BLE001
            # Raise rather than return 0. This is the erasure path: a caller
            # that is told "0 rows removed" cannot tell "there was nothing to
            # delete" from "the delete failed and the content is still
            # searchable", and only one of those is safe to report to someone
            # who asked to be forgotten.
            log.exception("Vector purge failed — deleted content may remain searchable")
            raise VectorPurgeError(
                f"Could not purge vectors for {probe}: {err}"
            ) from err


def vector_store_for(embedder, deployment_id: str | None = None, **kwargs):
    """A :class:`HopsworksVectorStore` sized to an embedder.

    The dimension and model identity come from the embedder itself — from its
    ``dimension`` / ``model_id`` attributes when it has them (as the shipped
    :func:`sentence_transformer_embedder` does), otherwise by embedding a probe
    string. Getting this from the embedder rather than from a hand-passed number
    is what makes the "did someone swap the model?" check on connect meaningful.
    """
    import os

    if deployment_id is None:
        deployment_id = os.environ.get("DEPLOYMENT_ID")
        if deployment_id is None:
            raise RuntimeError(
                "DEPLOYMENT_ID is not set; pass deployment_id= explicitly."
            )
    dimension = getattr(embedder, "dimension", None)
    if dimension is None:
        dimension = len(embedder("dimension probe"))
    return HopsworksVectorStore(
        deployment_id,
        dimension,
        embedder_id=getattr(embedder, "model_id", "unknown"),
        **kwargs,
    )
