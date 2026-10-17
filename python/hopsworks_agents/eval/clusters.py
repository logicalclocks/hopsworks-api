"""Group triaged verdicts into failure clusters.

Twenty negative verdicts are usually four problems. A reviewer should handle a
problem, not a row, so every triaged verdict is filed under a cluster keyed on
its normalised ``failure_signature``, and the queue shows clusters.

The model is asked to reuse the agent's existing signatures when one fits (see
:mod:`triage`), which is what makes exact matching good enough to start with;
a reviewer can rename or merge what it gets wrong. Signatures are the fine
layer and belong to the team; the six issue categories are the coarse layer
and stay fixed.

Each cluster keeps a representative: the member a reviewer should read first,
and the one promotion turns into a task. It is the member with a usable
correction, of the highest severity, that the model was most confident about,
in that order -- and it only changes when a new member beats the current one,
so a cluster does not change face every run.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}

CLUSTER_STATUSES = ("open", "promoted", "dismissed")

#: How many existing signatures the model is shown. The largest first, so what it
#: reuses is what people already recognise.
KNOWN_SIGNATURES_LIMIT = 50


@dataclass
class Cluster:
    cluster_id: str
    deployment_id: int
    signature: str
    label: str = ""
    severity: str = "low"
    size: int = 0
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    representative_trace_id: str = ""
    representative_feedback_id: str = ""
    status: str = "open"
    promoted_task_id: str = ""
    dismiss_reason: str = ""
    decided_by: str = ""
    decided_at: str = ""
    #: Which score the representative earned; in memory only, so a better member can replace it.
    representative_score: tuple = field(
        default_factory=tuple, repr=False, compare=False
    )
    changed: bool = field(default=False, repr=False, compare=False)

    def to_row(self, now: datetime) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "deployment_id": self.deployment_id,
            "signature": self.signature,
            "label": self.label,
            "severity": self.severity,
            "size": self.size,
            "first_seen": self.first_seen or now,
            "last_seen": self.last_seen or now,
            "representative_trace_id": self.representative_trace_id,
            "representative_feedback_id": self.representative_feedback_id,
            "status": self.status,
            "promoted_task_id": self.promoted_task_id,
            "dismiss_reason": self.dismiss_reason,
            "decided_by": self.decided_by,
            "decided_at": self.decided_at,
            "updated_at": now,
        }


def _parse_when(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def from_api(entry: dict[str, Any]) -> Cluster:
    """A cluster as the backend lists it (camelCase), as the job works with it."""
    return Cluster(
        cluster_id=str(entry.get("clusterId") or ""),
        deployment_id=int(entry.get("deploymentId") or 0),
        signature=str(entry.get("signature") or ""),
        label=str(entry.get("label") or ""),
        severity=str(entry.get("severity") or "low"),
        size=int(entry.get("size") or 0),
        first_seen=_parse_when(entry.get("firstSeen")),
        last_seen=_parse_when(entry.get("lastSeen")),
        representative_trace_id=str(entry.get("representativeTraceId") or ""),
        representative_feedback_id=str(entry.get("representativeFeedbackId") or ""),
        status=str(entry.get("status") or "open"),
        promoted_task_id=str(entry.get("promotedTaskId") or ""),
        dismiss_reason=str(entry.get("dismissReason") or ""),
        decided_by=str(entry.get("decidedBy") or ""),
        decided_at=str(entry.get("decidedAt") or ""),
        # an existing representative was chosen by an earlier run; only a clearly better
        # member displaces it, which a mid-rank score expresses
        representative_score=(
            1,
            SEVERITY_RANK.get(str(entry.get("severity") or "low"), 0),
            0.0,
        ),
    )


def known_signatures(
    clusters: Iterable[Cluster], limit: int = KNOWN_SIGNATURES_LIMIT
) -> list[str]:
    """The signatures worth offering the model, largest clusters first. Dismissed.

    clusters are left out: a signature a reviewer said was not a problem should
    not attract new members.
    """
    live = [c for c in clusters if c.signature and c.status != "dismissed"]
    live.sort(key=lambda c: (-c.size, c.signature))
    seen: set[str] = set()
    out: list[str] = []
    for cluster in live:
        if cluster.signature in seen:
            continue
        seen.add(cluster.signature)
        out.append(cluster.signature)
        if len(out) >= limit:
            break
    return out


def _score(row: dict[str, Any]) -> tuple:
    return (
        1 if row.get("correction_status") == "usable" else 0,
        SEVERITY_RANK.get(str(row.get("severity") or "low"), 0),
        float(row.get("confidence") or 0.0),
    )


def assign_clusters(
    rows: Sequence[dict[str, Any]],
    existing: Iterable[Cluster],
    *,
    now: datetime | None = None,
) -> list[Cluster]:
    """File each triage row under a cluster, in place, and return the clusters that changed.

    Rows with no result (ungradable) or no signature are left unclustered: there
    is nothing to group them by, and a bucket called "unknown" would hide that.
    A dismissed cluster is not reopened by a new member; the member is filed under
    it so the count is honest, and the reviewer's decision stands until they change it.
    """
    now = now or datetime.now(tz=timezone.utc)
    by_signature: dict[str, Cluster] = {}
    for cluster in existing:
        if cluster.signature and cluster.signature not in by_signature:
            by_signature[cluster.signature] = cluster

    for row in rows:
        signature = str(row.get("failure_signature") or "").strip()
        if row.get("ungradable") or not signature:
            row["cluster_id"] = ""
            continue
        cluster = by_signature.get(signature)
        seen_at = _parse_when(row.get("created_at")) or now
        if cluster is None:
            cluster = Cluster(
                cluster_id=str(uuid.uuid4()),
                deployment_id=int(row.get("deployment_id") or 0),
                signature=signature,
                first_seen=seen_at,
                last_seen=seen_at,
            )
            by_signature[signature] = cluster
        row["cluster_id"] = cluster.cluster_id
        cluster.size += 1
        cluster.changed = True
        cluster.first_seen = min(cluster.first_seen or seen_at, seen_at)
        cluster.last_seen = max(cluster.last_seen or seen_at, seen_at)
        if SEVERITY_RANK.get(str(row.get("severity") or "low"), 0) > SEVERITY_RANK.get(
            cluster.severity, 0
        ):
            cluster.severity = str(row.get("severity"))
        score = _score(row)
        if (
            not cluster.representative_feedback_id
            or score > cluster.representative_score
        ):
            cluster.representative_feedback_id = str(row.get("feedback_id") or "")
            cluster.representative_trace_id = str(row.get("trace_id") or "")
            cluster.representative_score = score
            # the label is what a reviewer reads in the queue; the representative's summary is the
            # best one-line account the cluster has
            cluster.label = str(
                row.get("failure_summary") or cluster.label or signature
            )

    return [c for c in by_signature.values() if c.changed]
