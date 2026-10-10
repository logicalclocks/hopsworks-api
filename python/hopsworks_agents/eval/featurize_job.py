"""The scheduled job that writes ``agent_trace_features``.

Deliberately thin. Everything that can be wrong in an interesting way lives in
:mod:`features` and is tested without a cluster; this file is the part that
cannot be, so it is kept to reading rows, calling one function, and writing
rows.

Run as a Hopsworks Python job:

    python -m hopsworks_agents.eval.featurize_job --deployment-id 1035

Reads from the **online** store by default. The offline tables are empty unless
the deployment sets ``OTEL_TRACING_STORAGE=both`` (the default is ``online``),
so an offline-only reader would silently featurize nothing and look like a
broken job rather than a misconfigured deployment.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .features import select_ready_traces, trace_features


log = logging.getLogger(__name__)

SPANS_FG = "otel_spans"
SPAN_ATTRIBUTES_FG = "otel_span_attributes"
EVENTS_FG = "otel_events"
FEATURES_FG = "agent_trace_features"


def _read(feature_store: Any, name: str, online: bool) -> list[dict[str, Any]]:
    frame = feature_store.get_feature_group(name, 1).read(online=online)
    return frame.to_dict("records")


def _watermark(feature_store: Any, deployment_id: int, online: bool) -> datetime | None:
    """Newest ingestion time already featurized, or None on the first run.

    Kept on ``created_at`` rather than the trace's start time: a span held up
    in the sidecar's insert queue has an old start time and a fresh ingestion
    time, and a watermark on the former would step straight over it.
    """
    try:
        rows = _read(feature_store, FEATURES_FG, online)
    except Exception:  # noqa: BLE001 — first run, before the FG has any rows
        return None
    times = [
        r["created_at"]
        for r in rows
        if r.get("deployment_id") == deployment_id and r.get("created_at")
    ]
    return max(times) if times else None


def featurize(
    feature_store: Any,
    deployment_id: int,
    *,
    now: datetime | None = None,
    grace_minutes: float = 3.0,
    root_timeout_minutes: float = 30.0,
    online: bool = True,
    input_token_price_per_million: float | None = None,
    output_token_price_per_million: float | None = None,
) -> int:
    """Featurize every trace that has settled. Returns the number of rows."""
    now = now or datetime.now(tz=timezone.utc)
    spans = [
        s
        for s in _read(feature_store, SPANS_FG, online)
        if s.get("deployment_id") == deployment_id
    ]
    if not spans:
        log.info("No spans for deployment %s", deployment_id)
        return 0

    completeness = select_ready_traces(
        spans,
        now=now,
        grace=timedelta(minutes=grace_minutes),
        root_timeout=timedelta(minutes=root_timeout_minutes),
    )
    log.info(
        "deployment %s: %d ready, %d partial, %d still in flight",
        deployment_id,
        len(completeness.ready),
        len(completeness.partial),
        len(completeness.pending),
    )
    if not completeness.ready and not completeness.partial:
        return 0

    wanted = set(completeness.ready) | set(completeness.partial)
    attributes = [
        a
        for a in _read(feature_store, SPAN_ATTRIBUTES_FG, online)
        if a.get("trace_id") in wanted
    ]
    events = [
        e
        for e in _read(feature_store, EVENTS_FG, online)
        if e.get("trace_id") in wanted
    ]

    spans_by_trace: dict[str, list[dict[str, Any]]] = {}
    for span in spans:
        if span.get("trace_id") in wanted:
            spans_by_trace.setdefault(span["trace_id"], []).append(span)

    rows = []
    for trace_id, trace_spans in spans_by_trace.items():
        try:
            rows.append(
                trace_features(
                    trace_spans,
                    [a for a in attributes if a.get("trace_id") == trace_id],
                    [e for e in events if e.get("trace_id") == trace_id],
                    is_partial=trace_id in completeness.partial,
                    input_token_price_per_million=input_token_price_per_million,
                    output_token_price_per_million=output_token_price_per_million,
                )
            )
        except Exception:  # noqa: BLE001 — one bad trace must not fail the run
            log.exception("Could not featurize trace %s", trace_id)

    if rows:
        import pandas as pd

        feature_store.get_feature_group(FEATURES_FG, 1).insert(
            pd.DataFrame(rows), write_options={"mode": "append"}
        )
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", type=int, required=True)
    parser.add_argument(
        "--grace-minutes",
        type=float,
        default=3.0,
        help="derive this from the Stage 1 probe's "
        "trajectory-stable p95, not from a guess",
    )
    parser.add_argument("--root-timeout-minutes", type=float, default=30.0)
    parser.add_argument(
        "--offline",
        action="store_true",
        help="read the offline store; requires the deployment "
        "to run with OTEL_TRACING_STORAGE=both",
    )
    parser.add_argument("--input-token-price-per-million", type=float)
    parser.add_argument("--output-token-price-per-million", type=float)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    import hopsworks

    project = hopsworks.login()
    written = featurize(
        project.get_feature_store(),
        args.deployment_id,
        grace_minutes=args.grace_minutes,
        root_timeout_minutes=args.root_timeout_minutes,
        online=not args.offline,
        input_token_price_per_million=args.input_token_price_per_million,
        output_token_price_per_million=args.output_token_price_per_million,
    )
    log.info("Wrote %d trace feature rows", written)


if __name__ == "__main__":
    main()
