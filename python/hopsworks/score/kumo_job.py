"""Scoring job for ``hops score``: Kumo-Relational over ingested feature groups.

The job runs in the project on the ``scoring-pipeline`` environment, which ships
torch, pyg-lib, structured-data-models and the Kumo-Relational weights. It reads the
cached feature groups through Trino, bounded and projected, scores the query rows
against a labelled context, writes ``<name>_scores`` in one insert and publishes the
run by writing its ``<name>_score_runs`` row last. This file imports nothing from the
rest of ``hopsworks``, so the CLI uploads it as it is.

Usage: ``kumo_job.py --system <name> --config '<plan json>'``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shlex
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MODEL_REPO = "nvidia/Kumo-Relational"
DEFAULT_REVISION = "v1.0.1"
NUM_NEIGHBORS = (16, 16)
BATCH = 1_000
MAX_DEPTH = 2
SEED = "kumo-score-1"
FETCH_ROWS = 1_000

CONTEXT_ROWS_MAX = 1_000
QUERY_ROWS_MAX = 10_000
ENTITY_BYTES_MAX = 256 * 1024 * 1024
RELATED_ROWS_MAX = 100_000
RELATED_BYTES_MAX = 1024 * 1024 * 1024

LABEL = "label"
CUTOFF = "__cutoff__"
CONST = "const"


class ScoreRefused(Exception):
    """The run does not start: an input is over a cap or does not fit the plan."""


# region: ownership -------------------------------------------------------

APP_DIR = "Resources/hops-score"
MAX_CONCURRENT = "maxConcurrentExecutions"
PLATFORM_CLASS_KEY = "platformClass"
PLATFORM_CLASS = "scoring"


def owner_marker(name: str) -> str:
    """The token in the description of every feature group `hops score <name>` makes."""
    return f"hops-score:{name}"


def output_groups(name: str) -> tuple[str, str]:
    return f"{name}_scores", f"{name}_score_runs"


def group_marked(description: str | None, name: str) -> bool:
    return owner_marker(name) in (description or "").split()


def job_marked(config: dict, name: str, script: str) -> bool:
    """Whether the job is the system's.

    It runs `script` from the system's folder with `--system <name>` in its arguments, one
    execution at a time, in the scoring platform class.
    """
    if (
        config.get(MAX_CONCURRENT) != 1
        or config.get(PLATFORM_CLASS_KEY) != PLATFORM_CLASS
    ):
        return False
    try:
        args = shlex.split(str(config.get("defaultArgs") or ""))
    except ValueError:
        return False
    flagged = any(args[i : i + 2] == ["--system", name] for i in range(len(args)))
    app = "/" + str(config.get("appPath") or "")
    return flagged and app.endswith(f"/{APP_DIR}/{name}/{script}")


def check_inputs_are_not_outputs(name: str, groups: Any) -> None:
    """Refuse an input group named like an output of the system: teardown would delete it."""
    clash = sorted(set(groups) & set(output_groups(name)))
    if clash:
        raise ScoreRefused(
            f"{', '.join(clash)} is an input, and `hops score {name}` writes and deletes a "
            "group of that name. Choose another system name."
        )


def check_output_group(fg: Any, name: str) -> None:
    """Refuse an existing output group that `hops score <name>` did not make."""
    if fg is not None and not group_marked(getattr(fg, "description", None), name):
        raise ScoreRefused(
            f"feature group {fg.name} v{fg.version} exists without the marker "
            f"{owner_marker(name)} in its description: `hops score {name}` did not make it "
            "and leaves it alone. Choose another system name."
        )


@dataclass
class Link:
    left: str
    left_cols: list[str]
    right: str
    right_cols: list[str]

    def __str__(self) -> str:
        left = "+".join(f"{self.left}.{c}" for c in self.left_cols)
        right = "+".join(f"{self.right}.{c}" for c in self.right_cols)
        return f"{left}={right}"


@dataclass
class Group:
    version: int
    columns: list[list[str]]  # [name, type] of the projected columns
    primary_key: list[str] = field(default_factory=list)

    @property
    def names(self) -> list[str]:
        return [c[0] for c in self.columns]


@dataclass
class Plan:
    name: str
    entity: str
    keys: list[str]
    target: str
    groups: dict[str, Group]
    links: list[Link] = field(default_factory=list)
    positive: str | None = None
    cutoff: str | None = None
    where: str | None = None
    time: dict[str, str] = field(default_factory=dict)
    context_rows: int = CONTEXT_ROWS_MAX
    query_rows: int = QUERY_ROWS_MAX
    # Delta table version of each group, set by admit(): every later query reads it.
    pins: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> Plan:
        raw = json.loads(text)
        raw["groups"] = {g: Group(**v) for g, v in raw["groups"].items()}
        raw["links"] = [Link(**v) for v in raw["links"]]
        return cls(**raw)

    @property
    def related(self) -> list[str]:
        return [g for g in self.groups if g != self.entity]

    @property
    def scores_name(self) -> str:
        return output_groups(self.name)[0]

    @property
    def runs_name(self) -> str:
        return output_groups(self.name)[1]


# region: SQL -------------------------------------------------------------


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table(plan: Plan, group: str) -> str:
    """The group's offline table, at its pinned Delta version once admit() pinned it."""
    name = ident(f"{group}_{plan.groups[group].version}")
    if group in plan.pins:
        return f"{name} FOR VERSION AS OF {int(plan.pins[group])}"
    return name


def pin_sql(plan: Plan) -> str:
    """The current Delta version of each group's table, from the connector's $history table."""
    return " UNION ALL ".join(
        f"SELECT {literal(g)}, max(version) FROM "
        f"{ident(f'{g}_{plan.groups[g].version}$history')}"
        for g in sorted(plan.groups)
    )


def literal(value: Any) -> str:
    """A SQL literal for a key value read from a frame."""
    import numpy as np  # noqa: PLC0415 - the job image ships it
    import pandas as pd  # noqa: PLC0415

    if isinstance(value, (bool, np.bool_)):
        return "true" if value else "false"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return repr(float(value))
    if isinstance(value, (pd.Timestamp, datetime)):
        return f"TIMESTAMP '{pd.Timestamp(value).strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    return "'" + str(value).replace("'", "''") + "'"


def _sizes(plan: Plan, group: str) -> str:
    return " + ".join(
        f"coalesce(length(cast({ident(c)} as varchar)), 0)"
        for c in plan.groups[group].names
    )


def admission_sql(plan: Plan, group: str) -> str:
    """Row count and the summed byte length of the projected columns of one group."""
    return f"SELECT count(*), coalesce(sum({_sizes(plan, group)}), 0) FROM {table(plan, group)}"


def _order(plan: Plan) -> str:
    """Most recent by the cutoff column, else a seeded pseudo-random order; ties by key.

    The order is total, so the selection the reach counts and the rows read are the same.
    """
    keys = ", ".join(ident(k) for k in plan.keys)
    if plan.cutoff:
        return f"{ident(plan.cutoff)} DESC, {keys}"
    parts = ", '|', ".join(f"cast({ident(k)} as varchar)" for k in plan.keys)
    return f"md5(to_utf8(concat('{SEED}', '|', {parts}))), {keys}"


def _columns(plan: Plan, group: str) -> str:
    return ", ".join(ident(c) for c in plan.groups[group].names)


def query_sql(plan: Plan, columns: str | None = None) -> str:
    """The rows to score: the target is null, or the `--where` filter, at most query_rows."""
    cond = f"({plan.where})" if plan.where else f"{ident(plan.target)} IS NULL"
    return (
        f"SELECT {columns or _columns(plan, plan.entity)} FROM {table(plan, plan.entity)} "
        f"WHERE {cond} ORDER BY {_order(plan)} LIMIT {plan.query_rows}"
    )


def context_sql(plan: Plan, columns: str | None = None) -> str:
    """The labelled rows, at most context_rows, never a row the query takes."""
    conds = [f"{ident(plan.target)} IS NOT NULL"]
    if plan.cutoff:
        conds.append(f"{ident(plan.cutoff)} IS NOT NULL")
    if plan.where:
        conds.append(f"NOT coalesce(({plan.where}), false)")
    return (
        f"SELECT {columns or _columns(plan, plan.entity)} FROM {table(plan, plan.entity)} "
        f"WHERE {' AND '.join(conds)} ORDER BY {_order(plan)} LIMIT {plan.context_rows}"
    )


Hop = tuple[
    str, tuple[str, ...], str, tuple[str, ...]
]  # from, its columns, to, its columns


def paths(plan: Plan) -> list[tuple[Hop, ...]]:
    """Every path from the entity through the links, MAX_DEPTH hops at most, no group twice.

    A link is walked both ways. The list is sorted, so the order of the links changes nothing.
    """
    edges = set()
    for link in plan.links:
        left, right = tuple(link.left_cols), tuple(link.right_cols)
        edges.add((link.left, left, link.right, right))
        edges.add((link.right, right, link.left, left))
    out: list[tuple[Hop, ...]] = []

    def extend(path: tuple[Hop, ...], seen: frozenset[str]) -> None:
        here = path[-1][2] if path else plan.entity
        for edge in sorted(edges):
            if edge[0] == here and edge[2] not in seen:
                longer = (*path, edge)
                out.append(longer)
                if len(longer) < MAX_DEPTH:
                    extend(longer, seen | {edge[2]})

    extend((), frozenset([plan.entity]))
    reached = {p[-1][2] for p in out}
    unreached = [g for g in plan.related if g not in reached]
    if unreached:
        raise ScoreRefused(
            f"{', '.join(unreached)} not reachable from {plan.entity} within "
            f"{MAX_DEPTH} hops through the links."
        )
    return sorted(out, key=lambda p: (len(p), p))


def _reach_ctes(plan: Plan) -> tuple[list[str], dict[str, list[str]]]:
    """CTEs of the rows each path reaches from the selected entity rows, and each group's CTEs.

    `sel` is the selection: the query and the context rows, as the reads choose them. Each
    path's CTE holds the rows of its last group that match the rows of the path one hop
    shorter on the link's columns.
    """
    found = paths(plan)
    keys = sorted({c for p in found if p[0][0] == plan.entity for c in p[0][1]})
    cols = ", ".join(ident(c) for c in keys)
    ctes = [f"sel AS (({query_sql(plan, cols)}) UNION ({context_sql(plan, cols)}))"]
    alias: dict[tuple[Hop, ...], str] = {(): "sel"}
    by_group: dict[str, list[str]] = {}
    for i, path in enumerate(found):
        src, src_cols, dst, dst_cols = path[-1]
        name = f"p{i}"
        match = " AND ".join(
            f"s.{ident(sc)} = t.{ident(dc)}"
            for sc, dc in zip(src_cols, dst_cols, strict=True)
        )
        ctes.append(
            f"{name} AS (SELECT {_columns(plan, dst)} FROM {table(plan, dst)} t "
            f"WHERE EXISTS (SELECT 1 FROM {alias[path[:-1]]} s WHERE {match}))"
        )
        alias[path] = name
        by_group.setdefault(dst, []).append(name)
    return ctes, by_group


def _reached(plan: Plan, group: str, ctes: list[str]) -> str:
    """The union of the rows every path reaches in `group`, one row per primary key."""
    cols = _columns(plan, group)
    union = " UNION ALL ".join(f"SELECT {cols} FROM {c}" for c in ctes)
    pk = plan.groups[group].primary_key
    if not pk:
        return " UNION ".join(f"SELECT {cols} FROM {c}" for c in ctes)
    part = ", ".join(ident(k) for k in pk)
    return (
        f"SELECT {cols} FROM (SELECT {cols}, row_number() OVER (PARTITION BY {part}) "
        f'AS "__n" FROM ({union})) WHERE "__n" = 1'
    )


def related_sql(plan: Plan, group: str) -> str:
    """The rows of `group` reachable from the selected entity rows: what the job reads."""
    ctes, by_group = _reach_ctes(plan)
    return f"WITH {', '.join(ctes)} {_reached(plan, group, by_group[group])}"


def reach_sql(plan: Plan) -> str | None:
    """Rows and bytes of each related group's reachable rows: (group, rows, bytes) per group.

    The same rows related_sql reads. None when the plan has no related group.
    """
    if not plan.related:
        return None
    ctes, by_group = _reach_ctes(plan)
    counts = []
    for i, group in enumerate(sorted(by_group)):
        name = f"g{i}"
        ctes.append(f"{name} AS ({_reached(plan, group, by_group[group])})")
        counts.append(
            f"SELECT {literal(group)}, count(*), coalesce(sum({_sizes(plan, group)}), 0) "
            f"FROM {name}"
        )
    return f"WITH {', '.join(ctes)} " + " UNION ALL ".join(counts)


# region: reads -----------------------------------------------------------


class Reader:
    """Runs SQL through `sql` and logs each call.

    `sql` has `.rows(sql)` and `.frame(sql, max_rows, max_bytes)`; the frame read refuses as
    soon as a chunk would take it past either cap, and returns the frame and its bytes.
    """

    def __init__(self, sql: Any, log: Any = None) -> None:
        self.sql = sql
        self.log = log or (lambda msg: None)
        self.calls: list[tuple[str, str, int]] = []  # (kind, group, rows)

    def pins(self, sql: str) -> dict[str, int]:
        out = {}
        for group, version in self.sql.rows(sql):
            if version is None:
                raise ScoreRefused(f"{group} has no Delta history to pin.")
            out[group] = int(version)
        return out

    def admission(self, group: str, sql: str) -> tuple[int, int]:
        rows, size = self.sql.rows(sql)[0]
        self.calls.append(("admission", group, int(rows)))
        return int(rows), int(size)

    def reach(self, sql: str) -> dict[str, tuple[int, int]]:
        out = {}
        for group, rows, size in self.sql.rows(sql):
            self.calls.append(("admission", group, int(rows)))
            out[group] = (int(rows), int(size))
        return out

    def read(
        self, group: str, sql: str, max_rows: int, max_bytes: int
    ) -> tuple[Any, int]:
        started = time.perf_counter()
        frame, size = self.sql.frame(sql, max_rows, max_bytes, group)
        self.calls.append(("read", group, len(frame)))
        self.log(
            f"read {group}: {len(frame)} rows {size} bytes in "
            f"{time.perf_counter() - started:.1f}s"
        )
        return frame, size

    @property
    def reads(self) -> list[tuple[str, str, int]]:
        return [c for c in self.calls if c[0] == "read"]


def row_bytes(row: tuple) -> int:
    """A row's size as admission counts it: the text length of each non-null value."""
    return sum(len(str(v)) for v in row if v is not None)


def check_read(group: str, rows: int, size: int, max_rows: int, max_bytes: int) -> None:
    """Refuse a read about to hold more than its caps; called before a chunk is kept."""
    if rows > max_rows or size > max_bytes:
        raise ScoreRefused(
            f"{group}: the read reached {rows} rows, {size} bytes, over what is left of the "
            f"caps ({max_rows} rows, {max_bytes} bytes). Stopped before keeping more."
        )


def check_caps(plan: Plan) -> None:
    if plan.context_rows > CONTEXT_ROWS_MAX:
        raise ScoreRefused(
            f"--context-rows {plan.context_rows} is over {CONTEXT_ROWS_MAX}, the largest "
            "size measured: a larger context stays refused until its memory is measured."
        )
    if plan.query_rows > QUERY_ROWS_MAX:
        raise ScoreRefused(
            f"--query-rows {plan.query_rows} is over the cap of {QUERY_ROWS_MAX}."
        )
    if plan.context_rows < 2 or plan.query_rows < 1:
        raise ScoreRefused("--context-rows needs 2 or more, --query-rows 1 or more.")


def check_admission(
    plan: Plan, entity: tuple[int, int], reach: dict[str, tuple[int, int]]
) -> None:
    """Refuse over a cap.

    `entity` is (rows, bytes) of the projected entity group, `reach` the (rows, bytes) of each
    related group's rows reachable from the selected rows.
    """
    rows, size = entity
    if size > ENTITY_BYTES_MAX:
        raise ScoreRefused(
            f"{plan.entity}: {rows} rows, {size} bytes after projection, "
            f"over the cap of {ENTITY_BYTES_MAX} bytes for the entity group. Nothing was read."
        )
    rel_rows = sum(r for r, _ in reach.values())
    rel_bytes = sum(b for _, b in reach.values())
    detail = ", ".join(f"{g} {r}" for g, (r, _) in reach.items())
    if rel_rows > RELATED_ROWS_MAX:
        raise ScoreRefused(
            f"related rows reachable from the selected rows: {rel_rows} ({detail}), over the "
            f"cap of {RELATED_ROWS_MAX}. Narrow the selection with --where or --context-rows. "
            "Nothing was read."
        )
    if rel_bytes > RELATED_BYTES_MAX:
        raise ScoreRefused(
            f"related rows reachable from the selected rows: {rel_bytes} bytes after projection "
            f"({detail} rows), over the cap of {RELATED_BYTES_MAX} bytes. Nothing was read."
        )


def admit(plan: Plan, reader: Reader) -> tuple[Plan, dict[str, dict[str, int]]]:
    """Pin each group's Delta version, then count the selection's rows at those versions.

    The entity group and the related rows reachable from the selection, in Trino. Refuses over a cap before any frame is read. Returns the pinned plan: its reads see
    the state admission counted, whatever is written in between.
    """
    check_caps(plan)
    plan = replace(plan, pins=reader.pins(pin_sql(plan)))
    rows, size = reader.admission(plan.entity, admission_sql(plan, plan.entity))
    sql = reach_sql(plan)
    reach = reader.reach(sql) if sql else {}
    check_admission(plan, (rows, size), reach)
    stats = {plan.entity: {"rows": rows, "bytes": size}}
    stats.update({g: {"rows": r, "bytes": b} for g, (r, b) in reach.items()})
    reader.log(
        "admission ok at versions "
        + ", ".join(f"{g} @{v}" for g, v in sorted(plan.pins.items()))
        + ": "
        + ", ".join(
            f"{g} {s['rows']} rows {s['bytes']} bytes" for g, s in stats.items()
        )
    )
    return plan, stats


def read_related(plan: Plan, reader: Reader) -> dict[str, Any]:
    """The related rows reachable from the selected entity rows, as admission counted them.

    The related caps hold over all groups together, enforced while reading.
    """
    frames = {}
    rows = size = 0
    for group in sorted(plan.related):
        frame, got = reader.read(
            group,
            related_sql(plan, group),
            RELATED_ROWS_MAX - rows,
            RELATED_BYTES_MAX - size,
        )
        rows, size = rows + len(frame), size + got
        frames[group] = frame
    return frames


@dataclass
class Inputs:
    query: Any
    context: Any
    related: dict[str, Any]


def read_inputs(plan: Plan, reader: Reader) -> Inputs:
    """The bounded reads of a pinned plan. A query with no rows reads nothing else."""
    query, size = reader.read(
        plan.entity, query_sql(plan), plan.query_rows, ENTITY_BYTES_MAX
    )
    if query.empty:
        return Inputs(query, query.iloc[0:0], {})
    context, _ = reader.read(
        plan.entity, context_sql(plan), plan.context_rows, ENTITY_BYTES_MAX - size
    )
    return Inputs(query, context, read_related(plan, reader))


def load_inputs(plan: Plan, reader: Reader) -> Inputs:
    """Admission, then the reads at the versions admission pinned."""
    pinned, _ = admit(plan, reader)
    return read_inputs(pinned, reader)


# region: model -----------------------------------------------------------


def cpu_limit() -> int:
    """The pod's CPU limit in whole cores (cgroup v2 cpu.max), at least one; one when unlimited."""
    with contextlib.suppress(OSError, ValueError):
        quota, period = (
            Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
        )
        if quota != "max":
            return max(1, int(int(quota) / int(period)))
    return 1


def weights_path() -> str:
    path = os.environ.get("KUMO_RELATIONAL_WEIGHTS")
    if not path:
        raise ScoreRefused(
            "KUMO_RELATIONAL_WEIGHTS is not set: this job runs on the scoring-pipeline "
            "environment, which bakes the Kumo-Relational weights and names their path."
        )
    file = Path(path)
    if file.is_dir():
        file = file / "classifier.pt"
    if not file.is_file():
        raise ScoreRefused(
            f"KUMO_RELATIONAL_WEIGHTS points at {path}, which holds no classifier.pt."
        )
    return str(file)


def model_version() -> str:
    return (
        f"{MODEL_REPO}@{os.environ.get('KUMO_RELATIONAL_REVISION', DEFAULT_REVISION)}"
    )


def _naive_time(series: Any) -> Any:
    import pandas as pd  # noqa: PLC0415

    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _normalise(plan: Plan, group: str, frame: Any) -> Any:
    frame = frame.copy()
    for name, kind in plan.groups[group].columns:
        if kind.startswith(("timestamp", "date")) and name in frame:
            frame[name] = _naive_time(frame[name])
    return frame


def is_positive(plan: Plan, values: Any) -> Any:
    """Boolean series: the target equals the positive value (a boolean or a 0/1 target needs none)."""
    if plan.positive is not None:
        return values.astype(str).str.lower() == str(plan.positive).lower()
    distinct = set(values.dropna().unique().tolist())
    if distinct <= {True, False}:
        return values.astype(bool)
    if distinct <= {0, 1}:
        return values == 1
    sample = sorted(map(str, distinct))[:5]
    raise ScoreRefused(
        f"the target {plan.target} is not boolean or 0/1: pass --positive <value>. It holds {sample}."
    )


def build_relational(sdm: Any, plan: Plan, inputs: Inputs) -> tuple[Any, dict]:
    """The sampler over the groups, and the task link to the entity table."""
    import pandas as pd  # noqa: PLC0415

    entity_rows = pd.concat(
        [inputs.context, inputs.query], ignore_index=True
    ).drop_duplicates(subset=plan.keys)
    frames = {plan.entity: entity_rows, **inputs.related}
    key_cols: dict[str, set] = {g: set() for g in frames}
    for link in plan.links:
        key_cols[link.left].update(link.left_cols)
        key_cols[link.right].update(link.right_cols)
    key_cols[plan.entity].update(plan.keys)

    tables = {}
    for group, frame in frames.items():
        frame = _normalise(plan, group, frame)
        drop = []
        if group == plan.entity:
            drop.append(plan.target)
            if plan.cutoff and plan.time.get(group) != plan.cutoff:
                drop.append(plan.cutoff)
        frame = frame.drop(columns=drop)
        overrides = dict.fromkeys(key_cols[group], "id")
        if group in plan.time:
            overrides[plan.time[group]] = "datetime"
        stypes = sdm.infer_stypes(
            frame.head(10_000), overrides=overrides, text="drop", unsupported="drop"
        )
        tables[group] = sdm.TableTensor.from_pandas(df=frame, stypes=stypes)
    data = sdm.RelationalData(
        tables=tables,
        relationships=[
            {
                "left_table": link.left,
                "left_columns": tuple(link.left_cols),
                "right_table": link.right,
                "right_columns": tuple(link.right_cols),
            }
            for link in plan.links
        ],
    )
    sampler = data.sampler(time_columns=dict(plan.time))
    link = {
        "task_columns": tuple(plan.keys),
        "table": plan.entity,
        "table_columns": tuple(plan.keys),
    }
    return sampler, link


def task_frame(plan: Plan, rows: Any, started: Any, labelled: bool) -> Any:
    """Task table rows: keys, the cutoff time, a constant column, and the label when known."""
    out = rows[plan.keys].copy()
    if labelled and plan.cutoff:
        out[CUTOFF] = _naive_time(rows[plan.cutoff])
    else:
        out[CUTOFF] = started
    out[CONST] = 0.0
    if labelled:
        out[LABEL] = is_positive(plan, rows[plan.target]).astype(int).astype(str)
    return out


def _stypes(plan: Plan, labelled: bool) -> dict[str, str]:
    stypes = dict.fromkeys(plan.keys, "id")
    stypes.update({CUTOFF: "datetime", CONST: "numerical"})
    if labelled:
        stypes[LABEL] = "categorical"
    return stypes


def load_model(path: str) -> Any:
    import torch  # noqa: PLC0415 - the job image ships it
    from sdm.models import KumoRelational  # noqa: PLC0415

    model = KumoRelational(task="classification", pretrained=False, device="meta")
    state = torch.load(path, map_location="cpu", weights_only=True)
    model.models["classification"].load_state_dict(state, assign=True)
    model.eval()
    return model


def positive_column(out: Any) -> Any:
    frame = out.to_pandas()
    hits = [c for c in frame.columns if str(c) in ("1", "True", "1.0")]
    if not hits:
        raise ScoreRefused("the model returned no probability for the positive class.")
    return frame[hits[0]].to_numpy()


def score(plan: Plan, inputs: Inputs, started: Any, log: Any) -> Any:
    """Scores for the query rows: one context encoding, query batches of BATCH rows."""
    import numpy as np  # noqa: PLC0415
    import pandas as pd  # noqa: PLC0415
    import sdm  # noqa: PLC0415
    import torch  # noqa: PLC0415

    torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", cpu_limit())))
    t0 = time.perf_counter()
    model = load_model(weights_path())
    log(
        f"model loaded in {time.perf_counter() - t0:.1f}s, {torch.get_num_threads()} threads"
    )

    labels = is_positive(plan, inputs.context[plan.target])
    if labels.nunique() < 2:
        raise ScoreRefused(
            f"the {len(inputs.context)} labelled rows hold one class only: the model needs both."
        )
    sampler, link = build_relational(sdm, plan, inputs)
    kw = {
        "task_link": link,
        "num_neighbors": list(NUM_NEIGHBORS),
        "task_time_column": CUTOFF,
    }

    with torch.inference_mode():
        t0 = time.perf_counter()
        ctx_frame = task_frame(
            plan, _normalise(plan, plan.entity, inputs.context), started, True
        )
        ctx = sdm.TableTensor.from_pandas(df=ctx_frame, stypes=_stypes(plan, True))
        c, rt = sampler(ctx, **kw).to("cpu")
        model.clear()
        model.fit(
            x=c.drop_columns(LABEL), y=c[LABEL], related_tables=rt, num_estimators=1
        )
        log(f"context {len(ctx_frame)} rows encoded in {time.perf_counter() - t0:.1f}s")

        query = _normalise(plan, plan.entity, inputs.query)
        probs = []
        for i in range(0, len(query), BATCH):
            t0 = time.perf_counter()
            batch = task_frame(plan, query.iloc[i : i + BATCH], started, False)
            qt = sdm.TableTensor.from_pandas(df=batch, stypes=_stypes(plan, False))
            xq, rq = sampler(qt, **kw).to("cpu")
            probs.append(positive_column(model.predict(xq, rq)))
            log(
                f"batch {i // BATCH + 1}/{-(-len(query) // BATCH)}: {len(batch)} rows "
                f"in {time.perf_counter() - t0:.1f}s"
            )
    out = inputs.query[plan.keys].reset_index(drop=True)
    out["score"] = np.concatenate(probs).astype(float)
    return pd.DataFrame(out)


# region: publish ---------------------------------------------------------


def publish(
    scores: Any, run: Any, insert_scores: Any, insert_run: Any, log: Any
) -> None:
    """Write the scores in one insert, then the run row. The run row is the publication."""
    if len(scores):
        insert_scores(scores)
        log(f"scores insert done: {len(scores)} rows")
    insert_run(run)
    log(f"run {run['run_id'].iloc[0]} published")


def run_plan(
    plan: Plan, reader: Reader, insert_scores: Any, insert_run: Any, log: Any
) -> str:
    """One run: read, score, publish. Returns the run id."""
    import pandas as pd  # noqa: PLC0415

    started = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    run_id = f"{started.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    version = model_version()
    inputs = load_inputs(plan, reader)
    log(f"inputs: {len(inputs.query)} query rows, {len(inputs.context)} context rows")
    if inputs.query.empty:
        scored = pd.DataFrame(columns=[*plan.keys, "score"])
    else:
        scored = score(plan, inputs, started, log)
    completed = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    if len(scored):
        scored["run_id"] = run_id
        scored["scored_at"] = completed
        scored["predicted_label"] = (scored["score"] >= 0.5).astype(int)
        scored["model_version"] = version
        scored = scored[
            [
                *plan.keys,
                "run_id",
                "scored_at",
                "score",
                "predicted_label",
                "model_version",
            ]
        ]
    run = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "started_at": started,
                "completed_at": completed,
                "query_rows": len(inputs.query),
                "scored_rows": len(scored),
                "model_version": version,
            }
        ]
    )
    publish(scored, run, insert_scores, insert_run, log)
    return run_id


# region: entry -----------------------------------------------------------


class TrinoSql:
    """The project's Trino, on the delta catalog and the feature store's schema."""

    def __init__(self, project: Any, schema: str) -> None:
        self.conn = project.get_trino_api().connect(catalog="delta", schema=schema)

    def _run(self, sql: str) -> tuple[list[str], list[tuple]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        names = [str(c[0]) for c in cur.description or []]
        return names, cur.fetchall()

    def rows(self, sql: str) -> list[tuple]:
        return self._run(sql)[1]

    def frame(
        self, sql: str, max_rows: int, max_bytes: int, group: str = ""
    ) -> tuple[Any, int]:
        """The result and its bytes, fetched in chunks.

        Refuses, and cancels the query, before keeping a chunk that takes it past
        `max_rows` or `max_bytes`.
        """
        import pandas as pd  # noqa: PLC0415

        cur = self.conn.cursor()
        cur.execute(sql)
        names = [str(c[0]) for c in cur.description or []]
        kept: list = []
        size = 0
        while chunk := cur.fetchmany(FETCH_ROWS):
            size += sum(row_bytes(r) for r in chunk)
            try:
                check_read(group, len(kept) + len(chunk), size, max_rows, max_bytes)
            except ScoreRefused:
                cur.cancel()
                raise
            kept.extend(chunk)
        return pd.DataFrame(kept, columns=names), size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", required=True, help="The system name.")
    parser.add_argument("--config", required=True, help="The plan, as JSON.")
    args = parser.parse_args(argv)
    plan = Plan.from_json(args.config)
    t_start = time.perf_counter()

    def log(msg: str) -> None:
        print(
            f"[score {plan.name} +{time.perf_counter() - t_start:.1f}s] {msg}",
            flush=True,
        )

    import hopsworks  # noqa: PLC0415 - the job image ships it

    project = hopsworks.login()
    fs = project.get_feature_store()
    reader = Reader(TrinoSql(project, fs.name), log)
    marker = owner_marker(plan.name)
    try:
        if args.system != plan.name:
            raise ScoreRefused(
                f"--system {args.system} does not match the plan's system {plan.name}."
            )
        scores_fg = fs.get_or_create_feature_group(
            name=plan.scores_name,
            version=1,
            primary_key=[*plan.keys, "run_id"],
            event_time="scored_at",
            description=f"{marker} Scores of {plan.name}, one insert per run. "
            f"Model {model_version()}.",
        )
        runs_fg = fs.get_or_create_feature_group(
            name=plan.runs_name,
            version=1,
            primary_key=["run_id"],
            event_time="completed_at",
            description=f"{marker} Completed runs of {plan.name}: a run is current "
            "once its row is here.",
        )
        check_inputs_are_not_outputs(plan.name, plan.groups)
        # get_or_create returns a group of that name whoever made it: write only to ours.
        check_output_group(scores_fg, plan.name)
        check_output_group(runs_fg, plan.name)
        run_plan(
            plan,
            reader,
            lambda df: scores_fg.insert(df, wait=True),
            lambda df: runs_fg.insert(df, wait=True),
            log,
        )
    except ScoreRefused as exc:
        log(f"refused: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
