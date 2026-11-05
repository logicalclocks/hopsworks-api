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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MODEL_REPO = "nvidia/Kumo-Relational"
DEFAULT_REVISION = "v1.0.1"
NUM_NEIGHBORS = (16, 16)
BATCH = 1_000
MAX_DEPTH = 2
IN_CHUNK = 2_000
SEED = "kumo-score-1"

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


def owner_marker(name: str) -> str:
    """The token in the description of every feature group `hops score <name>` makes."""
    return f"hops-score:{name}"


def output_groups(name: str) -> tuple[str, str]:
    return f"{name}_scores", f"{name}_score_runs"


def group_marked(description: str | None, name: str) -> bool:
    return owner_marker(name) in (description or "").split()


def job_marked(config: dict, name: str, script: str) -> bool:
    """The job runs `script` from the system's folder with `--system <name>` in its arguments."""
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
    return ident(f"{group}_{plan.groups[group].version}")


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
    """Most recent by the cutoff column, else a seeded pseudo-random order."""
    if plan.cutoff:
        return f"{ident(plan.cutoff)} DESC"
    parts = ", '|', ".join(f"cast({ident(k)} as varchar)" for k in plan.keys)
    return f"md5(to_utf8(concat('{SEED}', '|', {parts})))"


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


def related_sql(plan: Plan, group: str, cols: list[str], keys: list[tuple]) -> str:
    """The rows of `group` whose `cols` hold one of `keys`."""
    if len(cols) == 1:
        match = f"{ident(cols[0])} IN ({', '.join(literal(k[0]) for k in keys)})"
    else:
        names = ", ".join(ident(c) for c in cols)
        rows = ", ".join("(" + ", ".join(literal(v) for v in k) + ")" for k in keys)
        match = f"({names}) IN ({rows})"
    return f"SELECT {_columns(plan, group)} FROM {table(plan, group)} WHERE {match}"


Step = tuple[
    str, list[str], str, list[str]
]  # source group, its columns, target group, its columns


def steps(plan: Plan) -> list[Step]:
    """The order the related groups are reached from the entity through the links, 2 hops at most."""
    depth = {plan.entity: 0}
    pending = list(plan.links)
    out: list[Step] = []
    progressed = True
    while pending and progressed:
        progressed = False
        for link in list(pending):
            if link.left in depth and link.right not in depth:
                src, src_cols, dst, dst_cols = (
                    link.left,
                    link.left_cols,
                    link.right,
                    link.right_cols,
                )
            elif link.right in depth and link.left not in depth:
                src, src_cols, dst, dst_cols = (
                    link.right,
                    link.right_cols,
                    link.left,
                    link.left_cols,
                )
            else:
                if link.left in depth and link.right in depth:
                    pending.remove(link)
                continue
            if depth[src] + 1 > MAX_DEPTH:
                raise ScoreRefused(
                    f"{dst} is more than {MAX_DEPTH} hops from {plan.entity}."
                )
            depth[dst] = depth[src] + 1
            out.append((src, src_cols, dst, dst_cols))
            pending.remove(link)
            progressed = True
    unreached = [g for g in plan.related if g not in depth]
    if unreached:
        raise ScoreRefused(
            f"{', '.join(unreached)} not reachable from {plan.entity} through the links."
        )
    return out


def reach_sql(plan: Plan) -> str | None:
    """Rows and bytes of the related rows reachable from the selected context and query rows.

    The same IN-filter the reads use, as a chain of semi-joins from the selected entity keys,
    one row per related group: (group, rows, bytes). None when the plan has no related group.
    """
    chain = steps(plan)
    if not chain:
        return None
    keys = list(
        dict.fromkeys(
            c for src, cols, _, _ in chain if src == plan.entity for c in cols
        )
    )
    cols = ", ".join(ident(c) for c in keys)
    ctes = [f"sel AS (({query_sql(plan, cols)}) UNION ({context_sql(plan, cols)}))"]
    alias = {plan.entity: "sel"}
    counts = []
    for i, (src, src_cols, dst, dst_cols) in enumerate(chain):
        name = f"r{i}"
        match = " AND ".join(
            f"s.{ident(sc)} = t.{ident(dc)}"
            for sc, dc in zip(src_cols, dst_cols, strict=True)
        )
        ctes.append(
            f"{name} AS (SELECT {_columns(plan, dst)} FROM {table(plan, dst)} t "
            f"WHERE EXISTS (SELECT 1 FROM {alias[src]} s WHERE {match}))"
        )
        alias[dst] = name
        counts.append(
            f"SELECT {literal(dst)}, count(*), coalesce(sum({_sizes(plan, dst)}), 0) FROM {name}"
        )
    return f"WITH {', '.join(ctes)} " + " UNION ALL ".join(counts)


# region: reads -----------------------------------------------------------


class Reader:
    """Runs SQL through `sql` (an object with `.frame(sql)` and `.rows(sql)`) and logs each call."""

    def __init__(self, sql: Any, log: Any = None) -> None:
        self.sql = sql
        self.log = log or (lambda msg: None)
        self.calls: list[tuple[str, str, int]] = []  # (kind, group, rows)

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

    def read(self, group: str, sql: str) -> Any:
        started = time.perf_counter()
        frame = self.sql.frame(sql)
        self.calls.append(("read", group, len(frame)))
        self.log(
            f"read {group}: {len(frame)} rows in {time.perf_counter() - started:.1f}s"
        )
        return frame

    @property
    def reads(self) -> list[tuple[str, str, int]]:
        return [c for c in self.calls if c[0] == "read"]


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


def admit(plan: Plan, reader: Reader) -> dict[str, dict[str, int]]:
    """Count the entity group and the related rows reachable from the selection, in Trino.

    Refuses over a cap before any frame is read.
    """
    check_caps(plan)
    rows, size = reader.admission(plan.entity, admission_sql(plan, plan.entity))
    sql = reach_sql(plan)
    reach = reader.reach(sql) if sql else {}
    check_admission(plan, (rows, size), reach)
    stats = {plan.entity: {"rows": rows, "bytes": size}}
    stats.update({g: {"rows": r, "bytes": b} for g, (r, b) in reach.items()})
    reader.log(
        "admission ok: "
        + ", ".join(
            f"{g} {s['rows']} rows {s['bytes']} bytes" for g, s in stats.items()
        )
    )
    return stats


def _key_tuples(frame: Any, cols: list[str]) -> list[tuple]:
    keys = frame[cols].dropna().drop_duplicates()
    return [tuple(row) for row in keys.itertuples(index=False, name=None)]


def read_related(plan: Plan, reader: Reader, entity_rows: Any) -> dict[str, Any]:
    """Related rows reachable from the entity rows through the links, in-filtered, 2 hops."""
    import pandas as pd  # noqa: PLC0415

    frames = {plan.entity: entity_rows}
    total = 0
    for src, src_cols, dst, dst_cols in steps(plan):
        keys = _key_tuples(frames[src], src_cols)
        parts = [
            reader.read(dst, related_sql(plan, dst, dst_cols, keys[i : i + IN_CHUNK]))
            for i in range(0, len(keys), IN_CHUNK)
        ]
        frame = (
            pd.concat(parts, ignore_index=True).drop_duplicates()
            if parts
            else reader.read(dst, _empty_sql(plan, dst))
        )
        total += len(frame)
        if total > RELATED_ROWS_MAX:
            raise ScoreRefused(
                f"related rows reached {total}, over the cap of {RELATED_ROWS_MAX}."
            )
        frames[dst] = frame
    return {g: f for g, f in frames.items() if g != plan.entity}


def _empty_sql(plan: Plan, group: str) -> str:
    return f"SELECT {_columns(plan, group)} FROM {table(plan, group)} WHERE false"


@dataclass
class Inputs:
    query: Any
    context: Any
    related: dict[str, Any]


def load_inputs(plan: Plan, reader: Reader) -> Inputs:
    """Admission, then the bounded reads. A query with no rows reads nothing else."""
    import pandas as pd  # noqa: PLC0415

    admit(plan, reader)
    query = reader.read(plan.entity, query_sql(plan))
    if query.empty:
        return Inputs(query, query.iloc[0:0], {})
    context = reader.read(plan.entity, context_sql(plan))
    entity_rows = pd.concat([context, query], ignore_index=True).drop_duplicates(
        subset=plan.keys
    )
    return Inputs(query, context, read_related(plan, reader, entity_rows))


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

    def frame(self, sql: str) -> Any:
        import pandas as pd  # noqa: PLC0415

        names, rows = self._run(sql)
        return pd.DataFrame(rows, columns=names)


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
