"""``hops score``: rank rows of ingested feature groups with Kumo-Relational.

The verb checks the inputs, deploys the job ``kumo-score-<name>`` on the ``scoring-pipeline``
environment with the script shipped in ``hopsworks.score``, runs it and reads the result.
Scores land in ``<name>_scores``; a run is current once its row is in ``<name>_score_runs``.
"""

from __future__ import annotations

import hashlib
import random
import re
import shlex
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session, versions, waits
from hopsworks.score import kumo_job
from hopsworks.score.kumo_job import Group, Link, Plan, ScoreRefused


ENVIRONMENT = "scoring-pipeline"
JOB_CORES = 4
JOB_MEMORY_MB = 6144
MAX_CONCURRENT = "maxConcurrentExecutions"
POLL_SECONDS = 10.0
POLL_JITTER_SECONDS = 2.0
WAIT_TIMEOUT = 3600
SCRIPT = Path(kumo_job.__file__)
DEFAULT_TOP = 20
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_COMPLEX_TYPES = ("array", "struct", "map", "binary")
_TIME_TYPES = ("timestamp", "date")


@dataclass
class GroupInfo:
    """What the checks need of a feature group."""

    name: str
    version: int
    columns: dict[str, str]  # name -> type
    primary_key: list[str] = field(default_factory=list)
    foreign_key: list[str] = field(default_factory=list)
    cached: bool = True


# region: links -----------------------------------------------------------


def parse_ref(text: str) -> tuple[str, str]:
    """`<group>.<column>` as two names."""
    group, dot, column = text.strip().partition(".")
    if not dot or not _NAME.fullmatch(group) or not _NAME.fullmatch(column):
        raise ScoreRefused(f"'{text}' is not <feature group>.<column>.")
    return group, column


def _side(text: str, side: str) -> tuple[str, list[str]]:
    refs = [parse_ref(part) for part in text.split("+")]
    groups = {g for g, _ in refs}
    if len(groups) != 1:
        raise ScoreRefused(
            f"the {side} side '{text}' of a link names more than one feature group."
        )
    return refs[0][0], [c for _, c in refs]


def parse_link(text: str) -> Link:
    """`deals.contact_id=contacts.id`; a composite key is `a.k1+a.k2=b.k1+b.k2`."""
    left, eq, right = text.partition("=")
    if not eq or "=" in right:
        raise ScoreRefused(f"'{text}' is not <group>.<col>=<group>.<col>.")
    lg, lc = _side(left, "left")
    rg, rc = _side(right, "right")
    if len(lc) != len(rc):
        raise ScoreRefused(f"link '{text}' joins {len(lc)} columns to {len(rc)}.")
    if lg == rg:
        raise ScoreRefused(f"link '{text}' joins feature group {lg} to itself.")
    return Link(lg, lc, rg, rc)


def _name(feature: Any) -> str:
    return getattr(feature, "name", feature)


def links_from_query(query: Any) -> list[Link]:
    """The joins a feature view recorded, as links from the left group to each right group."""
    left = query._left_feature_group.name
    out = []
    for join in query.joins:
        right_query = join.query
        on = [_name(f) for f in join._on or []]
        left_on = [_name(f) for f in join._left_on or []] or on
        right_on = [_name(f) for f in join._right_on or []] or on
        if not left_on:
            raise ScoreRefused(f"a join of {left} in the feature view names no key.")
        out.append(Link(left, left_on, right_query._left_feature_group.name, right_on))
        out.extend(links_from_query(right_query))
    return out


def _pair(link: Link) -> frozenset[str]:
    return frozenset((link.left, link.right))


def _candidates(entity: GroupInfo, other: GroupInfo) -> str:
    """Links a person could mean: a foreign key of one group against the primary key of the other."""
    found = []
    for fk in entity.foreign_key:
        for pk in other.primary_key:
            found.append(f"--link {entity.name}.{fk}={other.name}.{pk}")
    for fk in other.foreign_key:
        for pk in entity.primary_key:
            found.append(f"--link {other.name}.{fk}={entity.name}.{pk}")
    if found:
        return "Candidates from the recorded foreign keys: " + "; ".join(found) + "."
    return (
        f"No foreign key is recorded on {entity.name} or {other.name}: name the columns "
        f"with --link {other.name}.<column>={entity.name}.<column>. Equal column names are never linked."
    )


def _check_group(info: GroupInfo) -> None:
    if not info.cached:
        raise ScoreRefused(
            f"{info.name} is an external feature group: Trino and the Python engine cannot "
            f"read it. Copy it into a feature group first: `hops fg ingest {info.name}_in "
            f"--connector <source> --table <table> --primary-key <cols>`."
        )


def _check_link(link: Link, infos: dict[str, GroupInfo]) -> None:
    for group, cols in ((link.left, link.left_cols), (link.right, link.right_cols)):
        for col in cols:
            if col not in infos[group].columns:
                raise ScoreRefused(
                    f"link {link}: {group} has no column {col}. "
                    f"It has {', '.join(infos[group].columns)}."
                )
    for lc, rc in zip(link.left_cols, link.right_cols, strict=True):
        lt, rt = (
            infos[link.left].columns[lc].lower(),
            infos[link.right].columns[rc].lower(),
        )
        if lt != rt:
            raise ScoreRefused(
                f"link {link}: {link.left}.{lc} is {lt} and {link.right}.{rc} is {rt}. "
                "The types must match."
            )


def _depths(entity: str, links: list[Link]) -> dict[str, int]:
    depth = {entity: 0}
    queue = [entity]
    while queue:
        node = queue.pop(0)
        for link in links:
            for here, there in ((link.left, link.right), (link.right, link.left)):
                if here == node and there not in depth:
                    depth[there] = depth[node] + 1
                    queue.append(there)
    return depth


def _projection(info: GroupInfo) -> list[list[str]]:
    return [
        [n, t]
        for n, t in info.columns.items()
        if not t.lower().startswith(_COMPLEX_TYPES)
    ]


def build_plan(
    name: str,
    entity: str,
    infos: dict[str, GroupInfo],
    links: list[Link],
    target: str,
    positive: str | None = None,
    time: list[str] | None = None,
    cutoff: str | None = None,
    where: str | None = None,
    context_rows: int = kumo_job.CONTEXT_ROWS_MAX,
    query_rows: int = kumo_job.QUERY_ROWS_MAX,
) -> Plan:
    """Validate the inputs and make the plan the job runs.

    `infos` holds the entity group and every group a link, `--time` or `--cutoff` names.
    Raises ScoreRefused with the reason; nothing is read.
    """
    if entity not in infos:
        raise ScoreRefused(f"{entity} is not among the feature groups given.")
    ent = infos[entity]
    for info in infos.values():
        _check_group(info)
    if not ent.primary_key:
        raise ScoreRefused(f"{entity} has no primary key: scores are keyed by it.")
    if target not in ent.columns:
        raise ScoreRefused(
            f"{entity} has no column {target}. It has {', '.join(ent.columns)}."
        )

    seen: dict[frozenset[str], Link] = {}
    for link in links:
        for group in (link.left, link.right):
            if group not in infos:
                raise ScoreRefused(
                    f"{group} (in link {link}) is not a feature group of this project."
                )
        _check_link(link, infos)
        first = seen.setdefault(_pair(link), link)
        if first is not link and str(first) != str(link):
            raise ScoreRefused(
                f"two links join {link.left} and {link.right}: {first} and {link}. "
                "Keep the one that is meant."
            )
    links = list(seen.values())

    time_map: dict[str, str] = {}
    for ref in time or []:
        group, col = parse_ref(ref)
        if group in time_map:
            raise ScoreRefused(
                f"--time names two columns of {group}: {time_map[group]} and {col}."
            )
        time_map[group] = col
    mentioned = {entity, *time_map, *(g for lk in links for g in (lk.left, lk.right))}
    if cutoff:
        cutoff_group, cutoff_col = parse_ref(cutoff)
        if cutoff_group != entity:
            raise ScoreRefused(
                f"--cutoff must be a column of {entity}, not of {cutoff_group}."
            )
        mentioned.add(cutoff_group)
    else:
        cutoff_col = None
    for group, col in {
        **time_map,
        **({entity: cutoff_col} if cutoff_col else {}),
    }.items():
        if group not in infos:
            raise ScoreRefused(
                f"{group} (in --time or --cutoff) is not a feature group of this project."
            )
        if col not in infos[group].columns:
            raise ScoreRefused(
                f"{group} has no column {col}. It has {', '.join(infos[group].columns)}."
            )
        if not infos[group].columns[col].lower().startswith(_TIME_TYPES):
            raise ScoreRefused(
                f"{group}.{col} is {infos[group].columns[col]}, not a timestamp or date."
            )

    depth = _depths(entity, links)
    for group in sorted(mentioned - set(depth)):
        raise ScoreRefused(
            f"{group} is not linked to {entity}, directly or through another group. "
            + _candidates(ent, infos[group])
        )
    for group, hops in depth.items():
        if hops > kumo_job.MAX_DEPTH:
            raise ScoreRefused(
                f"{group} is {hops} hops from {entity}; the most is {kumo_job.MAX_DEPTH}."
            )
    groups = {g: Group(infos[g].version, _projection(infos[g])) for g in depth}
    return Plan(
        name=name,
        entity=entity,
        keys=list(ent.primary_key),
        target=target,
        groups=groups,
        links=links,
        positive=positive,
        cutoff=cutoff_col,
        where=where,
        time=time_map,
        context_rows=context_rows,
        query_rows=query_rows,
    )


def unique_sql(plan: Plan, link: Link) -> str:
    """Rows and distinct keys of the right side of a link."""
    cols = ", ".join(kumo_job.ident(c) for c in link.right_cols)
    tbl = kumo_job.table(plan, link.right)
    return (
        f"SELECT (SELECT count(*) FROM {tbl}), "
        f"(SELECT count(*) FROM (SELECT DISTINCT {cols} FROM {tbl}))"
    )


def check_counts(link: Link, rows: int, distinct: int) -> None:
    """Refuse a link whose right side repeats a key: a row could join to several."""
    if rows != distinct:
        right = "+".join(f"{link.right}.{c}" for c in link.right_cols)
        raise ScoreRefused(
            f"link {link}: {right} is not unique, {rows} rows hold {distinct} distinct keys. "
            f"Link the other way round, or ingest {link.right} without repeated keys."
        )


def check_unique(plan: Plan, sql: Any) -> None:
    """Run the uniqueness count of every link through `sql` and refuse a repeated key."""
    for link in plan.links:
        rows, distinct = sql.rows(unique_sql(plan, link))[0]
        check_counts(link, rows, distinct)


# region: schedule --------------------------------------------------------

_MIN_PERIOD = "one run per hour"


def daily_cron(project_id: Any, name: str) -> str:
    """A daily Quartz cron whose minute and hour follow a stable hash of the project and system."""
    digest = hashlib.sha256(f"{project_id}/{name}".encode()).digest()
    return f"0 {digest[0] % 60} {digest[1] % 24} * * ?"


def resolve_schedule(value: str, project_id: Any, name: str) -> str:
    """The cron to set: the hashed daily one for `auto`, else the user's, kept as given."""
    from hopsworks.cli.commands import job as job_cmd  # noqa: PLC0415 - job imports fg

    if value == "auto":
        return daily_cron(project_id, name)
    cron = job_cmd._expand_cron_alias(value)
    fields = cron.split()
    if len(fields) not in (6, 7):
        raise ScoreRefused(
            f"'{value}' is not a Quartz cron (6 or 7 fields, such as `0 30 2 * * ?`)."
        )
    if not (re.fullmatch(r"\d+", fields[0]) and re.fullmatch(r"\d+", fields[1])):
        raise ScoreRefused(
            f"'{value}' fires more often than hourly. The floor is {_MIN_PERIOD}: "
            "give one second and one minute, such as `0 30 * * * ?`."
        )
    return cron


# region: current run -----------------------------------------------------


def _runs_table(name: str) -> str:
    return kumo_job.ident(f"{name}_score_runs_1")


def current_run_sql(name: str) -> str:
    return (
        "SELECT run_id, started_at, completed_at, query_rows, scored_rows, model_version "
        f"FROM {_runs_table(name)} ORDER BY completed_at DESC, run_id DESC LIMIT 1"
    )


def ranking_sql(name: str, run_id: Any, top: int) -> str:
    return (
        f"SELECT * FROM {kumo_job.ident(f'{name}_scores_1')} "
        f"WHERE run_id = {kumo_job.literal(run_id)} ORDER BY score DESC LIMIT {int(top)}"
    )


def current_run(sql: Any, name: str) -> dict[str, Any] | None:
    """The latest completed run: the newest row of `<name>_score_runs`."""
    rows = sql.rows(current_run_sql(name))
    if not rows:
        return None
    keys = (
        "run_id",
        "started_at",
        "completed_at",
        "query_rows",
        "scored_rows",
        "model_version",
    )
    return dict(zip(keys, rows[0], strict=True))


def current_ranking(
    sql: Any, name: str, top: int = DEFAULT_TOP
) -> tuple[dict | None, Any]:
    """The current run and its top scores. Only the runs table says which run is current."""
    run = current_run(sql, name)
    if run is None or not run["scored_rows"]:
        return run, None
    return run, sql.frame(ranking_sql(name, run["run_id"], top))


# region: command ---------------------------------------------------------


def _job_name(name: str) -> str:
    return f"kumo-score-{name}"


def run_refusal(exc: BaseException, name: str) -> ScoreRefused | None:
    """The refusal of a second run: the server's HTTP 409 on the execution start, with its message."""
    from hopsworks_common.client.exceptions import (  # noqa: PLC0415 - SDK loaded on use
        RestAPIError,
    )

    cause = exc
    while cause is not None and not isinstance(cause, RestAPIError):
        cause = cause.__cause__
    if cause is None or cause.response.status_code != RestAPIError.STATUS_CODE_CONFLICT:
        return None
    try:
        body = cause.response.json()
    except ValueError:
        body = {}
    reason = body.get("usrMsg") or body.get("errorMsg") or "an execution is running"
    job_name = _job_name(name)
    return ScoreRefused(
        f"job {job_name} is already running: {reason} "
        f"One run per system: wait for it to end, or stop it with `hops job stop {job_name}`."
    )


def _fail(exc: ScoreRefused) -> click.ClickException:
    return click.ClickException(str(exc))


def _info(fg: Any) -> GroupInfo:
    from hsfs.feature_group import FeatureGroup  # noqa: PLC0415 - SDK loaded on use

    return GroupInfo(
        name=fg.name,
        version=fg.version,
        columns={f.name: f.type for f in fg.features},
        primary_key=list(fg.primary_key or []),
        foreign_key=list(getattr(fg, "foreign_key", None) or []),
        cached=isinstance(fg, FeatureGroup),
    )


def _sql(ctx: click.Context) -> Any:
    return kumo_job.TrinoSql(
        session.get_project(ctx), session.get_feature_store(ctx).name
    )


@click.command("score")
@click.argument("name")
@click.option(
    "--entity", help="Feature group of the rows to score: one row per entity."
)
@click.option(
    "--target",
    help="Column of the entity group holding the label; null marks a row to score.",
)
@click.option(
    "--positive",
    help="Value of the target that counts as positive; not needed for a boolean or 0/1 target.",
)
@click.option("--fv", "feature_view", help="Feature view whose joins are the links.")
@click.option(
    "--link",
    "links",
    multiple=True,
    help="Link a.col=b.col, repeatable; a composite key is a.k1+a.k2=b.k1+b.k2.",
)
@click.option(
    "--time",
    "times",
    multiple=True,
    help="<group>.<column>: the time of a group's rows, one per group.",
)
@click.option("--cutoff", help="<entity>.<column>: when each label became known.")
@click.option(
    "--where",
    help="SQL filter on the entity group that picks the rows to score instead of a null target.",
)
@click.option(
    "--context-rows",
    type=int,
    default=kumo_job.CONTEXT_ROWS_MAX,
    show_default=True,
    help="Labelled rows the model reads as examples.",
)
@click.option(
    "--query-rows",
    type=int,
    default=kumo_job.QUERY_ROWS_MAX,
    show_default=True,
    help="Rows scored per run.",
)
@click.option(
    "--schedule",
    "schedule",
    is_flag=False,
    flag_value="auto",
    help="Run on a cron: bare, daily at a time hashed from the project and name; or a Quartz cron, at most hourly.",
)
@click.option("--show", is_flag=True, help="Print the current ranking.")
@click.option(
    "--top", type=int, default=DEFAULT_TOP, show_default=True, help="Rows to print."
)
@click.option(
    "--delete",
    "delete",
    is_flag=True,
    help="Delete the job, its schedule and both feature groups.",
)
@click.option("--yes", is_flag=True, help="Skip the confirmation of --delete.")
@click.option(
    "--wait/--no-wait",
    default=False,
    help="For humans: poll the execution every 10 s (plus up to 2 s of jitter) until it ends, then print the current run.",
)
@click.option(
    "--timeout",
    type=int,
    default=3600,
    show_default=True,
    help="Seconds --wait polls before it gives up.",
)
@click.option(
    "--start-timeout",
    type=int,
    default=600,
    show_default=True,
    help="Seconds --wait allows for the execution to reach RUNNING.",
)
@click.pass_context
def score_cmd(
    ctx: click.Context,
    name: str,
    entity: str | None,
    target: str | None,
    positive: str | None,
    feature_view: str | None,
    links: tuple[str, ...],
    times: tuple[str, ...],
    cutoff: str | None,
    where: str | None,
    context_rows: int,
    query_rows: int,
    schedule: str | None,
    show: bool,
    top: int,
    delete: bool,
    yes: bool,
    wait: bool,
    timeout: int,
    start_timeout: int,
) -> None:
    """Rank rows by their chance of a label.

    `hops score deals --entity deals --target won --link deals.contact_id=contacts.id`
    ranks the open deals (target null) against the closed ones with Kumo-Relational,
    on CPU, as the job kumo-score-deals. Scores go to deals_scores; `--show` prints the
    current run. `hops score deals` again re-runs the job. The verb starts one execution
    and returns; `--wait` polls until it ends.

    \f
    Args:
        ctx: Click context.
        name: System name; the job is `kumo-score-<name>`, the groups `<name>_scores`, `<name>_score_runs`.
        entity: Feature group of the rows to score.
        target: Label column of the entity group.
        positive: Value of the target that is positive.
        feature_view: Feature view whose joins are the links.
        links: Explicit links.
        times: `<group>.<column>` time columns.
        cutoff: `<entity>.<column>` the time each label became known.
        where: SQL filter choosing the rows to score.
        context_rows: Labelled rows used as examples.
        query_rows: Rows scored per run.
        schedule: Cron, or bare for the hashed daily one.
        show: Print the current ranking and stop.
        top: Rows `--show` prints.
        delete: Delete the system.
        yes: Skip the confirmation.
        wait: Poll the execution until it ends, then print the current run.
        timeout: Seconds `--wait` polls.
        start_timeout: Seconds `--wait` allows for RUNNING.
    """
    try:
        if delete:
            _delete(ctx, name, yes)
        elif show:
            _show(ctx, name, top)
        else:
            _score(
                ctx,
                name,
                entity,
                target,
                positive,
                feature_view,
                links,
                times,
                cutoff,
                where,
                context_rows,
                query_rows,
                schedule,
                top,
                start_timeout,
                wait,
                timeout,
            )
    except ScoreRefused as exc:
        raise _fail(exc) from exc


def _show(ctx: click.Context, name: str, top: int) -> None:
    run, frame = current_ranking(_sql(ctx), name, top)
    if run is None:
        raise click.ClickException(
            f"{name} has no completed run: run `hops score {name}`."
        )
    if output.JSON_MODE:
        rows = [] if frame is None else frame.to_dict(orient="records")
        output.print_json({"run": run, "scores": rows})
        return
    if frame is None:
        output.info(
            "The current run %s (completed %s) scored none.",
            run["run_id"],
            run["completed_at"],
        )
        return
    output.info(
        "Run %s in %s_scores, started %s, completed %s: %s of %s query rows scored, model %s",
        run["run_id"],
        name,
        run["started_at"],
        run["completed_at"],
        run["scored_rows"],
        run["query_rows"],
        run["model_version"],
    )
    output.print_table(
        [c.upper() for c in frame.columns],
        [[str(v) for v in r] for r in frame.itertuples(index=False)],
    )


def _delete(ctx: click.Context, name: str, yes: bool) -> None:
    project = session.get_project(ctx)
    fs = session.get_feature_store(ctx)
    found: list[tuple[str, Any]] = []
    try:
        job = project.get_job_api().get_job(_job_name(name))
    except Exception:  # noqa: BLE001 - absent
        job = None
    if job is not None:
        found.append((f"job {_job_name(name)} and its schedule", job))
    for group in (f"{name}_scores", f"{name}_score_runs"):
        fg = versions.feature_group(fs, group, 1)
        if fg is not None:
            found.append((f"feature group {group} v1", fg))
    if not found:
        raise click.ClickException(f"Nothing of {name} to delete.")
    output.info(
        "Deleting: %s. The input feature groups stay.",
        "; ".join(label for label, _ in found),
    )
    if not yes:
        output.confirm(f"Delete the scoring system '{name}'?")
    for label, obj in found:
        try:
            obj.delete()
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Could not delete {label}: {exc}") from exc
        output.success("✓ Deleted %s", label)


def _gather(
    ctx: click.Context,
    entity: str,
    feature_view: str | None,
    links_in: tuple[str, ...],
    times: tuple[str, ...],
    cutoff: str | None,
) -> tuple[list[Link], dict[str, GroupInfo]]:
    from hopsworks.cli.commands import fg as fg_cmd  # noqa: PLC0415 - fg imports job

    links = [parse_link(text) for text in links_in]
    if feature_view:
        fv = versions.feature_view(session.get_feature_store(ctx), feature_view, None)
        if fv is None:
            raise ScoreRefused(f"Feature view '{feature_view}' not found.")
        links = links_from_query(fv.query) + links
    names = {entity, *(g for lk in links for g in (lk.left, lk.right))}
    names |= {parse_ref(t)[0] for t in times}
    if cutoff:
        names.add(parse_ref(cutoff)[0])
    return links, {n: _info(fg_cmd._get_fg(ctx, n, None)) for n in sorted(names)}


def _score(
    ctx,
    name,
    entity,
    target,
    positive,
    feature_view,
    links_in,
    times,
    cutoff,
    where,
    context_rows,
    query_rows,
    schedule,
    top,
    start_timeout,
    wait=False,
    timeout=WAIT_TIMEOUT,
) -> None:
    project = session.get_project(ctx)
    api = project.get_job_api()
    job_name = _job_name(name)
    try:
        job = api.get_job(job_name)
    except Exception as exc:  # noqa: BLE001
        if not versions.not_found(exc):
            raise click.ClickException(
                f"Could not check job '{job_name}': {exc}"
            ) from exc
        job = None

    cron = resolve_schedule(schedule, project.id, name) if schedule else None
    if entity is None:
        if job is None:
            raise click.UsageError(f"No system {name} yet: pass --entity and --target.")
        _run_and_report(ctx, job, name, top, start_timeout, cron, True, wait, timeout)
        return
    if not target:
        raise click.UsageError("--target is required with --entity.")
    if project.get_environment_api().get_environment(ENVIRONMENT) is None:
        raise ScoreRefused(
            f"This project has no {ENVIRONMENT} environment, which carries the model. "
            "The platform installs it; ask the administrator."
        )

    links, infos = _gather(ctx, entity, feature_view, links_in, times, cutoff)
    plan = build_plan(
        name,
        entity,
        infos,
        links,
        target,
        positive,
        list(times),
        cutoff,
        where,
        context_rows,
        query_rows,
    )
    sql = _sql(ctx)
    reader = kumo_job.Reader(sql, lambda msg: output.info("%s", msg))
    kumo_job.admit(plan, reader)
    check_unique(plan, sql)
    if not plan.time:
        output.warn(
            "No --time given: the scores may use rows from after each label's time."
        )
    elif not plan.cutoff:
        output.warn(
            "No --cutoff given: the scores may use rows from after each label's time."
        )

    dataset = project.get_dataset_api()
    dest = f"Resources/jobs/{job_name}"
    from hopsworks.cli.commands.files import makedirs  # noqa: PLC0415

    try:
        makedirs(dataset, dest)
        uploaded = dataset.upload(
            local_path=str(SCRIPT), upload_path=dest, overwrite=True
        )
        config = api.get_configuration("PYTHON")
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not prepare the job: {exc}") from exc
    config["appPath"] = uploaded or f"{dest}/{SCRIPT.name}"
    config["environmentName"] = ENVIRONMENT
    config[MAX_CONCURRENT] = 1
    config["defaultArgs"] = f"--config {shlex.quote(plan.to_json())}"
    config["resourceConfig"] = {
        **config.get("resourceConfig", {}),
        "cores": JOB_CORES,
        "memory": JOB_MEMORY_MB,
    }
    try:
        job = api.create_job(name=job_name, config=config)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not deploy job '{job_name}': {exc}") from exc
    output.success("✓ Deployed job %s on %s", job_name, ENVIRONMENT)
    _run_and_report(
        ctx, job, name, top, start_timeout, cron, wait=wait, timeout=timeout
    )


def _run_and_report(
    ctx,
    job,
    name,
    top,
    start_timeout,
    cron,
    update=False,
    wait=False,
    timeout=WAIT_TIMEOUT,
) -> None:
    from hopsworks.cli.commands import job as job_cmd  # noqa: PLC0415 - job imports fg

    job_name = _job_name(name)
    if update and job.config.get(MAX_CONCURRENT) != 1:
        job.config[MAX_CONCURRENT] = 1
        try:
            job = job.save()
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not update job '{job_name}': {exc}"
            ) from exc
    if cron:
        try:
            job.schedule(cron_expression=cron, max_active_runs=1)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not schedule job '{job_name}': {exc}"
            ) from exc
        output.success("✓ Scheduled %s (%s)", job_name, cron)
    try:
        execution = job_cmd._run(job, job_name, False, "run")
    except click.ClickException as exc:
        refusal = run_refusal(exc, name)
        if refusal is None:
            raise
        raise _fail(refusal) from exc
    if execution is None:
        return
    if wait:
        _wait_and_show(ctx, job, name, execution, top, start_timeout, timeout)
        return
    output.info("Read the scores once the run has ended: hops score %s --show", name)


class _JitteredWatch(waits.Watch):
    """A Watch that sleeps POLL_SECONDS plus random jitter, so waiting clients do not poll in step."""

    def sleep(self) -> None:
        time.sleep(poll_interval())


def poll_interval() -> float:
    return POLL_SECONDS + random.uniform(0, POLL_JITTER_SECONDS)  # noqa: S311 - jitter, not security


def _wait_and_show(ctx, job, name, execution, top, start_timeout, timeout) -> None:
    from hopsworks.cli.commands import job as job_cmd  # noqa: PLC0415 - job imports fg

    job_name = _job_name(name)
    watch = _JitteredWatch(
        f"{job_name}: execution #{execution.id}", start_timeout, POLL_SECONDS
    )
    final = job_cmd._follow(job, job_name, execution.id, watch, timeout)
    if final.success is False:
        job_cmd._failed(job_name, final, "", watch.last_reason)
    if final.success is None:
        raise click.ClickException(
            f"{job_name}: execution #{final.id} still {final.state} after {timeout} s. "
            f"It keeps running: read it with `hops score {name} --show` when it ends."
        )
    _show(ctx, name, top)
