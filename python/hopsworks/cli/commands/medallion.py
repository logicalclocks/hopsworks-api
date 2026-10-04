"""``hops medallion`` — medallion layers built by the Factory, starting with silver.

``hops medallion silver --answers FILE`` records a silver layer's request, as
the Hopsworks UI's New Medallion Layer page collects it, in
``<slug>/system.yaml``, registers the layer with the project's ML systems
registry so the Factory lists it, and starts Claude Code with
``/hops-silver <slug>`` to build it.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "skills"
    / "data"
    / "hops-medallion"
    / "references"
    / "silver_template"
)
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
TASKS = (
    "deduplicate",
    "cast_types",
    "standardize",
    "handle_nulls",
    "validate",
    "mask_pii",
    "conform_entities",
    "surrogate_keys",
    "referential_checks",
)
ENGINES = ("dbt_trino", "pyspark")
LIFECYCLES = ("dev", "staging", "prod")
# Quartz cron, as `hops job schedule` takes it; /hops-silver may refine the time.
CADENCES = {
    "hourly": "0 0 * * * ?",
    "daily": "0 0 1 * * ?",
    "weekly": "0 0 1 ? * MON",
}
HISTORY = ("latest", "full")
DELETES = ("ignore", "propagate")
SCHEMA_CHANGES = ("fail", "evolve")
LOOKBACKS = ("0", "1d", "7d")
# Stale after a missed run plus slack; replayed windows after an outage are
# capped at about two weeks of runs.
FRESHNESS_HOURS = {"hourly": 2, "daily": 26, "weekly": 170}
MAX_CATCHUP_RUNS = {"hourly": 48, "daily": 14, "weekly": 4}
ANSWER_KEYS = {
    "slug",
    "name",
    "description",
    "sources",
    "tasks",
    "extra_tasks",
    "engine",
    "cadence",
    "lifecycle",
    "history",
    "deletes",
    "schema_changes",
    "lookback",
    "max_reject_pct",
    "alert_on_failure",
    "freshness_hours",
}


def _problems(answers: dict) -> list[str]:
    problems = [f"unknown answer {k!r}" for k in sorted(set(answers) - ANSWER_KEYS)]
    if not SLUG.match(str(answers.get("slug", ""))):
        problems.append(
            "slug must be lowercase letters, digits and hyphens, starting with a letter"
        )
    sources = answers.get("sources") or []
    if not sources:
        problems.append(
            "a silver layer needs at least one bronze feature group in sources"
        )
    for source in sources:
        if not isinstance(source, dict) or not source.get("name"):
            problems.append(f"source {source!r} needs a name")
    problems += [
        f"unknown task {t!r}" for t in answers.get("tasks") or [] if t not in TASKS
    ]
    if answers.get("engine", "dbt_trino") not in ENGINES:
        problems.append(f"engine must be one of {', '.join(ENGINES)}")
    if answers.get("cadence", "daily") not in CADENCES:
        problems.append(f"cadence must be one of {', '.join(CADENCES)}")
    if answers.get("lifecycle", "dev") not in LIFECYCLES:
        problems.append(f"lifecycle must be one of {', '.join(LIFECYCLES)}")
    for key, allowed in (
        ("history", HISTORY),
        ("deletes", DELETES),
        ("schema_changes", SCHEMA_CHANGES),
        ("lookback", LOOKBACKS),
    ):
        if key in answers and answers[key] not in allowed:
            problems.append(f"{key} must be one of {', '.join(allowed)}")
    pct = answers.get("max_reject_pct", 5)
    if not isinstance(pct, (int, float)) or not 0 <= pct <= 100:
        problems.append("max_reject_pct must be a number from 0 to 100")
    hours = answers.get("freshness_hours", 1)
    if not isinstance(hours, (int, float)) or hours <= 0:
        problems.append("freshness_hours must be a positive number")
    return problems


def _create(cwd: Path, answers: dict) -> Path:
    """Copy the template into ``cwd/<slug>`` and write the answers into its system.yaml."""
    import yaml

    target = cwd / answers["slug"]
    if (target / "system.yaml").exists():
        raise click.ClickException(f"{target} already holds a system.yaml")
    target.mkdir(parents=True, exist_ok=True)
    for item in TEMPLATE.iterdir():
        name = ".gitignore" if item.name == "gitignore" else item.name
        shutil.copy(item, target / name)
    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))
    cadence = answers.get("cadence", "daily")
    doc["layer"].update(
        name=answers.get("name") or answers["slug"],
        slug=answers["slug"],
        description=answers.get("description", ""),
        lifecycle=answers.get("lifecycle", "dev"),
    )
    doc["sources"] = [
        {
            "name": s["name"],
            "version": s.get("version", 1),
            "arrival_column": s.get("arrival_column"),
        }
        for s in answers["sources"]
    ]
    doc["tasks"] = list(answers.get("tasks") or [])
    doc["extra_tasks"] = answers.get("extra_tasks", "")
    doc["engine"] = answers.get("engine", "dbt_trino")
    doc["schedule"] = {
        "cadence": cadence,
        "cron": CADENCES[cadence],
        "catchup": True,
        "max_catchup_runs": MAX_CATCHUP_RUNS[cadence],
    }
    doc["history"] = answers.get("history", "latest")
    doc["deletes"] = answers.get("deletes", "ignore")
    doc["schema_changes"] = answers.get("schema_changes", "fail")
    doc["late_data"] = {"lookback": answers.get("lookback", "0")}
    doc["quality"] = {
        "max_reject_pct": answers.get("max_reject_pct", 5),
        "alert_on_failure": bool(answers.get("alert_on_failure", True)),
    }
    doc["freshness"] = {
        "max_age_hours": answers.get("freshness_hours", FRESHNESS_HOURS[cadence])
    }
    (target / "system.yaml").write_text(
        yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    # Its own git work tree, so every change the build makes is a commit.
    if shutil.which("git"):
        subprocess.run(["git", "init", "-q", str(target)], check=False)
    return target


def _launch(target: Path, launch: bool) -> None:
    slug = target.name
    command = ["claude", f"/hops-silver {slug}"]
    if not launch or not shutil.which("claude"):
        click.echo(f'\nBuild it with:  cd {target} && claude "/hops-silver {slug}"')
        return
    if os.environ.get("TMUX") and shutil.which("tmux"):
        subprocess.run(
            ["tmux", "new-window", "-n", slug, "-c", str(target), shlex.join(command)],
            check=True,
        )
        click.echo(
            f"\nBuilding in the tmux window '{slug}'; the Hopsworks UI shows its progress."
        )
        return
    # In the layer directory, so Claude Code reads its AGENTS.md.
    os.chdir(target)
    os.execvp("claude", command)


@click.group("medallion")
def medallion_group() -> None:
    """Medallion layers (bronze, silver, gold) built by the Factory."""


@medallion_group.command("silver")
@click.option(
    "--answers",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="A JSON file of the layer's answers, as the Hopsworks UI writes it.",
)
@click.option(
    "--no-launch", is_flag=True, help="Record the layer but do not start Claude Code."
)
@click.pass_context
def medallion_silver(ctx: click.Context, answers: Path, no_launch: bool) -> None:
    """Record a silver layer in ./<slug>/system.yaml, register it, and build it with Claude Code.

    The answers name the bronze feature groups (sources), the silver tasks,
    any extra tasks in the user's words, the engine (dbt_trino or pyspark),
    the cadence of the incremental job and the lifecycle of the tables.

    Args:
        ctx: Click context.
        answers: The layer's answers, as the Hopsworks UI collects them.
        no_launch: Record the layer only.
    """
    from hopsworks.cli.commands import mlsystem

    data: dict[str, Any] = json.loads(answers.read_text(encoding="utf-8"))
    problems = _problems(data)
    if problems:
        raise click.ClickException("invalid answers:\n  " + "\n  ".join(problems))
    target = _create(Path.cwd(), data)
    output.success(f"Silver layer recorded in {target / 'system.yaml'}")
    try:
        mlsystem.register(ctx, target, data.get("name") or data["slug"])
    except Exception as exc:  # noqa: BLE001 - the layer is recorded either way
        output.warn(
            f"Not registered in the project's Factory ({exc}); run `hops mlsystem register {target}`."
        )
    _launch(target, not no_launch)


def _entry(ctx: click.Context, name_or_id: str) -> dict:
    from hopsworks_common.core import ml_system_api

    session.get_project(ctx)
    for entry in ml_system_api._list():
        if str(entry.get("id")) == name_or_id or entry.get("name") == name_or_id:
            return entry
    raise click.ClickException(f"No layer {name_or_id!r} in the project's Factory.")


def _delete_assets(ctx: click.Context, doc: dict) -> None:
    """Delete the layer's job, then its silver and rejects feature groups; what is gone is skipped."""
    project = session.get_project(ctx)
    outputs = doc.get("outputs") or {}
    job_name = (outputs.get("job") or {}).get("name")
    if job_name:
        job = project.get_job_api().get_job(job_name)
        if job is None:
            output.info(f"job {job_name}: gone")
        else:
            job.delete()
            output.success(f"✓ Deleted job {job_name}")
    fs = project.get_feature_store()
    for table in [*(outputs.get("tables") or []), *(outputs.get("rejects") or [])]:
        name, version = table.get("name"), table.get("version", 1)
        if not name:
            continue
        # None means it does not exist; any other failure raises and stops the
        # delete, which leaves the layer listed to be deleted again.
        fg = fs.get_feature_group(name, version=version)
        if fg is None:
            output.info(f"feature group {name} v{version}: gone")
            continue
        fg.delete()
        output.success(f"✓ Deleted feature group {name} v{version}")


@medallion_group.command("delete")
@click.argument("name_or_id")
@click.option(
    "--assets",
    is_flag=True,
    help="Also delete the layer's job, its silver feature groups and its directory.",
)
@click.option("--yes", is_flag=True, help="Do not ask for confirmation.")
@click.pass_context
def medallion_delete(
    ctx: click.Context, name_or_id: str, assets: bool, yes: bool
) -> None:
    """Remove a layer from the Factory, and with --assets what it created.

    Bronze feature groups are never deleted. The registry entry goes last, so
    a delete that stops part way leaves the layer listed to be deleted again.

    Args:
        ctx: Click context.
        name_or_id: The layer's name or registry id.
        assets: Also delete its job, silver feature groups and directory.
        yes: Skip the confirmation.
    """
    import yaml
    from hopsworks.cli.commands import mlsystem
    from hopsworks_common.core import ml_system_api

    entry = _entry(ctx, name_or_id)
    if not yes:
        click.confirm(
            f"Delete layer {entry.get('name')}"
            + (" with its job, silver tables and directory" if assets else "")
            + "?",
            abort=True,
        )
    if assets:
        directory = mlsystem._local_dir(entry)
        spec = directory / "system.yaml" if directory else None
        if spec is None or not spec.exists():
            raise click.ClickException(
                f"Cannot read the layer's system.yaml at {spec}; delete without --assets."
            )
        _delete_assets(ctx, yaml.safe_load(spec.read_text(encoding="utf-8")) or {})
        shutil.rmtree(directory, ignore_errors=True)
        output.success(f"✓ Deleted {directory}")
    ml_system_api._remove(entry["id"])
    output.success(f"✓ Removed {entry.get('name')} from the Factory")


def _layer(ctx: click.Context, name_or_id: str) -> tuple[dict, Path, dict]:
    """The layer's registry entry, its directory under the terminal's mount, and its system.yaml."""
    import yaml
    from hopsworks.cli.commands import mlsystem

    entry = _entry(ctx, name_or_id)
    directory = mlsystem._local_dir(entry)
    spec = directory / "system.yaml" if directory else None
    if spec is None or not spec.is_file():
        raise click.ClickException(
            f"Cannot read the system.yaml of {entry.get('name')}; run this in a Hopsworks terminal."
        )
    return entry, directory, yaml.safe_load(spec.read_text(encoding="utf-8")) or {}


@medallion_group.command("status")
@click.argument("name_or_id")
@click.option(
    "--hours",
    type=click.IntRange(min=1),
    default=24,
    show_default=True,
    help="How far back to read the job runs.",
)
@click.option("--no-summary", is_flag=True, help="Skip the summary Claude writes.")
@click.pass_context
def medallion_status(
    ctx: click.Context, name_or_id: str, hours: int, no_summary: bool
) -> None:
    """Report the health of a layer as an HTML page, status/report.html in its directory.

    For the layer's job, its runs in the last HOURS with the log tail of each
    failure; for each silver and rejects table, its rows, when it was last
    written against the freshness target, its share of rejected rows against
    the quality gate, and its file layout from the table's files, as
    hops-table-maintenance reads them. The Factory's Status button runs this.

    Args:
        ctx: Click context.
        name_or_id: The layer's name or registry id.
        hours: How far back to read job runs.
        no_summary: Skip the Claude summary.
    """
    from hopsworks.cli import health, silver_status

    project = session.get_project(ctx)
    _, directory, doc = _layer(ctx, name_or_id)
    facts = silver_status.collect(project, doc, directory.name, hours)
    if output.JSON_MODE:
        output.print_json(facts)
        return
    summary = None if no_summary else health.summarize(facts)
    target = directory / "status" / "report.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(health.render(facts, summary), encoding="utf-8")
    c = facts["counts"]
    output.success(
        f"{facts['overall']}: {c['failed_runs']} of {c['runs']} job runs failed in {hours} h, "
        f"{c['table_problems']} of {c['tables']} tables with problems; report in {target}"
    )


@medallion_group.command("backfill")
@click.argument("name_or_id")
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Block until the backfill execution ends.",
)
@click.pass_context
def medallion_backfill(ctx: click.Context, name_or_id: str, wait: bool) -> None:
    """Reprocess every bronze row into the layer's silver tables.

    Runs the layer's job once over a window from the epoch to now, so it reads
    the whole history of every bronze table; the job's upsert on the primary
    key and event time makes rows already in silver unchanged. A plain run of
    a scheduled job would get the last cron interval instead, which is why
    this is a backfill.

    Args:
        ctx: Click context.
        name_or_id: The layer's name or registry id.
        wait: Block until the execution ends.
    """
    from datetime import datetime, timezone

    project = session.get_project(ctx)
    entry, _directory, doc = _layer(ctx, name_or_id)
    job_name = ((doc.get("outputs") or {}).get("job") or {}).get("name")
    job = project.get_job_api().get_job(job_name) if job_name else None
    if job is None:
        raise click.ClickException(
            f"{entry.get('name')} has no job yet; build it with /hops-silver first."
        )
    start = datetime(1970, 1, 1, tzinfo=timezone.utc)
    end = datetime.now(timezone.utc).replace(microsecond=0)
    execution = job.run(await_termination=wait, start_time=start, end_time=end)
    state = getattr(execution, "final_status", None) or getattr(execution, "state", "?")
    output.success(
        f"Backfill of {entry.get('name')}: job {job_name}, execution #{getattr(execution, 'id', '?')}, "
        f"window {start.date()} to {end.isoformat()} ({state})"
    )
    if wait and state in ("FAILED", "KILLED"):
        raise click.ClickException(
            f"the backfill failed; read its log with hops job logs {job_name} --stdout --tail 200"
        )
