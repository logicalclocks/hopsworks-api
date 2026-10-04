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
    doc["schedule"] = {"cadence": cadence, "cron": CADENCES[cadence]}
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
        try:
            fs.get_feature_group(name, version=version).delete()
            output.success(f"✓ Deleted feature group {name} v{version}")
        except Exception as exc:  # noqa: BLE001 - a missing table is not an error here
            output.info(f"feature group {name} v{version}: {exc}")


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
