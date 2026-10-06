"""Silver and gold medallion layers, built by the ``medallion-silver`` and ``medallion-gold`` factories.

``hops factory run medallion-silver|medallion-gold`` records a layer's request,
as the factory's form collects it, in ``<slug>/system.yaml``, registers the
layer so the Factory lists it, and starts Claude Code with
``/hops-silver <slug>`` or ``/hops-gold <slug>`` to build it.
A gold layer is built as data marts, each added, changed and deleted on its own;
those commands, the backfill and the layer's status are ``hops factory system``
commands.
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


REFERENCES = (
    Path(__file__).resolve().parents[2]
    / "skills"
    / "data"
    / "hops-medallion"
    / "references"
)
TEMPLATE = REFERENCES / "silver_template"
GOLD_TEMPLATE = REFERENCES / "gold_template"
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
# A layer's medallion repository is hops-<its slug without -silver or -gold>.
LAYER_SUFFIX = re.compile(r"-(silver|gold)$")
REPO = re.compile(r"^hops-[a-z0-9][a-z0-9-]*$")
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
    "repo",
}
MODELINGS = ("star", "snowflake")
GRAIN_TYPES = (
    "transaction",
    "periodic_snapshot",
    "accumulating_snapshot",
    "aggregate",
)
ON_CHECK_FAILURE = ("fail", "quarantine", "warn")
GOLD_KEYS = {
    "slug",
    "name",
    "description",
    "queries",
    "modeling",
    "lifecycle",
    "sources",
    "standards",
    "mart",
    "repo",
}
MART_KEYS = {
    "slug",
    "name",
    "description",
    "cadence",
    "freshness_hours",
    "requirements",
}
# The requirement questions of references/gold-marts.md.
REQUIREMENT_KEYS = {
    "analysts",
    "decisions",
    "example_queries",
    "approver",
    "existing_tables",
    "grain",
    "metrics",
    "late_data",
    "restate",
    "reconcile",
    "refresh_checks",
    "invariants",
    "on_check_failure",
    "access",
    "share",
}
MART_PHASES = ("requirements", "design", "code", "backfill", "schedule", "verify")
# Proposed by the Factory; references/gold-marts.md, Standards.
DEFAULT_STANDARDS = {
    "naming": "fct_<process> for facts, dim_<entity> for dimensions, agg_<process>_<grain> for aggregates; snake case in the business's terms; never the name of an existing feature group",
    "modeling": "one declared grain per fact; a surrogate key on every dimension; no measure without a unit; no null foreign keys, an unknown member instead",
    "documentation": "a description on every table and column, every metric's formula in its feature group description, and a README.md per mart listing its tables, grain, metrics, owner and approver",
    "quality": "dbt tests for keys, not-null foreign keys, accepted values and the mart's invariants; reconciliation checks on every refresh; failures handled as the mart's on_check_failure says",
}


def _source_problems(sources: Any, lower: str) -> list[str]:
    if not sources:
        return [f"needs at least one {lower} feature group in sources"]
    problems = []
    for source in sources:
        if not isinstance(source, dict) or not source.get("name"):
            problems.append(f"source {source!r} needs a name")
        elif source.get("cadence", "daily") not in CADENCES:
            problems.append(
                f"source {source['name']}: cadence must be one of {', '.join(CADENCES)}"
            )
    return problems


def _repo_problems(answers: dict) -> list[str]:
    repo = answers.get("repo")
    if repo and not REPO.match(str(repo)):
        return ["repo must be hops- followed by lowercase letters, digits and hyphens"]
    return []


def _problems(answers: dict) -> list[str]:
    problems = [f"unknown answer {k!r}" for k in sorted(set(answers) - ANSWER_KEYS)]
    problems += _repo_problems(answers)
    if not SLUG.match(str(answers.get("slug", ""))):
        problems.append(
            "slug must be lowercase letters, digits and hyphens, starting with a letter"
        )
    problems += _source_problems(answers.get("sources"), "bronze")
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
    targets = hours.values() if isinstance(hours, dict) else [hours]
    if any(not isinstance(h, (int, float)) or h <= 0 for h in targets):
        problems.append("freshness_hours must be a positive number of hours")
    return problems


def _repo_prefix(slug: str) -> str:
    """The medallion a layer belongs to: its slug without a -silver or -gold suffix."""
    return LAYER_SUFFIX.sub("", slug) or slug


def _layer_dirs(cwd: Path):
    """Every layer directory under cwd: in a medallion repository, or on its own as before."""
    for spec in [*cwd.glob("hops-*/*/system.yaml"), *cwd.glob("*/system.yaml")]:
        yield spec.parent


def _silver_repo(cwd: Path, sources: list[dict]) -> Path | None:
    """The medallion repository of the silver layer that builds any of these tables, if it has one."""
    import yaml

    wanted = {s.get("name") for s in sources}
    for directory in _layer_dirs(cwd):
        if not REPO.match(directory.parent.name) or directory.parent == cwd:
            continue
        doc = (
            yaml.safe_load((directory / "system.yaml").read_text(encoding="utf-8"))
            or {}
        )
        built = {t.get("name") for t in (doc.get("outputs") or {}).get("tables") or []}
        if _kind(doc) == "silver" and built & wanted:
            return directory.parent
    return None


def _repo_dir(cwd: Path, answers: dict, kind: str) -> Path:
    """The medallion repository a new layer goes in, one git work tree for its silver and gold layers.

    The answers' `repo`, else for gold the repository of the silver layer
    that builds its sources, else `hops-<prefix>` from the layer's slug,
    reused when it exists.
    """
    if answers.get("repo"):
        return cwd / answers["repo"]
    if kind == "gold":
        found = _silver_repo(cwd, answers.get("sources") or [])
        if found:
            return found
    return cwd / f"hops-{_repo_prefix(answers['slug'])}"


def _copy_template(repo: Path, slug: str, template: Path) -> tuple[Path, dict]:
    """Copy a layer template into ``repo/<slug>``, a git work tree; returns the directory and its system.yaml."""
    import yaml

    target = repo / slug
    if (target / "system.yaml").exists():
        raise click.ClickException(f"{target} already holds a system.yaml")
    target.mkdir(parents=True, exist_ok=True)
    # One work tree for the medallion's layers, pushed as one GitHub repository.
    if shutil.which("git") and not (repo / ".git").exists():
        subprocess.run(["git", "init", "-q", str(repo)], check=False)
    for item in template.iterdir():
        name = ".gitignore" if item.name == "gitignore" else item.name
        shutil.copy(item, target / name)
    return target, yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))


def _write(directory: Path, doc: dict, message: str | None = None) -> None:
    """Write system.yaml, and commit the change in the layer's work tree when given a message."""
    import yaml

    (directory / "system.yaml").write_text(
        yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    if not shutil.which("git"):
        return
    if message:
        # Only this layer's directory: the work tree holds the other layers too.
        git = ["git", "-C", str(directory)]
        subprocess.run([*git, "add", "-A", "--", "."], check=False)
        subprocess.run(
            [*git, "commit", "-q", "-m", message, "--", "."],
            check=False,
            capture_output=True,
        )


def _sync_cadences(doc: dict, hours: Any = None) -> None:
    """Give each cadence the sources use a job schedule and a freshness target, keeping those already set.

    One job runs per cadence, so a cadence no source uses any more loses both.
    `hours` overrides the freshness targets, one number or one per cadence.
    """
    sources = doc.get("sources") or []
    used = [c for c in CADENCES if any(s.get("cadence") == c for s in sources)]
    schedule = doc.setdefault("schedule", {})
    known = schedule.get("cadences") or {}
    schedule["cadences"] = {
        c: known.get(c)
        or {"cron": CADENCES[c], "max_catchup_runs": MAX_CATCHUP_RUNS[c]}
        for c in used
    }
    targets = (doc.get("freshness") or {}).get("max_age_hours")
    if not isinstance(targets, dict):
        targets = dict.fromkeys(CADENCES, targets)
    doc["freshness"] = {
        "max_age_hours": {
            c: (hours.get(c) if isinstance(hours, dict) else hours)
            or targets.get(c)
            or FRESHNESS_HOURS[c]
            for c in used
        }
    }


def _create(cwd: Path, answers: dict) -> Path:
    """Copy the silver template into ``cwd/<slug>`` and write the answers into its system.yaml."""
    repo = _repo_dir(cwd, answers, "silver")
    target, doc = _copy_template(repo, answers["slug"], TEMPLATE)
    doc["layer"]["repo"] = {"name": repo.name}
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
            "cadence": s.get("cadence", cadence),
            "arrival_column": s.get("arrival_column"),
        }
        for s in answers["sources"]
    ]
    doc["tasks"] = list(answers.get("tasks") or [])
    doc["extra_tasks"] = answers.get("extra_tasks", "")
    doc["engine"] = answers.get("engine", "dbt_trino")
    doc["schedule"] = {"cadence": cadence, "catchup": True, "cadences": {}}
    doc["history"] = answers.get("history", "latest")
    doc["deletes"] = answers.get("deletes", "ignore")
    doc["schema_changes"] = answers.get("schema_changes", "fail")
    doc["late_data"] = {"lookback": answers.get("lookback", "0")}
    doc["quality"] = {
        "max_reject_pct": answers.get("max_reject_pct", 5),
        "alert_on_failure": bool(answers.get("alert_on_failure", True)),
    }
    doc["freshness"] = {"max_age_hours": {}}
    _sync_cadences(doc, answers.get("freshness_hours"))
    _write(target, doc)
    return target


def _mart_problems(mart: Any, taken: set[str] = frozenset()) -> list[str]:
    if not isinstance(mart, dict):
        return ["a gold layer needs a data mart"]
    problems = [f"unknown mart answer {k!r}" for k in sorted(set(mart) - MART_KEYS)]
    slug = str(mart.get("slug", ""))
    if not SLUG.match(slug):
        problems.append(
            "the mart's slug must be lowercase letters, digits and hyphens, starting with a letter"
        )
    elif slug in taken:
        problems.append(f"the layer already has a data mart {slug!r}")
    if mart.get("cadence", "daily") not in CADENCES:
        problems.append(f"the mart's cadence must be one of {', '.join(CADENCES)}")
    hours = mart.get("freshness_hours", 1)
    if hours is not None and (not isinstance(hours, (int, float)) or hours <= 0):
        problems.append("freshness_hours must be a positive number of hours")
    requirements = mart.get("requirements") or {}
    if not isinstance(requirements, dict):
        return [*problems, "requirements must be an object"]
    problems += [
        f"unknown requirement {k!r}"
        for k in sorted(set(requirements) - REQUIREMENT_KEYS)
    ]
    if not str(requirements.get("example_queries") or "").strip():
        problems.append(
            "a data mart needs example_queries: questions with the answers you expect, which verify it"
        )
    grain = requirements.get("grain") or {}
    if grain.get("type") and grain["type"] not in GRAIN_TYPES:
        problems.append(f"grain.type must be one of {', '.join(GRAIN_TYPES)}")
    failure = requirements.get("on_check_failure")
    if failure and failure not in ON_CHECK_FAILURE:
        problems.append(
            f"on_check_failure must be one of {', '.join(ON_CHECK_FAILURE)}"
        )
    return problems


def _gold_problems(answers: dict) -> list[str]:
    problems = [f"unknown answer {k!r}" for k in sorted(set(answers) - GOLD_KEYS)]
    problems += _repo_problems(answers)
    if not SLUG.match(str(answers.get("slug", ""))):
        problems.append(
            "slug must be lowercase letters, digits and hyphens, starting with a letter"
        )
    problems += _source_problems(answers.get("sources"), "silver")
    if answers.get("modeling", "star") not in MODELINGS:
        problems.append(f"modeling must be one of {', '.join(MODELINGS)}")
    if answers.get("lifecycle", "dev") not in LIFECYCLES:
        problems.append(f"lifecycle must be one of {', '.join(LIFECYCLES)}")
    standards = answers.get("standards") or {}
    if not isinstance(standards, dict) or set(standards) - set(DEFAULT_STANDARDS):
        problems.append(f"standards takes {', '.join(DEFAULT_STANDARDS)}")
    return problems + _mart_problems(answers.get("mart"))


def _mart(answers: dict) -> dict:
    """A data mart's entry in system.yaml, before it is built."""
    cadence = answers.get("cadence", "daily")
    return {
        "slug": answers["slug"],
        "name": answers.get("name") or answers["slug"],
        "description": answers.get("description", ""),
        "status": "draft",
        "cadence": cadence,
        "freshness_hours": answers.get("freshness_hours") or FRESHNESS_HOURS[cadence],
        "requirements": dict(answers.get("requirements") or {}),
        "phases": {phase: {"status": "pending"} for phase in MART_PHASES},
        "tables": [],
        "jobs": [],
        "applied": {},
    }


def _create_gold(cwd: Path, answers: dict) -> Path:
    """Copy the gold template into ``cwd/<slug>`` and write the answers, with the first data mart, into its system.yaml."""
    repo = _repo_dir(cwd, answers, "gold")
    target, doc = _copy_template(repo, answers["slug"], GOLD_TEMPLATE)
    doc["layer"]["repo"] = {"name": repo.name}
    doc["layer"].update(
        name=answers.get("name") or answers["slug"],
        slug=answers["slug"],
        description=answers.get("description", ""),
        queries=answers.get("queries", ""),
        modeling=answers.get("modeling", "star"),
        lifecycle=answers.get("lifecycle", "dev"),
    )
    doc["sources"] = [
        {"name": s["name"], "version": s.get("version", 1)} for s in answers["sources"]
    ]
    standards = answers.get("standards") or {}
    doc["standards"] = {k: standards.get(k) or v for k, v in DEFAULT_STANDARDS.items()}
    doc["marts"] = [_mart(answers["mart"])]
    _write(target, doc)
    return target


def _launch(target: Path, launch: bool, request: str | None = None) -> None:
    """Start Claude Code on the layer: `request` is the slash command, /hops-silver <slug> by default."""
    slug = target.name
    request = request or f"/hops-silver {slug}"
    command = ["claude", request]
    if not launch or not shutil.which("claude"):
        click.echo(f'\nBuild it with:  cd {target} && claude "{request}"')
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


def _register(ctx: click.Context, target: Path, name: str, layer: str) -> None:
    from hopsworks.cli import factory_spec
    from hopsworks.cli.commands import mlsystem

    try:
        factory_spec.record_factory(ctx.meta.get(factory_spec.META), target)
        mlsystem.register(
            ctx, target, name, factory_spec.factory_name(ctx, f"medallion-{layer}")
        )
    except Exception as exc:  # noqa: BLE001 - the layer is recorded either way
        output.warn(
            f"Not registered in the project's Factory ({exc}); run `hops factory system register {target} --factory medallion-{layer}`."
        )


def create_silver(ctx: click.Context, data: dict, launch: bool) -> Path:
    """Record a silver layer in ./<slug>/system.yaml, register it, and build it with Claude Code.

    The answers name the bronze feature groups (sources) with the cadence each
    is refreshed at, the silver tasks, any extra tasks in the user's words, the
    engine (dbt_trino or pyspark), the settings and the lifecycle of the tables.
    """
    # A factory form picks each bronze table as {table: {name, version}, cadence}.
    data = {
        **data,
        "sources": [
            {**s["table"], **{k: v for k, v in s.items() if k != "table"}}
            if isinstance(s, dict) and isinstance(s.get("table"), dict)
            else s
            for s in data.get("sources") or []
        ],
    }
    problems = _problems(data)
    if problems:
        raise click.ClickException("invalid answers:\n  " + "\n  ".join(problems))
    target = _create(Path.cwd(), data)
    output.success(f"Silver layer recorded in {target / 'system.yaml'}")
    _register(ctx, target, data.get("name") or data["slug"], "silver")
    _launch(target, launch)
    return target


def create_gold(ctx: click.Context, data: dict, launch: bool) -> Path:
    """Record a gold layer and its first data mart in ./<slug>/system.yaml, register it, and build it with Claude Code.

    The answers describe the queries the layer serves, its Kimball model
    (star or snowflake), the silver feature groups it reads, the standards
    every mart follows, and the first data mart with its requirements.
    More marts are added by an Add data mart change request.
    """
    problems = _gold_problems(data)
    if problems:
        raise click.ClickException("invalid answers:\n  " + "\n  ".join(problems))
    target = _create_gold(Path.cwd(), data)
    output.success(f"Gold layer recorded in {target / 'system.yaml'}")
    _register(ctx, target, data.get("name") or data["slug"], "gold")
    _launch(target, launch, f"/hops-gold {target.name}")
    return target


def _kind(doc: dict) -> str:
    return (doc.get("layer") or {}).get("kind") or "silver"


def silver_jobs(outputs: dict) -> list[dict]:
    """The silver layer's jobs, one per cadence; a layer built before that has one `job`."""
    jobs = [j for j in outputs.get("jobs") or [] if j.get("name")]
    if not jobs and (outputs.get("job") or {}).get("name"):
        jobs = [outputs["job"]]
    return jobs


def layer_jobs(doc: dict) -> list[dict]:
    """The jobs a layer runs: silver's one per cadence, gold's every data mart's, each with its `mart`."""
    if _kind(doc) != "gold":
        return silver_jobs(doc.get("outputs") or {})
    return [
        {**job, "mart": mart.get("slug")}
        for mart in doc.get("marts") or []
        for job in mart.get("jobs") or []
        if job.get("name")
    ]


def layer_tables(doc: dict) -> list[dict]:
    """The tables a layer built, each once, with its `kind` (silver, rejects, or gold) and, in gold, its `mart`."""
    if _kind(doc) != "gold":
        outputs = doc.get("outputs") or {}
        return [
            {**t, "kind": kind}
            for kind, key in (("silver", "tables"), ("rejects", "rejects"))
            for t in outputs.get(key) or []
            if t.get("name")
        ]
    seen: dict[tuple, dict] = {}
    for mart in doc.get("marts") or []:
        for table in mart.get("tables") or []:
            key = (table.get("name"), int(table.get("version", 1)))
            if not table.get("name"):
                continue
            # The mart that builds a shared dimension owns it, not one that reads it.
            if key not in seen or (seen[key].get("shared") and not table.get("shared")):
                seen[key] = {
                    **table,
                    "table_kind": table.get("kind"),
                    "kind": "gold",
                    "mart": mart.get("slug"),
                }
    return list(seen.values())


def _protected(fg: Any, sources: set[tuple[str, int]], kind: str | None) -> str | None:
    """Why a feature group may not be deleted by a system: a layer of `kind`, or an ML system when None."""
    if (fg.name, int(fg.version)) in sources:
        return "a source of this system"
    try:
        tags = fg.get_tags() or {}
    except Exception:  # noqa: BLE001 - a table whose tags cannot be read is judged by the sources alone
        return None
    tag = tags.get("medallion_table")
    value = getattr(tag, "value", tag)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    layer = value.get("layer") if isinstance(value, dict) else None
    lower = {"silver": ("bronze",), "gold": ("bronze", "silver")}.get(
        kind, ("bronze", "silver", "gold")
    )
    return f"a {layer} table" if layer in lower else None


def delete_assets(
    ctx: click.Context, doc: dict, job_names: list[str], tables: list[dict]
) -> None:
    """Delete some of what a system built: jobs, then feature groups; what is gone is skipped.

    Every feature group is checked before anything is deleted: one the system reads (its
    `sources`, or a feature group among `requirements.data_sources`), or one tagged as a lower
    medallion layer (any layer, for an ML system), stops the delete with nothing gone.
    """
    project = session.get_project(ctx)
    kind = (doc.get("layer") or {}).get("kind")
    sources = {
        (s.get("name"), int(s.get("version", 1)))
        for s in [
            *(doc.get("sources") or []),
            *(
                d
                for d in (doc.get("requirements") or {}).get("data_sources") or []
                if isinstance(d, dict) and d.get("kind") == "feature_group"
            ),
        ]
        if isinstance(s, dict)
    }
    fs = project.get_feature_store() if tables else None
    found = []
    for table in tables:
        name, version = table.get("name"), int(table.get("version", 1))
        if not name:
            continue
        # None means it does not exist; any other failure raises and stops the
        # delete, which leaves the system as it was, to be deleted again.
        fg = fs.get_feature_group(name, version=version)
        if fg is None:
            output.info(f"feature group {name} v{version}: gone")
            continue
        why = _protected(fg, sources, kind)
        if why:
            raise click.ClickException(
                f"{name} v{version} is {why}; a system never deletes it"
            )
        found.append(fg)
    for job_name in job_names:
        job = project.get_job_api().get_job(job_name)
        if job is None:
            output.info(f"job {job_name}: gone")
        else:
            job.delete()
            output.success(f"✓ Deleted job {job_name}")
    for fg in found:
        fg.delete()
        output.success(f"✓ Deleted feature group {fg.name} v{fg.version}")


def delete_layer(ctx: click.Context, directory: Path, doc: dict) -> str:
    """Delete a layer's jobs and feature groups (in gold, every data mart's), then its directory.

    A layer never deletes the tables it reads: a feature group that is one of
    its sources, or is tagged as a lower layer (bronze, and silver from gold),
    stops the delete before anything is deleted.
    """
    delete_assets(ctx, doc, [j["name"] for j in layer_jobs(doc)], layer_tables(doc))
    shutil.rmtree(directory, ignore_errors=True)
    repo = directory.parent
    # In a medallion repository the other layers stay; record that this one went.
    if (repo / ".git").exists() and shutil.which("git"):
        git = ["git", "-C", str(repo)]
        subprocess.run([*git, "add", "-A", "--", directory.name], check=False)
        subprocess.run(
            [
                *git,
                "commit",
                "-q",
                "-m",
                f"[{directory.name}] delete layer",
                "--",
                directory.name,
            ],
            check=False,
            capture_output=True,
        )
    return "deleted"


def layer_status(
    project: Any,
    directory: Path,
    doc: dict,
    hours: int,
    no_summary: bool,
    out: Path | None = None,
) -> None:
    """Report the health of a layer as an HTML page, by default status/report.html in its directory.

    For each of the layer's jobs, its runs in the last HOURS with the log tail
    of each failure; for each table it built, its rows, when it was last
    written against the freshness target, in silver its share of rejected rows
    against the quality gate, and its file layout from the table's files, as
    hops-table-maintenance reads them.
    """
    from hopsworks.cli import health, silver_status

    facts = silver_status.collect(project, doc, directory.name, hours)
    if output.JSON_MODE:
        output.print_json(facts)
        return
    summary = None if no_summary else health.summarize(facts)
    target = out or directory / "status" / "report.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(health.render(facts, summary), encoding="utf-8")
    c = facts["counts"]
    output.success(
        f"{facts['overall']}: {c['failed_runs']} of {c['runs']} job runs failed in {hours} h, "
        f"{c['table_problems']} of {c['tables']} tables with problems; report in {target}"
    )
