"""``hops build`` — the structured interview for a new ML system, then the build.

Asks what the system should predict and the questions that follow from it,
writing every answer to ``<slug>/system.yaml`` as it is given, then starts
Claude Code with ``/hops-build <slug>`` to complete the specification and build
it. Menus are plain prompts, so each question appears at once; the only model
call is one Haiku ``claude -p`` that reads the user's description and
recommends a system type and a name. The login and the listings the later
questions need run in the background while the first question is answered.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


REFERENCES = (
    Path(__file__).resolve().parents[2] / "skills" / "ml" / "hops-reqs" / "references"
)
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
SYSTEM_TYPES = {
    "batch": "predictions on a schedule, read from a table, a report or a dashboard",
    "realtime": "a prediction per request, answered in milliseconds by a deployment",
    "agent": "an LLM that reasons over your data and tools",
}
INTERPRET = """You recommend how to build an ML system on Hopsworks.
The user wants to predict: {problem}
Feature groups in their project: {feature_groups}
Reply with one JSON object and nothing else:
{{"system_type": "batch" | "realtime" | "agent",
  "reason": "<one short sentence>",
  "name": "<a title of two to four words>",
  "slug": "<two or three lowercase words joined by hyphens>",
  "feature_groups": ["<names from the list above that fit the problem>"]}}"""


def _load(path: Path, name: str) -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# region Prompts


def _heading(text: str) -> None:
    click.echo()
    click.echo(click.style(text, bold=True))


def _choose(question: str, options: list[tuple[str, str]], default: int = 0) -> int:
    """Ask a numbered question; return the index of the chosen option."""
    _heading(question)
    for i, (label, hint) in enumerate(options, 1):
        line = f"  {i}. {label}"
        if hint:
            line += click.style(f"  {hint}", dim=True)
        click.echo(line)
    picked = click.prompt(
        "Choose", type=click.IntRange(1, len(options)), default=default + 1
    )
    return picked - 1


def _choose_many(question: str, options: list[str], default: list[int]) -> list[int]:
    """Ask for any number of numbered options, as `1,3`; empty keeps the default."""
    _heading(question)
    for i, label in enumerate(options, 1):
        click.echo(f"  {i}. {label}")
    shown = ",".join(str(i + 1) for i in default) or "none"
    while True:
        raw = click.prompt("Choose, comma-separated", default=shown)
        if raw.strip() == "none":
            return []
        try:
            picked = sorted({int(part) - 1 for part in raw.split(",") if part.strip()})
        except ValueError:
            picked = [-1]
        if all(0 <= i < len(options) for i in picked):
            return picked
        output.warn("Pick numbers from the list.")


# endregion

# region Background work


class _Prefetch(threading.Thread):
    """Log in and list what the later questions offer, while the user types."""

    def __init__(self, ctx: click.Context) -> None:
        super().__init__(daemon=True)
        self.ctx = ctx
        # The target comes from the local config, so writing it never waits on a login.
        cfg = session.get_config(ctx)
        self.host = cfg.host or ""
        self.project = cfg.project or ""
        self.feature_groups: list[tuple[str, int]] = []
        self.deployments: list[str] = []
        self.error: str | None = None

    def run(self) -> None:
        try:
            session.require_auth(self.ctx)
            project = session.get_project(self.ctx)
            self.project = self.project or getattr(project, "name", "")
            for fs in session.get_accessible_feature_stores(self.ctx):
                for fg in fs.get_feature_groups():
                    self.feature_groups.append((fg.name, fg.version))
            try:
                serving = project.get_model_serving()
                self.deployments = [d.name for d in serving.get_deployments()]
            except Exception:  # noqa: BLE001 - serving may be disabled
                self.deployments = []
        except (Exception, SystemExit) as exc:  # noqa: BLE001 - reported, not fatal
            self.error = str(exc)

    def ready(self) -> _Prefetch:
        if self.is_alive():
            output.info("Waiting for the project listing...")
            self.join(timeout=30)
        if self.is_alive():
            self.error = "the project did not answer within 30 seconds"
        return self


def _interpret(problem: str, feature_groups: list[str]) -> dict | None:
    """One Haiku call reading the description; None when claude is missing or slow."""
    claude = shutil.which("claude")
    if not claude:
        return None
    prompt = INTERPRET.format(
        problem=problem, feature_groups=", ".join(feature_groups) or "none yet"
    )
    try:
        done = subprocess.run(
            # No tools and no skills: the user's words are in the prompt, and a
            # one-shot answer needs neither.
            [
                claude,
                "-p",
                "--model",
                "haiku",
                "--tools",
                "",
                "--disable-slash-commands",
            ]
            + ["--strict-mcp-config", "--no-session-persistence", prompt],
            capture_output=True,
            text=True,
            timeout=60,
            stdin=subprocess.DEVNULL,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r"\{.*\}", done.stdout, re.S)
    if not match:
        return None
    try:
        answer = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    if answer.get("system_type") not in SYSTEM_TYPES:
        answer.pop("system_type", None)
    if not SLUG.match(str(answer.get("slug", ""))):
        answer.pop("slug", None)
    answer["feature_groups"] = [
        name for name in answer.get("feature_groups") or [] if name in feature_groups
    ]
    return answer


# endregion

# region The system file


class _System:
    """One system directory: its system.yaml, validated and registered on every write."""

    def __init__(self, target: Path) -> None:
        self.target = target
        self.suggested: list[str] = []
        # The shipped set.py, not the system's copy: a system created by an older
        # template may predate the UI registration.
        self.setter = _load(REFERENCES / "system_template" / "set.py", "system_set")
        self.doc = _read(target)

    @property
    def requirements(self) -> dict:
        return self.doc.setdefault("requirements", {})

    def put(self, dotted: str, value: Any, append: bool = False) -> None:
        node = self.doc
        *parents, last = dotted.split(".")
        for part in parents:
            node = node.setdefault(part, {})
        if append:
            node.setdefault(last, []).append(value)
        else:
            node[last] = value

    def save(self) -> None:
        import yaml

        problems = self.setter._validator().validate(self.doc)
        if problems:
            raise click.ClickException(
                "system.yaml would be invalid:\n  " + "\n  ".join(problems)
            )
        self.setter._write_atomically(
            self.target / "system.yaml",
            yaml.safe_dump(self.doc, sort_keys=False, allow_unicode=True, width=100),
        )
        self.setter.register(self.doc, self.target)


def _read(target: Path) -> dict:
    import yaml

    path = target / "system.yaml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _systems(cwd: Path) -> list[Path]:
    return sorted(p.parent for p in cwd.glob("*/system.yaml"))


def _create(cwd: Path, slug: str, example: str | None = None) -> _System:
    new_system = _load(REFERENCES / "new_system.py", "new_system")
    target = cwd / slug
    if (target / "system.yaml").exists():
        raise click.ClickException(
            f"{target} already holds a system; run `hops build {slug}` to resume it."
        )
    try:
        new_system.create(target, example)
    except SystemExit as exc:
        raise click.ClickException(str(exc)) from exc
    return _System(target)


# endregion

# region The interview


def _problem(prefetch: _Prefetch, cwd: Path) -> _System:
    choice = _choose(
        "What should the ML system predict?",
        [
            ("Describe what I want to predict", "recommended"),
            ("Example ML system", "synthetic data, an app included"),
        ],
    )
    if choice == 1:
        examples = _load(REFERENCES / "new_system.py", "new_system").examples()
        names = list(examples)
        picked = names[
            _choose("Which example?", [(examples[name]["label"], "") for name in names])
        ]
        system = _create(cwd, picked, example=picked)
        _target(system, prefetch)
        system.save()
        return system

    problem = click.prompt(
        click.style("Describe it in a sentence", bold=True),
        prompt_suffix="\n> ",
    ).strip()
    names = [name for name, _ in prefetch.ready().feature_groups]
    # Haiku takes about six seconds; the repository question does not depend on it,
    # so it is asked while the description is read.
    reading: dict = {}
    reader = threading.Thread(
        target=lambda: reading.update(_interpret(problem, names) or {}), daemon=True
    )
    reader.start()
    repo = _repository_choice(cwd)
    if reader.is_alive():
        output.info("Reading your description...")
    reader.join(timeout=60)
    advice = dict(reading)
    recommended = advice.get("system_type", "batch")
    order = [recommended, *(t for t in SYSTEM_TYPES if t != recommended)]
    labels = {"batch": "Batch", "realtime": "Real-time", "agent": "Agentic"}
    options = [
        (
            labels[t] + (" (recommended)" if t == recommended and advice else ""),
            advice.get("reason", "")
            if t == recommended and advice.get("reason")
            else SYSTEM_TYPES[t],
        )
        for t in order
    ]
    system_type = order[_choose("What type of ML system?", options)]
    slug = click.prompt(
        "Short name for the system (lowercase, hyphens)",
        default=advice.get("slug") or "ml-system",
        value_proc=_slug,
    )
    system = _create(cwd, slug)
    system.put("schema_version", 1)
    system.put(
        "system",
        {
            "name": advice.get("name") or slug.replace("-", " ").capitalize(),
            "slug": slug,
        },
    )
    _target(system, prefetch)
    system.put("system.status", "draft")
    system.put("requirements.status", "pending")
    system.put("requirements.description", problem)
    system.put("requirements.system_type", system_type)
    system.put("system.repo", {"url": repo})
    system.save()
    system.suggested = advice.get("feature_groups", [])
    return system


def _slug(value: str) -> str:
    if not SLUG.match(value):
        raise click.BadParameter("lowercase letters, digits and hyphens, e.g. churn")
    return value


def _target(system: _System, prefetch: _Prefetch) -> None:
    system.put(
        "system.target",
        {"cluster": prefetch.host, "project": prefetch.project, "stage": "development"},
    )


def _cadence(system: _System) -> None:
    options = ["hourly", "daily", "weekly"]
    picked = _choose(
        "How often are predictions made, or the dashboard updated?",
        [
            ("Hourly", ""),
            ("Daily", "recommended for most"),
            ("Weekly", ""),
            ("Other", ""),
        ],
        default=1,
    )
    cadence = (
        options[picked]
        if picked < 3
        else click.prompt("Cadence, e.g. every 6 hours or a cron expression")
    )
    system.put("requirements.sla", {"batch": {"cadence": cadence}})
    system.save()


def _data(ctx: click.Context, system: _System, prefetch: _Prefetch) -> None:
    groups = prefetch.ready().feature_groups
    suggested = getattr(system, "suggested", [])
    extra = [
        "Upload or point at files",
        "Generate synthetic data",
        "Add a new data source",
    ]
    options = [f"{name} v{version}" for name, version in groups] + extra
    default = [i for i, (name, _) in enumerate(groups) if name in suggested]
    if not groups:
        default = [len(options) - 2]
    for i in _choose_many("Which data should it learn from?", options, default):
        if i < len(groups):
            name, version = groups[i]
            source = {"name": name, "kind": "feature_group", "version": version}
            source["status"] = "present"
        elif options[i] == extra[0]:
            source = _files(system)
        elif options[i] == extra[1]:
            source = _synthetic(system)
        else:
            source = _datasource(ctx, system)
            if source is None:
                continue
        system.put("requirements.data_sources", source, append=True)
    system.save()


def _files(system: _System) -> dict:
    path = Path(click.prompt("Path to the file or directory")).expanduser()
    name = click.prompt("Name for this source", default=path.stem.replace("-", "_"))
    home = os.environ.get("HOPSFS_USER_HOME_DIR", "")
    mount = home.split("/Users/", 1)[0] if "/Users/" in home else ""
    if mount and str(path.resolve()).startswith(mount + "/"):
        location = str(path.resolve())[len(mount) + 1 :]
    else:
        location = f"Resources/{system.target.name}/data/"
        subprocess.run(["hops", "files", "mkdir", location], check=False)
        done = subprocess.run(
            ["hops", "files", "upload", str(path), location], check=False
        )
        if done.returncode != 0:
            raise click.ClickException(f"uploading {path} failed")
    return {"name": name, "kind": "file", "location": location, "status": "present"}


def _synthetic(system: _System) -> dict:
    shape = ["batch", "events"][
        _choose(
            "What shape is the synthetic data?",
            [
                ("Tables", "one row per entity"),
                ("Events", "a stream of timestamped events"),
            ],
        )
    ]
    name = click.prompt("Name for this source", value_proc=_ident)
    story = click.prompt(
        "Its story in one sentence (who, how many, what drives the label)"
    )
    system.put(f"data.{name}.generator.story", story)
    system.put("data.status", "pending")
    return {
        "name": name,
        "kind": "synthetic",
        "shape": shape,
        "status": "needs_generation",
    }


def _ident(value: str) -> str:
    if not re.match(r"^[a-z][a-z0-9_]*$", value):
        raise click.BadParameter("lowercase letters, digits and underscores")
    return value


def _datasource(ctx: click.Context, system: _System) -> dict | None:
    """Create a connector through `hops datasource create`; secrets go in by environment."""
    from hopsworks.cli.commands.datasource import datasource_group

    create = datasource_group.commands["create"]
    kinds = sorted(create.commands)
    kind = kinds[
        _choose(
            "Which system holds the data?",
            [(k, create.commands[k].get_short_help_str(60)) for k in kinds],
            default=kinds.index("sql") if "sql" in kinds else 0,
        )
    ]
    command = create.commands[kind]
    argv = ["hops", "datasource", "create", kind]
    env = dict(os.environ)
    for param in command.params:
        envvar = getattr(param, "envvar", None)
        secret = isinstance(envvar, str) and envvar.startswith("HOPSWORKS_DS_")
        label = param.name.replace("_", " ")
        if isinstance(param, click.Argument):
            connector = click.prompt("Connector name", value_proc=_ident)
            argv.append(connector)
        elif secret:
            value = click.prompt(
                f"{label} (hidden; empty for none)",
                hide_input=True,
                default="",
                show_default=False,
            )
            if value:
                env[envvar] = value
        elif param.required:
            choices = getattr(param.type, "choices", None)
            text = f"{label}" + (f" ({'/'.join(choices)})" if choices else "")
            argv += [param.opts[0], click.prompt(text)]
    output.info("Running: " + shlex.join(argv))
    done = subprocess.run(argv, env=env, check=False)
    if done.returncode != 0:
        output.warn("The data source was not created; continuing without it.")
        return None
    table = click.prompt(
        "Table to read, if you know it (empty to choose later)",
        default="",
        show_default=False,
    )
    source = {"name": connector, "kind": "datasource", "type": kind}
    source.update({"connector": connector, "status": "connected"})
    if table:
        source["table"] = table
    return source


def _batch(ctx: click.Context, system: _System, prefetch: _Prefetch) -> None:
    if not system.requirements.get("sla"):
        _cadence(system)
    if not system.requirements.get("data_sources"):
        _data(ctx, system, prefetch)
    if "app" in system.doc:
        return
    picked = _choose(
        "How are the predictions used?",
        [
            ("A dashboard", "a list people read, refreshed on the cadence"),
            ("An app", "a Python app with a JavaScript UI"),
            ("Neither", "another system reads the prediction table"),
        ],
    )
    if picked == 2:
        system.put("requirements.consumers", "api")
        system.put("app", {"wanted": False, "status": "skipped"})
    else:
        how = click.prompt("Describe who uses them and how")
        system.put("requirements.consumers", "ui")
        system.put(
            "app",
            {
                "wanted": True,
                "kind": "dashboard" if picked == 0 else "query_ui",
                "description": how,
                "status": "pending",
            },
        )
    system.save()


def _realtime(ctx: click.Context, system: _System, prefetch: _Prefetch) -> None:
    if not system.requirements.get("data_sources"):
        _data(ctx, system, prefetch)
    if "app" not in system.doc:
        wanted = click.confirm(
            click.style("Build an app to try the deployment?", bold=True), default=True
        )
        system.put("requirements.consumers", "api")
        system.put(
            "app",
            {
                "wanted": wanted,
                "kind": "query_ui",
                "status": "pending" if wanted else "skipped",
            },
        )
        system.save()
    if system.requirements.get("sla"):
        return
    latency = [50, 100, 500]
    picked = _choose(
        "Latency: p99 under",
        [("50 ms", ""), ("100 ms", "recommended"), ("500 ms", ""), ("Other", "")],
        default=1,
    )
    p99 = latency[picked] if picked < 3 else click.prompt("p99 in ms", type=int)
    rates = [10, 100, 1000]
    picked = _choose(
        "Throughput: requests per second",
        [("10", ""), ("100", "recommended"), ("1000", ""), ("Other", "")],
        default=1,
    )
    qps = rates[picked] if picked < 3 else click.prompt("Requests per second", type=int)
    system.put("requirements.sla", {"realtime": {"p99_ms": p99, "throughput_qps": qps}})
    system.save()


def _agent(ctx: click.Context, system: _System, prefetch: _Prefetch) -> None:
    if not system.requirements.get("data_sources"):
        _data(ctx, system, prefetch)
        wanted = click.confirm(
            click.style("Build an app to try the agent?", bold=True), default=True
        )
        system.put(
            "app",
            {
                "wanted": wanted,
                "kind": "chat",
                "status": "pending" if wanted else "skipped",
            },
        )
        system.put("requirements.sla", {"agent": {"p99_ms": 5000, "throughput_qps": 2}})
    if system.doc.get("inference", {}).get("agent"):
        return
    deployments = prefetch.ready().deployments
    options = [(name, "a deployment in this project") for name in deployments]
    options.append(
        ("A provider endpoint", "OpenAI, Anthropic or any OpenAI-compatible URL")
    )
    picked = _choose("Which LLM should the agent use?", options)
    if picked < len(deployments):
        llm: dict = {"deployment": deployments[picked]}
    else:
        llm = {
            "endpoint": click.prompt(
                "Endpoint URL", default="https://api.openai.com/v1"
            ),
            "model": click.prompt("Model name"),
            "api_key_secret": click.prompt(
                "Name of the project secret holding the API key (not the key)"
            ),
        }
    system.put(
        "inference", {"mode": "agent", "agent": {"llm": llm}, "status": "pending"}
    )
    system.save()
    output.info(
        "Agentic systems are recorded in full; this version does not build them yet."
    )


def _repository_choice(cwd: Path) -> str:
    """The repository URL the code goes to, or "new" for one created at build start."""
    origin = subprocess.run(
        ["git", "-C", str(cwd), "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    options = [("A new private GitHub repository", "created when the build starts")]
    if origin:
        options.insert(0, (f"This repository ({origin})", "recommended"))
    picked = _choose("Where should the code go?", options)
    return origin if origin and picked == 0 else "new"


def _repository(system: _System) -> None:
    system.put("system.repo", {"url": _repository_choice(system.target.parent)})
    system.save()


# endregion

# region Launch


def _launch(system: _System, launch: bool) -> None:
    slug = system.target.name
    command = ["claude", f"/hops-build {slug}"]
    printable = f'claude "/hops-build {slug}"'
    status = system.target / "status.py"
    output.success(f"Interview recorded in {system.target / 'system.yaml'}")
    subprocess.run([sys.executable, str(status)], check=False)
    if not launch or not shutil.which("claude"):
        click.echo(f"\nBuild it with:  cd {system.target.parent} && {printable}")
        return
    if os.environ.get("TMUX") and shutil.which("tmux"):
        subprocess.run(
            ["tmux", "new-window", "-n", slug, "-c", str(system.target.parent)]
            + [shlex.join(command)],
            check=True,
        )
        click.echo(
            f"\nBuilding in the tmux window '{slug}'; the Hopsworks UI shows its progress."
        )
        return
    os.chdir(system.target.parent)
    os.execvp("claude", command)


@click.command("build")
@click.argument("slug", required=False)
@click.option(
    "--no-launch",
    is_flag=True,
    help="Record the interview but do not start Claude Code.",
)
@click.pass_context
def build_cmd(ctx: click.Context, slug: str | None, no_launch: bool) -> None:
    """Interview for a new ML system, then build it with Claude Code.

    Asks what to predict and the questions that follow (batch, real-time or
    agentic; cadence or SLAs; data; how predictions are used; where the code
    goes), writing each answer to ./<slug>/system.yaml. Then starts
    `claude "/hops-build <slug>"`, in a new tmux window when run inside tmux,
    which completes the specification and builds the system. With SLUG,
    resumes that system's interview, or starts its build when the interview is
    done.

    Args:
        ctx: Click context.
        slug: An existing system in this directory to resume.
        no_launch: Record the interview only.
    """
    prefetch = _Prefetch(ctx)
    prefetch.start()
    try:
        _interview(ctx, prefetch, slug, not no_launch)
    finally:
        # A login still importing the SDK at interpreter exit fails noisily.
        prefetch.join(timeout=15)


def _interview(
    ctx: click.Context, prefetch: _Prefetch, slug: str | None, launch: bool
) -> None:
    cwd = Path.cwd()
    existing = _systems(cwd)
    system: _System | None = None
    if slug:
        if not (cwd / slug / "system.yaml").exists():
            raise click.ClickException(f"no system {slug!r} in {cwd}")
        system = _System(cwd / slug)
    elif existing:
        pending = [
            p
            for p in existing
            if (_read(p).get("requirements") or {}).get("status") != "met"
        ]
        options = [(f"Continue {p.name}", "interview not finished") for p in pending]
        options.append(("Start a new ML system", ""))
        picked = _choose("What do you want to build?", options)
        if picked < len(pending):
            system = _System(pending[picked])
    if system is None:
        system = _problem(prefetch, cwd)
    if system.requirements.get("status") == "met":
        _launch(system, launch)
        return
    prefetch.ready()
    if prefetch.error:
        output.warn(
            f"Could not list the project ({prefetch.error}); lists will be empty."
        )
    if not system.doc.get("system", {}).get("example"):
        {"batch": _batch, "realtime": _realtime, "agent": _agent}[
            system.requirements["system_type"]
        ](ctx, system, prefetch)
    elif system.requirements.get("system_type") == "agent":
        _agent(ctx, system, prefetch)
    if not system.doc.get("system", {}).get("repo"):
        _repository(system)
    _launch(system, launch)


# endregion
