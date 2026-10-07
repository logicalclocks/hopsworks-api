"""The ML systems built by the ``ml-batch``, ``ml-realtime`` and ``ml-agent`` factories.

``hops factory run`` hands this the answers the factory's form collected: they
are written to ``<slug>/system.yaml``, what they leave out is asked, and Claude
Code starts with ``/hops-build <slug>`` to complete the specification and build
it. The login and the listings the later questions need run in the background
while the first question is answered.
"""

from __future__ import annotations

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
SYSTEM_TYPES = ("batch", "realtime", "agent")


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
        self.error: str | None = None

    def run(self) -> None:
        try:
            session.require_auth(self.ctx)
            project = session.get_project(self.ctx)
            self.project = self.project or getattr(project, "name", "")
            for fs in session.get_accessible_feature_stores(self.ctx):
                for fg in fs.get_feature_groups():
                    self.feature_groups.append((fg.name, fg.version))
        except (Exception, SystemExit) as exc:  # noqa: BLE001 - reported, not fatal
            self.error = str(exc)

    def ready(self) -> _Prefetch:
        if self.is_alive():
            output.info("Waiting for the project listing...")
            self.join(timeout=30)
        if self.is_alive():
            self.error = "the project did not answer within 30 seconds"
        return self


# endregion

# region The system file


class _System:
    """One system directory: its system.yaml, validated on every write."""

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


def _read(target: Path) -> dict:
    import yaml

    path = target / "system.yaml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _create(cwd: Path, slug: str, example: str | None = None) -> _System:
    new_system = _load(REFERENCES / "new_system.py", "new_system")
    target = cwd / slug
    if (target / "system.yaml").exists():
        raise click.ClickException(
            f"{target} already holds a system; run `hops factory run <factory> {slug}` to resume it."
        )
    try:
        new_system.create(target, example)
    except SystemExit as exc:
        raise click.ClickException(str(exc)) from exc
    return _System(target)


# endregion

# region The interview


ANSWER_KEYS = {
    "slug",
    "example",
    "name",
    "description",
    "system_type",
    "sla",
    "consumers",
    "data_sources",
    "app",
    "repo",
    "llm",
    "reference_code",
    "monitoring",
    "sources",
}


def _from_answers(prefetch: _Prefetch, cwd: Path, answers: dict) -> _System:
    """A system from the answers a factory's form collected.

    Keys are those of ANSWER_KEYS; `slug` is required, and names the
    directory and the repository. With `example`, the example is the draft and
    the answers override it. What the answers leave out is asked as usual.
    `llm: "account"` records that the agent's LLM is in the user's account
    environment variables, which the UI has set.
    `reference_code` is a path or URL of sample code the system is based on.
    `monitoring` is `{"feature_logging": bool, "watch": str}`, for batch and realtime systems: whether predictions log their features, and what to monitor and alert on.
    An existing system of that slug is resumed and the answers are not applied again.
    """
    unknown = set(answers) - ANSWER_KEYS
    if unknown:
        raise click.BadParameter(
            f"unknown keys {', '.join(sorted(unknown))}", param_hint="--answers"
        )
    slug = _slug(str(answers.get("slug") or ""))
    if (cwd / slug / "system.yaml").exists():
        output.info(f"{slug} already exists here; resuming it.")
        return _System(cwd / slug)
    example = answers.get("example")
    if example:
        examples = _load(REFERENCES / "new_system.py", "new_system").examples()
        if example not in examples:
            raise click.BadParameter(
                f"{example!r} is not one of {', '.join(examples)}",
                param_hint="--answers",
            )
        system = _create(cwd, slug, example=example)
    else:
        system = _create(cwd, slug)
        system.put("schema_version", 1)
        system.put(
            "system", {"name": slug.replace("-", " ").capitalize(), "slug": slug}
        )
        system.put("system.version", "0.1.0")
        system.put("system.status", "draft")
        system.put("requirements.status", "pending")
    _target(system, prefetch)
    _apply_answers(system, answers)
    system.save()
    return system


# When a scheduled run fires for each fixed cadence, unless the answers say: the UI's defaults.
DEFAULT_RUN_AT = {"hourly": ":00", "daily": "02:00", "weekly": "Mon 02:00"}
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "e",
    "eg",
    "for",
    "from",
    "g",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "per",
    "so",
    "that",
    "the",
    "their",
    "them",
    "this",
    "to",
    "with",
    "each",
    "about",
    "over",
    "every",
    "one",
}


def _source_name(story: str, index: int, taken: set[str]) -> str:
    """A synthetic source's name from the first words of its description, made unique."""
    words = [
        w
        for w in re.split(r"[^a-z0-9]+", story.lower())
        if w and w not in _STOP_WORDS and not w[0].isdigit()
    ][:3]
    base = "_".join(words) or f"source_{index + 1}"
    name, n = base, 2
    while name in taken:
        name, n = f"{base}_{n}", n + 1
    taken.add(name)
    return name


def _as_ident(name: str) -> str:
    """A name typed in a form as an identifier: "Clickstream data" is clickstream_data."""
    ident = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return f"s_{ident}" if ident[:1].isdigit() else ident


def _normalized(answers: dict, kind: str | None) -> dict:
    """The answers as _apply_answers reads them, from the shapes a factory form sends.

    A form sends its data as `sources` (feature groups, synthetic data and files), an app it
    does not want as `app.kind: none`, a batch run time that may not fit the cadence, and
    monitoring even when nothing is asked; the interview and older clients send the rest as is.
    """
    answers = dict(answers)
    sources = answers.pop("sources", None)
    if isinstance(sources, dict):
        listed = []
        for fg in sources.get("feature_groups") or []:
            listed.append(
                {
                    "name": fg.get("name"),
                    "kind": "feature_group",
                    "version": fg.get("version") or 1,
                }
            )
        taken = {
            _as_ident(str(s["name"]))
            for s in sources.get("synthetic") or []
            if s.get("name")
        }
        for i, synthetic in enumerate(sources.get("synthetic") or []):
            name = _as_ident(str(synthetic.get("name") or "")) or _source_name(
                str(synthetic.get("story") or ""), i, taken
            )
            listed.append(
                {
                    "name": name,
                    "kind": "synthetic",
                    "shape": synthetic.get("shape") or "batch",
                    "story": synthetic.get("story"),
                }
            )
        for i, file in enumerate(sources.get("files") or []):
            name = _as_ident(str(file.get("name") or "")) or f"files_{i + 1}"
            listed.append({"name": name, "kind": "file"})
        if listed:
            answers["data_sources"] = listed
    app = answers.get("app")
    if isinstance(app, dict) and app.get("kind") == "none":
        answers["app"] = {"wanted": False}
    if "consumers" not in answers and isinstance(answers.get("app"), dict):
        wanted = answers["app"].get("wanted", True)
        answers["consumers"] = "ui" if kind == "batch" and wanted else "api"
    batch = (answers.get("sla") or {}).get("batch")
    if isinstance(batch, dict) and batch.get("cadence") in DEFAULT_RUN_AT:
        fallback = DEFAULT_RUN_AT[batch["cadence"]]
        at = str(batch.get("at") or "")
        fits = (
            at
            and at.startswith(":") == fallback.startswith(":")
            and at[:1].isupper() == fallback[:1].isupper()
        )
        batch["at"] = at if fits else fallback
    monitoring = answers.get("monitoring")
    if (
        isinstance(monitoring, dict)
        and not monitoring.get("feature_logging")
        and not str(monitoring.get("watch") or "").strip()
    ):
        answers.pop("monitoring")
    return answers


def _apply_answers(system: _System, answers: dict) -> None:
    answers = _normalized(
        answers, answers.get("system_type") or system.requirements.get("system_type")
    )
    for key, dotted in (
        ("name", "system.name"),
        ("description", "requirements.description"),
        ("sla", "requirements.sla"),
        ("consumers", "requirements.consumers"),
        ("reference_code", "requirements.reference_code"),
    ):
        if answers.get(key):
            system.put(dotted, answers[key])
    kind = answers.get("system_type")
    if kind:
        if kind not in SYSTEM_TYPES:
            raise click.BadParameter(
                f"system_type {kind!r} is not one of {', '.join(SYSTEM_TYPES)}",
                param_hint="--answers",
            )
        system.put("requirements.system_type", kind)
    kind = system.requirements.get("system_type")
    if kind == "agent" and not system.requirements.get("sla"):
        system.put("requirements.sla", {"agent": {"p99_ms": 5000, "throughput_qps": 2}})
    if answers.get("data_sources"):
        # A source the draft already has keeps what the answers do not say, such
        # as where an example's documents are uploaded or what it writes.
        drafted = {
            s.get("name"): s for s in system.requirements.get("data_sources") or []
        }
        sources = []
        for source in answers["data_sources"]:
            name = _ident(str(source.get("name") or ""))
            source_kind = source.get("kind") or "synthetic"
            entry = {**drafted.get(name, {}), "name": name, "kind": source_kind}
            if source_kind == "feature_group":
                entry.update(version=int(source.get("version") or 1), status="present")
            elif source_kind == "file":
                entry.setdefault("status", "needs_download")
            else:
                entry["kind"] = "synthetic"
                entry["shape"] = (
                    "events" if source.get("shape") == "events" else "batch"
                )
                entry["status"] = "needs_generation"
                if source.get("story"):
                    system.put(f"data.{name}.generator.story", source["story"])
                system.put("data.status", "pending")
            sources.append(entry)
        system.put("requirements.data_sources", sources)
    app = answers.get("app")
    if app:
        wanted = bool(app.get("wanted", True))
        system.put(
            "app",
            {
                **(system.doc.get("app") or {}),
                **{k: v for k, v in app.items() if k in ("kind", "description") and v},
                "wanted": wanted,
                "status": "pending" if wanted else "skipped",
            },
        )
    monitoring = answers.get("monitoring")
    if monitoring:
        if not isinstance(monitoring, dict) or kind not in ("batch", "realtime"):
            raise click.BadParameter(
                "monitoring is an object, for a batch or realtime system",
                param_hint="--answers",
            )
        record = {"feature_logging": bool(monitoring.get("feature_logging"))}
        watch = str(monitoring.get("watch") or "").strip()
        if watch:
            record["watch"] = watch
        system.put("requirements.monitoring", record)
    if answers.get("repo"):
        system.put("system.repo", {"url": answers["repo"]})
    if kind == "agent" and answers.get("llm") == "account":
        record = {
            "endpoint_env": LLM_VARS["url"],
            "api_key_env": LLM_VARS["api_key"],
            "model_env": LLM_VARS["model"],
        }
        if system.doc.get("inference"):
            system.put("inference.agent.llm", record)
        else:
            system.put(
                "inference",
                {"mode": "agent", "agent": {"llm": record}, "status": "pending"},
            )


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
    qps = rates[picked] if picked < 3 else click.prompt("Requests per second", type=float)
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
    agent = (system.doc.get("inference") or {}).get("agent") or {}
    if agent.get("llm"):
        return
    # The account variables are read through the login the prefetch made.
    prefetch.ready()
    llm = _llm_env_vars(system.target.name)
    if agent:
        system.put("inference.agent.llm", llm)
    else:
        system.put(
            "inference", {"mode": "agent", "agent": {"llm": llm}, "status": "pending"}
        )
    system.save()


LLM_VARS = {"url": "LLM_URL", "api_key": "LLM_API_KEY", "model": "LLM_MODEL"}


def _llm_env_vars(slug: str) -> dict:
    """Ask for the agent's LLM and keep it in the user's account environment variables.

    Hopsworks sets account variables in every job, app and deployment the user
    starts, so the agent reads the endpoint and key from its environment and the
    key never enters system.yaml, the repository or a Claude session: it is read
    here without echo. Returns what system.yaml records, the variables' names.
    """
    record = {
        "endpoint_env": LLM_VARS["url"],
        "api_key_env": LLM_VARS["api_key"],
        "model_env": LLM_VARS["model"],
    }
    try:
        from hopsworks_common.core import env_var_api

        api = env_var_api.EnvVarsApi()
        present = {v.name for v in api.get_env_vars(include_value=False)}
    except Exception as exc:  # noqa: BLE001 - the build still runs without an LLM
        output.warn(
            f"Could not read your account environment variables ({exc}); set "
            f"{LLM_VARS['url']}, {LLM_VARS['api_key']} and {LLM_VARS['model']} in "
            "Account settings, Environment variables, before the agent is deployed."
        )
        return record
    if {LLM_VARS["url"], LLM_VARS["api_key"]} <= present and click.confirm(
        click.style(
            f"Use the LLM in your account settings ({LLM_VARS['url']}, {LLM_VARS['api_key']})?",
            bold=True,
        ),
        default=True,
    ):
        return record
    click.echo(
        f"The {slug} agent calls an OpenAI-compatible chat endpoint. The URL, model "
        "and API key are saved as your account environment variables, visible only to you."
    )
    url = click.prompt("LLM endpoint URL", default="https://api.openai.com/v1").strip()
    model = click.prompt("Model", default="gpt-4o-mini").strip()
    key = click.prompt("API key (not shown)", hide_input=True).strip()
    for name, value in (
        (LLM_VARS["url"], url),
        (LLM_VARS["model"], model),
        (LLM_VARS["api_key"], key),
    ):
        api.set_env_var(name, value, visibility="PRIVATE")
    output.success(
        f"Saved {LLM_VARS['url']}, {LLM_VARS['model']} and {LLM_VARS['api_key']} in your account settings."
    )
    return {**record, "endpoint": url, "model": model}


def _in_hopsworks_home(path: Path) -> bool:
    home = os.environ.get("HOPSFS_USER_HOME_DIR")
    if not home:
        return False
    resolved, root = path.resolve(), Path(home).resolve()
    return resolved == root or root in resolved.parents


def _repository_choice(cwd: Path) -> str:
    """The repository URL the code goes to, or "new" for one created at build start.

    A Hopsworks home is never offered, even when an earlier build made it a work
    tree: each system there is a repository of its own, rooted at its directory.
    """
    if _in_hopsworks_home(cwd):
        return "new"
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


def _register(ctx: click.Context, system: _System) -> None:
    """Add the system to the project's registry, so every member sees it in the Hopsworks UI."""
    from hopsworks.cli import factory_spec
    from hopsworks.cli.commands import mlsystem

    name = (system.doc.get("system") or {}).get("name")
    try:
        factory_spec.record_factory(ctx.meta.get(factory_spec.META), system.target)
        kind = system.requirements.get("system_type") or "batch"
        mlsystem.register(
            ctx, system.target, name, factory_spec.factory_name(ctx, f"ml-{kind}")
        )
    except Exception as exc:  # noqa: BLE001 - the interview is recorded either way
        output.warn(
            f"Not registered in the project's ML systems ({exc}); run `hops factory system register {system.target}`."
        )


def _launch(ctx: click.Context, system: _System, launch: bool) -> None:
    slug = system.target.name
    command = ["claude", f"/hops-build {slug}"]
    printable = f'claude "/hops-build {slug}"'
    status = system.target / "status.py"
    output.success(f"Interview recorded in {system.target / 'system.yaml'}")
    _register(ctx, system)
    subprocess.run([sys.executable, str(status)], check=False)
    if not launch or not shutil.which("claude"):
        click.echo(f"\nBuild it with:  cd {system.target} && {printable}")
        return
    if os.environ.get("TMUX") and shutil.which("tmux"):
        subprocess.run(
            ["tmux", "new-window", "-n", slug, "-c", str(system.target)]
            + [shlex.join(command)],
            check=True,
        )
        click.echo(
            f"\nBuilding in the tmux window '{slug}'; the Hopsworks UI shows its progress."
        )
        return
    # In the system directory, so Claude Code reads its AGENTS.md.
    os.chdir(system.target)
    os.execvp("claude", command)


def create(ctx: click.Context, answers: dict, launch: bool) -> Path:
    """Record the ML system the answers describe in ./<slug>/system.yaml, ask what they leave out, then build it.

    An existing system of that slug is resumed and the answers are not applied again.
    """
    return _run(
        ctx, launch, lambda prefetch: _from_answers(prefetch, Path.cwd(), answers)
    )


def resume(ctx: click.Context, target: Path, launch: bool) -> None:
    """Ask what the system in `target` still lacks, then start or resume its build."""
    _run(ctx, launch, lambda prefetch: _System(target))


def _run(ctx: click.Context, launch: bool, open_system) -> Path:
    prefetch = _Prefetch(ctx)
    prefetch.start()
    try:
        system = open_system(prefetch)
        _finish(ctx, prefetch, system, launch)
    finally:
        # A login still importing the SDK at interpreter exit fails noisily.
        prefetch.join(timeout=15)
    return system.target


def _finish(
    ctx: click.Context, prefetch: _Prefetch, system: _System, launch: bool
) -> None:
    if system.requirements.get("status") == "met":
        _launch(ctx, system, launch)
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
    _launch(ctx, system, launch)


# endregion
