"""``hops factory`` — the software factories of this project.

Two are built in: ``mlsystem`` builds ML systems (feature, training and
inference pipelines, and an app) and ``medallion`` builds silver and gold
layers; each keeps its own command group, ``hops factory mlsystem ...`` and
``hops factory medallion ...``. A project's own factories are YAML definitions
(apiVersion hopsworks.ai/factory/v1) its data owners create, import, clone and
delete; each gets the same commands, ``hops factory <name> create|list|...``.
"""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path

import click
import yaml
from hopsworks.cli import factory_spec, output, session
from hopsworks.cli.commands import build, medallion, mlsystem
from hopsworks.cli.commands.medallion import medallion_group
from hopsworks.cli.commands.mlsystem import mlsystem_group


FACTORIES = (mlsystem_group, medallion_group)
TEMPLATE = Path(__file__).resolve().parent.parent / "templates" / "hops-factory.md"


class _FactoryGroup(click.Group):
    """Resolves a name that is not a built-in or a command to the project factory of that name."""

    def get_command(self, ctx: click.Context, cmd_name: str):
        command = super().get_command(ctx, cmd_name)
        if command is not None or ctx.resilient_parsing:
            return command
        if not factory_spec.NAME.match(cmd_name):
            return None
        try:
            from hopsworks_common.core import factory_api

            session.get_project(ctx)
            definition = factory_api._get(cmd_name)
        except Exception:  # noqa: BLE001 - an unknown name reads as an unknown command
            return None
        return _project_factory_group(definition)


@click.group("factory", cls=_FactoryGroup)
def factory_group() -> None:
    """The software factories: mlsystem builds ML systems, medallion builds silver and gold layers, and the project's own."""


for _group in FACTORIES:
    factory_group.add_command(_group)


# region Managing factories


@factory_group.command("list")
@click.pass_context
def factory_list(ctx: click.Context) -> None:
    """List the factories: the built-in ones and the project's own, with how many systems each built."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factories = factory_api._list()
    if output.JSON_MODE:
        output.print_json(
            [
                {k: v for k, v in f.items() if k not in ("spec", "definition")}
                for f in factories
            ]
        )
        return
    output.print_table(
        ["NAME", "TITLE", "KIND", "VERSION", "SYSTEMS", "ENABLED"],
        [
            [
                f.get("name"),
                f.get("title"),
                "built-in" if f.get("builtin") else "project",
                f.get("currentVersion"),
                f.get("systems", 0),
                "yes" if f.get("enabled") else "no",
            ]
            for f in factories
        ],
    )


@factory_group.command("get")
@click.argument("name")
@click.option("--version", type=int, help="An earlier version of the definition.")
@click.pass_context
def factory_get(ctx: click.Context, name: str, version: int | None) -> None:
    """Print the YAML definition of factory NAME."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factory = factory_api._get(name, version)
    if output.JSON_MODE:
        output.print_json(factory)
        return
    click.echo(factory.get("definition", ""), nl=False)


@factory_group.command("export")
@click.argument("name")
@click.option("--version", type=int, help="An earlier version of the definition.")
@click.option(
    "-o",
    "--output",
    "out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="The file to write; defaults to <name>.factory.yaml.",
)
@click.pass_context
def factory_export(
    ctx: click.Context, name: str, version: int | None, out: Path | None
) -> None:
    """Write the YAML definition of factory NAME to a file, to import in another project or cluster."""
    from hopsworks_common.core import factory_api

    project = session.get_project(ctx)
    factory = factory_api._get(name, version)
    target = out or Path(f"{name}.factory.yaml")
    header = f"# Exported from project {getattr(project, 'name', '?')}, factory {name} version {factory.get('version')}.\n"
    target.write_text(header + factory.get("definition", ""), encoding="utf-8")
    output.success(f"Wrote {target}")


@factory_group.command("validate")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def factory_validate(file: Path) -> None:
    """Check the factory definition in FILE, without a cluster."""
    found = factory_spec.text_problems(file.read_text(encoding="utf-8"))
    if found:
        raise click.ClickException("\n  ".join(["invalid definition:", *found]))
    output.success(f"{file} is a valid factory definition")


@factory_group.command("import")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--name", help="Import it under this name instead of the one it carries.")
@click.option(
    "--update",
    is_flag=True,
    help="Save it as a new version of the factory of its name.",
)
@click.option("--yes", is_flag=True, help="Skip the review and the confirmation.")
@click.pass_context
def factory_import(
    ctx: click.Context, file: Path, name: str | None, update: bool, yes: bool
) -> None:
    """Import the factory definition in FILE, written here or exported from another project.

    The review prints its questions, phases, skills and its build instructions in full:
    Claude Code follows those instructions in your Terminal, with your credentials.
    """
    from hopsworks_common.core import factory_api

    text = file.read_text(encoding="utf-8")
    found = factory_spec.text_problems(text)
    if found:
        raise click.ClickException("\n  ".join(["invalid definition:", *found]))
    doc = factory_spec.parse(text)
    if not yes:
        _review(doc)
        click.confirm("Import this factory?", abort=True)
    session.get_project(ctx)
    if update:
        factory = factory_api._update(name or doc["name"], text)
    else:
        factory = factory_api._create(text, name)
    output.success(
        f"Factory {factory.get('name')} version {factory.get('currentVersion')} saved"
    )


@factory_group.command("clone")
@click.argument("source")
@click.argument("name")
@click.option("--title", help="Its title; defaults to the source's title and (copy).")
@click.pass_context
def factory_clone(
    ctx: click.Context, source: str, name: str, title: str | None
) -> None:
    """Clone factory SOURCE, built-in or the project's own, into a new project factory NAME."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    original = factory_api._get(source)
    text = original.get("definition", "")
    new_title = title or f"{original.get('title', source)} (copy)"
    text = _replace_top(text, "title", json.dumps(new_title))
    factory = factory_api._create(text, name)
    output.success(f"Cloned {source} into {factory.get('name')}")


@factory_group.command("enable")
@click.argument("name")
@click.pass_context
def factory_enable(ctx: click.Context, name: str) -> None:
    """Show the project factory NAME in the Factory again."""
    _set_enabled(ctx, name, True)


@factory_group.command("disable")
@click.argument("name")
@click.pass_context
def factory_disable(ctx: click.Context, name: str) -> None:
    """Hide the project factory NAME from the Factory; its systems are kept."""
    _set_enabled(ctx, name, False)


@factory_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def factory_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete the project factory NAME; refused while systems it built still exist."""
    from hopsworks_common.core import factory_api

    if not yes:
        click.confirm(f"Delete factory {name}?", abort=True)
    session.get_project(ctx)
    factory_api._delete(name)
    output.success(f"Deleted factory {name}")


def _set_enabled(ctx: click.Context, name: str, enabled: bool) -> None:
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factory_api._set_enabled(name, enabled)
    output.success(f"Factory {name} {'enabled' if enabled else 'disabled'}")


def _replace_top(text: str, key: str, value: str) -> str:
    """The YAML with its top-level `key` set to `value`, keeping everything else as written."""
    line = re.compile(rf"(?m)^{key}:[^\n]*$")
    if line.search(text):
        return line.sub(lambda _: f"{key}: {value}", text, count=1)
    return f"{key}: {value}\n{text}"


def _review(doc: dict) -> None:
    click.echo(f"\n{doc.get('title')} ({doc.get('name')})")
    if doc.get("description"):
        click.echo(f"  {doc['description']}")
    click.echo("\nQuestions:")
    for field in factory_spec.fields(doc):
        flag = " (required)" if field.get("required") else ""
        click.echo(f"  - {field.get('label')} [{field.get('type')}]{flag}")
    click.echo(
        "\nPhases: " + ", ".join(p.get("label", "?") for p in doc.get("phases", []))
    )
    build_spec = doc.get("build") or {}
    if build_spec.get("builtin"):
        click.echo(f"Builds with the built-in {build_spec['builtin']} factory.")
    if build_spec.get("skills"):
        click.echo("Skills: " + ", ".join(build_spec["skills"]))
    if build_spec.get("instructions"):
        click.echo("\nClaude Code will follow these instructions in your Terminal:\n")
        click.echo(build_spec["instructions"])


# endregion

# region A project factory's commands


def _project_factory_group(definition: dict) -> click.Group:
    """The commands of a project factory: create, list, status, register, remove and delete."""
    name = definition["name"]

    @click.group(
        name,
        help=f"{definition.get('title')}: {definition.get('description') or 'a project factory.'}",
    )
    def group() -> None:
        pass

    @group.command("create")
    @click.option(
        "--answers",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        required=True,
        help="A JSON file of the answers to the factory's questions, as the Hopsworks UI writes it.",
    )
    @click.option(
        "--no-launch",
        is_flag=True,
        help="Record the system but do not start Claude Code.",
    )
    @click.pass_context
    def create(ctx: click.Context, answers: Path, no_launch: bool) -> None:
        """Record a new system in ./<slug>/system.yaml, register it, and build it with Claude Code."""
        create_system(ctx, definition, answers, not no_launch)

    @group.command("list")
    @click.pass_context
    def listing(ctx: click.Context) -> None:
        """List the systems this factory built, newest first."""
        from hopsworks_common.core import ml_system_api

        session.get_project(ctx)
        systems = ml_system_api._list(name)
        if output.JSON_MODE:
            output.print_json(systems)
            return
        output.print_table(
            ["ID", "NAME", "OWNER", "VERSION", "PATH"],
            [
                [
                    s.get("id"),
                    s.get("name"),
                    s.get("ownerName") or s.get("owner"),
                    s.get("factoryVersion"),
                    s.get("pathToCode"),
                ]
                for s in systems
            ],
        )

    @group.command("register")
    @click.argument(
        "path",
        type=click.Path(file_okay=False, path_type=Path),
        default=Path(),
    )
    @click.option(
        "--name", "display_name", help="Display name; defaults to the directory name."
    )
    @click.pass_context
    def register(ctx: click.Context, path: Path, display_name: str | None) -> None:
        """Register the system in PATH (default: the current directory) with this factory, or refresh it."""
        entry = mlsystem.register(ctx, path, display_name, name)
        output.success(f"Registered {entry.get('name')} at {entry.get('pathToCode')}")

    group.add_command(mlsystem.mlsystem_status)
    group.add_command(mlsystem.mlsystem_remove)
    group.add_command(mlsystem.mlsystem_delete)
    return group


def create_system(
    ctx: click.Context, definition: dict, answers_path: Path, launch: bool
) -> Path | None:
    """Create a system with a project factory from the answers file; returns its directory.

    A factory whose build is a built-in's (a clone of mlsystem or medallion) hands the
    component's answers to that built-in's create, with the factory's own answers and
    instructions recorded in system.yaml; any other writes system.yaml itself and starts
    Claude Code on the command generated from the factory's instructions.
    """
    spec = definition["spec"]
    try:
        answers = json.loads(answers_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise click.BadParameter(str(exc), param_hint="--answers") from exc
    if not isinstance(answers, dict):
        raise click.BadParameter(
            "the answers must be a JSON object", param_hint="--answers"
        )
    found = factory_spec.answer_problems(spec, answers)
    if found:
        raise click.ClickException("\n  ".join(["invalid answers:", *found]))
    shown = factory_spec.visible(spec, answers)
    build_spec = spec.get("build") or {}
    builtin = build_spec.get("builtin")
    if builtin:
        own = {
            f["id"]: answers[f["id"]]
            for f in shown
            if f["type"] != "component" and f["id"] in answers
        }
        ctx.meta[factory_spec.META] = {
            "name": definition["name"],
            "version": definition["version"],
            "instructions": build_spec.get("instructions"),
            "extra": own,
        }
        components = {
            f["component"]: f["id"] for f in shown if f["type"] == "component"
        }
        _delegate(ctx, builtin, components, answers, launch)
        return None
    return _create_own(ctx, definition, spec, shown, answers, launch)


def _delegate(
    ctx: click.Context, builtin: str, components: dict, answers: dict, launch: bool
) -> None:
    targets = {
        "mlsystem.requirements": lambda path: ctx.invoke(
            build.create_cmd,
            slug=None,
            no_launch=not launch,
            example=None,
            answers=path,
        ),
        "medallion.silver": lambda path: ctx.invoke(
            medallion.medallion_silver, answers=path, no_launch=not launch
        ),
        "medallion.gold": lambda path: ctx.invoke(
            medallion.medallion_gold, answers=path, no_launch=not launch
        ),
    }
    for component, field_id in components.items():
        if component.split(".")[0] != builtin or not isinstance(
            answers.get(field_id), dict
        ):
            continue
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", prefix="hops-factory-", delete=False
        ) as handle:
            json.dump(answers[field_id], handle)
        try:
            targets[component](Path(handle.name))
        finally:
            Path(handle.name).unlink(missing_ok=True)
        return
    raise click.ClickException(f"the answers hold no {builtin} section to build from")


def _create_own(
    ctx: click.Context,
    definition: dict,
    spec: dict,
    shown: list[dict],
    answers: dict,
    launch: bool,
) -> Path:
    name, version = definition["name"], definition["version"]
    slug = factory_spec.slug_of(spec, answers)
    target = Path.cwd() / slug
    system_file = target / "system.yaml"
    if system_file.exists():
        output.info(f"{slug} already exists here; resuming it.")
    else:
        target.mkdir(parents=True, exist_ok=True)
        phases = [
            {k: p[k] for k in ("key", "label", "minutes") if k in p}
            for p in spec["phases"]
        ]
        doc: dict = {
            "schema_version": 1,
            "factory": {"name": name, "version": version, "phases": phases},
            "system": {
                "name": slug.replace("-", " ").capitalize(),
                "slug": slug,
                "version": "0.1.0",
                "status": "draft",
            },
            "requirements": {
                f["id"]: answers[f["id"]] for f in shown if f["id"] in answers
            },
        }
        for phase in phases:
            doc.setdefault(phase["key"], {})["status"] = "pending"
        system_file.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    _write_build_files(target, definition, spec, slug)
    output.success(f"Recorded in {system_file}")
    try:
        mlsystem.register(ctx, target, slug, name)
    except Exception as exc:  # noqa: BLE001 - the system is recorded either way
        output.warn(
            f"Not registered in the project's Factory ({exc}); run `hops factory {name} register {target}`."
        )
    medallion._launch(target, launch, f"/hops-factory-{name} {slug}")
    return target


def _write_build_files(target: Path, definition: dict, spec: dict, slug: str) -> None:
    """The system's AGENTS.md and the slash command Claude Code builds it with."""
    name = definition["name"]
    build_spec = spec.get("build") or {}
    skills = build_spec.get("skills") or []
    values = {
        "{name}": name,
        "{title}": spec.get("title", name),
        "{version}": str(definition["version"]),
        "{slug}": slug,
        "{phases}": ", ".join(f"`{p['key']}` ({p['label']})" for p in spec["phases"]),
        "{skills}": (
            "- Load these skills before you start: "
            + ", ".join(f"**{s}**" for s in skills)
            + ".\n"
            if skills
            else ""
        ),
    }
    text = TEMPLATE.read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(key, value)
    # Last, so nothing in the user's instructions is read as a placeholder.
    text = text.replace("{instructions}", build_spec.get("instructions", "").strip())
    commands = target / ".claude" / "commands"
    commands.mkdir(parents=True, exist_ok=True)
    (commands / f"hops-factory-{name}.md").write_text(text, encoding="utf-8")
    agents = target / "AGENTS.md"
    if not agents.exists():
        agents.write_text(
            f"# {slug}\n\n"
            f"Built by the {spec.get('title', name)} factory (`{name}`) from `system.yaml`.\n"
            f'Build or resume it with `claude "/hops-factory-{name} {slug}"` in this directory.\n',
            encoding="utf-8",
        )


# endregion
