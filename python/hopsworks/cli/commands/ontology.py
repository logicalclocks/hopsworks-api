"""``hops ontology``: ask an ``ontology`` factory system questions about its data, and check it can answer them.

Both commands reach the system's app through the Hopsworks app proxy with a proxy token, which only a Data owner or Data scientist can mint, and only on a cluster with ``hopsable_proxy_enabled``.
The app is named after the system's slug.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click
from hopsworks.cli import output, session
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import ontology_api


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


@click.group("ontology")
def ontology_group() -> None:
    """Ontology systems: ask questions about the data in plain language, check readiness."""


def _shown(events: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    for event in events:
        if event.get("type") == "step":
            output.info("  %s", ontology_api._step_line(event))
        yield event


def _cell(value: Any) -> str:
    return "" if value is None else str(value)


def _print_answer(answer: dict[str, Any]) -> None:
    if output.JSON_MODE:
        output.print_json(answer)
        return
    click.echo(answer["answer"] or "(no answer text)")
    if answer["sql"]:
        click.echo("\nSQL:\n" + answer["sql"])
    rows = answer["rows"]
    if rows:
        headers = list(dict.fromkeys(key for row in rows for key in row))
        click.echo("")
        output.print_table(
            headers, [[_cell(row.get(h)) for h in headers] for row in rows]
        )
    if answer["truncated"]:
        click.echo(f"(the first {len(rows)} of {answer['row_count']} rows)")


@ontology_group.command("ask")
@click.argument("slug")
@click.argument("question")
@click.option(
    "--conversation-id",
    help="Continue the conversation with this UUID: follow-up questions resolve against its earlier turns, and the turn is saved in it.",
)
@click.option(
    "--timeout",
    type=float,
    default=900.0,
    show_default=True,
    help="Seconds to wait for the answer.",
)
@click.pass_context
def ontology_ask(
    ctx: click.Context,
    slug: str,
    question: str,
    conversation_id: str | None,
    timeout: float,
) -> None:
    """Ask the ontology system SLUG a QUESTION; print the answer, its SQL and its rows.

    The agent's steps are printed as it takes them.
    At most 100 rows are shown, and the output says how many the query returned.

    Args:
        ctx: Click context.
        slug: The system, which is also its app's name.
        question: The question, in plain language.
        conversation_id: The conversation to continue.
        timeout: Seconds to wait for the answer.
    """
    session.get_project(ctx)
    failure = None
    try:
        answer = ontology_api._answer(
            _shown(ontology_api._ask(slug, question, conversation_id, timeout=timeout))
        )
    except RestAPIError as exc:
        failure = ontology_api._refusal(exc, slug)
        if failure is None:
            raise
    # OSError covers a timeout and every requests connection error.
    except (RuntimeError, OSError) as exc:
        failure = f"{slug}: {exc}"
    # Raised outside the handler, so the CLI's REST error rewriting does not append the raw error again.
    if failure:
        raise click.ClickException(failure)
    _print_answer(answer)


@ontology_group.command("status")
@click.argument("slug")
@click.pass_context
def ontology_status(ctx: click.Context, slug: str) -> None:
    """Show whether the ontology system SLUG can answer questions, and what stands in the way.

    Exits 1 when it cannot.

    Args:
        ctx: Click context.
        slug: The system, which is also its app's name.
    """
    project = session.get_project(ctx)
    app = project.get_app_api().get_app(slug)
    if app is None:
        raise click.ClickException(f"no app {slug} in project {project.name}")
    facts: dict[str, Any] = {
        "app": slug,
        "state": app.state,
        "serving": app.serving,
        "url": app.app_url,
    }
    if app.serving:
        failure = None
        try:
            facts.update(ontology_api._readiness(slug))
        except RestAPIError as exc:
            failure = ontology_api._refusal(exc, slug)
            if failure is None:
                raise
        if failure:
            raise click.ClickException(failure)
    else:
        facts.update(
            ready=False,
            blockers=[
                f"The app is {app.state}; start it with `hops app start {slug}`."
            ],
            unverified=[],
        )

    if output.JSON_MODE:
        output.print_json(facts)
    else:
        shown = {k: v for k, v in facts.items() if k not in ("blockers", "unverified")}
        output.print_table(
            ["FIELD", "VALUE"],
            [[k, ", ".join(v) if isinstance(v, list) else v] for k, v in shown.items()],
        )
        for line in facts["blockers"]:
            output.warn("blocker: %s", line)
        for line in facts["unverified"]:
            output.info("unverified: %s", line)
    if not facts["ready"]:
        ctx.exit(1)
