"""``hops search`` — feature-store search, project-scoped or global.

Wraps ``project.get_search_api().feature_store(...)``: a single REST call
against the OpenSearch ``featurestore`` index that returns matching
feature groups, feature views, training datasets, and features. Project
scope is the default; ``--global`` searches every project the caller has
access to.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


_DOC_TYPES = ["all", "feature_group", "feature_view", "training_dataset", "feature"]


@click.group("search")
def search_group() -> None:
    """Search the Hopsworks feature store (project-scoped by default)."""


@search_group.command("ls")
@click.argument("term", required=False)
@click.option(
    "--global",
    "global_search",
    is_flag=True,
    help="Search across all projects the caller can see (default: current project only).",
)
@click.option(
    "--type",
    "doc_type",
    type=click.Choice(_DOC_TYPES, case_sensitive=False),
    default="all",
    show_default=True,
    help="Restrict results to one entity type.",
)
@click.option(
    "--keyword",
    "keywords",
    multiple=True,
    help="Filter by keyword. Repeat for multiple keywords.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    help='Filter by tag, format "name:key=value". Repeat for multiple tags.',
)
@click.option("--limit", type=int, default=20, show_default=True, help="Max results.")
@click.pass_context
def search_ls(
    ctx: click.Context,
    term: str | None,
    global_search: bool,
    doc_type: str,
    keywords: tuple[str, ...],
    tags: tuple[str, ...],
    limit: int,
) -> None:
    """Search for entities matching TERM.

    Args:
        ctx: Click context.
        term: Search string. May be omitted if ``--keyword`` or ``--tag``
            is given (the SDK requires at least one of the three).
        global_search: When True, search across all projects.
        doc_type: One of ``all``, ``feature_group``, ``feature_view``,
            ``training_dataset``, ``feature``.
        keywords: Repeatable ``--keyword`` filter.
        tags: Repeatable ``--tag name:key=value`` filter.
        limit: Page size cap.
    """
    if not term and not keywords and not tags:
        raise click.UsageError(
            "Provide a search TERM, or at least one --keyword / --tag filter."
        )

    parsed_tags = [_parse_tag(t) for t in tags]

    project = session.get_project(ctx)
    api = project.get_search_api()

    common_kwargs: dict[str, Any] = {
        "search_term": term,
        "keyword_filter": list(keywords) or None,
        "tag_filter": parsed_tags or None,
        "limit": limit,
        "global_search": global_search,
    }

    try:
        if doc_type == "all":
            result = api.feature_store(**common_kwargs)
            rows = (
                [_row("feature_group", r) for r in result.feature_groups]
                + [_row("feature_view", r) for r in result.feature_views]
                + [_row("training_dataset", r) for r in result.training_datasets]
                + [_row("feature", r) for r in result.features]
            )
        elif doc_type == "feature_group":
            rows = [
                _row("feature_group", r) for r in api.feature_groups(**common_kwargs)
            ]
        elif doc_type == "feature_view":
            rows = [_row("feature_view", r) for r in api.feature_views(**common_kwargs)]
        elif doc_type == "training_dataset":
            rows = [
                _row("training_dataset", r)
                for r in api.training_datasets(**common_kwargs)
            ]
        else:  # feature
            rows = [_row("feature", r) for r in api.features(**common_kwargs)]
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Search failed: {exc}") from exc

    if not rows:
        scope = "all projects" if global_search else "current project"
        output.info("No results in %s.", scope)
        return

    output.print_table(["KIND", "NAME", "VERSION", "PROJECT", "DESCRIPTION"], rows)


def _row(kind: str, item: Any) -> list[Any]:
    """Flatten a SearchResultItem into a fixed-width table row.

    Features don't have a ``version`` attribute; fall back to ``-``.
    Description is truncated to keep the table readable on narrow shells.
    """
    name = getattr(item, "name", "?")
    version = getattr(item, "version", None)
    project_obj = getattr(item, "project", None)
    project_name = getattr(project_obj, "name", "-") if project_obj else "-"
    desc = (getattr(item, "description", None) or "").splitlines()[0:1]
    desc_text = (desc[0] if desc else "")[:60]
    return [kind, name, str(version) if version else "-", project_name, desc_text]


def _parse_tag(spec: str) -> dict[str, str]:
    """Parse ``name:key=value`` into the {name, key, value} dict the SDK expects.

    Raise ``click.BadParameter`` (rather than ``ValueError``) so the CLI
    user sees a clean message instead of a Python traceback.
    """
    if ":" not in spec or "=" not in spec:
        raise click.BadParameter(
            f"--tag must be 'name:key=value', got {spec!r}", param_hint="--tag"
        )
    name, rest = spec.split(":", 1)
    key, value = rest.split("=", 1)
    if not (name.strip() and key.strip() and value.strip()):
        raise click.BadParameter(
            f"--tag fields must be non-empty, got {spec!r}", param_hint="--tag"
        )
    return {"name": name.strip(), "key": key.strip(), "value": value.strip()}
