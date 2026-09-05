"""``hops search`` — search the project's OpenSearch index, or every project's.

One REST call against the ``featurestore`` index returns matching feature
groups, feature views, training datasets, features, jobs, apps, models,
deployments and agents. Project scope is the default; ``--global`` searches
every project the caller belongs to.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


# CLI spelling -> backend docType. Apps are jobs of type PythonApp and agents are
# deployments without a registered model; the backend reports each in its own
# bucket, so JOB excludes apps and DEPLOYMENT excludes agents.
_DOC_TYPES = {
    "all": "ALL",
    "feature_group": "FEATUREGROUP",
    "feature_view": "FEATUREVIEW",
    "training_dataset": "TRAININGDATASET",
    "feature": "FEATURE",
    "job": "JOB",
    "app": "APP",
    "model": "MODEL",
    "deployment": "DEPLOYMENT",
    "agent": "AGENT",
}

# Result attribute -> KIND label, in display order.
_BUCKETS = [
    ("feature_groups", "feature_group"),
    ("feature_views", "feature_view"),
    ("training_datasets", "training_dataset"),
    ("features", "feature"),
    ("jobs", "job"),
    ("apps", "app"),
    ("models", "model"),
    ("deployments", "deployment"),
    ("agents", "agent"),
]


@click.command(
    "search",
    epilog="""\b
Examples:
  hops search "credit card"            free text over names, descriptions and features
  hops search "credit card" --global   the same across every project you belong to
  hops search --type model minilm      only models; also job, app, deployment, agent, feature, ...
  hops search --keyword pii            entities carrying the keyword
  hops search --tag quality:owner=risk entities whose tag "quality" has key owner = risk
  hops search --tag gdpr:pii=true --keyword finance   filters combine
""",
)
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
    type=click.Choice(list(_DOC_TYPES), case_sensitive=False),
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
def search_cmd(
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
        doc_type: One of the ``--type`` choices.
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

    try:
        result = api._search(
            doc_type=_DOC_TYPES[doc_type.lower()],
            search_term=term,
            keyword_filter=list(keywords) or None,
            tag_filter=parsed_tags or None,
            limit=limit,
            global_search=global_search,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Search failed: {exc}") from exc

    rows = [
        _row(kind, item)
        for attr, kind in _BUCKETS
        for item in getattr(result, attr, None) or []
    ]

    if not rows:
        if global_search:
            output.info("No results in any project you belong to.")
        else:
            output.info(
                "No results in project %s. Add --global to search every project "
                "you belong to.",
                getattr(project, "name", None) or "?",
            )
        return

    output.print_table(["KIND", "NAME", "VERSION", "PROJECT", "DETAIL"], rows)


def _row(kind: str, item: Any) -> list[Any]:
    """Flatten a search result item into a fixed-width table row.

    Features and jobs have no version; models and jobs have no description, so
    DETAIL falls back to the type-specific field the index carries for them
    (framework, job type, serving tool). Text is cut to one line of 60 chars.
    """
    name = getattr(item, "name", "?")
    version = getattr(item, "version", None)
    project_obj = getattr(item, "project", None)
    project_name = getattr(project_obj, "name", "-") if project_obj else "-"
    raw = getattr(item, "raw_data", None) or {}
    detail = (
        getattr(item, "description", None)
        or raw.get("framework")
        or raw.get("jobType")
        or raw.get("servingTool")
        or ""
    )
    first_line = detail.splitlines()[0] if detail else ""
    return [kind, name, str(version) if version else "-", project_name, first_line[:60]]


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
