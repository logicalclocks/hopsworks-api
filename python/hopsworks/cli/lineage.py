"""Shared rendering for ``hops <entity> lineage`` commands.

The SDK's provenance accessors (``get_*_provenance``,
``get_parent_feature_groups``, ...) return
``hsfs.core.explicit_provenance.Links`` results, which bucket related
artifacts into ``accessible`` (full domain objects the user can read),
``inaccessible`` (exist but no access), ``deleted`` (removed), and ``faulty``
(corrupted).
This module flattens one or more such results into a single table or JSON
payload so every entity's lineage command renders identically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hopsworks.cli import output


if TYPE_CHECKING:
    from collections.abc import Iterator


# Links buckets, in the order they are listed; the bucket name is the status.
_BUCKETS = ("accessible", "inaccessible", "deleted", "faulty")


def fetch(fn: Any) -> Any:
    """Call a provenance accessor and return its result.

    The SDK accessors return None (or an empty Links) when an edge is simply
    absent and raise RestAPIError only on real backend errors. We deliberately
    do NOT swallow exceptions here: a permission, auth, or server error must
    surface and fail the command rather than render misleadingly as "no
    lineage". Returns None only when the accessor itself is absent (fn is None).

    Args:
        fn: A zero-argument provenance accessor, or None.

    Returns:
        The accessor's result (which may itself be None), or None if fn is None.
    """
    return fn() if fn is not None else None


def _iter_artifacts(links: Any) -> Iterator[tuple[Any, str]]:
    """Yield ``(artifact, status)`` pairs from a Links result or plain value.

    Accepts an ``explicit_provenance.Links`` (whose four buckets carry the
    status), a list of objects, or a single object.
    This lets callers mix provenance links with a directly-resolved object,
    such as the model a deployment serves.
    """
    if links is None:
        return
    if hasattr(links, "accessible") and hasattr(links, "deleted"):
        for bucket in _BUCKETS:
            for art in getattr(links, bucket, None) or []:
                yield art, bucket
        return
    if isinstance(links, (list, tuple)):
        for art in links:
            yield art, "accessible"
        return
    yield links, "accessible"


def render(entity: str, sections: list[tuple[str, str, Any]]) -> None:
    """Render lineage for an entity as a table or JSON.

    Args:
        entity: Human label for the entity whose lineage this is.
        sections: ``(direction, relationship, links)`` triples, where
            ``links`` is a Links result, a list of artifacts, a single
            object, or None when the SDK has no such edge.
    """
    if output.JSON_MODE:
        payload: dict[str, Any] = {"entity": entity, "links": []}
        for direction, relationship, links in sections:
            artifacts = [
                {
                    "name": getattr(art, "name", None),
                    "version": getattr(art, "version", None),
                    "status": status,
                }
                for art, status in _iter_artifacts(links)
            ]
            if artifacts:
                payload["links"].append(
                    {
                        "direction": direction,
                        "relationship": relationship,
                        "artifacts": artifacts,
                    }
                )
        output.print_json(payload)
        return

    rows = [
        [
            direction,
            relationship,
            getattr(art, "name", "?"),
            getattr(art, "version", "-"),
            status,
        ]
        for direction, relationship, links in sections
        for art, status in _iter_artifacts(links)
    ]
    if not rows:
        output.info("No lineage links found for %s.", entity)
        return
    output.print_table(
        ["DIRECTION", "RELATIONSHIP", "ARTIFACT", "VERSION", "STATUS"], rows
    )
