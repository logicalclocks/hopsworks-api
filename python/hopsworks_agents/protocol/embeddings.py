"""Embedding models from the model registry, so a serving pod never downloads one.

``sentence_transformers`` fetches a model from the Hugging Face hub the first
time it is named. On a laptop that is fine; in a deployment it is a download
on every pod start, from a network the cluster may not have. The two
functions here split that in two: :func:`register_sentence_transformer`
downloads once, wherever there is internet, and puts the files in the
project's model registry; :func:`load_sentence_transformer` loads from the
registry, which is inside the cluster, and only falls back to the hub when
the model was never registered.

The registry name is the hub name with everything but letters, digits and
underscores replaced, so ``all-MiniLM-L6-v2`` is ``all_MiniLM_L6_v2``; pass
``name`` to choose another.
"""

from __future__ import annotations

import logging
import re
import tempfile
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover
    from sentence_transformers import SentenceTransformer

log = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def registry_name(model_name: str) -> str:
    """The model registry name for a hub model name."""
    return re.sub(r"[^A-Za-z0-9_]", "_", model_name)


def _registry(project):
    if project is None:
        import hopsworks

        project = hopsworks.login()
    return project.get_model_registry()


def _latest(registry, name: str, version: int | None):
    """The registered model, or None; the highest version unless one is asked for."""
    if version is not None:
        return registry.get_model(name, version)
    models = registry.get_models(name) or []
    return max(models, key=lambda m: m.version) if models else None


def register_sentence_transformer(
    model_name: str = DEFAULT_MODEL,
    *,
    name: str | None = None,
    description: str | None = None,
    force: bool = False,
    project: Any = None,
):
    """Download a sentence-transformers model once and put it in the model registry.

        register_sentence_transformer("all-MiniLM-L6-v2")

    Run this where there is internet: a notebook, or the feature pipeline that
    embeds the data, so the same model that wrote the vectors is the one the
    agent later loads. It is idempotent: a model already registered under the
    name is returned as is, and ``force=True`` registers a new version.
    ``project`` is the project to use, ``hopsworks.login()`` when omitted.

    Returns:
        The registered ``Model``.
    """
    from sentence_transformers import SentenceTransformer

    name = name or registry_name(model_name)
    registry = _registry(project)
    existing = _latest(registry, name, None)
    if existing is not None and not force:
        log.info(
            "%s is already in the model registry as %s v%s",
            model_name,
            name,
            existing.version,
        )
        return existing
    with tempfile.TemporaryDirectory() as tmp:
        SentenceTransformer(model_name).save(tmp)
        model = registry.python.create_model(
            name,
            description=description
            or f"sentence-transformers {model_name}, registered by hopsworks_agents",
        )
        saved = model.save(tmp)
    log.info("Registered %s as %s v%s", model_name, name, saved.version)
    return saved


def load_sentence_transformer(
    model_name: str = DEFAULT_MODEL,
    *,
    name: str | None = None,
    version: int | None = None,
    fallback: bool = True,
    project: Any = None,
) -> SentenceTransformer:
    """A ``SentenceTransformer`` from the model registry.

        embed = load_sentence_transformer("all-MiniLM-L6-v2")

    The files come from the registry when the model was registered with
    :func:`register_sentence_transformer` (the highest version, or
    ``version``), downloaded through the SDK's model cache so a pod fetches
    them once. When it was not, or the registry cannot be reached, the model
    comes from the hub as before, with a warning; ``fallback=False`` makes that
    an error instead, for deployments that must never reach the internet.
    """
    from sentence_transformers import SentenceTransformer

    name = name or registry_name(model_name)
    registered = None
    try:
        registered = _latest(_registry(project), name, version)
    except Exception:  # noqa: BLE001 — a registry problem is not an embedding problem
        if not fallback:
            raise
        log.exception("Could not look up %s in the model registry", name)
    if registered is not None:
        path = registered.download()
        log.info(
            "Loading %s from the model registry (%s v%s)",
            model_name,
            name,
            registered.version,
        )
        return SentenceTransformer(path)
    if not fallback:
        raise LookupError(
            f"{model_name} is not in the model registry as {name!r}; register it "
            f"once with register_sentence_transformer({model_name!r})"
        )
    log.warning(
        "%s is not in the model registry; downloading it from the hub. Register it "
        "once with register_sentence_transformer(%r) so deployments load it locally.",
        model_name,
        model_name,
    )
    return SentenceTransformer(model_name)
