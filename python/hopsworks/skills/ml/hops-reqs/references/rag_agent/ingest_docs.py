# ruff: noqa: INP001
"""Embed the help desk documents into the feature group the agent searches.

    python ingest_docs.py [--docs Resources/helpdesk-docs] [--pages <name>] [--fg <name>] [--model <name>]

Runs as a job (`<slug>-ingest-docs`), after `hops fg parse <docs> --to <pages>`
and again whenever documents are added: the pages group holds every document
under the docs directory as Markdown, one row per page. Each page is cut into
chunks, each chunk embedded with the sentence-transformers model from the Model
Registry, and written with its document name, HopsFS path, a file-browser URL,
page and offset. Chunk ids are stable, so a re-run overwrites a document's rows;
rows of a document that was removed from the directory, or of chunks it no
longer has, are deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
from dataclasses import dataclass

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_logger = logging.getLogger("ingest_docs")

# region Pages, paragraphs and chunks
#
# A chunk is a run of paragraphs from one page, cut at a paragraph boundary: at
# least MIN_CHARS unless the page ends first, and at most MAX_CHARS, a longer
# paragraph being cut at sentence ends. Each keeps the 1-based `page` (a PDF
# page or an image; 1 for a document hops fg parse reads whole) and the 0-based
# `offset` of its first paragraph in that page.

MIN_CHARS = 400
MAX_CHARS = 1200


@dataclass(frozen=True)
class Chunk:
    """One embeddable part of a document."""

    doc_name: str
    page: int
    offset: int
    text: str

    @property
    def chunk_id(self) -> str:
        """Stable across runs, so re-ingesting a document overwrites its rows."""
        key = f"{self.doc_name}\0{self.page}\0{self.offset}"
        return hashlib.sha1(key.encode()).hexdigest()[:16]


def _paragraphs(text: str) -> list[str]:
    return [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]


def _split_long(paragraph: str) -> list[str]:
    if len(paragraph) <= MAX_CHARS:
        return [paragraph]
    parts, current = [], ""
    for sentence in re.split(r"(?<=[.!?])\s+", paragraph):
        if current and len(current) + len(sentence) + 1 > MAX_CHARS:
            parts.append(current)
            current = ""
        # A sentence longer than MAX_CHARS is cut where it must be.
        while len(sentence) > MAX_CHARS:
            parts.append(sentence[:MAX_CHARS])
            sentence = sentence[MAX_CHARS:]
        current = f"{current} {sentence}".strip()
    if current:
        parts.append(current)
    return parts


def chunk_pages(doc_name: str, pages: list[tuple[int, list[str]]]) -> list[Chunk]:
    """Cut each (page number, paragraphs) into chunks, never across a page."""
    chunks = []
    for page_number, paragraphs in pages:
        text, first = "", 0
        for offset, paragraph in enumerate(paragraphs):
            for part in _split_long(paragraph):
                if text and len(text) + len(part) + 2 > MAX_CHARS:
                    chunks.append(Chunk(doc_name, page_number, first, text))
                    text = ""
                if not text:
                    first = offset
                text = f"{text}\n\n{part}" if text else part
                if len(text) >= MIN_CHARS:
                    chunks.append(Chunk(doc_name, page_number, first, text))
                    text = ""
        if text:
            chunks.append(Chunk(doc_name, page_number, first, text))
    return chunks


def read_pages(fs, group: str, docs_dir: str) -> dict[str, list[tuple[int, list[str]]]]:
    """{document name under docs_dir: [(page, paragraphs)]} from the hops fg parse group."""
    found = fs.get_feature_groups(group) or []
    if not found:
        raise SystemExit(f"{group} does not exist: run hops fg parse {docs_dir} --to {group} first")
    frame = max(found, key=lambda fg: fg.version).select(["path", "page", "text"]).read()
    docs: dict[str, list[tuple[int, list[str]]]] = {}
    for row in frame.sort_values(["path", "page"]).itertuples(index=False):
        name = row.path.removeprefix(f"{docs_dir.strip('/')}/")
        docs.setdefault(name, []).append((max(int(row.page), 1), _paragraphs(row.text or "")))
    return docs


# endregion


def embedding_index(dimension: int):
    """A cosine index over the `embedding` column."""
    from hsfs.embedding import EmbeddingIndex, SimilarityFunctionType

    index = EmbeddingIndex()
    index.add_embedding(
        "embedding", dimension, similarity_function_type=SimilarityFunctionType.COSINE
    )
    return index


def load_model(project, name: str, version: int | None):
    """The sentence-transformers model registered by register_embedder.py."""
    from sentence_transformers import SentenceTransformer

    registry = project.get_model_registry()
    model = (
        registry.get_model(name, version=version)
        if version
        else max(registry.get_models(name), key=lambda m: m.version)
    )
    return SentenceTransformer(model.download()), model


def rows(project, chunks: list, docs_dir: str, encoder) -> pd.DataFrame:
    """One row per chunk; `url` is relative to the Hopsworks UI's origin."""
    vectors = encoder.encode([c.text for c in chunks], batch_size=32, normalize_embeddings=True)
    folder = f"/p/{project.id}/settings/fb/path/{docs_dir}"
    return pd.DataFrame(
        {
            "chunk_id": [c.chunk_id for c in chunks],
            "doc_name": [c.doc_name for c in chunks],
            "path": [f"/Projects/{project.name}/{docs_dir}/{c.doc_name}" for c in chunks],
            "url": [folder for _ in chunks],
            "page": [c.page for c in chunks],
            "offset": [c.offset for c in chunks],
            "text": [c.text for c in chunks],
            "embedding": [list(map(float, v)) for v in vectors],
        }
    )


def main() -> None:
    """Read, chunk, embed and write every document in the docs directory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", default="Resources/helpdesk-docs")
    parser.add_argument("--pages", default="helpdesk_docs_pages")
    parser.add_argument("--fg", default="helpdesk_doc_chunks")
    parser.add_argument("--fg-version", type=int, default=1)
    parser.add_argument("--model", default="helpdesk_embedder")
    parser.add_argument("--model-version", type=int, default=None)
    # A scheduled run appends -start_time <fire instant> (and may append -end_time):
    # the instant the schedule fired, which ends the window rather than starting it.
    # They are accepted and ignored; a scheduled run's window is HOPS_START_TIME/HOPS_END_TIME.
    parser.add_argument("-start_time", dest="_fired_at", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_fired_end", help=argparse.SUPPRESS)
    args = parser.parse_args()

    import hopsworks

    project = hopsworks.login()
    fs = project.get_feature_store()
    encoder, model = load_model(project, args.model, args.model_version)
    docs = read_pages(fs, args.pages, args.docs)
    _logger.info("%d documents in %s", len(docs), args.pages)
    chunks = []
    for name, pages in docs.items():
        found = chunk_pages(name, pages)
        _logger.info("%s: %d chunks", name, len(found))
        chunks.extend(found)

    fg = fs.get_or_create_feature_group(
        args.fg,
        version=args.fg_version,
        description=f"Help desk document chunks from {args.docs}, embedded with {model.name} v{model.version}",
        primary_key=["chunk_id"],
        online_enabled=True,
        embedding_index=embedding_index(encoder.get_sentence_embedding_dimension()),
        statistics_config=False,
    )
    if chunks:
        fg.insert(rows(project, chunks, args.docs, encoder), wait=True)

    # Rows of documents removed from the directory, or chunks a document lost.
    current = {c.chunk_id for c in chunks}
    try:
        stored = fg.read(dataframe_type="pandas")[["chunk_id"]]
    except Exception:  # noqa: BLE001 - a new feature group has no offline rows yet
        stored = pd.DataFrame({"chunk_id": []})
    stale = stored[~stored["chunk_id"].isin(current)]
    if len(stale):
        fg.remove_rows(stale)
    _logger.info(
        "wrote %d chunks, removed %d stale, into %s v%s",
        len(chunks),
        len(stale),
        args.fg,
        args.fg_version,
    )


if __name__ == "__main__":
    main()
