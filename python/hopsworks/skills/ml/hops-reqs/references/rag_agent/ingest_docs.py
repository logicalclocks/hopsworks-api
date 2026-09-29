# ruff: noqa: INP001
"""Embed the help desk documents into the feature group the agent searches.

    python ingest_docs.py [--docs Resources/helpdesk-docs] [--fg <name>] [--model <name>]

Runs as a job (`<slug>-ingest-docs`), and again whenever documents are added:
every .pdf, .txt, .md, .docx and .odt file in the docs directory is cut into
chunks, each chunk embedded with the sentence-transformers model from
the Model Registry, and written with its document name, HopsFS path, a
file-browser URL, page and offset. Chunk ids are stable, so a re-run overwrites
a document's rows; rows of a document that was removed from the directory, or
of chunks it no longer has, are deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_logger = logging.getLogger("ingest_docs")

# region Documents: pages, paragraphs and chunks
#
# A chunk is a run of paragraphs from one page, cut at a paragraph boundary: at
# least MIN_CHARS unless the page ends first, and at most MAX_CHARS, a longer
# paragraph being cut at sentence ends. Each keeps the 1-based `page` (a PDF
# page; a page break in a Word or OpenDocument file or a form feed in text; 1
# otherwise) and the 0-based `offset` of its first paragraph in that page.

SUFFIXES = (".pdf", ".txt", ".md", ".docx", ".odt")
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


def _pages_pdf(path: Path) -> list[list[str]]:
    from pypdf import PdfReader

    pages = []
    for page in PdfReader(str(path)).pages:
        text = page.extract_text() or ""
        # PDF text has a line break at every visual line; a paragraph ends at a
        # blank line or at a line that ends a sentence and is short.
        text = re.sub(r"(?<![.!?:])\n(?!\n)", " ", text)
        pages.append(_paragraphs(text))
    return pages


def _pages_docx(path: Path) -> list[list[str]]:
    from docx import Document

    pages: list[list[str]] = [[]]
    for paragraph in Document(str(path)).paragraphs:
        breaks = paragraph._p.xml.count('w:type="page"') + paragraph._p.xml.count(
            "lastRenderedPageBreak"
        )
        if breaks and pages[-1]:
            pages.append([])
        if paragraph.text.strip():
            pages[-1].append(paragraph.text.strip())
    return pages


def _pages_odt(path: Path) -> list[list[str]]:
    from odf import teletype
    from odf.opendocument import load
    from odf.text import P

    document = load(str(path))
    # A page break in an ODT is a paragraph style with break-before or break-after.
    breaking = set()
    for style in document.automaticstyles.childNodes:
        for child in getattr(style, "childNodes", []):
            attrs = {str(k[1]): v for k, v in getattr(child, "attributes", {}).items()}
            if "page" in (attrs.get("break-before"), attrs.get("break-after")):
                breaking.add(style.getAttribute("name"))
    pages: list[list[str]] = [[]]
    for paragraph in document.getElementsByType(P):
        if paragraph.getAttribute("stylename") in breaking and pages[-1]:
            pages.append([])
        text = teletype.extractText(paragraph).strip()
        if text:
            pages[-1].append(text)
    return pages


def _pages_text(path: Path) -> list[list[str]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    return [_paragraphs(page) for page in text.split("\f")]


def read_pages(path: Path) -> list[list[str]]:
    """The paragraphs of each page of `path`, which must have one of SUFFIXES."""
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return _pages_pdf(path)
    if suffix == ".docx":
        return _pages_docx(path)
    if suffix == ".odt":
        if not zipfile.is_zipfile(path):
            raise ValueError(f"{path.name} is not an OpenDocument file")
        return _pages_odt(path)
    if suffix in (".txt", ".md"):
        return _pages_text(path)
    raise ValueError(f"{path.name}: only {', '.join(SUFFIXES)} are read")


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


def chunk_pages(doc_name: str, pages: list[list[str]]) -> list[Chunk]:
    """Cut each page's paragraphs into chunks, never across a page."""
    chunks = []
    for page_number, paragraphs in enumerate(pages, start=1):
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


def chunk_document(path: Path) -> list[Chunk]:
    """Every chunk of the document at `path`, named by its file name."""
    return chunk_pages(path.name, read_pages(path))


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
    vectors = encoder.encode(
        [c.text for c in chunks], batch_size=32, normalize_embeddings=True
    )
    folder = f"/p/{project.id}/settings/fb/path/{docs_dir}"
    return pd.DataFrame(
        {
            "chunk_id": [c.chunk_id for c in chunks],
            "doc_name": [c.doc_name for c in chunks],
            "path": [
                f"/Projects/{project.name}/{docs_dir}/{c.doc_name}" for c in chunks
            ],
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
    parser.add_argument("--fg", default="helpdesk_doc_chunks")
    parser.add_argument("--fg-version", type=int, default=1)
    parser.add_argument("--model", default="helpdesk_embedder")
    parser.add_argument("--model-version", type=int, default=None)
    args = parser.parse_args()

    import hopsworks

    project = hopsworks.login()
    datasets = project.get_dataset_api()
    encoder, model = load_model(project, args.model, args.model_version)
    names = sorted(
        Path(p).name
        for p in datasets.list(args.docs)
        if Path(p).suffix.lower() in SUFFIXES
    )
    _logger.info("%d documents in %s", len(names), args.docs)

    chunks = []
    with tempfile.TemporaryDirectory() as tmp:
        for name in names:
            local = datasets.download(f"{args.docs}/{name}", tmp, overwrite=True)
            try:
                found = chunk_document(Path(local))
            except Exception as exc:  # noqa: BLE001 - one unreadable file must not stop the rest
                _logger.warning("skipped %s: %s", name, exc)
                continue
            _logger.info("%s: %d chunks", name, len(found))
            chunks.extend(found)

    fs = project.get_feature_store()
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
