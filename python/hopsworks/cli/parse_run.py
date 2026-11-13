"""One run of ``hops fg parse``, in the group's job: files under one HopsFS path become text rows.

The group is keyed by ``(path, page)``: ``path`` is the file's path in the
project, ``page`` its 1-based page for a PDF or an image, 0 for a file read
whole. ``text`` is the page as Markdown, tables as Markdown tables, so
``hops fg embed GROUP --text text`` takes the group as it is.

A group parses one path, named in its description. A run:

1. Lists the files under the path through the HopsFS mount, and reads the
   sha256 of each file the group holds. A file with the same sha256 is
   skipped; a file the path no longer has is deleted from the group.
2. Refuses before any parse a file over the size cap, and a run whose files
   to parse have more pages than the page cap.
3. Converts each file, a PDF in ranges of ``range_pages`` pages: the peak
   memory is set by one range (50 pages peaked at 5.5 to 5.9 GB on 4 CPU),
   not by the file's length. Before each range the file and range go to a
   state file, so a run killed inside a file leaves it named; the next run
   tries that file once more and then skips it, naming it.
4. Stops after the range that passes the time budget. The files it finished
   are written (one upsert, one delete), the rest are named; the next run
   skips the finished files by sha256 and goes on.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


STATE_DIR = "Resources/hopsable-parse"
OWNER = "hopsable.parse.owner="
PATH = "hopsable.parse.path="
KEYS = ["path", "page"]
#: Pages per Docling convert of a PDF: the measured bound on a run's peak memory.
RANGE_PAGES = 50
#: A page with fewer characters in its text layer than this is read with OCR.
TEXT_LAYER_CHARS = 16
#: Read as pages (PDF) or as one image page; the rest of SUFFIXES is read whole as page 0.
PAGED = (".pdf",)
IMAGES = (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp")
WHOLE = (".docx", ".pptx", ".xlsx", ".html", ".htm", ".md", ".csv", ".txt")
SUFFIXES = PAGED + IMAGES + WHOLE
#: Caps of one run, unless an admin set the Hopsworks variables below.
MAX_PAGES = 500
MAX_FILE_MB = 500
MAX_MINUTES = 60
VAR_MAX_PAGES = "platform_jobs_parse_max_pages"
VAR_MAX_FILE_MB = "platform_jobs_parse_max_file_mb"
VAR_MAX_MINUTES = "platform_jobs_parse_max_minutes"
VAR_MIN_INTERVAL = "platform_jobs_parse_min_interval_minutes"
#: Times a file may stop a run (a kill inside it) before runs skip it.
STOPS_BEFORE_SKIP = 2


class ParseError(Exception):
    """The run stopped; the message says what was and was not written."""


class OutOfTime(Exception):
    """A file's conversion passed the run's time budget before its last range."""


def description(path: str, group: str) -> str:
    """The group's description: its path and its owner, which `described` reads back."""
    return f"Pages of {path} as Markdown, by hops fg parse. {PATH}{path} {OWNER}{group}"


_DESCRIBED = re.compile(
    re.escape(PATH) + r"(?P<path>.+) " + re.escape(OWNER) + r"(?P<owner>\S+)$"
)


def described(thing: Any) -> dict | None:
    """{"path", "owner"} from a group or job description `description` wrote; None otherwise."""
    match = _DESCRIBED.search((getattr(thing, "description", "") or "").rstrip())
    return match.groupdict() if match else None


def normal_path(path: str) -> str:
    """A project-relative path without leading, trailing or doubled slashes."""
    parts = [p for p in path.strip().split("/") if p not in ("", ".")]
    if ".." in parts:
        raise ParseError(f"'{path}' leaves the project: give a path inside it.")
    return "/".join(parts)


def under(path: str, root: str) -> bool:
    return path == root or path.startswith(root + "/")


def list_files(mount: Path, root: str) -> dict[str, int]:
    """{project-relative path: bytes} of every file under `root` with one of SUFFIXES."""
    base = mount / root
    if base.is_file():
        found = [base]
    elif base.is_dir():

        def unreadable(exc: OSError) -> None:
            # A folder left out would read as files removed, and their rows would be deleted.
            raise ParseError(
                f"Could not list {exc.filename}: {exc.strerror}. Nothing was written."
            ) from exc

        found = [
            Path(d) / f
            for d, _, files in os.walk(base, onerror=unreadable)
            for f in files
        ]
    else:
        raise ParseError(f"{root} is not in the project. Nothing was written.")
    out = {}
    for f in found:
        if f.suffix.lower() in SUFFIXES:
            out[f.relative_to(mount).as_posix()] = f.stat().st_size
    return dict(sorted(out.items()))


def sha256(file: Path) -> str:
    digest = hashlib.sha256()
    with open(file, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class Plan:
    """What a run does with the files under its path."""

    parse: list[str]
    unchanged: list[str]
    removed: list[str]
    skipped: dict[str, str] = field(default_factory=dict)


def plan(
    files: dict[str, str], held: dict[str, set[str]], stops: dict[str, int]
) -> Plan:
    """Files to parse, unchanged and removed, from {path: sha256} on HopsFS and {path: sha256s} in the group.

    A path whose rows hold more than one sha256 (an upsert whose delete
    failed) is parsed again. A file that stopped STOPS_BEFORE_SKIP runs is
    skipped while its content is the one that stopped them; a new content is
    parsed again.
    """
    out = Plan(parse=[], unchanged=[], removed=sorted(set(held) - set(files)))
    for path, digest in files.items():
        if held.get(path) == {digest}:
            out.unchanged.append(path)
        elif stops.get(f"{path}@{digest}", 0) >= STOPS_BEFORE_SKIP:
            out.skipped[path] = (
                f"stopped {STOPS_BEFORE_SKIP} runs while it was parsed; "
                "parse it with a smaller --range-pages or split it"
            )
        else:
            out.parse.append(path)
    return out


class State:
    """The run's progress in `<mount>/Resources/hopsable-parse/<group>.json`.

    `current` is the file and range a run is converting; a run that ends
    clears it. A run that finds it set counts one stop for that file.
    """

    def __init__(self, mount: Path, group: str):
        self.file = mount / STATE_DIR / f"{group}.json"
        try:
            self.data = json.loads(self.file.read_text())
        except FileNotFoundError:
            self.data = {}
        except (OSError, ValueError) as exc:
            raise ParseError(
                f"Could not read {self.file}: {exc}. Nothing was written."
            ) from exc
        self.data.setdefault("stops", {})

    def take_over(self) -> str | None:
        """Counts the stop a killed run left; returns what it was converting, or None."""
        current = self.data.pop("current", None)
        if not current:
            return None
        key = current["file"]
        self.data["stops"][key] = self.data["stops"].get(key, 0) + 1
        self._save()
        return f"{current['file'].rsplit('@', 1)[0]} pages {current['first']}-{current['last']}"

    def at(self, path: str, digest: str, first: int, last: int) -> None:
        self.data["current"] = {
            "file": f"{path}@{digest}",
            "first": first,
            "last": last,
        }
        self._save()

    def done(self, path: str, digest: str) -> None:
        self.data.pop("current", None)
        self.data["stops"].pop(f"{path}@{digest}", None)
        self._save()

    def never(self, path: str, digest: str) -> None:
        """Runs skip this content of the file from now on."""
        self.data["stops"][f"{path}@{digest}"] = STOPS_BEFORE_SKIP
        self._save()

    def end(self) -> None:
        self.data.pop("current", None)
        self._save()

    def _save(self) -> None:
        self.file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data))
        os.replace(tmp, self.file)


def ranges(pages: int, size: int) -> list[tuple[int, int]]:
    """1-based inclusive page ranges of at most `size` pages."""
    return [(s, min(s + size - 1, pages)) for s in range(1, pages + 1, size)]


def pdf_pages(file: Path) -> tuple[int, bool]:
    """(pages, whether every page has a text layer)."""
    import pypdfium2  # noqa: PLC0415 - in the document-pipeline image, with Docling

    pdf = pypdfium2.PdfDocument(str(file))
    try:
        texts = [
            len(pdf[i].get_textpage().get_text_range().strip()) for i in range(len(pdf))
        ]
    finally:
        pdf.close()
    return len(texts), all(n >= TEXT_LAYER_CHARS for n in texts)


def pages_of(file: Path) -> int:
    """Pages a file counts for the page cap: a PDF its pages, any other file (read whole) 1."""
    return pdf_pages(file)[0] if file.suffix.lower() in PAGED else 1


class Converter:
    """Docling converters for one run, made once: OCR on and off, the table mode fixed."""

    def __init__(self, accurate: bool, threads: int):
        from docling.datamodel.accelerator_options import (
            AcceleratorOptions,  # noqa: PLC0415
        )
        from docling.datamodel.pipeline_options import (  # noqa: PLC0415
            PdfPipelineOptions,
            TableFormerMode,
        )
        from docling.document_converter import (  # noqa: PLC0415
            DocumentConverter,
            ImageFormatOption,
            InputFormat,
            PdfFormatOption,
        )

        def options(ocr: bool) -> Any:
            o = PdfPipelineOptions()
            o.artifacts_path = os.environ.get("DOCLING_ARTIFACTS_PATH") or None
            o.accelerator_options = AcceleratorOptions(
                device="cpu", num_threads=threads
            )
            o.do_ocr = ocr
            o.do_table_structure = True
            o.table_structure_options.mode = (
                TableFormerMode.ACCURATE if accurate else TableFormerMode.FAST
            )
            return o

        self._made: dict[bool, Any] = {}
        self._factory = lambda ocr: DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=options(ocr)),
                InputFormat.IMAGE: ImageFormatOption(pipeline_options=options(True)),
            }
        )
        self.parser = (
            f"docling {_version()} tables={'accurate' if accurate else 'fast'}"
        )

    def get(self, ocr: bool) -> Any:
        if ocr not in self._made:
            self._made[ocr] = self._factory(ocr)
        return self._made[ocr]


def _version() -> str:
    from importlib.metadata import version  # noqa: PLC0415

    return version("docling")


def convert(
    conv: Converter,
    file: Path,
    *,
    ocr: str,
    range_pages: int,
    on_range: Callable[[int, int], None],
    past_budget: Callable[[], bool] = lambda: False,
) -> list[tuple[int, str]]:
    """[(page, Markdown)] of one file. `ocr`: "auto" (pages without text), "on" or "off".

    Raises OutOfTime after a range when `past_budget()` and ranges are left.
    """
    suffix = file.suffix.lower()
    if suffix == ".txt":
        on_range(0, 0)
        return [(0, file.read_text(encoding="utf-8", errors="replace"))]
    if suffix not in PAGED:
        on_range(0 if suffix in WHOLE else 1, 0 if suffix in WHOLE else 1)
        result = conv.get(True).convert(str(file))
        try:
            return [(0 if suffix in WHOLE else 1, result.document.export_to_markdown())]
        finally:
            result.input.release()
    pages, has_text = pdf_pages(file)
    use_ocr = {"on": True, "off": False}.get(ocr, not has_text)
    out = []
    for first, last in ranges(pages, range_pages):
        if out and past_budget():
            raise OutOfTime
        on_range(first, last)
        result = conv.get(use_ocr).convert(str(file), page_range=(first, last))
        try:
            for page in sorted(result.document.pages):
                out.append((page, result.document.export_to_markdown(page_no=page)))
        finally:
            result.input.release()
            del result
    return out


@dataclass
class Outcome:
    """What a run did, for the job log and `--json`."""

    parsed: list[str] = field(default_factory=list)
    unchanged: int = 0
    removed: list[str] = field(default_factory=list)
    rows: int = 0
    deleted_rows: int = 0
    left: list[str] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    stopped_by: str | None = None


def run(
    *,
    fg: Any,
    mount: Path,
    root: str,
    group: str,
    conv: Converter,
    ocr: str,
    range_pages: int,
    max_pages: int,
    max_file_mb: int,
    max_minutes: int,
    log: Callable[..., None],
    clock: Callable[[], float] = time.monotonic,
) -> Outcome:
    """Converge `fg` onto the files under `root`; rows under any other path are left alone."""
    budget_s = max_minutes * 60
    run_start = clock()
    deadline = run_start + budget_s
    sizes = list_files(mount, root)
    state = State(mount, group)
    stopped = state.take_over()
    if stopped:
        log("The last run stopped inside %s", stopped)
    held_rows = {k: d for k, d in _held(fg).items() if under(k[0], root)}
    held: dict[str, set[str]] = {}
    for (path, _), digest in held_rows.items():
        held.setdefault(path, set()).add(digest)
    over = {p for p, n in sizes.items() if n > max_file_mb * 1_000_000}
    digests = {p: sha256(mount / p) for p in sizes if p not in over}
    todo = plan(digests, held, state.data["stops"])
    out = Outcome(
        unchanged=len(todo.unchanged), removed=todo.removed, skipped=todo.skipped
    )
    for path in sorted(over):
        out.skipped[path] = (
            f"is over {max_file_mb} MB, the file size cap of this cluster"
        )
    at = datetime.now(timezone.utc)
    rows, gone = [], [k for k in held_rows if k[0] in todo.removed]
    pages_left = max_pages
    for i, path in enumerate(todo.parse):
        if clock() >= deadline:
            out.left = todo.parse[i:]
            out.stopped_by = f"the {max_minutes} min budget of a run"
            break
        digest = digests[path]
        file_start = clock()
        # Counting pages opens the file: a kill there counts as a stop of this file too.
        state.at(path, digest, 0, 0)
        try:
            pages = pages_of(mount / path)
        except Exception as exc:  # noqa: BLE001 - one bad file does not stop the others
            state.done(path, digest)
            out.skipped[path] = f"could not be read: {exc}"
            continue
        if pages > max_pages:
            state.done(path, digest)
            out.skipped[path] = (
                f"has {pages} pages, over the {max_pages} pages a run may parse on this cluster; "
                "split it, or ask an admin"
            )
            continue
        if pages > pages_left:
            state.done(path, digest)
            out.left = todo.parse[i:]
            out.stopped_by = f"the {max_pages} pages a run may parse"
            break
        pages_left -= pages

        def on_range(first: int, last: int, path=path, digest=digest) -> None:
            state.at(path, digest, first, last)
            log("%s pages %d-%d", path, first, last)

        try:
            converted = convert(
                conv, mount / path, ocr=ocr, range_pages=range_pages, on_range=on_range,
                past_budget=lambda: clock() >= deadline,
            )  # fmt: skip
        except OutOfTime:
            state.done(path, digest)
            out.stopped_by = f"the {max_minutes} min budget of a run"
            if file_start - run_start <= budget_s / 10:
                # It had (nearly) the whole budget: the next run would stop the same way.
                state.never(path, digest)
                out.skipped[path] = (
                    f"does not finish within the {max_minutes} min budget of a run; "
                    "split it, or ask an admin for a longer budget"
                )
                out.left = todo.parse[i + 1 :]
            else:
                out.left = todo.parse[i:]
            break
        except Exception as exc:  # noqa: BLE001 - one bad file does not stop the others
            state.done(path, digest)
            out.skipped[path] = f"Docling could not read it: {exc}"
            continue
        state.done(path, digest)
        # A file with no text keeps one empty row, so its sha256 is held and it is not parsed again.
        converted = converted or [(0, "")]
        log("%s: %d pages in %.0f s", path, len(converted), clock() - file_start)
        out.parsed.append(path)
        kept = {page for page, _ in converted}
        gone += [k for k in held_rows if k[0] == path and k[1] not in kept]
        rows += [
            {"path": path, "page": page, "text": text, "file_sha256": digest,
             "parser": conv.parser, "parsed_at": at}
            for page, text in converted
        ]  # fmt: skip
    _write(fg, rows, gone)
    state.end()
    out.rows, out.deleted_rows = len(rows), len(gone)
    return out


def _held(fg: Any) -> dict[tuple[str, int], str]:
    """{(path, page): file_sha256} of the group's rows."""
    try:
        frame = fg.select(["path", "page", "file_sha256"]).read()
    except Exception as exc:  # noqa: BLE001
        raise ParseError(
            f"Could not read {fg.name}: {exc}. Nothing was written."
        ) from exc
    return {(r.path, int(r.page)): r.file_sha256 for r in frame.itertuples(index=False)}


def _write(fg: Any, rows: list[dict], gone: Iterable[tuple[str, int]]) -> None:
    """One upsert of the new pages, then one delete of the pages and files that are gone."""
    import pandas as pd  # noqa: PLC0415

    gone = list(gone)
    try:
        if rows:
            fg.insert(pd.DataFrame(rows))
        if gone:
            fg.remove_rows(pd.DataFrame(gone, columns=KEYS))
    except Exception as exc:  # noqa: BLE001
        raise ParseError(
            f"Could not write {fg.name}: {exc}. Run hops fg parse again: finished files are kept "
            "only once written."
        ) from exc
