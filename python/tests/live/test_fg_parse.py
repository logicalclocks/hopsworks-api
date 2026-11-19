#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""`hops fg parse` against a real project (HABLE-258).

The test uploads a folder of documents, parses it with the command (the parse
runs as the group's job, with Docling, on the document-pipeline environment),
reads the group back, changes and removes files, runs again, and deletes
everything with `hops fg parse delete`.

Runs only with HOPS_LIVE=1, a cluster login of a test user in HOPSWORKS_HOST,
HOPSWORKS_API_KEY and HOPSWORKS_PROJECT, and a backend with the parse platform
class and the document-pipeline environment. HOPS_PARSE_ENV names another
environment for the job.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("HOPS_LIVE") != "1", reason="needs HOPS_LIVE=1 and a cluster login"
)

ENV_ARGS = (
    ("--env", os.environ["HOPS_PARSE_ENV"]) if os.environ.get("HOPS_PARSE_ENV") else ()
)
#: A parse pod starts in minutes; three small files parse well inside this.
RUN_TIMEOUT_S = 1800


def _hops(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "hopsworks.cli", *args], capture_output=True, text=True
    )


def _hops_json(*args: str) -> dict:
    result = _hops("--json", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def _pdf(pages: list[str]) -> bytes:
    """A PDF whose pages hold one line of Helvetica text each: a real text layer."""
    objects = ["<< /Type /Catalog /Pages 2 0 R >>", ""]
    kids = []
    for text in pages:
        content = f"BT /F1 18 Tf 72 720 Td ({text}) Tj ET".encode()
        page_id, stream_id = len(objects) + 1, len(objects) + 2
        kids.append(f"{page_id} 0 R")
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {stream_id} 0 R "
            "/Resources << /Font << /F1 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> >> >> >>"
        )
        objects.append(
            f"<< /Length {len(content)} >>\nstream\n{content.decode()}\nendstream"
        )
    objects[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>"
    out, offsets = b"%PDF-1.4\n", []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n{body}\nendobj\n".encode()
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets).encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return out


@pytest.fixture(scope="module")
def project():
    import hopsworks

    return hopsworks.login()


@pytest.fixture
def folder(project):
    """A fresh folder in the project, removed with everything in it when the test ends."""
    dataset = project.get_dataset_api()
    root = f"Resources/parse-live-{uuid.uuid4().hex[:8]}"
    dataset.mkdir(root)
    yield root
    dataset.remove(root)


def _upload(project, root: str, files: dict[str, bytes]) -> None:
    dataset = project.get_dataset_api()
    with tempfile.TemporaryDirectory() as tmp:
        for name, body in files.items():
            local = Path(tmp) / name
            local.write_bytes(body)
            dataset.upload(str(local), root, overwrite=True)


def _parse(root: str, group: str) -> None:
    started = _hops_json("fg", "parse", root, "--to", group, *ENV_ARGS)
    waited = _hops("job", "wait", f"{group}_parse", "--execution", str(started["execution"]),
                   "--timeout", str(RUN_TIMEOUT_S))  # fmt: skip
    assert waited.returncode == 0, waited.stdout + waited.stderr


def _rows(project, group: str) -> dict[tuple[str, int], dict]:
    fg = max(
        project.get_feature_store().get_feature_groups(group), key=lambda g: g.version
    )
    frame = fg.read(dataframe_type="pandas")
    return {(r["path"], int(r["page"])): r for r in frame.to_dict("records")}


def test_parse_converges_on_the_folder_and_delete_removes_everything(project, folder):
    group = f"parse_live_{uuid.uuid4().hex[:6]}"
    _upload(project, folder, {
        "report.pdf": _pdf(["Revenue grew in the third quarter", "Costs fell in the fourth quarter"]),
        "table.md": b"# Prices\n\n| item | price |\n|---|---|\n| tea | 3 |\n| cake | 5 |\n",
        "notes.txt": b"Returns take 30 days.",
    })  # fmt: skip
    try:
        _parse(folder, group)
        rows = _rows(project, group)
        assert set(rows) == {
            (f"{folder}/report.pdf", 1), (f"{folder}/report.pdf", 2),
            (f"{folder}/table.md", 0), (f"{folder}/notes.txt", 0),
        }  # fmt: skip
        assert "third quarter" in rows[(f"{folder}/report.pdf", 1)]["text"]
        assert "fourth quarter" in rows[(f"{folder}/report.pdf", 2)]["text"]
        assert "| tea" in rows[(f"{folder}/table.md", 0)]["text"]
        assert rows[(f"{folder}/notes.txt", 0)]["parser"].startswith("docling 2.")

        # Unchanged files are not parsed again.
        _parse(folder, group)
        again = _rows(project, group)
        assert {k: v["parsed_at"] for k, v in again.items()} == {
            k: v["parsed_at"] for k, v in rows.items()
        }

        # One file changes, one goes: only those rows move.
        _upload(project, folder, {"notes.txt": b"Returns take 14 days."})
        project.get_dataset_api().remove(f"{folder}/report.pdf")
        _parse(folder, group)
        after = _rows(project, group)
        assert set(after) == {(f"{folder}/table.md", 0), (f"{folder}/notes.txt", 0)}
        assert "14 days" in after[(f"{folder}/notes.txt", 0)]["text"]
        assert (
            after[(f"{folder}/table.md", 0)]["parsed_at"]
            == rows[(f"{folder}/table.md", 0)]["parsed_at"]
        )

        # The group parses one path.
        other = _hops("fg", "parse", "Resources", "--to", group, *ENV_ARGS)
        assert (
            other.returncode != 0
            and f"{group} parses {folder}" in other.stdout + other.stderr
        )
    finally:
        deleted = _hops("fg", "parse", "delete", group, "--yes")
        assert deleted.returncode == 0, deleted.stdout + deleted.stderr
    from hopsworks.cli import embed_run

    assert embed_run.versions_of(project.get_feature_store(), group) == []
    gone = _hops("job", "info", f"{group}_parse")
    assert gone.returncode != 0
    assert not project.get_dataset_api().exists(
        f"Resources/hopsable-parse/{group}.json"
    )
