# ruff: noqa: INP001
"""Job entry point for the integration suite: fetch the run bundle, run pytest inside the cluster.

Deployed as the `<slug>-tests` job in the environment of the pipeline under test
(or its `<env>-tests` clone when that environment lacks pytest):

    hops job deploy <slug>-tests tests/run_integration.py --env <environment> \
        --args "--bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz tests/integration/test_feature_pipeline.py"

Exits with pytest's status, where zero collected tests is a failure, and writes
the pass, fail and error counts to result.json.
"""

from __future__ import annotations

import argparse
import os
import sys
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


# region bundle prelude: identical in every entrypoint, see hops-reqs/references/bundle.md
def _load_bundle(bundle: str) -> tuple[Path, dict, dict]:
    """Fetch and unpack a run bundle, check it against its manifest, load system.yaml.

    `bundle` is a HopsFS path (Resources/<slug>/runs/<run_id>/bundle.tar.gz) or a
    local file. The bundle's src/ goes first on sys.path, so the harness a run
    imports is the one its commit names. Returns (workdir, manifest, system).
    """
    import hashlib
    import json
    import sys
    import tarfile
    import tempfile
    from pathlib import Path

    import yaml

    archive = Path(bundle)
    if not archive.is_file():
        import hopsworks

        download_dir = tempfile.mkdtemp(prefix="bundle-download-")
        archive = Path(
            hopsworks.login()
            .get_dataset_api()
            .download(bundle, download_dir, overwrite=True)
        )
    workdir = Path(tempfile.mkdtemp(prefix="bundle-"))
    with tarfile.open(archive) as tar:
        tar.extractall(workdir, filter="data")
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    present = {
        p.relative_to(workdir).as_posix() for p in workdir.rglob("*") if p.is_file()
    } - {"manifest.json"}
    if present != set(manifest["files"]):
        raise SystemExit(f"bundle {bundle}: its files differ from its manifest")
    for rel, digest in manifest["files"].items():
        if hashlib.sha256((workdir / rel).read_bytes()).hexdigest() != digest:
            raise SystemExit(f"bundle {bundle}: {rel} does not match its manifest")
    sys.path.insert(0, str(workdir / "src"))
    system = yaml.safe_load((workdir / "system.yaml").read_text(encoding="utf-8"))
    return workdir, manifest, system


def _write_result(manifest: dict, result: dict) -> str:
    """Write result.json beside the bundle; the orchestrator imports it into system.yaml.

    The run id and commit are echoed from the manifest so the orchestrator can
    check the result belongs to the row it wrote before submission. With
    HOPS_RESULT_DIR set the file is written there instead of uploaded.
    """
    import json
    import os
    import tempfile
    from pathlib import Path

    payload = {"run_id": manifest["run_id"], "commit": manifest["commit"], **result}
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    local_dir = os.environ.get("HOPS_RESULT_DIR")
    if local_dir:
        path = Path(local_dir) / "result.json"
        path.write_text(text, encoding="utf-8")
        return str(path)
    local = Path(tempfile.mkdtemp(prefix="result-")) / "result.json"
    local.write_text(text, encoding="utf-8")
    target = f"Resources/{manifest['slug']}/runs/{manifest['run_id']}"
    import hopsworks

    hopsworks.login().get_dataset_api().upload(str(local), target, overwrite=True)
    return f"{target}/result.json"


# endregion


def counts(junit_xml: Path) -> dict[str, int]:
    """Totals from a pytest JUnit report."""
    root = ET.parse(junit_xml).getroot()
    suites = [root] if root.tag == "testsuite" else list(root)
    total = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in total:
            total[key] += int(suite.get(key, 0))
    total["passed"] = (
        total["tests"] - total["failures"] - total["errors"] - total["skipped"]
    )
    return total


def main(argv: list[str] | None = None) -> int:
    """Run the requested integration tests from the bundle and record the outcome."""
    parser = argparse.ArgumentParser(
        description="Run integration tests from a run bundle."
    )
    parser.add_argument("--bundle", required=True)
    parser.add_argument("paths", nargs="*", default=["tests/integration"])
    args = parser.parse_args(argv)

    workdir, manifest, _ = _load_bundle(args.bundle)
    os.environ["HOPS_TEST_RUN_ID"] = manifest["run_id"]
    import pytest

    report = workdir / "junit.xml"
    status = pytest.main(
        [
            *(str(workdir / p) for p in args.paths),
            f"--rootdir={workdir}",
            "-p",
            "no:cacheprovider",
            f"--junitxml={report}",
        ]
    )
    summary = counts(report) if report.exists() else {"tests": 0}
    if summary.get("tests", 0) == 0:
        status = 1
    _write_result(manifest, {"exit_status": int(status), **summary})
    return int(status)


if __name__ == "__main__":
    sys.exit(main())
