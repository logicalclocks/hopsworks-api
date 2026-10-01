# ruff: noqa: INP001
"""Register NVIDIA's pretrained Kumo Tabular classifier in the Model Registry.

Run as the job `<slug>-register-kumo` in `<slug>-jobs-env`:

    hops job deploy <slug>-register-kumo src/<slug_pkg>/register_kumo.py \
        --env <slug>-jobs-env --run --wait

Nothing is trained. Kumo Tabular is an in-context learner: each request carries a
small labelled table, the context, with the rows to classify, so one checkpoint
serves any table. This job downloads the medium classifier, `medium/classifier.pt`
of `nvidia/Kumo-Tabular`, with the repository's README and LICENSE at a pinned
revision, and registers `kumo_tabular` with them; the deployment's predictor loads
the checkpoint from the model's files. A second run finds the revision registered
and leaves it. The weights are under the OpenMDW 1.1 license.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path


REPO = "nvidia/Kumo-Tabular"
REVISION = "4f0dca60610d68f933b978e17ff8f66be3ec3b5b"
SIZE = "medium"
FILES = [f"{SIZE}/classifier.pt", "README.md", "LICENSE"]


def already_registered(registry, name: str, revision: str) -> bool:
    """Whether a version of `name` was registered from `revision`; its description says so."""
    try:
        models = registry.get_models(name) or []
    except Exception:  # noqa: BLE001 - no such model yet
        return False
    return any(revision in (model.description or "") for model in models)


def download(directory: Path, revision: str) -> None:
    """The checkpoint, README and LICENSE at `revision`, in their repository layout."""
    from huggingface_hub import hf_hub_download

    for name in FILES:
        hf_hub_download(REPO, name, revision=revision, local_dir=directory)
    # hf_hub_download keeps its cache metadata next to the files; the model needs none of it.
    shutil.rmtree(directory / ".cache", ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Register the Kumo Tabular classifier."
    )
    parser.add_argument("--name", default="kumo_tabular")
    parser.add_argument("--revision", default=REVISION)
    args = parser.parse_args(argv)

    import hopsworks

    registry = hopsworks.login().get_model_registry()
    if already_registered(registry, args.name, args.revision):
        print(f"{args.name}: {REPO}@{args.revision} is already registered")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        download(directory, args.revision)
        model = registry.torch.create_model(
            name=args.name,
            description=f"NVIDIA Kumo Tabular ({SIZE}) in-context classifier, {REPO}@{args.revision}",
        )
        model.save(str(directory))
    print(f"{args.name}: registered {REPO}@{args.revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
