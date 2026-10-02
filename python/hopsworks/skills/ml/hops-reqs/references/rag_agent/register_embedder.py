# ruff: noqa: INP001
"""Download a sentence-transformers model from Hugging Face into the Model Registry.

    python register_embedder.py [--repo sentence-transformers/all-MiniLM-L6-v2] [--name helpdesk_embedder]

Runs once as a job (`<slug>-register-embedder`). The ingestion job and the
agent both load the model from the registry, so documents and queries are
embedded by the same pinned revision, and nothing downloads from Hugging Face at
query time. A model already registered at that revision is left alone.
"""

from __future__ import annotations

import argparse
import json
import logging
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_logger = logging.getLogger("register_embedder")

# Weights for other runtimes in the same repository, which sentence-transformers
# does not load: skipping them keeps the registered model to the safetensors.
OTHER_RUNTIMES = [
    "*.onnx",
    "onnx/*",
    "openvino/*",
    "*.ot",
    "*.h5",
    "*.msgpack",
    "pytorch_model.bin",
    ".gitattributes",
]


def main() -> None:
    """Register the model unless this revision is already there."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--revision", default=None, help="a commit; default: main")
    parser.add_argument("--name", default="helpdesk_embedder")
    args = parser.parse_args()

    import hopsworks
    from huggingface_hub import HfApi, snapshot_download
    from sentence_transformers import SentenceTransformer

    revision = HfApi().model_info(args.repo, revision=args.revision).sha
    project = hopsworks.login()
    registry = project.get_model_registry()
    for existing in registry.get_models(args.name) or []:
        if (existing.description or "").endswith(f"@{revision}"):
            _logger.info("%s v%s is %s@%s", args.name, existing.version, args.repo, revision)
            return

    with tempfile.TemporaryDirectory() as tmp:
        path = snapshot_download(
            args.repo, revision=revision, local_dir=tmp, ignore_patterns=OTHER_RUNTIMES
        )
        encoder = SentenceTransformer(path)
        dimension = encoder.get_sentence_embedding_dimension()
        with open(f"{path}/hopsworks_embedder.json", "w") as f:
            json.dump({"repo": args.repo, "revision": revision, "dimension": dimension}, f)
        model = registry.python.create_model(
            args.name,
            metrics={"dimension": dimension},
            description=f"Sentence embeddings for help desk search: {args.repo}@{revision}",
        )
        model.save(path)
    _logger.info("registered %s v%s (%d dimensions)", args.name, model.version, dimension)


if __name__ == "__main__":
    main()
