"""Deleting what an ML system created, for ``hops mlsystem delete``.

The assets come from the system's ``system.yaml``, conservatively: only what the
build writes or names as its own (feature groups it writes, its feature view,
its models, jobs, deployments and app, environments named after the system,
data sources it created, ``Resources/<slug>``), never a feature group it only
reads or a base environment it runs on.

Every step treats an asset that is already gone as done, and the steps run
downstream first (an app before the model it serves, a feature view before the
feature groups it joins) and stop at the first failure, so a run that fails
part way can simply be run again.
The GitHub repository and the registry entry go last, and only after every
asset step succeeded: while the entry exists, the system stays listed and the
delete can be retried.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


# Parent keys under which a `feature_group` is one the system created.
_CREATED_FG = {"writes", "mounted_as", "ingested"}
_NAMED_VERSION = re.compile(r"^(\S+) v(\d+)\b")
_MENTION = re.compile(r"\b([A-Za-z_][\w]*) v(\d+)\b")


@dataclass(frozen=True)
class Asset:
    """One thing to delete; `version` None means every version."""

    kind: str
    name: str
    version: int | None = None

    def __str__(self) -> str:
        return f"{self.kind} {self.name}" + (
            f" v{self.version}" if self.version is not None else ""
        )


# Downstream first: nothing is deleted while something still in the list reads it.
ORDER = [
    "app",
    "deployment",
    "job",
    "model",
    "feature view",
    "feature group",
    "data source",
    "environment",
    "directory",
]


def _walk(node: Any, path: tuple = ()):
    if isinstance(node, dict):
        yield path, node
        for key, value in node.items():
            yield from _walk(value, (*path, key))
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value, path)


def _strings(node: Any):
    if isinstance(node, dict):
        for value in node.values():
            yield from _strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _strings(value)
    elif isinstance(node, str):
        yield node


def _version(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def inventory(doc: dict, slug: str) -> list[Asset]:
    """The assets `doc` (a parsed system.yaml) says the system `slug` created, in deletion order."""
    found: list[Asset] = []

    def add(asset: Asset) -> None:
        if asset.name and asset not in found:
            found.append(asset)

    training = doc.get("training") or {}
    fv = training.get("feature_view") or {}
    if isinstance(fv, dict) and fv.get("name"):
        add(Asset("feature view", str(fv["name"])))
    app = doc.get("app") or {}
    if app.get("name") and app.get("wanted") is not False:
        add(Asset("app", str(app["name"])))

    written: list[str] = []
    groups: list[Asset] = []
    mentioned: set[tuple[str, int | None]] = set()
    for text in _strings(doc):
        mentioned.update((m.group(1), int(m.group(2))) for m in _MENTION.finditer(text))
    for path, node in _walk(doc):
        parent = path[-1] if path else None
        group = node.get("feature_group")
        if isinstance(group, str):
            version = _version(node.get("version"))
            # The data block lists only sources that were not already present.
            if (
                _CREATED_FG & set(path) or path[:1] == ("data",)
            ) and group not in written:
                written.append(group)
            mentioned.add((group, version))
        job = node.get("job")
        if isinstance(job, str):
            add(Asset("job", job))
        elif isinstance(job, dict) and isinstance(job.get("name"), str):
            add(Asset("job", job["name"]))
        model = node.get("model")
        if isinstance(model, dict) and isinstance(model.get("name"), str):
            add(Asset("model", model["name"]))
        # A RAG system's embedding model, registered from Hugging Face.
        embedder = node.get("embedding_model")
        if isinstance(embedder, dict) and isinstance(embedder.get("name"), str):
            add(Asset("model", embedder["name"]))
        elif isinstance(model, str) and path[:1] == ("training",):
            named = _NAMED_VERSION.match(model)
            if named:
                add(Asset("model", named.group(1)))
        if path[:1] == ("inference",) and isinstance(node.get("deployment"), str):
            add(Asset("deployment", node["deployment"]))
        if parent == "connector" and node.get("created") and node.get("name"):
            add(Asset("data source", str(node["name"])))
        # Only environments cloned for this system; a shared base keeps its own name.
        name = node.get("name")
        if (
            "environment" in path
            and isinstance(name, str)
            and name.startswith(f"{slug}-")
        ):
            add(Asset("environment", name))
    # Every version of a group the system writes that the file names, as a
    # {feature_group, version} or as "<name> v<N>" (earlier rounds, history);
    # all versions when it names none. In reverse order of discovery, so the
    # predictions and derived groups go before their sources, newest first.
    for group in reversed(written):
        versions = sorted(
            {v for g, v in mentioned if g == group and v is not None}, reverse=True
        )
        for version in versions or [None]:
            groups.append(Asset("feature group", group, version))
    add(Asset("directory", f"Resources/{slug}"))

    rank = {kind: i for i, kind in enumerate(ORDER)}
    rest = sorted(found, key=lambda a: rank[a.kind])
    at = rank["feature group"]
    return (
        [a for a in rest if rank[a.kind] < at]
        + groups
        + [a for a in rest if rank[a.kind] > at]
    )


def _missing(exc: Exception) -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 404 or "not found" in str(exc).lower()


class Deleter:
    """Deletes assets of one project; each call returns "deleted" or "gone", or raises."""

    def __init__(self, project: Any) -> None:
        self.project = project
        self._fs = None

    @property
    def fs(self) -> Any:
        if self._fs is None:
            self._fs = self.project.get_feature_store()
        return self._fs

    def delete(self, asset: Asset) -> str:
        """Delete `asset`, stopping or unscheduling it first where that is needed."""
        return getattr(self, "_" + asset.kind.replace(" ", "_"))(asset)

    def _each(self, items: list) -> str:
        for item in items:
            item.delete()
        return "deleted" if items else "gone"

    def _app(self, asset: Asset) -> str:
        app = self.project.get_app_api().get_app(asset.name)
        if app is None:
            return "gone"
        # An app that is not running cannot be stopped.
        with contextlib.suppress(Exception):
            app.stop()
        app.delete()
        return "deleted"

    def _deployment(self, asset: Asset) -> str:
        deployment = self.project.get_model_serving().get_deployment(asset.name)
        if deployment is None:
            return "gone"
        deployment.delete(force=True)
        return "deleted"

    def _job(self, asset: Asset) -> str:
        job = self.project.get_job_api().get_job(asset.name)
        if job is None:
            return "gone"
        # A job with no schedule cannot be unscheduled.
        with contextlib.suppress(Exception):
            job.unschedule()
        job.delete()
        return "deleted"

    def _model(self, asset: Asset) -> str:
        return self._each(
            self.project.get_model_registry().get_models(asset.name) or []
        )

    def _feature_view(self, asset: Asset) -> str:
        try:
            views = self.fs.get_feature_views(asset.name) or []
        except Exception as exc:
            if _missing(exc):
                return "gone"
            raise
        # A feature view's training datasets are deleted with it.
        return self._each(views)

    def _feature_group(self, asset: Asset) -> str:
        try:
            if asset.version is None:
                groups = self.fs.get_feature_groups(asset.name) or []
            else:
                groups = [self.fs.get_feature_group(asset.name, asset.version)]
        except Exception as exc:
            if _missing(exc):
                return "gone"
            raise
        return self._each([g for g in groups if g is not None])

    def _data_source(self, asset: Asset) -> str:
        from hopsworks_common.core import rest

        try:
            rest._send_request(
                "DELETE",
                rest._project_path(
                    "featurestores", self.fs.id, "storageconnectors", asset.name
                ),
            )
        except Exception as exc:
            if _missing(exc):
                return "gone"
            raise
        return "deleted"

    def _environment(self, asset: Asset) -> str:
        env = self.project.get_environment_api().get_environment(asset.name)
        if env is None:
            return "gone"
        env.delete()
        return "deleted"

    def _directory(self, asset: Asset) -> str:
        datasets = self.project.get_dataset_api()
        if not datasets.exists(asset.name):
            return "gone"
        datasets.remove(asset.name)
        return "deleted"


# region GitHub repository


def repo_of(doc: dict) -> tuple[str, str, str] | None:
    """`(host, owner, name)` of the system's recorded GitHub repository, or None."""
    repo = (doc.get("system") or {}).get("repo") or {}
    url = str(repo.get("url") or "")
    found = re.match(
        r"^(?:https://|git@)([^/:]+)[/:]([^/]+)/([^/]+?)(?:\.git)?/?$", url
    )
    return (found.group(1), found.group(2), found.group(3)) if found else None


def _github(host: str, method: str, route: str) -> tuple[int, Any]:
    """Call the GitHub API through `gh` when it is logged in, else with a stored token."""
    if shutil.which("gh"):
        done = subprocess.run(
            ["gh", "api", "--hostname", host, "-X", method, "-i", route],
            capture_output=True,
            text=True,
            check=False,
        )
        head, _, body = done.stdout.partition("\r\n\r\n")
        status = re.match(r"HTTP/\S+ (\d+)", head)
        if status:
            code = int(status.group(1))
            return code, json.loads(body) if body.strip() else None
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        filled = subprocess.run(
            ["git", "credential", "fill"],
            input=f"protocol=https\nhost={host}\n\n",
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        token = dict(
            line.split("=", 1) for line in filled.splitlines() if "=" in line
        ).get("password")
    if not token:
        raise RuntimeError(
            "no GitHub access: log in with `gh auth login`, or add a GitHub token in Account Settings"
        )
    import requests

    base = (
        "https://api.github.com" if host == "github.com" else f"https://{host}/api/v3"
    )
    response = requests.request(
        method,
        f"{base}/{route}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        timeout=30,
    )
    return response.status_code, response.json() if response.content else None


def delete_repo(doc: dict, slug: str, project: str) -> str:
    """Delete the system's GitHub repository when it is one the build created for it alone.

    The build names a repository it creates `<slug>` or `<slug>-<project>[-N]`, and
    each build works on its own `hops/...` branch, so a repository with another
    name, or with another build's branch, holds more than this system and is kept.

    Raises:
        RuntimeError: the repository is not this system's alone, or GitHub refused.
    """
    found = repo_of(doc)
    if found is None:
        return "gone"
    host, owner, name = found
    if not re.fullmatch(rf"{re.escape(slug)}(-{re.escape(project)}(-\d+)?)?", name):
        raise RuntimeError(
            f"{owner}/{name} is not named after {slug}, so it may hold more than this system; delete it on GitHub if it should go"
        )
    status, branches = _github(
        host, "GET", f"repos/{owner}/{name}/branches?per_page=100"
    )
    if status == 404:
        return "gone"
    if status != 200:
        raise RuntimeError(f"could not read {owner}/{name}: HTTP {status}")
    own = str(((doc.get("system") or {}).get("repo") or {}).get("branch") or "")
    others = [
        b["name"]
        for b in branches or []
        if b["name"].startswith("hops/")
        and b["name"] != own
        and not b["name"].startswith(f"{own}/")
    ]
    if others:
        raise RuntimeError(
            f"{owner}/{name} also holds other builds ({', '.join(sorted(others))}), so it is kept"
        )
    status, _ = _github(host, "DELETE", f"repos/{owner}/{name}")
    if status == 404:
        return "gone"
    if status == 403:
        raise RuntimeError(
            f"GitHub refused to delete {owner}/{name}; the token needs the delete_repo scope "
            f"(`gh auth refresh -h {host} -s delete_repo`)"
        )
    if status != 204:
        raise RuntimeError(f"GitHub answered HTTP {status} deleting {owner}/{name}")
    return "deleted"


# endregion


def run(
    steps: list[tuple[str, Callable[[], str]]], report: Callable[[str, str], None]
) -> str | None:
    """Run the steps in order, reporting each outcome, up to the first that fails.

    Returns:
        The label of the step that failed, or None when every step succeeded.
        The later steps are not run: they are upstream of it, and deleting one
        of them first could leave the failed one broken.
    """
    for label, step in steps:
        try:
            report(label, step())
        except Exception as exc:  # noqa: BLE001 - reported, and the run stops here
            report(label, f"failed: {exc}")
            return label
    return None
