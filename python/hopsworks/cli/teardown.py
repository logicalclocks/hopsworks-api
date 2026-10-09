"""Deleting what an ML system created, for ``hops factory system delete``.

The assets come from the system's ``system.yaml``, conservatively: only what the
build writes or names as its own (feature groups it writes, its feature view,
its models, jobs, deployments and app, environments named after the system,
data sources it created, ``Resources/<slug>``), never a feature group it only
reads or a base environment it runs on.

Every step treats an asset that is already gone as done, and the steps run
downstream first (an app before the model it serves, a feature view before the
feature groups it joins) and stop at the first failure, so a run that fails
part way can simply be run again.
The GitHub repository (or only the system's branch, when the repository holds
other builds), the code directory and the registry entry go last, in that
order, and only after every asset step succeeded: while the entry exists, the
system stays listed and the delete can be retried, and a retry that finds the
code already gone goes straight to the entry.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


# Parent keys under which a `feature_group` is one the system created.
_CREATED_FG = {"writes", "mounted_as", "ingested"}
_NAMED_VERSION = re.compile(r"^(\S+) v(\d+)\b")
_MENTION = re.compile(r"\b([A-Za-z_][\w]*) v(\d+)\b")


@dataclass(frozen=True)
class Asset:
    """One thing to delete.

    `version` None means every version; with `owned_by`, only what the
    description names that system in (for a feature group without a version,
    every version whose description does). A job named `<slug>-*` means every
    job with that prefix.
    """

    kind: str
    name: str
    version: int | None = None
    owned_by: str | None = None

    def __str__(self) -> str:
        if self.kind == "job" and self.name.endswith("-*"):
            return f"jobs {self.name} (any not named above)"
        if self.owned_by and self.kind == "feature group" and self.version is None:
            return f"{self.kind} {self.name} (every other version {self.owned_by} made)"
        shown = f"{self.kind} {self.name}" + (
            f" v{self.version}" if self.version is not None else ""
        )
        return shown + (f" (if {self.owned_by} made it)" if self.owned_by else "")


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
    """The assets `doc` (a parsed system.yaml) says the system `slug` created, in deletion order.

    Args:
        doc: The parsed system.yaml.
        slug: The system.

    Returns:
        The assets.
    """
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
        # Earlier rounds' versions the file no longer names; a version counts
        # as this system's only when its description names the system.
        if versions:
            groups.append(Asset("feature group", group, owned_by=slug))
    # Jobs follow the <slug>-<purpose> naming; some are named only in prose.
    add(Asset("job", f"{slug}-*"))
    add(Asset("directory", f"Resources/{slug}"))

    rank = {kind: i for i, kind in enumerate(ORDER)}
    rest = sorted(found, key=lambda a: rank[a.kind])
    at = rank["feature group"]
    return (
        [a for a in rest if rank[a.kind] < at]
        + groups
        + [a for a in rest if rank[a.kind] > at]
    )


def _derives(name: str, slug: str) -> bool:
    """Whether `name` carries the slug, as the build names what it creates (`<slug>-app`, `<slug_pkg>_predictions`, `<slug>model`)."""
    plain = re.sub(r"[-_]", "", name.lower())
    return re.sub(r"[-_]", "", slug.lower()) in plain


def plan(
    doc: dict, slug: str, others: dict[str, dict]
) -> tuple[list[Asset], list[str]]:
    """The assets to delete for the system `slug`, and why each one left out is kept.

    A system made from an example starts with the example's system.yaml, so its
    names alone do not prove it created an asset.
    `others` maps every other registered system's slug to its system.yaml: what
    any of them names is never deleted, and an asset whose name does not carry
    `slug` is deleted only where its description names `slug`.

    Args:
        doc: The parsed system.yaml.
        slug: The system.
        others: Every other registered system's system.yaml, by slug.

    Returns:
        The assets to delete, and a line for each asset kept.
    """
    claimed: dict[tuple[str, str], str] = {}
    for other, other_doc in others.items():
        for asset in inventory(other_doc, other):
            claimed.setdefault((asset.kind, asset.name), other)
    assets: list[Asset] = []
    kept: list[str] = []
    for asset in inventory(doc, slug):
        owner = claimed.get((asset.kind, asset.name))
        if owner:
            kept.append(f"kept {asset}: {owner} names it too")
        elif asset.owned_by or _derives(asset.name, slug):
            assets.append(asset)
        else:
            assets.append(replace(asset, owned_by=slug))
    return assets, kept


def _names(slug: str):
    return re.compile(rf"(?<![\w-]){re.escape(slug)}(?![\w-])")


def _missing(exc: Exception) -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status == 404 or "not found" in str(exc).lower()


class Deleter:
    """Deletes assets of one project; each call returns "deleted", "gone" or why it kept the asset, or raises."""

    def __init__(self, project: Any, other_slugs: tuple[str, ...] = ()) -> None:
        self.project = project
        # Other systems in the project, whose `<slug>-*` jobs are theirs.
        self.other_slugs = other_slugs
        self._fs = None

    @property
    def fs(self) -> Any:
        if self._fs is None:
            self._fs = self.project.get_feature_store()
        return self._fs

    def delete(self, asset: Asset) -> str:
        """Delete `asset`, stopping or unscheduling it first where that is needed.

        Args:
            asset: The asset.

        Returns:
            The outcome.
        """
        return getattr(self, "_" + asset.kind.replace(" ", "_"))(asset)

    def _each(self, asset: Asset, items: list) -> str:
        """Delete the items, only those whose description names `asset.owned_by` when it is set."""
        found = [i for i in items if i is not None]
        mine = found
        if asset.owned_by:
            owner = _names(asset.owned_by)
            mine = [
                i for i in found if owner.search(getattr(i, "description", None) or "")
            ]
        # The earlier versions of a group the system writes may all be another's.
        sweep = asset.kind == "feature group" and asset.version is None
        if found and not mine and not sweep:
            return f"kept {asset}: not created by {asset.owned_by}"
        stop = {"app": "stop", "job": "unschedule"}.get(asset.kind)
        for item in mine:
            # A job with no schedule, or an app that is not running, cannot be stopped.
            if stop:
                with contextlib.suppress(Exception):
                    getattr(item, stop)()
            if asset.kind == "deployment":
                item.delete(force=True)
            else:
                item.delete()
        return "deleted" if mine else "gone"

    def _app(self, asset: Asset) -> str:
        return self._each(asset, [self.project.get_app_api().get_app(asset.name)])

    def _deployment(self, asset: Asset) -> str:
        serving = self.project.get_model_serving()
        return self._each(asset, [serving.get_deployment(asset.name)])

    def _job(self, asset: Asset) -> str:
        if asset.name.endswith("-*"):
            return self._jobs_named(asset.name[:-1])
        return self._each(asset, [self.project.get_job_api().get_job(asset.name)])

    def _jobs_named(self, prefix: str) -> str:
        jobs = [
            j
            for j in self.project.get_job_api().get_jobs() or []
            if j.name.startswith(prefix)
            # `churn-example-v2-train` is churn-example-v2's, not churn-example's.
            and not any(
                j.name.startswith(f"{o}-")
                for o in self.other_slugs
                if o.startswith(prefix)
            )
        ]
        return self._each(Asset("job", prefix + "*"), jobs)

    def _model(self, asset: Asset) -> str:
        return self._each(
            asset, self.project.get_model_registry().get_models(asset.name) or []
        )

    def _feature_view(self, asset: Asset) -> str:
        try:
            views = self.fs.get_feature_views(asset.name) or []
        except Exception as exc:
            if _missing(exc):
                return "gone"
            raise
        # A feature view's training datasets are deleted with it.
        return self._each(asset, views)

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
        return self._each(asset, groups)

    def _data_source(self, asset: Asset) -> str:
        from hopsworks_common.core import rest

        # A data source has no description to prove whose it is.
        if asset.owned_by:
            return f"kept {asset}: not created by {asset.owned_by}"
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
    """`(host, owner, name)` of the system's recorded GitHub repository, or None.

    Args:
        doc: The parsed system.yaml.

    Returns:
        The repository.
    """
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


def _git(
    directory: Path | None, *args: str, host: str | None = None
) -> subprocess.CompletedProcess:
    """Run git; with `host`, a remote operation that may sign in to it through `gh`.

    A terminal keeps `gh`'s login in the HopsFS home, but git's link to it
    (`gh auth setup-git`) lives in ~/.gitconfig, which a terminal restart
    resets; reading a private repository then fails for want of a username.
    `gh auth git-credential` is added after any helper already configured, so
    a stored token or the gh login answers, whichever is there.
    """
    helper = (
        ["-c", f"credential.https://{host}.helper=!gh auth git-credential"]
        if host and shutil.which("gh")
        else []
    )
    return subprocess.run(
        ["git", *helper, *args],
        cwd=directory if directory and directory.is_dir() else None,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )


def delete_repo(
    doc: dict, slug: str, project: str, directory: Path | None = None
) -> str:
    """Delete the system's GitHub repository, or only its branch when the repository holds more.

    The build names a repository it creates `hops-<slug>` or `hops-<slug>-<project>[-N]`,
    and each build works on its own `hops/...` branch. Only a repository named so,
    with no other build's branch, is deleted: one with another name (including a
    `<slug>` repository from before the prefix), or with another build's branch,
    holds more than this system, so its branches (`hops/<branch>` and
    `hops/<branch>/...`) are deleted and the repository is kept; GitHub closes a
    pull request whose branch is deleted.
    Branches are read and deleted with git, which any of the gh login, a token
    or an SSH key allows; deleting a whole repository needs the GitHub API.

    Args:
        doc: The parsed system.yaml.
        slug: The system.
        project: The project's name.
        directory: The system's directory, for its git remote.

    Returns:
        The outcome.

    Raises:
        RuntimeError: git or GitHub refused.
    """
    found = repo_of(doc)
    if found is None:
        return "gone"
    host, owner, name = found
    remote = ""
    if directory is not None:
        remote = _git(directory, "remote", "get-url", "origin").stdout.strip()
    remote = remote or f"https://{host}/{owner}/{name}.git"
    listed = _git(directory, "ls-remote", "--heads", remote, host=host)
    if listed.returncode != 0:
        if re.search(r"not found|does not exist", listed.stderr, re.IGNORECASE):
            return "gone"
        raise RuntimeError(f"could not read {owner}/{name}: {listed.stderr.strip()}")
    branches = [
        line.split("refs/heads/", 1)[1]
        for line in listed.stdout.splitlines()
        if "refs/heads/" in line
    ]
    own = str(((doc.get("system") or {}).get("repo") or {}).get("branch") or "")
    # Only branches the build cut are ever deleted: an example that works on
    # the default branch deletes its repository or nothing.
    mine = [
        b
        for b in branches
        if b.startswith("hops/") and own and (b == own or b.startswith(f"{own}/"))
    ]
    others = [b for b in branches if b.startswith("hops/") and b not in mine]
    alone = re.fullmatch(
        rf"hops-{re.escape(slug)}(-{re.escape(project)}(-\d+)?)?", name
    )
    if others or not alone:
        if not mine:
            if own in branches:
                return (
                    f"kept {owner}/{name}; its {own} branch is not one the build made"
                )
            return "gone"
        pushed = _git(directory, "push", remote, "--delete", *mine, host=host)
        if pushed.returncode != 0:
            raise RuntimeError(
                f"could not delete {', '.join(mine)}: {pushed.stderr.strip()}"
            )
        why = "holds other builds" if others else f"is not named hops-{slug}"
        return f"deleted {', '.join(mine)}; kept {owner}/{name}, which {why}"
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


def delete_code(directory: Path | None) -> str:
    """Delete the system's code directory, its git work tree included.

    Args:
        directory: The system's directory.

    Returns:
        The outcome.
    """
    if directory is None or not directory.exists():
        return "gone"
    shutil.rmtree(directory)
    return "deleted"


# endregion


def run(
    steps: list[tuple[str, Callable[[], str]]], report: Callable[[str, str], None]
) -> str | None:
    """Run the steps in order, reporting each outcome, up to the first that fails.

    Args:
        steps: Each step's label and what runs it.
        report: Called with each step's label and outcome.

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
