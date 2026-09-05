"""Git-state sync for ``hops session push`` (HWORKS-3147).

Collects the cwd's git context (remotes, branch, HEAD) into the teleport
manifest and arranges the credential the pod needs to check out the same repo
at the same commit before it resumes the session. Two ways to authenticate the
pod, chosen by the user the first time and remembered:

* ``ssh``: a passphrase-free SSH private key staged once into the user's
  private HopsFS home. Either an existing key, or one generated here for
  Hopsworks (``ssh-keygen``, then ``gh ssh-key add`` when the GitHub CLI is
  logged in). Generation is offered on Linux, macOS and WSL; a native Windows
  host must point at an existing key.
* ``token``: a personal access token registered with Hopsworks
  (``hops git provider set``). The pod clones over HTTPS through the
  credential store its entrypoint fills from that token, so no key travels.
  This is the only option for an HTTPS remote, and an SSH remote is rewritten
  to its HTTPS form for the pod when the user picks it.

Passphrase-protected keys are unsupported (no agent runs on the pod).

Everything here is gated and consent-driven: nothing is collected or uploaded
unless the cwd is a git work tree, the branch's remote is an SSH remote, and
the user answered yes (or has a persisted "always"). The consent answer and
the chosen key path persist in ``~/.hops.toml`` under a ``[gitsync]`` table of
their own — deliberately not in the ``[default]`` profile, whose save path
rewrites a fixed set of keys and would drop ours.
"""

from __future__ import annotations

import contextlib
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import click
from hopsworks.cli import config, output
from hopsworks.cli.commands import git as git_cmd


_UNSUPPORTED_REMOTE = "git sync needs an SSH or HTTPS remote"
_UNSUPPORTED_KEY = "git sync not supported for passphrase-protected ssh keys"
# The key generated for Hopsworks when the user asks for a new one; kept apart
# from their personal identity so it can be revoked on its own.
_NEW_KEY_NAME = "hopsworks_teleport_ed25519"
_NEW_KEY_COMMENT = "hopsworks-teleport"


def _git(args: list[str], cwd: str | None = None) -> tuple[int, str]:
    """Run a git command and return ``(returncode, stripped stdout)``.

    Never raises: a missing git binary or a crash reads as a nonzero rc, and
    the callers treat every failure as "skip the sync", not "fail the push".
    """
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return 1, ""
    return proc.returncode, proc.stdout.strip()


def _is_ssh_url(url: str) -> bool:
    return url.startswith("ssh://") or (
        "@" in url.split("/", 1)[0] and ":" in url and not url.startswith("http")
    )


def _ssh_host(url: str) -> str:
    """The host ssh would connect to for a git SSH URL."""
    if url.startswith("ssh://"):
        rest = url[len("ssh://") :].split("/", 1)[0]
    else:  # scp-like git@host:path
        rest = url.split(":", 1)[0]
    host = rest.rsplit("@", 1)[-1]
    return host.split(":", 1)[0]


def _is_https_url(url: str) -> bool:
    return url.startswith(("https://", "http://"))


def _https_host(url: str) -> str:
    """The host of an HTTPS git URL, without credentials or port."""
    rest = url.split("://", 1)[1].split("/", 1)[0]
    return rest.rsplit("@", 1)[-1].split(":", 1)[0]


def _remote_host(url: str) -> str:
    return _https_host(url) if _is_https_url(url) else _ssh_host(url)


def _ssh_to_https(url: str) -> str:
    """Rewrite a git SSH URL to the HTTPS form of the same repository.

    ``git@github.com:org/repo.git`` -> ``https://github.com/org/repo.git`` and
    ``ssh://git@host[:port]/org/repo.git`` -> ``https://host/org/repo.git``. Used
    when the pod authenticates with a provider token: the token only works over
    HTTPS, while the laptop keeps its SSH remote untouched.
    """
    if _is_https_url(url):
        return url
    if url.startswith("ssh://"):
        rest = url[len("ssh://") :]
        hostpart, _, path = rest.partition("/")
        host = hostpart.rsplit("@", 1)[-1].split(":", 1)[0]
        return f"https://{host}/{path}"
    hostpart, _, path = url.partition(":")
    host = hostpart.rsplit("@", 1)[-1]
    return f"https://{host}/{path}"


def _can_generate_key() -> bool:
    """Whether a new key can be generated on this host.

    Linux, macOS and WSL (which reports Linux) qualify when ssh-keygen is present.
    Native Windows does not; the user points at an existing key instead.
    """
    return platform.system() != "Windows" and shutil.which("ssh-keygen") is not None


def _generate_key(path: Path) -> bool:
    """Create a passphrase-free ed25519 keypair at ``path`` (and ``path.pub``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        path.parent.chmod(0o700)
    try:
        proc = subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                _NEW_KEY_COMMENT,
                "-f",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0 and path.is_file()


def _gh_add_key(pub: Path) -> bool | None:
    """Register ``pub`` with GitHub through the gh CLI.

    None when gh is absent or not logged in (the caller prints manual steps),
    True on success, False when gh refused (already registered, no scope).
    """
    gh = shutil.which("gh")
    if not gh:
        return None
    try:
        status = subprocess.run(
            [gh, "auth", "status"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if status.returncode != 0:
            return None
        proc = subprocess.run(
            [gh, "ssh-key", "add", str(pub), "--title", _NEW_KEY_COMMENT],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        output.warn("gh ssh-key add failed: %s", (proc.stderr or proc.stdout).strip())
        return False
    return True


def _repo_state() -> dict | None:
    """The cwd's git context, or None when the cwd is not inside a work tree.

    Detached HEAD or a branch without a remote also return None, after a short
    info line: there is nothing the pod could check out and pull.
    """
    rc, inside = _git(["rev-parse", "--is-inside-work-tree"])
    if rc != 0 or inside != "true":
        return None
    _, root = _git(["rev-parse", "--show-toplevel"])
    _, branch = _git(["rev-parse", "--abbrev-ref", "HEAD"])
    if not root or not branch or branch == "HEAD":
        output.info("git sync skipped: detached HEAD.")
        return None
    rc, remote = _git(["config", f"branch.{branch}.remote"])
    if rc != 0 or not remote:
        remote = "origin"
    rc, url = _git(["remote", "get-url", remote])
    if rc != 0 or not url:
        output.info("git sync skipped: branch %s has no remote.", branch)
        return None
    remotes: dict[str, str] = {}
    _, names = _git(["remote"])
    for name in names.splitlines():
        rc, u = _git(["remote", "get-url", name])
        if rc == 0 and u:
            remotes[name] = u
    _, head = _git(["rev-parse", "HEAD"])
    root_path = Path(root)
    try:
        rel = str(Path.cwd().resolve().relative_to(root_path.resolve()))
    except ValueError:
        rel = "."
    return {
        "root": str(root_path),
        "root_rel_cwd": "" if rel == "." else rel,
        "remotes": remotes,
        "remote": remote,
        "url": url,
        "branch": branch,
        "head": head,
    }


def _prefs() -> dict:
    try:
        import tomllib
    except ImportError:  # pragma: no cover - 3.10 path
        import tomli as tomllib
    try:
        with config.CONFIG_PATH.open("rb") as f:
            return tomllib.load(f).get("gitsync", {})
    except (OSError, ValueError):
        return {}


def _save_prefs(**updates) -> None:
    """Merge ``updates`` into the ``[gitsync]`` table, preserving everything else."""
    import tomli_w

    try:
        import tomllib
    except ImportError:  # pragma: no cover - 3.10 path
        import tomli as tomllib
    try:
        with config.CONFIG_PATH.open("rb") as f:
            existing = tomllib.load(f)
    except (OSError, ValueError):
        existing = {}
    table = existing.get("gitsync", {})
    table.update({k: v for k, v in updates.items() if v is not None})
    existing["gitsync"] = table
    try:
        fd = os.open(
            str(config.CONFIG_PATH), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600
        )
        with os.fdopen(fd, "wb") as f:
            tomli_w.dump(existing, f)
    except OSError as exc:
        # A dropped preference means the consent prompt comes back next push;
        # say why instead of looking like the answer was never given.
        output.warn("Could not persist the git-sync preference (%s).", exc)


def _default_key(host: str) -> Path:
    """The key ssh would use for ``host``: first existing IdentityFile, else id_rsa."""
    try:
        proc = subprocess.run(
            ["ssh", "-G", host],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        for line in proc.stdout.splitlines():
            if line.startswith("identityfile "):
                candidate = Path(line.split(" ", 1)[1]).expanduser()
                if candidate.is_file():
                    return candidate
    except (OSError, subprocess.SubprocessError):
        pass
    return Path.home() / ".ssh" / "id_rsa"


def _key_passphrase_free(key: Path) -> bool:
    """Whether the private key can be used without a passphrase."""
    try:
        proc = subprocess.run(
            ["ssh-keygen", "-y", "-P", "", "-f", str(key)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _public_key_line(key: Path) -> str | None:
    """The public key derived from the private key, for fingerprint comparison."""
    try:
        proc = subprocess.run(
            ["ssh-keygen", "-y", "-P", "", "-f", str(key)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return proc.stdout.strip() if proc.returncode == 0 else None


def _ensure_clean_and_pushed(state: dict, interactive: bool) -> bool:
    """The pod can only reconstruct committed-and-pushed state; enforce or offer.

    Returns True when the tree is clean and HEAD is contained in the branch's
    upstream, running an interactive commit+push when offered and accepted.
    """
    root = state["root"]
    _, dirty = _git(["status", "--porcelain"], cwd=root)
    upstream_ref = f"{state['remote']}/{state['branch']}"
    _git(["fetch", state["remote"], state["branch"]], cwd=root)  # freshen the ref
    rc, _ = _git(["merge-base", "--is-ancestor", "HEAD", upstream_ref], cwd=root)
    unpushed = rc != 0
    if not dirty and not unpushed:
        return True
    if not interactive:
        output.info(
            "git sync skipped: uncommitted or unpushed work (commit and push, "
            "then re-run)."
        )
        return False
    if not click.confirm(
        "You have uncommitted/unpushed work. Commit and push it now?",
        default=False,
    ):
        output.info(
            "git sync skipped. To include your work: git add … && "
            "git commit && git push"
        )
        return False
    if dirty:
        _, staged = _git(["diff", "--cached", "--name-only"], cwd=root)
        if not staged:
            # Show exactly what would go, and stage TRACKED files only (`-u`):
            # `-A` would sweep untracked files -- .env, keys, build output --
            # into a commit that is about to be pushed to a remote. Default is
            # No; the user opts in to staging with the list in front of them.
            output.info("Uncommitted changes:")
            for line in dirty.splitlines():
                output.info("  %s", line)
            if click.confirm(
                "Nothing is staged; stage the modified tracked files (untracked "
                "files stay local)?",
                default=False,
            ):
                _git(["add", "-u"], cwd=root)
                _, staged = _git(["diff", "--cached", "--name-only"], cwd=root)
        if staged:
            msg = click.prompt(
                "Commit message",
                default="hops session push: sync work in progress",
            )
            rc, _ = _git(["commit", "-m", msg], cwd=root)
            if rc != 0:
                output.warn("git commit failed; git sync skipped.")
                return False
    rc, _ = _git(["push", state["remote"], state["branch"]], cwd=root)
    if rc != 0:
        output.warn("git push failed; git sync skipped.")
        return False
    _, head = _git(["rev-parse", "HEAD"], cwd=root)
    state["head"] = head
    # Anything left unstaged (user declined staging) stays local by choice.
    _, still_dirty = _git(["status", "--porcelain"], cwd=root)
    if still_dirty:
        output.warn("Unstaged changes remain local; the pod gets the pushed state.")
    return True


def _stage_key(dataset_api, teleport_user_root: str, key: Path) -> str | None:
    """Upload the key (and its .pub) once into ``Users/<u>/.ssh/``; return its name.

    Present already → skip the upload, comparing the staged .pub against the
    local key and warning on a mismatch. The home is mode 0700, so containment
    comes from the directory, matching the teleport transcript store.
    """
    ssh_dir = f"{teleport_user_root}/.ssh"
    key_name = key.name
    pub_line = _public_key_line(key)
    staged: list[str] = []
    with contextlib.suppress(Exception):
        staged = [Path(p).name for p in dataset_api.list(ssh_dir)]
    if key_name in staged:
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "staged.pub"
            with contextlib.suppress(Exception):
                dataset_api.download(
                    f"{ssh_dir}/{key_name}.pub", local_path=str(local), overwrite=True
                )
                staged_pub = local.read_text().strip()
                if pub_line and staged_pub.split()[:2] != pub_line.split()[:2]:
                    output.warn(
                        "A different key named %s is already staged; keeping it. "
                        "Rename your key or remove the staged one to replace.",
                        key_name,
                    )
        return key_name
    with contextlib.suppress(Exception):
        dataset_api.mkdir(ssh_dir)
    try:
        dataset_api.upload(local_path=str(key), upload_path=ssh_dir, overwrite=False)
        if pub_line:
            with tempfile.TemporaryDirectory() as tmp:
                pub = Path(tmp) / f"{key_name}.pub"
                pub.write_text(pub_line + "\n")
                dataset_api.upload(
                    local_path=str(pub), upload_path=ssh_dir, overwrite=True
                )
    except Exception as exc:  # noqa: BLE001
        output.warn("Could not stage the SSH key (%s); git sync skipped.", exc)
        return None
    output.success("✓ Staged SSH key %s in your private home", key_name)
    return key_name


def _ensure_token(host: str, interactive: bool) -> bool:
    """Make sure a provider token for ``host`` is registered with Hopsworks.

    Uses the one already registered when present; otherwise, interactively,
    asks for the username and token (hidden) and registers them, exactly like
    ``hops git provider set``. Non-interactive runs never prompt.
    """
    provider = git_cmd.provider_for_host(host)
    if provider is None:
        if not interactive:
            return False
        provider = click.prompt(
            f"Which Git provider is {host}?",
            type=click.Choice(["GitHub", "GitLab", "BitBucket"], case_sensitive=False),
            default="GitLab" if "gitlab" in host.lower() else "GitHub",
        )
        provider = git_cmd.canonical_provider(provider)
    try:
        existing = git_cmd.find_provider(provider, host)
    except Exception as exc:  # noqa: BLE001 - a lookup failure must not break the push
        output.warn("Could not read your Git providers (%s); git sync skipped.", exc)
        return False
    if existing:
        output.info(
            "Using your registered %s token for %s (username %s).",
            provider,
            host,
            existing.username,
        )
        return True
    if not interactive:
        output.info(
            "git sync skipped: no %s token registered for %s. Register one with "
            "`hops git provider set --provider %s`.",
            provider,
            host,
            provider.lower(),
        )
        return False
    output.info(
        "The pod clones over HTTPS with a %s personal access token registered with "
        "Hopsworks (it needs repo read access; it is stored in your account, never "
        "in the manifest).",
        provider,
    )
    if not click.confirm(f"Register a {provider} token for {host} now?", default=True):
        output.info("git sync skipped.")
        return False
    username = click.prompt(f"{provider} username")
    token = click.prompt(f"{provider} personal access token", hide_input=True)
    try:
        git_cmd.register_provider(provider, username, token, host)
    except Exception as exc:  # noqa: BLE001
        output.warn("Could not register the token (%s); git sync skipped.", exc)
        return False
    output.success("✓ Registered %s token for %s", provider, host)
    return True


def _choose_method(
    host: str, https_remote: bool, prefs: dict, interactive: bool
) -> str | None:
    """Pick how the pod authenticates: ``ssh``, ``ssh-new`` or ``token``.

    An HTTPS remote can only work with a token. For an SSH remote the stored
    preference wins; otherwise the user chooses once and the choice persists.
    """
    if https_remote:
        return "token"
    method = prefs.get("method")
    if method in ("ssh", "token"):
        return method
    if not interactive:
        return "ssh"  # the pre-existing default: the key ssh -G resolves
    can_generate = _can_generate_key()
    output.info("How should the terminal pod authenticate to %s?", host)
    output.info("  [1] an existing SSH private key")
    if can_generate:
        output.info(
            "  [2] a new passphrase-free SSH key created for Hopsworks and "
            "registered with GitHub. Prerequisite: 'gh' must be installed."
        )
    else:
        output.info(
            "  [2] (unavailable here: Windows without ssh-keygen; use [1] or [3])"
        )
    provider = git_cmd.provider_for_host(host) or "GitHub"
    output.info("  [3] a %s personal access token", provider)
    choices = ["1", "3"] + (["2"] if can_generate else [])
    pick = click.prompt("Choice", type=click.Choice(choices), default="1")
    method = {"1": "ssh", "2": "ssh-new", "3": "token"}[pick]
    _save_prefs(method="token" if method == "token" else "ssh")
    return method


def maybe_collect(dataset_api, teleport_user_root: str) -> dict | None:
    """Run the git-sync gates and consent flow; return the manifest ``git`` object.

    None means no sync (not a git dir, unsupported remote or key, declined, or
    a gate could not pass non-interactively). The session push itself always
    proceeds regardless.
    """
    state = _repo_state()
    if state is None:
        return None
    url = state["url"]
    https_remote = _is_https_url(url)
    if not https_remote and not _is_ssh_url(url):
        output.info(_UNSUPPORTED_REMOTE)
        return None
    host = _remote_host(url)

    interactive = not output.JSON_MODE and sys.stdin.isatty()
    prefs = _prefs()
    answer = prefs.get("answer")
    if answer == "never":
        return None
    if answer != "always":
        if not interactive:
            return None
        choice = click.prompt(
            "Sync git state to the terminal? [a]lways / [y]es this time / "
            "[n]ot now / n[e]ver",
            type=click.Choice(["a", "y", "n", "e"], case_sensitive=False),
            default="a",
        ).lower()
        if choice == "e":
            _save_prefs(answer="never")
            return None
        if choice == "n":
            return None
        if choice == "a":
            _save_prefs(answer="always")

    method = _choose_method(host, https_remote, prefs, interactive)
    if method is None:
        return None

    common = {
        "root_rel_cwd": state["root_rel_cwd"],
        "remote": state["remote"],
        "branch": state["branch"],
        "head": state["head"],
    }

    if method == "token":
        if not _ensure_token(host, interactive):
            return None
        if not _ensure_clean_and_pushed(state, interactive):
            return None
        # The pod only has the token, so every remote it gets is the HTTPS form;
        # the laptop's own remotes are left as they are.
        return {
            **common,
            "auth": "token",
            "remotes": {n: _ssh_to_https(u) for n, u in state["remotes"].items()},
            "head": state["head"],
        }

    if method == "ssh-new":
        key = Path.home() / ".ssh" / _NEW_KEY_NAME
        if key.is_file():
            output.info("Reusing the Hopsworks key at %s.", key)
        elif _generate_key(key):
            output.success("✓ Generated a passphrase-free ed25519 key at %s", key)
        else:
            output.warn("ssh-keygen failed; git sync skipped.")
            return None
        pub = key.with_suffix(".pub")
        added = _gh_add_key(pub)
        if added is True:
            output.success(
                "✓ Added the public key to your GitHub account (gh ssh-key add)"
            )
        elif added is None:
            output.info(
                "GitHub CLI not available or not logged in. Add this public key at "
                "https://github.com/settings/keys (or your provider's SSH keys page):"
            )
            with contextlib.suppress(OSError):
                output.info("  %s", pub.read_text().strip())
    else:
        default_key = Path(prefs.get("key_file", "")).expanduser()
        if not default_key.is_file():
            default_key = _default_key(host)
        if interactive:
            # A dedicated deploy key is safer to stage than a personal identity key.
            key = Path(
                click.prompt("SSH key file", default=str(default_key))
            ).expanduser()
        else:
            key = default_key
        if not key.is_file():
            output.info("git sync skipped: %s not found.", key)
            return None
    if not _key_passphrase_free(key):
        output.info(_UNSUPPORTED_KEY)
        return None
    _save_prefs(key_file=str(key))

    if not _ensure_clean_and_pushed(state, interactive):
        return None

    key_name = _stage_key(dataset_api, teleport_user_root, key)
    if not key_name:
        return None

    return {
        **common,
        "auth": "ssh",
        "remotes": state["remotes"],
        "head": state["head"],
        "key_name": key_name,
    }
