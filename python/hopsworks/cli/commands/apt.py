"""``hops apt`` — install Ubuntu packages in a Jupyter server or terminal, without root.

Hopsworks containers never run with privilege escalation, so ``sudo apt-get`` is
not available at runtime (custom commands are the way to put a package in an
environment for good). ``hops apt install`` gets most of the way as the normal
user:

1. ``apt-get`` runs as the user with its own state and cache under
   ``$HOPS_APT_PREFIX`` (``/srv/hops/apt-packages``), against the image's sources
   plus the cluster's apt proxy sources (``$HOPS_APT_SOURCES``, set by Hopsworks),
   and downloads the packages and the dependencies the image does not have.
   Signatures are checked against the image's keyrings as usual.
2. Each ``.deb`` is unpacked with ``dpkg-deb -x`` into ``$HOPS_APT_PREFIX/root``,
   whose ``bin`` and ``lib`` directories the Hopsworks images put at the end of
   ``PATH`` and ``LD_LIBRARY_PATH``.

What this cannot do, by design: run maintainer scripts (a warning names the
packages that have them), or replace a package the image already has (that
would shadow the image's copy, so it is refused with a pointer to custom
commands). Installs live in the container and are gone when it restarts.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import shutil
import subprocess
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import click
from hopsworks.cli import output


if TYPE_CHECKING:
    from collections.abc import Iterator


PREFIX_ENV = "HOPS_APT_PREFIX"
SOURCES_ENV = "HOPS_APT_SOURCES"
DEFAULT_PREFIX = "/srv/hops/apt-packages"
SYSTEM_STATUS = Path("/var/lib/dpkg/status")
SYSTEM_SOURCES = Path("/etc/apt/sources.list")
SYSTEM_SOURCE_PARTS = Path("/etc/apt/sources.list.d")

# A package name as apt takes it, optionally with :arch and =version. Anything
# else, an option above all, never reaches apt-get's command line.
_PACKAGE = re.compile(r"^[a-z0-9][a-z0-9+.\-]*(:[a-z0-9\-]+)?(=[A-Za-z0-9.+:~\-]+)?$")
# Run at install time. prerm/postrm only matter on removal, and the ldconfig triggers of library packages do
# nothing here since the libraries are found through LD_LIBRARY_PATH.
_SCRIPTS = ("preinst", "postinst", "config")
_ALTERNATIVE = re.compile(
    r"update-alternatives\s+--install\s+(\S+)\s+\S+\s+(\S+)\s+(\d+)(.*)"
)
_SLAVE = re.compile(r"--slave\s+(\S+)\s+\S+\s+(\S+)")
# Lines of a postinst that need nothing doing beyond what hops apt does itself.
_TRIVIAL = re.compile(
    r"^(#.*|set -e|fi|then|else|esac|;;|exit 0|true|case \"?\$1\"? in|\S+\)|"
    r"if \[ \"?\$1\"? = \"?configure\"? \]; then|update-alternatives --remove .*)$"
)
# The directories the images append to PATH / LD_LIBRARY_PATH; also what `hops apt env` prints.
BIN_DIRS = ("usr/local/bin", "usr/bin", "bin", "usr/sbin", "sbin")
LIB_DIRS = (
    "usr/lib/x86_64-linux-gnu",
    "lib/x86_64-linux-gnu",
    "usr/lib/aarch64-linux-gnu",
    "lib/aarch64-linux-gnu",
    "usr/lib",
    "lib",
)


@dataclass
class Layout:
    """Where ``hops apt`` keeps its state; all under one user-writable prefix."""

    prefix: Path

    @property
    def root(self) -> Path:
        return self.prefix / "root"

    @property
    def state(self) -> Path:
        return self.prefix / "state"

    @property
    def cache(self) -> Path:
        return self.prefix / "cache"

    @property
    def archives(self) -> Path:
        return self.cache / "archives"

    @property
    def info(self) -> Path:
        return self.state / "info"

    @property
    def user_status(self) -> Path:
        """The packages ``hops apt`` installed, as dpkg status paragraphs."""
        return self.state / "user-status"

    @property
    def status(self) -> Path:
        """The image's dpkg status plus ``user_status``: what apt resolves against."""
        return self.state / "status"

    @property
    def source_parts(self) -> Path:
        return self.state / "sources.list.d"

    @classmethod
    def from_env(cls) -> Layout:
        return cls(Path(os.environ.get(PREFIX_ENV) or DEFAULT_PREFIX))


@dataclass
class Unpacked:
    name: str
    version: str
    files: list[str] = field(default_factory=list)
    scripts: list[str] = field(default_factory=list)


# --- dpkg status files ------------------------------------------------------------------------


def parse_status(text: str) -> list[dict[str, str]]:
    """Paragraphs of a dpkg status file, continuation lines kept on their field."""
    paragraphs: list[dict[str, str]] = []
    current: dict[str, str] = {}
    last = None
    for line in text.splitlines():
        if not line.strip():
            if current:
                paragraphs.append(current)
            current, last = {}, None
        elif line[0] in " \t" and last:
            current[last] += "\n" + line
        elif ":" in line:
            key, _, value = line.partition(":")
            last = key.strip()
            current[last] = value.strip()
    if current:
        paragraphs.append(current)
    return paragraphs


def format_status(paragraphs: list[dict[str, str]]) -> str:
    return "".join(
        "".join(f"{k}: {v}\n" for k, v in p.items()) + "\n" for p in paragraphs
    )


def installed_names(paragraphs: list[dict[str, str]]) -> set[str]:
    return {
        p["Package"]
        for p in paragraphs
        if "Package" in p and p.get("Status", "").endswith(" installed")
    }


# --- external tools, kept small so tests can replace them ------------------------------------


def run(args: list[str]) -> None:
    """Runs a command, its output going to the user's terminal; a failure is a ClickException."""
    result = subprocess.run(args, check=False)  # noqa: S603 - fixed argv, validated names
    if result.returncode != 0:
        raise click.ClickException(
            f"{args[0]} {args[1] if len(args) > 1 else ''} failed (exit {result.returncode})"
        )


def deb_fields(deb: Path) -> dict[str, str]:
    out = subprocess.run(
        ["dpkg-deb", "-f", str(deb)], check=True, capture_output=True, text=True
    )
    return parse_status(out.stdout)[0]


def deb_scripts(deb: Path) -> dict[str, str]:
    """The maintainer scripts of ``deb`` (preinst, postinst, ...), by name."""
    scripts: dict[str, str] = {}
    with subprocess.Popen(
        ["dpkg-deb", "--ctrl-tarfile", str(deb)], stdout=subprocess.PIPE
    ) as proc:
        assert proc.stdout is not None
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as tar:
            for member in tar:
                name = os.path.basename(member.name)
                extracted = tar.extractfile(member) if member.isfile() else None
                if name in _SCRIPTS and extracted is not None:
                    scripts[name] = extracted.read().decode("utf-8", "replace")
    return scripts


def deb_extract(deb: Path, root: Path) -> list[str]:
    """Unpacks ``deb`` into ``root``; returns the paths it holds, relative to ``root``."""
    with subprocess.Popen(
        ["dpkg-deb", "--fsys-tarfile", str(deb)], stdout=subprocess.PIPE
    ) as proc:
        assert proc.stdout is not None
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as tar:
            members = [m.name for m in tar]
    subprocess.run(["dpkg-deb", "-x", str(deb), str(root)], check=True)
    return [os.path.normpath(m) for m in members if os.path.normpath(m) != "."]


# --- the work ---------------------------------------------------------------------------------


@contextlib.contextmanager
def locked(layout: Layout) -> Iterator[None]:
    layout.prefix.mkdir(parents=True, exist_ok=True)
    with open(layout.prefix / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def apt_options(layout: Layout) -> list[str]:
    options = {
        "Dir::State": str(layout.state / "apt"),
        "Dir::State::status": str(layout.status),
        "Dir::Cache": str(layout.cache),
        "Dir::Log": str(layout.state / "log"),
        "Dir::Etc::sourcelist": str(SYSTEM_SOURCES),
        "Dir::Etc::sourceparts": str(layout.source_parts),
        # The locks live in our own directories, which only this user writes, and we hold ours.
        "Debug::NoLocking": "1",
        "APT::Get::Assume-Yes": "true",
        "APT::Install-Recommends": "false",
    }
    # apt reads no SSL_CERT_FILE; the package CA bundle Hopsworks sets for pip and uv applies to it too.
    ca = os.environ.get("SSL_CERT_FILE")
    if ca and Path(ca).is_file():
        options["Acquire::https::CaInfo"] = ca
    return [arg for k, v in options.items() for arg in ("-o", f"{k}={v}")]


def prepare(layout: Layout) -> list[dict[str, str]]:
    """Creates the layout and the files apt reads; returns the image's status paragraphs."""
    for d in (
        layout.root,
        layout.info,
        layout.archives / "partial",
        layout.state / "apt" / "lists" / "partial",
        layout.state / "log",
        layout.source_parts,
    ):
        d.mkdir(parents=True, exist_ok=True)
    system = parse_status(SYSTEM_STATUS.read_text()) if SYSTEM_STATUS.is_file() else []
    user = (
        parse_status(layout.user_status.read_text())
        if layout.user_status.is_file()
        else []
    )
    layout.status.write_text(format_status(system + user))
    for old in layout.source_parts.iterdir():
        old.unlink()
    if SYSTEM_SOURCE_PARTS.is_dir():
        for part in sorted(SYSTEM_SOURCE_PARTS.iterdir()):
            if part.is_file() and part.suffix in (".list", ".sources"):
                (layout.source_parts / part.name).symlink_to(part)
    sources = os.environ.get(SOURCES_ENV, "")
    if sources.strip():
        (layout.source_parts / "hopsworks-proxy.list").write_text(
            sources if sources.endswith("\n") else sources + "\n"
        )
    return system


def relink(root: Path, files: list[str]) -> None:
    """Points absolute symlinks into ``root`` where their target is there, and drops setuid/setgid bits."""
    for rel in files:
        path = root / rel
        if path.is_symlink():
            target = os.readlink(path)
            if target.startswith("/") and (root / target.lstrip("/")).exists():
                path.unlink()
                path.symlink_to(root / target.lstrip("/"))
        elif path.is_file():
            mode = path.stat().st_mode
            if mode & 0o6000:
                path.chmod(mode & ~0o6000)


def _joined(script: str) -> list[str]:
    """The script's logical lines: continuations joined, stripped."""
    return [
        line.strip()
        for line in script.replace("\\\n", " ").splitlines()
        if line.strip()
    ]


def alternatives(script: str) -> dict[str, str]:
    """The links ``update-alternatives --install`` lines in ``script`` create, link to target, highest priority first."""
    best: dict[str, tuple[int, str]] = {}
    for line in _joined(script):
        match = _ALTERNATIVE.search(line)
        if not match:
            continue
        link, target, priority, rest = (
            match.group(1),
            match.group(2),
            int(match.group(3)),
            match.group(4),
        )
        pairs = [(link, target)] + [
            (m.group(1), m.group(2)) for m in _SLAVE.finditer(rest)
        ]
        for slave_link, slave_target in pairs:
            if slave_link not in best or best[slave_link][0] < priority:
                best[slave_link] = (priority, slave_target)
    return {link: target for link, (_, target) in best.items()}


def needs_setup(scripts: dict[str, str]) -> bool:
    """Whether the install-time scripts do anything beyond update-alternatives, which hops apt reproduces."""
    for name, script in scripts.items():
        if name != "postinst":
            return True
        lines = [line for line in _joined(script) if not line.startswith("#!")]
        if any(
            not (_TRIVIAL.match(line) or _ALTERNATIVE.search(line)) for line in lines
        ):
            return True
    return False


def link_alternatives(root: Path, links: dict[str, str]) -> list[str]:
    """Creates the alternatives links inside ``root`` where the target is there; returns them relative to ``root``."""
    made = []
    for link, target in links.items():
        source, dest = root / link.lstrip("/"), root / target.lstrip("/")
        if dest.exists() and not source.exists() and not source.is_symlink():
            source.parent.mkdir(parents=True, exist_ok=True)
            source.symlink_to(dest)
            made.append(link.lstrip("/"))
    return made


def install(layout: Layout, packages: list[str]) -> list[Unpacked]:
    bad = [p for p in packages if not _PACKAGE.match(p)]
    if bad:
        raise click.UsageError(f"not a package name: {', '.join(bad)}")
    with locked(layout):
        system = prepare(layout)
        options = apt_options(layout)
        run(["apt-get", *options, "-q", "update"])
        for stale in layout.archives.glob("*.deb"):
            stale.unlink()
        run(["apt-get", *options, "-q", "--download-only", "install", "--", *packages])
        debs = sorted(layout.archives.glob("*.deb"))
        image_has = installed_names(system)
        fields = {deb: deb_fields(deb) for deb in debs}
        clash = sorted(
            f["Package"] for f in fields.values() if f["Package"] in image_has
        )
        if clash:
            raise click.ClickException(
                "these need a different version of packages the image already has, which would shadow the image's "
                f"own: {', '.join(clash)}. Install them with custom commands on the environment instead."
            )
        user = (
            parse_status(layout.user_status.read_text())
            if layout.user_status.is_file()
            else []
        )
        requested = {p.split("=")[0].split(":")[0] for p in packages}
        unpacked = []
        for deb in debs:
            f = fields[deb]
            scripts = {k: v for k, v in deb_scripts(deb).items() if k in _SCRIPTS}
            files = deb_extract(deb, layout.root)
            relink(layout.root, files)
            links = {}
            if "postinst" in scripts:
                links = alternatives(scripts["postinst"])
            files += link_alternatives(layout.root, links)
            (layout.info / f"{f['Package']}.list").write_text("\n".join(files) + "\n")
            entry = {k: v for k, v in f.items() if k not in ("Description",)}
            entry["Status"] = "install ok installed"
            entry["X-Hops-Apt-Auto"] = "no" if f["Package"] in requested else "yes"
            user = [p for p in user if p.get("Package") != f["Package"]] + [entry]
            setup = sorted(scripts) if needs_setup(scripts) else []
            unpacked.append(Unpacked(f["Package"], f.get("Version", ""), files, setup))
            deb.unlink()
        layout.user_status.write_text(format_status(user))
        return unpacked


def remove(layout: Layout, packages: list[str]) -> list[str]:
    with locked(layout):
        user = (
            parse_status(layout.user_status.read_text())
            if layout.user_status.is_file()
            else []
        )
        known = {p.get("Package") for p in user}
        missing = [p for p in packages if p not in known]
        if missing:
            raise click.ClickException(
                f"not installed with hops apt: {', '.join(missing)}"
            )
        for name in packages:
            listing = layout.info / f"{name}.list"
            files = listing.read_text().split() if listing.is_file() else []
            for rel in sorted(files, key=len, reverse=True):
                path = layout.root / rel
                if path.is_symlink() or path.is_file():
                    path.unlink()
                elif path.is_dir():
                    with contextlib.suppress(OSError):
                        path.rmdir()  # only when empty: another package may share it
            listing.unlink(missing_ok=True)
        layout.user_status.write_text(
            format_status([p for p in user if p.get("Package") not in packages])
        )
        return packages


def installed(layout: Layout) -> list[dict[str, str]]:
    return (
        parse_status(layout.user_status.read_text())
        if layout.user_status.is_file()
        else []
    )


def env_exports(layout: Layout) -> str:
    bins = ":".join(str(layout.root / d) for d in BIN_DIRS)
    libs = ":".join(str(layout.root / d) for d in LIB_DIRS)
    return f'export PATH="$PATH:{bins}"\nexport LD_LIBRARY_PATH="${{LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}}{libs}"\n'


def _on_path(layout: Layout) -> bool:
    return str(layout.root / "usr/bin") in os.environ.get("PATH", "").split(":")


# --- click ------------------------------------------------------------------------------------


@click.group("apt")
def apt_group() -> None:
    """Install Ubuntu packages for this container, without root (lost on restart)."""


@apt_group.command("install")
@click.argument("packages", nargs=-1, required=True)
def install_cmd(packages: tuple[str, ...]) -> None:
    """Download PACKAGES and their missing dependencies from the configured repositories and unpack them."""
    if shutil.which("apt-get") is None or shutil.which("dpkg-deb") is None:
        raise click.ClickException(
            "apt-get and dpkg-deb are needed; this container has no apt"
        )
    layout = Layout.from_env()
    unpacked = install(layout, list(packages))
    if not unpacked:
        output.info(
            "Nothing to do: the image or hops apt already has %s.", ", ".join(packages)
        )
        return
    output.success(
        "Unpacked %s into %s.",
        ", ".join(f"{u.name} {u.version}" for u in unpacked),
        layout.root,
    )
    with_scripts = [u.name for u in unpacked if u.scripts]
    if with_scripts:
        output.warn(
            "These packages have install scripts, which hops apt does not run; they may need setup by hand or a "
            "custom-commands build: %s",
            ", ".join(with_scripts),
        )
    if not _on_path(layout):
        output.info(
            'This container\'s PATH does not include them yet. Run: eval "$(hops apt env)"'
        )
    output.info(
        "Programs started from now on see them; restart a Jupyter kernel to load new libraries."
    )


@apt_group.command("remove")
@click.argument("packages", nargs=-1, required=True)
def remove_cmd(packages: tuple[str, ...]) -> None:
    """Remove PACKAGES installed with hops apt (their dependencies stay)."""
    removed = remove(Layout.from_env(), list(packages))
    output.success("Removed %s.", ", ".join(removed))


@apt_group.command("list")
def list_cmd() -> None:
    """List the packages installed with hops apt in this container."""
    rows = [
        (
            p.get("Package", ""),
            p.get("Version", ""),
            "dependency" if p.get("X-Hops-Apt-Auto") == "yes" else "requested",
        )
        for p in installed(Layout.from_env())
    ]
    output.print_table(["package", "version", "installed as"], rows)


@apt_group.command("env")
def env_cmd() -> None:
    """Print the shell exports that put hops apt packages on PATH and LD_LIBRARY_PATH."""
    click.echo(env_exports(Layout.from_env()), nl=False)
