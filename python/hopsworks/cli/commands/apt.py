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
2. Each ``.deb`` is unpacked with ``dpkg-deb -x`` into a staging directory, and
   only when every package unpacked cleanly are the files moved into
   ``$HOPS_APT_PREFIX/root``; a failure while moving puts back what was there.
3. Each program gets a wrapper in ``$HOPS_APT_PREFIX/bin``, the one directory the
   Hopsworks images append to ``PATH``.
   The wrapper sets ``LD_LIBRARY_PATH`` to the prefix's libraries for that
   program alone, so no image process or notebook kernel loads a library from the
   user-writable prefix unless it opts in with ``eval "$(hops apt env --libraries)"``.

What this cannot do, by design: run maintainer scripts (a warning names the
packages that have them), replace a package the image already has, or ship a
shared library whose name the image already has (both would shadow the image's
copy, so they are refused with a pointer to custom commands).
Installs live in the container and are gone when it restarts.
"""

from __future__ import annotations

import contextlib
import os
import re
import shlex
import shutil
import subprocess
import sys
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
SYSTEM_ROOT = Path("/")
SYSTEM_STATUS = Path("/var/lib/dpkg/status")
SYSTEM_SOURCES = Path("/etc/apt/sources.list")
SYSTEM_SOURCE_PARTS = Path("/etc/apt/sources.list.d")

# A package name as apt takes it, optionally with :arch and =version. Anything
# else, an option above all, never reaches apt-get's command line.
_PACKAGE = re.compile(r"^[a-z0-9][a-z0-9+.\-]*(:[a-z0-9\-]+)?(=[A-Za-z0-9.+:~\-]+)?$")
# A program name that gets a wrapper; anything else is left without one.
_PROGRAM = re.compile(r"^[A-Za-z0-9_+][A-Za-z0-9._+\-]*$")
# A shared library, by file name: libfoo.so, libfoo.so.1, libfoo.so.1.2.3.
_SHARED_LIBRARY = re.compile(r"\.so(\.[0-9.]+)?$")
# Run at install time. prerm/postrm only matter on removal, and the ldconfig triggers of library packages do
# nothing here since the wrappers find the libraries through LD_LIBRARY_PATH.
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
# Where packages put programs (wrapped into Layout.bin, in this order of preference) and libraries.
BIN_DIRS = ("usr/local/bin", "usr/bin", "bin", "usr/sbin", "sbin")
LIB_DIRS = (
    "usr/lib/x86_64-linux-gnu",
    "lib/x86_64-linux-gnu",
    "usr/lib/aarch64-linux-gnu",
    "lib/aarch64-linux-gnu",
    "usr/local/lib",
    "usr/lib",
    "lib",
)


@dataclass
class Layout:
    """Where ``hops apt`` keeps its state; all under one user-writable prefix."""

    prefix: Path

    @property
    def root(self) -> Path:
        """The tree the packages are unpacked into, laid out like ``/``."""
        return self.prefix / "root"

    @property
    def bin(self) -> Path:
        """The wrappers of the installed programs; the only directory the images put on ``PATH``."""
        return self.prefix / "bin"

    @property
    def staging(self) -> Path:
        """Where packages are unpacked before they are moved into ``root``."""
        return self.prefix / "staging"

    @property
    def state(self) -> Path:
        """Apt's and ``hops apt``'s own state."""
        return self.prefix / "state"

    @property
    def cache(self) -> Path:
        """Apt's cache, with the downloaded packages."""
        return self.prefix / "cache"

    @property
    def archives(self) -> Path:
        """Where apt downloads the packages."""
        return self.cache / "archives"

    @property
    def info(self) -> Path:
        """One file per installed package, listing its paths one per line."""
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
        """The apt sources: the image's, plus the cluster's proxy."""
        return self.state / "sources.list.d"

    @classmethod
    def from_env(cls) -> Layout:
        """The layout under ``$HOPS_APT_PREFIX``, or the default prefix.

        Returns:
            The layout.
        """
        return cls(Path(os.environ.get(PREFIX_ENV) or DEFAULT_PREFIX))


@dataclass
class Unpacked:
    """A package ``hops apt install`` put into the prefix."""

    name: str
    version: str
    files: list[str] = field(default_factory=list)
    scripts: list[str] = field(default_factory=list)


@dataclass
class Plan:
    """What an install would do: the packages apt chose, and what blocks them."""

    debs: list[Path] = field(default_factory=list)
    fields: dict[Path, dict[str, str]] = field(default_factory=dict)
    members: dict[Path, list[str]] = field(default_factory=dict)
    requested: set[str] = field(default_factory=set)
    in_image: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)

    @property
    def additions(self) -> list[tuple[str, str, bool]]:
        """The packages to add, as name, version and whether it was asked for."""
        return [
            (f["Package"], f.get("Version", ""), f["Package"] in self.requested)
            for f in (self.fields[deb] for deb in self.debs)
        ]


# region dpkg status files


def parse_status(text: str) -> list[dict[str, str]]:
    """Parses a dpkg status file.

    Args:
        text: The file's contents.

    Returns:
        Its paragraphs, continuation lines kept on their field.
    """
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
    """Formats paragraphs as a dpkg status file.

    Args:
        paragraphs: The paragraphs, as `parse_status` returns them.

    Returns:
        The file's contents.
    """
    return "".join(
        "".join(f"{k}: {v}\n" for k, v in p.items()) + "\n" for p in paragraphs
    )


def _installed_names(paragraphs: list[dict[str, str]]) -> set[str]:
    return {
        p["Package"]
        for p in paragraphs
        if "Package" in p and p.get("Status", "").endswith(" installed")
    }


def _relation_names(value: str) -> list[list[str]]:
    """A Depends-style field as groups of alternative package names, versions and arches dropped."""
    groups = []
    for group in value.split(","):
        names = [
            re.split(r"[\s(:]", alt.strip(), maxsplit=1)[0] for alt in group.split("|")
        ]
        names = [n for n in names if n]
        if names:
            groups.append(names)
    return groups


def _provided(paragraphs: list[dict[str, str]]) -> set[str]:
    """The package names these paragraphs satisfy: their own and what they provide."""
    names = set()
    for p in paragraphs:
        if "Package" in p:
            names.add(p["Package"])
        for group in _relation_names(p.get("Provides", "")):
            names.update(group)
    return names


def _write_atomically(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


# endregion
# region external tools, kept small so tests can replace them


def _run(args: list[str]) -> None:
    """Runs a command, its output going to the user's terminal; a failure is a ClickException."""
    result = subprocess.run(args, check=False)  # noqa: S603 - fixed argv, validated names
    if result.returncode != 0:
        raise click.ClickException(
            f"{args[0]} {args[1] if len(args) > 1 else ''} failed (exit {result.returncode})"
        )


def _deb_fields(deb: Path) -> dict[str, str]:
    out = subprocess.run(
        ["dpkg-deb", "-f", str(deb)], check=True, capture_output=True, text=True
    )
    return parse_status(out.stdout)[0]


def _deb_scripts(deb: Path) -> dict[str, str]:
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


def _deb_members(deb: Path) -> list[str]:
    """The paths ``deb`` holds, as its data tarball names them (``./usr/bin/x``)."""
    with subprocess.Popen(
        ["dpkg-deb", "--fsys-tarfile", str(deb)], stdout=subprocess.PIPE
    ) as proc:
        assert proc.stdout is not None
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as tar:
            return [m.name for m in tar]


def _deb_extract(deb: Path, dest: Path) -> None:
    subprocess.run(["dpkg-deb", "-x", str(deb), str(dest)], check=True)


def _check_platform() -> None:
    if not sys.platform.startswith("linux"):
        raise click.ClickException(
            "hops apt installs Ubuntu packages in a Hopsworks Jupyter server or terminal; it runs on Linux only"
        )


# endregion
# region paths


def _safe_relative(name: str) -> str | None:
    """``name`` as a path relative to the prefix root, or None when it is absolute, climbs out, or has a newline."""
    if "\n" in name:
        return None
    rel = os.path.normpath(name)
    if rel == "." or os.path.isabs(rel) or rel == ".." or rel.startswith("../"):
        return None
    return rel


def _inside(root: Path, rel: str) -> Path:
    """``root / rel``, provided nothing on the way, not even a symlinked directory, leads out of ``root``.

    The last component is not followed, so a symlink there is the path itself, as unlink and rename want it.
    """
    if _safe_relative(rel) != rel:
        raise ValueError(f"not a path inside the prefix: {rel!r}")
    path = root / rel
    real_root = os.path.realpath(root)
    parent = os.path.realpath(path.parent)
    if parent != real_root and not parent.startswith(real_root + os.sep):
        raise ValueError(f"leads out of the prefix: {rel!r}")
    return path


def _read_list(path: Path) -> list[str]:
    """A package's file list: one path per line, so names with spaces survive."""
    return (
        [line for line in path.read_text().splitlines() if line]
        if path.is_file()
        else []
    )


def _remove_files(root: Path, files: list[str]) -> None:
    """Deletes ``files`` under ``root``, deepest first; directories only when empty. Paths leading out are skipped."""
    for rel in sorted(set(files), key=len, reverse=True):
        try:
            path = _inside(root, rel)
        except ValueError:
            continue
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            with contextlib.suppress(OSError):
                path.rmdir()  # only when empty: another package may share it


def _walk(top: Path, rel: str = "") -> Iterator[tuple[str, bool]]:
    """The paths under ``top`` as (relative path, is a directory), each directory before what it holds; symlinks not followed."""
    with os.scandir(top / rel if rel else top) as entries:
        for entry in sorted(entries, key=lambda e: e.name):
            sub = f"{rel}/{entry.name}" if rel else entry.name
            is_dir = entry.is_dir(follow_symlinks=False)
            yield sub, is_dir
            if is_dir:
                yield from _walk(top, sub)


# endregion
# region the work


@contextlib.contextmanager
def _locked(layout: Layout) -> Iterator[None]:
    import fcntl  # Unix only; imported here so `hops --help` loads this module anywhere.

    layout.prefix.mkdir(parents=True, exist_ok=True)
    with open(layout.prefix / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _apt_options(layout: Layout) -> list[str]:
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


def _system_status() -> list[dict[str, str]]:
    return parse_status(SYSTEM_STATUS.read_text()) if SYSTEM_STATUS.is_file() else []


def _prepare(layout: Layout) -> list[dict[str, str]]:
    """Creates the layout and the files apt reads; returns the image's status paragraphs."""
    for d in (
        layout.root,
        layout.bin,
        layout.info,
        layout.archives / "partial",
        layout.state / "apt" / "lists" / "partial",
        layout.state / "log",
        layout.source_parts,
    ):
        d.mkdir(parents=True, exist_ok=True)
    system = _system_status()
    layout.status.write_text(format_status(system + installed(layout)))
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


def _image_libraries() -> set[str]:
    """The shared-library file names in the image's library directories."""
    names = set()
    for d in LIB_DIRS:
        with contextlib.suppress(OSError):
            names.update(
                n for n in os.listdir(SYSTEM_ROOT / d) if _SHARED_LIBRARY.search(n)
            )
    return names


def _joined(script: str) -> list[str]:
    """The script's logical lines: continuations joined, stripped."""
    return [
        line.strip()
        for line in script.replace("\\\n", " ").splitlines()
        if line.strip()
    ]


def alternatives(script: str) -> dict[str, str]:
    """Reads the links the ``update-alternatives --install`` lines of a maintainer script create.

    Args:
        script: The postinst.

    Returns:
        Link to target, the highest priority winning, slave links included.
    """
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
    """Tells whether install-time scripts do anything beyond update-alternatives, which hops apt reproduces.

    Args:
        scripts: The package's install-time maintainer scripts, by name.

    Returns:
        True when the package may need setup that hops apt does not do.
    """
    for name, script in scripts.items():
        if name != "postinst":
            return True
        lines = [line for line in _joined(script) if not line.startswith("#!")]
        if any(
            not (_TRIVIAL.match(line) or _ALTERNATIVE.search(line)) for line in lines
        ):
            return True
    return False


def plan(layout: Layout, packages: list[str]) -> Plan:
    """Downloads ``packages`` and the dependencies the image lacks, and checks what installing them would shadow.

    Must run under the prefix lock.
    Nothing is unpacked.

    Args:
        layout: The prefix.
        packages: Package names, as apt takes them.

    Returns:
        The plan, its conflicts listing what blocks the install.
    """
    bad = [p for p in packages if not _PACKAGE.match(p)]
    if bad:
        raise click.UsageError(f"not a package name: {', '.join(bad)}")
    system = _prepare(layout)
    options = _apt_options(layout)
    _run(["apt-get", *options, "-q", "update"])
    for stale in layout.archives.glob("*.deb"):
        stale.unlink()
    _run(["apt-get", *options, "-q", "--download-only", "install", "--", *packages])
    result = Plan(debs=sorted(layout.archives.glob("*.deb")))
    result.requested = {p.split("=")[0].split(":")[0] for p in packages}
    image_has = _installed_names(system)
    result.in_image = sorted(result.requested & image_has)
    libraries = _image_libraries()
    for deb in result.debs:
        f = result.fields[deb] = _deb_fields(deb)
        name = f["Package"]
        members = []
        for member in _deb_members(deb):
            rel = _safe_relative(member)
            if rel is None:
                if os.path.normpath(member) != ".":
                    result.conflicts.append(
                        f"{name} holds a path outside its root: {member!r}"
                    )
                continue
            members.append(rel)
        result.members[deb] = members
        if name in image_has:
            result.conflicts.append(
                f"{name} {f.get('Version', '')}: a different version of a package the image has"
            )
        shadowing = sorted(
            {os.path.basename(m) for m in members if _SHARED_LIBRARY.search(m)}
            & libraries
        )
        if shadowing:
            result.conflicts.append(
                f"{name} ships {', '.join(shadowing)}, which the image already has"
            )
    return result


def _conflict_error(conflicts: list[str]) -> click.ClickException:
    return click.ClickException(
        "these would shadow the image's own files:\n  "
        + "\n  ".join(conflicts)
        + "\nInstall them with custom commands on the environment instead."
    )


def _stage(
    layout: Layout, the_plan: Plan
) -> dict[str, tuple[Path, list[str], dict[str, str]]]:
    """Unpacks each package into its own staging directory; returns name -> (staging dir, files, scripts)."""
    staged: dict[str, tuple[Path, list[str], dict[str, str]]] = {}
    all_files = {m for members in the_plan.members.values() for m in members}

    def exists(rel: str) -> bool:
        return rel in all_files or os.path.lexists(layout.root / rel)

    for deb in the_plan.debs:
        name = the_plan.fields[deb]["Package"]
        stage = layout.staging / name
        stage.mkdir(parents=True)
        _deb_extract(deb, stage)
        files = list(the_plan.members[deb])
        for rel in files:
            path = _inside(stage, rel)
            if path.is_symlink():
                target = os.readlink(path)
                inner = (
                    _safe_relative(target.lstrip("/"))
                    if target.startswith("/")
                    else None
                )
                if inner and exists(inner):
                    path.unlink()
                    path.symlink_to(layout.root / inner)
            elif path.is_file():
                mode = path.stat().st_mode
                if mode & 0o6000:
                    path.chmod(mode & ~0o6000)
        scripts = {k: v for k, v in _deb_scripts(deb).items() if k in _SCRIPTS}
        for link, target in alternatives(scripts.get("postinst", "")).items():
            link_rel = (
                _safe_relative(link.lstrip("/")) if link.startswith("/") else None
            )
            target_rel = (
                _safe_relative(target.lstrip("/")) if target.startswith("/") else None
            )
            if (
                not link_rel
                or not target_rel
                or not exists(target_rel)
                or exists(link_rel)
            ):
                continue
            source = _inside(stage, link_rel)
            source.parent.mkdir(parents=True, exist_ok=True)
            source.symlink_to(layout.root / target_rel)
            all_files.add(link_rel)
            files.append(link_rel)
        staged[name] = (stage, files, scripts)
    return staged


def _owners(layout: Layout, skip: set[str]) -> dict[str, str]:
    """The non-directory files of the installed packages other than ``skip``, by path."""
    owners = {}
    for listing in layout.info.glob("*.list"):
        name = listing.name[: -len(".list")]
        if name in skip:
            continue
        for rel in _read_list(listing):
            path = layout.root / rel
            if path.is_symlink() or not path.is_dir():
                owners[rel] = name
    return owners


def _commit(
    layout: Layout, staged: dict[str, tuple[Path, list[str], dict[str, str]]]
) -> None:
    """Moves the staged files into the root; on any failure, takes them out again and puts back what they replaced."""
    owners = _owners(layout, set(staged))
    claimed: dict[str, str] = {}
    for name, (stage, _, _) in staged.items():
        for rel, is_dir in _walk(stage):
            if is_dir:
                continue
            other = owners.get(rel) or claimed.get(rel)
            if other and other != name:
                raise click.ClickException(f"{name} and {other} both ship {rel}")
            claimed[rel] = name
    backups = layout.staging / ".replaced"
    backups.mkdir()
    moved: list[tuple[Path, Path | None]] = []
    made: list[Path] = []
    try:
        for name, (stage, _, _) in staged.items():
            for rel, is_dir in _walk(stage):
                dest = _inside(layout.root, rel)
                if is_dir:
                    if dest.is_dir() and not dest.is_symlink():
                        continue
                    if os.path.lexists(dest):
                        raise click.ClickException(
                            f"{name} needs a directory at {rel}, where there is a file"
                        )
                    dest.mkdir()
                    made.append(dest)
                    continue
                if dest.is_dir() and not dest.is_symlink():
                    raise click.ClickException(
                        f"{name} ships a file at {rel}, where there is a directory"
                    )
                backup = None
                if os.path.lexists(dest):
                    backup = backups / str(len(moved))
                    os.replace(dest, backup)
                moved.append((dest, backup))
                os.replace(stage / rel, dest)
    except BaseException:
        for dest, backup in reversed(moved):
            with contextlib.suppress(OSError):
                if os.path.lexists(dest):
                    dest.unlink()
                if backup is not None:
                    os.replace(backup, dest)
        for d in reversed(made):
            with contextlib.suppress(OSError):
                d.rmdir()
        raise


def _write_wrappers(layout: Layout) -> None:
    """Rewrites ``layout.bin``: one wrapper per program in the root, running it with the prefix's libraries."""
    layout.bin.mkdir(parents=True, exist_ok=True)
    for old in layout.bin.iterdir():
        if old.is_file() or old.is_symlink():
            old.unlink()
    real_root = os.path.realpath(layout.root)
    libs = ":".join(str(layout.root / d) for d in LIB_DIRS)
    done = set()
    for d in BIN_DIRS:
        try:
            directory = _inside(layout.root, d)
        except ValueError:
            continue
        if not directory.is_dir() or directory.is_symlink():
            continue
        for program in sorted(directory.iterdir()):
            name = program.name
            real = os.path.realpath(program)
            if (
                name in done
                or not _PROGRAM.match(name)
                or not real.startswith(real_root + os.sep)
                or not os.path.isfile(real)
                or not os.access(real, os.X_OK)
            ):
                continue
            done.add(name)
            wrapper = layout.bin / name
            wrapper.write_text(
                "#!/bin/sh\n"
                "# hops apt: runs the program with the hops apt libraries; nothing else sees them.\n"
                f'LD_LIBRARY_PATH={shlex.quote(libs)}"${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}"\n'
                "export LD_LIBRARY_PATH\n"
                f'exec {shlex.quote(str(program))} "$@"\n'
            )
            wrapper.chmod(0o755)


def install(layout: Layout, packages: list[str]) -> list[Unpacked]:
    """Installs ``packages`` and the dependencies the image lacks into the prefix.

    Either every package lands or none: they are unpacked into staging first, and a failure while moving them in
    puts back what was there.

    Args:
        layout: The prefix.
        packages: Package names, as apt takes them.

    Returns:
        The packages unpacked.
    """
    with _locked(layout):
        the_plan = plan(layout, packages)
        if the_plan.conflicts:
            raise _conflict_error(the_plan.conflicts)
        if not the_plan.debs:
            return []
        shutil.rmtree(layout.staging, ignore_errors=True)
        try:
            staged = _stage(layout, the_plan)
            _commit(layout, staged)
            user = installed(layout)
            unpacked = []
            for deb in the_plan.debs:
                f = the_plan.fields[deb]
                name = f["Package"]
                _, files, scripts = staged[name]
                listing = layout.info / f"{name}.list"
                stale = set(_read_list(listing)) - set(files)
                _write_atomically(listing, "".join(rel + "\n" for rel in files))
                _remove_files(layout.root, sorted(stale))
                entry = {k: v for k, v in f.items() if k != "Description"}
                entry["Status"] = "install ok installed"
                entry["X-Hops-Apt-Auto"] = "no" if name in the_plan.requested else "yes"
                user = [p for p in user if p.get("Package") != name] + [entry]
                setup = sorted(scripts) if needs_setup(scripts) else []
                unpacked.append(Unpacked(name, f.get("Version", ""), files, setup))
            _write_atomically(layout.user_status, format_status(user))
            _write_wrappers(layout)
        finally:
            shutil.rmtree(layout.staging, ignore_errors=True)
            for deb in the_plan.debs:
                deb.unlink(missing_ok=True)
        return unpacked


def dry_run(layout: Layout, packages: list[str]) -> Plan:
    """Works out what installing ``packages`` would do, without unpacking anything.

    Args:
        layout: The prefix.
        packages: Package names, as apt takes them.

    Returns:
        The plan.
    """
    with _locked(layout):
        try:
            return plan(layout, packages)
        finally:
            for deb in layout.archives.glob("*.deb"):
                deb.unlink()


def remove(layout: Layout, packages: list[str]) -> list[str]:
    """Removes packages ``hops apt`` installed, unless another installed package still needs one of them.

    Args:
        layout: The prefix.
        packages: Package names.

    Returns:
        The packages removed.
    """
    with _locked(layout):
        user = installed(layout)
        known = {p.get("Package") for p in user}
        missing = [p for p in packages if p not in known]
        if missing:
            raise click.ClickException(
                f"not installed with hops apt: {', '.join(missing)}"
            )
        removing = set(packages)
        kept = [p for p in user if p.get("Package") not in removing]
        available = _provided(_system_status()) | _provided(kept)
        needed = []
        for p in kept:
            for field_name in ("Pre-Depends", "Depends"):
                for group in _relation_names(p.get(field_name, "")):
                    if not any(n in available for n in group) and removing & set(group):
                        needed.append(
                            f"{' or '.join(sorted(removing & set(group)))} (needed by {p['Package']})"
                        )
        if needed:
            raise click.ClickException(
                f"still needed: {', '.join(sorted(set(needed)))}. Remove the packages that need them too, in the same command."
            )
        for name in packages:
            listing = layout.info / f"{name}.list"
            _remove_files(layout.root, _read_list(listing))
            listing.unlink(missing_ok=True)
        _write_atomically(layout.user_status, format_status(kept))
        _write_wrappers(layout)
        return packages


def installed(layout: Layout) -> list[dict[str, str]]:
    """Lists the packages ``hops apt`` installed.

    Args:
        layout: The prefix.

    Returns:
        Their dpkg status paragraphs.
    """
    return (
        parse_status(layout.user_status.read_text())
        if layout.user_status.is_file()
        else []
    )


def env_exports(layout: Layout, libraries: bool = False) -> str:
    """Shell exports that put the hops apt programs, and optionally its libraries, on the search paths.

    Args:
        layout: The prefix.
        libraries: Whether to add the prefix's libraries to ``LD_LIBRARY_PATH`` for every program started from the shell.

    Returns:
        The export lines.
    """
    exports = f'export PATH="$PATH:{layout.bin}"\n'
    if libraries:
        libs = ":".join(str(layout.root / d) for d in LIB_DIRS)
        exports += (
            f'export LD_LIBRARY_PATH="${{LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}}{libs}"\n'
        )
    return exports


def _on_path(layout: Layout) -> bool:
    return str(layout.bin) in os.environ.get("PATH", "").split(":")


# endregion
# region click


@click.group("apt")
def apt_group() -> None:
    """Install Ubuntu packages for this container, without root (lost on restart)."""


@apt_group.command("install")
@click.argument("packages", nargs=-1, required=True)
@click.option(
    "--dry-run",
    "show_plan",
    is_flag=True,
    help="Show what would be added and what blocks it, without installing.",
)
def install_cmd(packages: tuple[str, ...], show_plan: bool) -> None:
    """Download PACKAGES and their missing dependencies from the configured repositories and unpack them.

    Args:
        packages: Package names.
        show_plan: Only show what would be installed.
    """
    _check_platform()
    if shutil.which("apt-get") is None or shutil.which("dpkg-deb") is None:
        raise click.ClickException(
            "apt-get and dpkg-deb are needed; this container has no apt"
        )
    layout = Layout.from_env()
    if show_plan:
        _show_plan(dry_run(layout, list(packages)))
        return
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
        "Programs started from now on find them. Their libraries are only on the programs' own path; "
        'to load them from Python or other programs, run: eval "$(hops apt env --libraries)"'
    )


def _show_plan(the_plan: Plan) -> None:
    if the_plan.in_image:
        output.info("Already in the image: %s", ", ".join(the_plan.in_image))
    if the_plan.debs:
        output.print_table(
            ["package", "version", "would be installed as"],
            [
                (name, version, "requested" if asked else "dependency")
                for name, version, asked in the_plan.additions
            ],
        )
    elif not the_plan.conflicts:
        output.info("Nothing to install.")
    if the_plan.conflicts:
        raise _conflict_error(the_plan.conflicts)


@apt_group.command("remove")
@click.argument("packages", nargs=-1, required=True)
def remove_cmd(packages: tuple[str, ...]) -> None:
    """Remove PACKAGES installed with hops apt; refused while another installed package needs one of them.

    Args:
        packages: Package names.
    """
    _check_platform()
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
@click.option(
    "--libraries",
    is_flag=True,
    help="Also put the hops apt libraries on LD_LIBRARY_PATH, for every program the shell starts.",
)
def env_cmd(libraries: bool) -> None:
    """Print the shell exports that put hops apt programs on PATH.

    Args:
        libraries: Also export LD_LIBRARY_PATH.
    """
    click.echo(env_exports(Layout.from_env(), libraries), nl=False)


# endregion
