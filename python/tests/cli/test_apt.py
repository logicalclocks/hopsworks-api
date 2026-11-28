"""Tests for ``hops apt``: rootless installs of Ubuntu packages into a user prefix.

apt-get and dpkg-deb are replaced by fakes that act on the test's own files, so
the tests run anywhere; the real tools are exercised by the loadtest on a cluster.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import apt
from hopsworks.cli.main import cli


if TYPE_CHECKING:
    from pathlib import Path


SYSTEM_STATUS = """Package: libc6
Status: install ok installed
Version: 2.42-1

Package: zlib1g
Status: install ok installed
Version: 1:1.3
"""


class FakeTools:
    """apt-get "downloads" the debs in ``offer``; dpkg-deb reads them as fields + files."""

    def __init__(self, layout: apt.Layout):
        self.layout = layout
        self.offer: dict[str, dict] = {}
        self.calls: list[list[str]] = []

    def run(self, args):
        self.calls.append(args)
        if "install" in args:
            for name in self.offer:
                (self.layout.archives / f"{name}.deb").write_text(name)

    def fields(self, deb: Path):
        spec = self.offer[deb.read_text()]
        return {
            "Package": deb.read_text(),
            "Version": spec.get("version", "1.0"),
            "Description": "x",
        }

    def scripts(self, deb: Path):
        return self.offer[deb.read_text()].get("scripts", {})

    def extract(self, deb: Path, root: Path):
        files = []
        for rel, content in self.offer[deb.read_text()].get("files", {}).items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, tuple) and content[0] == "link":
                path.symlink_to(content[1])
            else:
                path.write_text(content)
                if rel.endswith("-suid"):
                    path.chmod(0o4755)
            files.append(rel)
        return files


@pytest.fixture
def layout(tmp_path, monkeypatch):
    status = tmp_path / "dpkg-status"
    status.write_text(SYSTEM_STATUS)
    parts = tmp_path / "sources.list.d"
    parts.mkdir()
    (parts / "ubuntu.sources").write_text("Types: deb\n")
    (parts / "ignored.save").write_text("x")
    monkeypatch.setattr(apt, "SYSTEM_STATUS", status)
    monkeypatch.setattr(apt, "SYSTEM_SOURCE_PARTS", parts)
    monkeypatch.setenv(apt.PREFIX_ENV, str(tmp_path / "prefix"))
    monkeypatch.delenv(apt.SOURCES_ENV, raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    return apt.Layout.from_env()


@pytest.fixture
def tools(layout, monkeypatch):
    fake = FakeTools(layout)
    monkeypatch.setattr(apt, "run", fake.run)
    monkeypatch.setattr(apt, "deb_fields", fake.fields)
    monkeypatch.setattr(apt, "deb_scripts", fake.scripts)
    monkeypatch.setattr(apt, "deb_extract", fake.extract)
    return fake


def test_install_unpacks_into_the_prefix_and_records_the_packages(layout, tools):
    tools.offer = {
        "figlet": {"version": "2.2.5", "files": {"usr/bin/figlet": "bin"}},
        "libfoo1": {"files": {"usr/lib/x86_64-linux-gnu/libfoo.so.1": "lib"}},
    }
    unpacked = apt.install(layout, ["figlet"])

    assert [u.name for u in unpacked] == ["figlet", "libfoo1"]
    assert (layout.root / "usr/bin/figlet").read_text() == "bin"
    rows = {p["Package"]: p for p in apt.installed(layout)}
    assert rows["figlet"]["X-Hops-Apt-Auto"] == "no"
    assert rows["libfoo1"]["X-Hops-Apt-Auto"] == "yes"
    assert rows["figlet"]["Status"] == "install ok installed"
    assert "Description" not in rows["figlet"]
    # The downloaded debs are not kept.
    assert not list(layout.archives.glob("*.deb"))


def test_apt_runs_as_the_user_against_private_state(layout, tools):
    apt.install(layout, ["figlet"])
    update, download = tools.calls
    assert update[0] == "apt-get" and update[-1] == "update"
    assert download[-3:] == ["install", "--", "figlet"]
    assert "--download-only" in download
    options = dict(a.split("=", 1) for a in download if "=" in a and "::" in a)
    assert options["Dir::State::status"] == str(layout.status)
    assert options["Dir::Cache"] == str(layout.cache)
    assert options["Dir::Etc::sourceparts"] == str(layout.source_parts)
    assert options["APT::Install-Recommends"] == "false"
    # Nothing of the image's apt state is written to: every path is under the prefix.
    for key in (
        "Dir::State",
        "Dir::State::status",
        "Dir::Cache",
        "Dir::Log",
        "Dir::Etc::sourceparts",
    ):
        assert options[key].startswith(str(layout.prefix))


def test_apt_resolves_against_the_image_and_previous_installs(layout, tools):
    tools.offer = {"figlet": {"files": {"usr/bin/figlet": "b"}}}
    apt.install(layout, ["figlet"])
    tools.offer = {}
    apt.install(layout, ["figlet"])
    names = [p["Package"] for p in apt.parse_status(layout.status.read_text())]
    assert names == ["libc6", "zlib1g", "figlet"]


def test_sources_are_the_images_plus_the_cluster_proxy(layout, tools, monkeypatch):
    monkeypatch.setenv(apt.SOURCES_ENV, "deb http://proxy:8081/ubuntu noble main")
    apt.install(layout, ["figlet"])
    parts = sorted(p.name for p in layout.source_parts.iterdir())
    assert parts == ["hopsworks-proxy.list", "ubuntu.sources"]
    assert (
        layout.source_parts / "hopsworks-proxy.list"
    ).read_text() == "deb http://proxy:8081/ubuntu noble main\n"
    assert (layout.source_parts / "ubuntu.sources").is_symlink()


def test_package_ca_bundle_is_passed_to_apt(layout, tools, monkeypatch, tmp_path):
    bundle = tmp_path / "ca-bundle.crt"
    bundle.write_text("pem")
    monkeypatch.setenv("SSL_CERT_FILE", str(bundle))
    apt.install(layout, ["figlet"])
    assert f"Acquire::https::CaInfo={bundle}" in tools.calls[0]


def test_refuses_to_shadow_a_package_the_image_has(layout, tools):
    tools.offer = {
        "figlet": {"files": {"usr/bin/figlet": "b"}},
        "libc6": {"version": "2.43-1"},
    }
    with pytest.raises(click.ClickException, match="libc6"):
        apt.install(layout, ["figlet"])
    # Nothing unpacked, nothing recorded.
    assert not (layout.root / "usr/bin/figlet").exists()
    assert apt.installed(layout) == []


@pytest.mark.parametrize(
    "name", ["-o", "--allow-unauthenticated", "Foo", "./local.deb", "a b", "pkg;rm"]
)
def test_only_package_names_reach_apt(layout, tools, name):
    with pytest.raises(click.UsageError):
        apt.install(layout, [name])
    assert tools.calls == []


@pytest.mark.parametrize(
    "name",
    ["figlet", "libssl3t64", "g++-14", "python3.13", "jq=1.7.1-3", "libfoo:amd64"],
)
def test_valid_package_names(layout, tools, name):
    apt.install(layout, [name])
    assert tools.calls[-1][-1] == name


def test_setuid_bits_are_dropped_and_absolute_links_point_into_the_prefix(
    layout, tools
):
    tools.offer = {
        "tool": {
            "files": {
                "usr/bin/tool-suid": "b",
                "usr/lib/tool/real": "r",
                "usr/bin/tool": ("link", "/usr/lib/tool/real"),
                "usr/bin/editor": ("link", "/etc/alternatives/editor"),
            }
        }
    }
    apt.install(layout, ["tool"])
    assert not os.stat(layout.root / "usr/bin/tool-suid").st_mode & 0o6000
    assert os.readlink(layout.root / "usr/bin/tool") == str(
        layout.root / "usr/lib/tool/real"
    )
    # A target the package does not ship is left alone.
    assert os.readlink(layout.root / "usr/bin/editor") == "/etc/alternatives/editor"


def test_remove_deletes_the_package_files_only(layout, tools):
    tools.offer = {
        "a": {"files": {"usr/bin/a": "a", "usr/share/doc/a/README": "x"}},
        "b": {"files": {"usr/bin/b": "b"}},
    }
    apt.install(layout, ["a", "b"])
    apt.remove(layout, ["a"])
    assert not (layout.root / "usr/bin/a").exists()
    assert (layout.root / "usr/bin/b").exists()
    assert [p["Package"] for p in apt.installed(layout)] == ["b"]


def test_remove_refuses_what_hops_apt_did_not_install(layout, tools):
    with pytest.raises(click.ClickException, match="libc6"):
        apt.remove(layout, ["libc6"])


def test_env_appends_the_prefix_after_the_system_paths(layout):
    exports = apt.env_exports(layout)
    assert exports.startswith(
        'export PATH="$PATH:' + str(layout.root / "usr/local/bin")
    )
    assert str(layout.root / "usr/lib/x86_64-linux-gnu") in exports


def test_cli_install_warns_about_install_scripts(layout, tools, monkeypatch):
    monkeypatch.setattr(apt.shutil, "which", lambda _: "/usr/bin/x")
    tools.offer = {
        "svc": {
            "scripts": {"postinst": "#!/bin/sh\nset -e\nadduser --system svc\n"},
            "files": {"usr/sbin/svc": "s"},
        }
    }
    result = CliRunner().invoke(cli, ["apt", "install", "svc"])
    assert result.exit_code == 0, result.output
    assert "svc" in result.output and "install scripts" in result.output


def test_cli_install_without_apt(layout, monkeypatch):
    monkeypatch.setattr(apt.shutil, "which", lambda _: None)
    result = CliRunner().invoke(cli, ["apt", "install", "figlet"])
    assert result.exit_code != 0
    assert "no apt" in result.output


def test_cli_list(layout, tools):
    tools.offer = {"figlet": {"version": "2.2.5", "files": {"usr/bin/figlet": "b"}}}
    apt.install(layout, ["figlet"])
    result = CliRunner().invoke(cli, ["apt", "list"])
    assert result.exit_code == 0, result.output
    assert "figlet" in result.output and "requested" in result.output


# Debian's figlet postinst, verbatim: the binary ships as figlet-utf8 and the command is an alternative.
FIGLET_POSTINST = """#!/bin/sh

set -e

if [ "$1" = configure ]; then
 # Remove old alternative if it exists
  update-alternatives --remove figlet /usr/bin/figlet-figlet || true

  # Add alternatives for figlet
  update-alternatives --install /usr/bin/figlet figlet \\
        /usr/bin/figlet-utf8 30 --slave /usr/share/man/man6/figlet.6.gz \\
\tfiglet.6.gz /usr/share/man/man6/figlet-utf8.6.gz
fi
"""


def test_alternatives_parses_install_and_slave_links():
    assert apt.alternatives(FIGLET_POSTINST) == {
        "/usr/bin/figlet": "/usr/bin/figlet-utf8",
        "/usr/share/man/man6/figlet.6.gz": "/usr/share/man/man6/figlet-utf8.6.gz",
    }


def test_alternatives_keeps_the_highest_priority():
    script = (
        "update-alternatives --install /usr/bin/editor editor /usr/bin/vim.basic 30\n"
        "update-alternatives --install /usr/bin/editor editor /usr/bin/nano 40\n"
    )
    assert apt.alternatives(script) == {"/usr/bin/editor": "/usr/bin/nano"}


def test_update_alternatives_alone_needs_no_setup():
    assert not apt.needs_setup({"postinst": FIGLET_POSTINST})
    assert apt.needs_setup({"postinst": "#!/bin/sh\nadduser --system x\n"})
    assert apt.needs_setup({"preinst": "#!/bin/sh\n"})


def test_install_creates_alternatives_inside_the_prefix(layout, tools):
    tools.offer = {
        "figlet": {
            "scripts": {"postinst": FIGLET_POSTINST},
            "files": {
                "usr/bin/figlet-utf8": "b",
                "usr/share/man/man6/figlet-utf8.6.gz": "m",
            },
        }
    }
    unpacked = apt.install(layout, ["figlet"])
    assert os.readlink(layout.root / "usr/bin/figlet") == str(
        layout.root / "usr/bin/figlet-utf8"
    )
    assert unpacked[0].scripts == []
    # The link is part of the package, so remove takes it too.
    apt.remove(layout, ["figlet"])
    assert not (layout.root / "usr/bin/figlet").is_symlink()


def test_hidden_files_keep_their_names(layout, tools):
    tools.offer = {"dots": {"files": {"usr/share/dots/.hidden": "h"}}}
    apt.install(layout, ["dots"])
    assert "usr/share/dots/.hidden" in (layout.info / "dots.list").read_text().split()
