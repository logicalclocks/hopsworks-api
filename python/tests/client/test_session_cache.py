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

import os
import stat
import subprocess
import sys
from unittest import mock

import pytest
from hopsworks_common.client import base, session_cache


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv(session_cache.ENV_VAR, "1")
    monkeypatch.setenv(session_cache.DIR_ENV_VAR, str(tmp_path))
    yield tmp_path
    session_cache.bind("h", 443, "k", None)


def test_nothing_is_kept_without_the_variable(tmp_path, monkeypatch):
    monkeypatch.delenv(session_cache.ENV_VAR, raising=False)
    monkeypatch.setenv(session_cache.DIR_ENV_VAR, str(tmp_path))
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("username", "meb")
    assert session_cache.get("username") is None
    assert list(tmp_path.iterdir()) == []


def test_a_later_process_reads_what_an_earlier_one_kept(cache_dir):
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("username", "meb")
    assert not session_cache.used()

    session_cache.bind("h", 443, "k", "p")
    assert session_cache.get("username") == "meb"
    assert session_cache.used()
    (session,) = [f for f in cache_dir.iterdir() if f.suffix == ".json"]
    assert stat.S_IMODE(session.stat().st_mode) == 0o600
    # The key never reaches the disk.
    assert "k" not in session.name and '"k"' not in session.read_text()


def test_another_key_or_project_is_another_session(cache_dir):
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("username", "meb")
    session_cache.bind("h", 443, "other", "p")
    assert session_cache.get("username") is None
    session_cache.bind("h", 443, "k", "q")
    assert session_cache.get("username") is None


def test_a_session_ends_at_its_ttl_or_its_certificate(cache_dir, monkeypatch):
    now = 1_000_000.0
    monkeypatch.setattr(session_cache.time, "time", lambda: now)
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("certs_folder", "/x", expires_at=now + 60)

    monkeypatch.setattr(session_cache.time, "time", lambda: now + 59)
    session_cache.bind("h", 443, "k", "p")
    assert session_cache.get("certs_folder") == "/x"

    monkeypatch.setattr(session_cache.time, "time", lambda: now + 61)
    session_cache.bind("h", 443, "k", "p")
    assert session_cache.get("certs_folder") is None


def test_invalidate_forgets_the_session_on_disk(cache_dir):
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("username", "meb")
    session_cache.invalidate()
    session_cache.bind("h", 443, "k", "p")
    assert session_cache.get("username") is None


def test_certificates_live_in_folders_the_cache_owns(cache_dir):
    session_cache.bind("h", 443, "k", "p")
    folder = session_cache.new_certs_folder()
    assert session_cache.owns(folder)
    assert stat.S_IMODE(os.stat(folder).st_mode) == 0o700
    assert not session_cache.owns("/tmp/1234-abcd")


def _response(status, body=None):
    response = mock.Mock(status_code=status, content=b"{}")
    response.json.return_value = body or {}
    return response


def test_a_refused_cached_session_logs_in_again_and_repeats_once(cache_dir):
    session_cache.bind("h", 443, "k", "p")
    session_cache.put("username", "meb")
    session_cache.bind("h", 443, "k", "p")
    session_cache.get("username")

    client = base.Client.__new__(base.Client)
    client._connected = True
    client._base_url = "https://h:443"
    client._auth = None
    client._verify = False
    client._session = mock.Mock()
    client._session.prepare_request.side_effect = lambda request: request
    client._session.send.side_effect = [
        _response(404, {"errorCode": 150004}),
        _response(200, {"items": []}),
    ]

    def refresh(path_params):
        session_cache.invalidate()
        return ["project", 2167, *path_params[2:]]

    client._refresh_session = mock.Mock(side_effect=refresh)

    assert client._send_request("GET", ["project", 999, "jobs"]) == {"items": []}
    client._refresh_session.assert_called_once()
    assert client._session.send.call_args.args[0].url.endswith("/project/2167/jobs")


def test_a_missing_job_is_no_stale_session():
    assert not base._session_went_stale(_response(404, {"errorCode": 130009}))
    assert base._session_went_stale(_response(404, {"errorCode": 150004}))
    assert base._session_went_stale(_response(401))
    assert base._session_went_stale(None)


def test_importing_the_cli_loads_no_heavy_dependency():
    # Every `hops` command pays for what `import hopsworks` loads; these load on first use.
    code = (
        "import sys, hopsworks.cli.main; "
        "print(sorted({m.split('.')[0] for m in sys.modules} & {'pyspark', 'trino', 'sqlalchemy', 'numpy', 'asyncio', 'cryptography', 'tqdm'}))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"
