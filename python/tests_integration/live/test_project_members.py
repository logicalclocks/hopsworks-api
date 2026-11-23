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
r"""Live check: `hops project members` add, set, transfer, list, remove and invite, as in a sandbox.

Not part of the unit suite (`pytest tests`). Run from the python directory:

    HOPSWORKS_LIVE_HOST=<host or url> HOPSWORKS_LIVE_KEY_FILE=<owner's API key file> \
        HOPSWORKS_LIVE_MEMBER_KEY_FILE=<another user's API key file> \
        pytest tests_integration/live/test_project_members.py

The owner's user must be able to create a project. The test creates one and runs `hops` in fresh
processes with the project chosen by the environment and the session cache on. The owner adds the
member as a Data scientist, whose own add is refused with Hopsworks' reason; the owner makes them a
Data owner and the owner, and they hand it back. The member then makes a scheduled job, the owner
removes them, and the schedule reads paused. A second test invites an address with no account, lists
it pending, is refused for an existing user's address, and cancels the invite with remove. Each project
is deleted at the end, waited for until it answers 404. A missing environment variable or an unreachable cluster fails the run: there is no skip.
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse

import pytest
import requests
import urllib3


DELETE_BUDGET_SECONDS = 600


def _required_env(name):
    value = os.environ.get(name)
    if not value:
        pytest.fail(
            f"{name} is not set: the live lane needs it, see the module docstring."
        )
    return value


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    host_value = _required_env("HOPSWORKS_LIVE_HOST").strip()
    host = urllib.parse.urlparse(host_value).hostname or host_value
    urllib3.disable_warnings()
    return {
        "host": host,
        "owner": _required_env("HOPSWORKS_LIVE_KEY_FILE"),
        "member": _required_env("HOPSWORKS_LIVE_MEMBER_KEY_FILE"),
        "home": tmp_path_factory.mktemp("home"),
    }


def _key(key_file):
    with open(key_file) as f:
        return f.read().strip()


def _rest(live, key_file, method, path, **kwargs):
    # The key is read here and never kept: pytest prints fixture values on a failure.
    return requests.request(
        method,
        f"https://{live['host']}/hopsworks-api/api{path}",
        headers={"Authorization": f"ApiKey {_key(key_file)}"},
        verify=False,
        timeout=120,
        **kwargs,
    )


def _hops(live, key_file, project, project_id, *args):
    """Runs `hops` in a new process the way a sandbox does: project from the environment, cache on."""
    env = {
        **os.environ,
        "HOME": str(live["home"]),
        "HOPSWORKS_HOST": live["host"],
        "HOPSWORKS_API_KEY": _key(key_file),
        "HOPSWORKS_PROJECT": project,
        "HOPSWORKS_PROJECT_ID": str(project_id),
        "HOPSWORKS_HOSTNAME_VERIFICATION": "false",
        "HOPS_SESSION_CACHE": "1",
        "HOPS_SESSION_DIR": str(live["home"] / f"session-{os.path.basename(key_file)}"),
        "HOPS_NO_INPUT": "1",
    }
    return subprocess.run(
        [sys.executable, "-c", "from hopsworks.cli.main import main; main()", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def _role(live, project_id, email):
    r = _rest(live, live["owner"], "GET", f"/project/{project_id}/projectMembers")
    assert r.ok, f"members: {r.status_code} {r.text[:300]}"
    for m in r.json():
        if (m.get("user", {}).get("email") or "").lower() == email.lower():
            return m.get("teamRole")
    return None


def _owner(live, key_file, project_id):
    r = _rest(live, key_file, "GET", f"/project/{project_id}")
    assert r.ok, f"project: {r.status_code} {r.text[:300]}"
    return r.json().get("owner")


def _scheduled_job(live, key_file, project_id, backing):
    """A Python job with a schedule months away, made with the given key; its name."""
    script = f"members_{int(time.time())}.py"
    code = b"print('scheduled', flush=True)\n"
    flow = {
        "templateId": -1,
        "flowChunkNumber": 1,
        "flowChunkSize": len(code),
        "flowCurrentChunkSize": len(code),
        "flowTotalSize": len(code),
        "flowIdentifier": f"{len(code)}_{script}",
        "flowFilename": script,
        "flowRelativePath": script,
        "flowTotalChunks": 1,
    }
    r = _rest(
        live,
        key_file,
        "POST",
        f"/project/{project_id}/dataset/upload/Resources?type=DATASET",
        data={k: str(v) for k, v in flow.items()},
        files={"file": (script, code)},
    )
    assert r.ok, f"upload: {r.status_code} {r.text[:300]}"
    r = _rest(live, key_file, "GET", f"/project/{project_id}/jobs/python/configuration")
    assert r.ok, f"defaults: {r.status_code} {r.text[:300]}"
    job = f"members-{int(time.time())}"
    config = {
        **r.json(),
        "appName": job,
        "appPath": f"hdfs:///Projects/{backing}/Resources/{script}",
    }
    r = _rest(live, key_file, "PUT", f"/project/{project_id}/jobs/{job}", json=config)
    assert r.ok, f"job: {r.status_code} {r.text[:300]}"
    r = _rest(
        live,
        key_file,
        "POST",
        f"/project/{project_id}/jobs/{job}/schedule/v2",
        json={
            "cronExpression": "0 0 3 1 1 ?",
            "enabled": True,
            "catchup": False,
            "startDateTime": int(time.time() * 1000),
        },
    )
    assert r.ok, f"schedule: {r.status_code} {r.text[:300]}"
    return job


def _create_project(live, name):
    r = _rest(
        live,
        live["owner"],
        "POST",
        "/project",
        json={
            "projectName": name,
            "description": "",
            "services": ["JOBS", "FEATURESTORE"],
            "projectTeam": [],
        },
    )
    assert r.status_code in (200, 201), f"create: {r.status_code} {r.text[:300]}"
    r = _rest(live, live["owner"], "GET", f"/project/getProjectInfo/{name}")
    assert r.ok, f"read: {r.status_code} {r.text[:300]}"
    return r.json()["projectId"]


def _invited(live, project_id):
    r = _rest(
        live, live["owner"], "GET", f"/project/{project_id}/projectMembers/invites"
    )
    assert r.ok, f"invites: {r.status_code} {r.text[:300]}"
    return {i["email"]: i["role"] for i in r.json()}


def _delete_and_wait(live, project_id):
    r = _rest(live, live["owner"], "POST", f"/project/{project_id}/delete")
    assert r.status_code in (200, 202, 204, 404), (
        f"delete {project_id}: {r.status_code} {r.text[:300]}"
    )
    deadline = time.monotonic() + DELETE_BUDGET_SECONDS
    while time.monotonic() < deadline:
        if (
            _rest(live, live["owner"], "GET", f"/project/{project_id}").status_code
            == 404
        ):
            return
        time.sleep(5)
    pytest.fail(f"project {project_id} still answers after {DELETE_BUDGET_SECONDS} s")


def test_the_owner_adds_sets_transfers_and_removes_a_member(live):
    name = f"hpm{int(time.time())}"
    owner_email = _rest(live, live["owner"], "GET", "/users/profile").json()["email"]
    member_email = _rest(live, live["member"], "GET", "/users/profile").json()["email"]
    project_id = None
    try:
        r = _rest(
            live,
            live["owner"],
            "POST",
            "/project",
            json={
                "projectName": name,
                "description": "",
                "services": ["JOBS", "FEATURESTORE"],
                "projectTeam": [],
            },
        )
        assert r.status_code in (200, 201), f"create: {r.status_code} {r.text[:300]}"
        r = _rest(live, live["owner"], "GET", f"/project/getProjectInfo/{name}")
        assert r.ok, f"read: {r.status_code} {r.text[:300]}"
        project_id = r.json()["projectId"]
        r = _rest(live, live["owner"], "GET", f"/project/{project_id}")
        assert r.ok, f"project: {r.status_code} {r.text[:300]}"
        backing = r.json().get("backingName") or name

        def owner(*args):
            return _hops(
                live, live["owner"], name, project_id, "project", "members", *args
            )

        def member(*args):
            return _hops(
                live, live["member"], name, project_id, "project", "members", *args
            )

        added = owner("add", member_email, "--role", "data scientist")
        assert added.returncode == 0, added.stderr[-2000:]
        assert _role(live, project_id, member_email) == "Data scientist"

        refused = member("add", owner_email, "--role", "Data owner")
        assert refused.returncode == 1, refused.stdout[-2000:]
        assert f"Could not add member '{owner_email}': " in refused.stderr

        set_ = owner("set", member_email, "--role", "Data owner")
        assert set_.returncode == 0, set_.stderr[-2000:]
        assert _role(live, project_id, member_email) == "Data owner"

        unconfirmed = owner("transfer", member_email)
        assert unconfirmed.returncode != 0
        assert _owner(live, live["owner"], project_id).lower() == owner_email.lower()

        moved = owner("transfer", member_email, "--yes")
        assert moved.returncode == 0, moved.stderr[-2000:]
        assert _owner(live, live["member"], project_id).lower() == member_email.lower()
        # The owner's cached session still holds the project as it read it before the transfer.
        after_move = owner("list")
        assert after_move.returncode == 0, after_move.stderr[-2000:]
        new_owner_row = next(
            line for line in after_move.stdout.splitlines() if member_email in line
        )
        assert new_owner_row.rstrip().endswith("*")
        back = member("transfer", owner_email, "--yes")
        assert back.returncode == 0, back.stderr[-2000:]
        assert _owner(live, live["owner"], project_id).lower() == owner_email.lower()

        listed = owner("list")
        assert listed.returncode == 0, listed.stderr[-2000:]
        owner_row = next(
            line for line in listed.stdout.splitlines() if owner_email in line
        )
        assert owner_row.rstrip().endswith("*")
        assert member_email in listed.stdout

        job = _scheduled_job(live, live["member"], project_id, backing)
        removed = owner("remove", member_email, "--yes")
        assert removed.returncode == 0, removed.stderr[-2000:]
        assert f"Paused the schedule of {job}" in removed.stderr
        assert _role(live, project_id, member_email) is None
        r = _rest(
            live, live["owner"], "GET", f"/project/{project_id}/jobs/{job}/schedule/v2"
        )
        assert r.ok, f"schedule read: {r.status_code} {r.text[:300]}"
        assert r.json().get("enabled") is False
    finally:
        if project_id is not None:
            _delete_and_wait(live, project_id)


def test_the_owner_invites_lists_and_cancels_an_address(live):
    name = f"hpi{int(time.time())}"
    address = f"hops-invite-{int(time.time())}@example.com"
    member_email = _rest(live, live["member"], "GET", "/users/profile").json()["email"]
    project_id = None
    try:
        project_id = _create_project(live, name)

        def owner(*args):
            return _hops(
                live, live["owner"], name, project_id, "project", "members", *args
            )

        invited = owner("invite", address, "--role", "data scientist")
        assert invited.returncode == 0, invited.stderr[-2000:]
        assert f"Invited {address} as Data scientist, until " in invited.stderr
        assert _invited(live, project_id) == {address: "Data scientist"}

        listed = owner("list")
        assert listed.returncode == 0, listed.stderr[-2000:]
        row = next(line for line in listed.stdout.splitlines() if address in line)
        assert "Data scientist (invited, until " in row

        refused = owner("invite", member_email, "--role", "Observer")
        assert refused.returncode == 1, refused.stdout[-2000:]
        assert f"Could not invite '{member_email}': " in refused.stderr

        cancelled = owner("remove", address)
        assert cancelled.returncode == 0, cancelled.stderr[-2000:]
        assert f"Cancelled the invite of {address}" in cancelled.stderr
        assert _invited(live, project_id) == {}
    finally:
        if project_id is not None:
            _delete_and_wait(live, project_id)


def test_member_usage_reads_every_member_for_the_owner_and_only_their_own_for_a_member(
    live,
):
    name = f"hpu{int(time.time())}"
    member_email = _rest(live, live["member"], "GET", "/users/profile").json()["email"]
    owner_email = _rest(live, live["owner"], "GET", "/users/profile").json()["email"]
    project_id = None
    try:
        project_id = _create_project(live, name)
        added = _hops(
            live,
            live["owner"],
            name,
            project_id,
            "project",
            "members",
            "add",
            member_email,
            "--role",
            "data scientist",
        )
        assert added.returncode == 0, added.stderr[-2000:]

        owner = _hops(
            live,
            live["owner"],
            name,
            project_id,
            "project",
            "members",
            "usage",
            "--since",
            "2d",
            "--json",
        )
        assert owner.returncode == 0, owner.stderr[-2000:]
        usage = json.loads(owner.stdout)
        assert usage["hours"] == 48
        assert usage["gpu"] is None and usage["storage"] is None
        assert {m["email"] for m in usage["members"]} <= {owner_email, member_email}

        member = _hops(
            live,
            live["member"],
            name,
            project_id,
            "project",
            "members",
            "usage",
            "--json",
        )
        assert member.returncode == 0, member.stderr[-2000:]
        mine = json.loads(member.stdout)
        assert mine["hours"] == 7 * 24
        assert {m["email"] for m in mine["members"]} <= {member_email}
        assert mine["notByMember"] is None

        table = _hops(
            live, live["owner"], name, project_id, "project", "members", "usage"
        )
        assert table.returncode == 0, table.stderr[-2000:]
        assert "EMAIL" in table.stdout and "Use, not the bill." in table.stdout

        too_long = _hops(
            live,
            live["owner"],
            name,
            project_id,
            "project",
            "members",
            "usage",
            "--since",
            "400d",
        )
        assert too_long.returncode == 1, too_long.stdout[-2000:]
        assert "Could not read member usage: " in too_long.stderr
    finally:
        if project_id is not None:
            _delete_and_wait(live, project_id)
