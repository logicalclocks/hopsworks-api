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
r"""Live check: `hops fg create-external --table` mounts a Postgres table in the schema the source lists.

Not part of the unit suite (`pytest tests`). Run from the python directory:

    HOPSWORKS_HOST=<host> HOPSWORKS_API_KEY=<key> HOPSWORKS_PROJECT=<project> \
        HOPSWORKS_LIVE_SQL_SOURCE=<Postgres data source> HOPSWORKS_LIVE_SQL_DATABASE=<database> \
        pytest tests_integration/live/test_create_external_schema.py

The source's database holds, readable by the source's role:

    CREATE SCHEMA hops_cli_test;
    CREATE TABLE hops_cli_test.hops_cli_only (id int PRIMARY KEY, label text);  -- row (1, 'only')
    CREATE TABLE hops_cli_test.hops_cli_twin (id int PRIMARY KEY, label text);  -- row (1, 'hops_cli_test')
    CREATE TABLE public.hops_cli_twin (id int PRIMARY KEY, label text);         -- row (1, 'public')

Each test creates at most one feature group named hopsclischema<random>, deletes it and polls until
it answers 404. A missing environment variable or an unreachable cluster fails the run: there is no skip.
"""

import json
import os
import subprocess
import sys
import time
import uuid

import pytest


SOURCE = os.environ["HOPSWORKS_LIVE_SQL_SOURCE"]
DATABASE = os.environ["HOPSWORKS_LIVE_SQL_DATABASE"]
DELETE_BUDGET_SECONDS = 120


def _hops(*args):
    return subprocess.run(
        [sys.executable, "-m", "hopsworks.cli", *args],
        capture_output=True,
        text=True,
        timeout=600,
    )


def _exists(name):
    """True or False from `fg info`; any other failure (login, server) fails the test."""
    result = _hops("--json", "fg", "info", name, "--version", "1")
    if result.returncode == 0:
        return True
    # A lookup error is appended after a colon; the bare message means no store has the group.
    missing = f"Error: Feature group '{name}' not found in any accessible feature store"
    lines = result.stderr.strip().splitlines()
    assert lines and lines[-1] == missing, result.stdout + result.stderr
    return False


@pytest.fixture
def fg_name():
    name = f"hopsclischema{uuid.uuid4().hex[:8]}"
    assert not _exists(name)
    yield name
    if _exists(name):
        deleted = _hops("fg", "delete", name, "--version", "1", "--yes")
        assert deleted.returncode == 0, deleted.stdout + deleted.stderr
        deadline = time.monotonic() + DELETE_BUDGET_SECONDS
        while _exists(name):
            assert time.monotonic() < deadline, (
                f"{name} v1 still exists: {deleted.stderr}"
            )
            time.sleep(5)


def _mount(name, table):
    return _hops(
        "fg", "create-external", name, "--connector", SOURCE,
        "--database", DATABASE, "--table", table,
        "--primary-key", "id", "--no-statistics",
    )  # fmt: skip


def _source_and_label(name):
    info = _hops("--json", "fg", "info", name, "--version", "1")
    assert info.returncode == 0, info.stderr
    preview = _hops("--json", "fg", "preview", name, "--version", "1", "--limit", "1")
    assert preview.returncode == 0, preview.stderr
    source = json.loads(info.stdout)["source"]
    return (source["database"], source["group"], source["table"]), json.loads(
        preview.stdout
    )[0]["label"]


def test_a_bare_table_mounts_in_the_one_schema_that_lists_it(fg_name):
    created = _mount(fg_name, "hops_cli_only")
    assert created.returncode == 0, created.stdout + created.stderr
    assert _source_and_label(fg_name) == (
        (DATABASE, "hops_cli_test", "hops_cli_only"),
        "only",
    )


def test_a_schema_table_mounts_that_schema(fg_name):
    created = _mount(fg_name, "hops_cli_test.hops_cli_twin")
    assert created.returncode == 0, created.stdout + created.stderr
    assert _source_and_label(fg_name) == (
        (DATABASE, "hops_cli_test", "hops_cli_twin"),
        "hops_cli_test",
    )


def test_a_bare_table_in_two_schemas_is_refused(fg_name):
    created = _mount(fg_name, "hops_cli_twin")
    assert created.returncode != 0
    assert "hops_cli_test.hops_cli_twin, public.hops_cli_twin" in created.stderr
    assert not _exists(fg_name)
