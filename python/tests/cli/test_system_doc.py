"""system.yaml writes, the build lease, registry provenance, teardown ownership and the parity comparison."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import session, system_doc, teardown
from hopsworks.cli.main import cli
from hopsworks_common.core import factory_api, ml_system_api


yaml = pytest.importorskip("yaml")
REFERENCES = (
    Path(system_doc.__file__).resolve().parents[1]
    / "skills"
    / "ml"
    / "hops-reqs"
    / "references"
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _system(tmp_path: Path, doc: dict | None = None) -> Path:
    target = tmp_path / "churn"
    target.mkdir()
    (target / "system.yaml").write_text(
        system_doc.dump(doc or {"schema_version": 1, "system": {"slug": "churn"}})
    )
    return target


def _sha(target: Path) -> str:
    return hashlib.sha256((target / "system.yaml").read_bytes()).hexdigest()


# region system.yaml writes


def test_an_update_is_redone_on_a_file_another_writer_replaced(tmp_path):
    target = _system(tmp_path)
    calls = []

    def change(doc):
        calls.append(dict(doc))
        if len(calls) == 1:
            # A writer that ignores the lock replaces the file between read and commit.
            (target / "system.yaml").write_text(
                system_doc.dump({**doc, "decisions": ["theirs"]})
            )
        doc.setdefault("changes", []).append("mine")

    doc = system_doc.update(target, change)
    assert len(calls) == 2
    on_disk = yaml.safe_load((target / "system.yaml").read_text())
    assert on_disk == doc and on_disk["decisions"] == ["theirs"]
    assert on_disk["changes"] == ["mine"]
    assert sorted(p.name for p in target.iterdir()) == ["system.yaml"]


def test_an_update_gives_up_on_a_file_that_keeps_changing(tmp_path):
    target = _system(tmp_path)

    def change(doc):
        (target / "system.yaml").write_text(system_doc.dump({**doc, "n": time.time()}))
        doc["mine"] = True

    with pytest.raises(system_doc.Conflict):
        system_doc.update(target, change, attempts=2)
    assert "mine" not in yaml.safe_load((target / "system.yaml").read_text())


def test_an_invalid_change_never_replaces_the_file(tmp_path):
    target = _system(tmp_path)
    before = (target / "system.yaml").read_text()
    with pytest.raises(system_doc.Invalid):
        system_doc.update(target, lambda doc: doc.pop("system"))
    with pytest.raises(system_doc.Invalid):
        system_doc.update(target, lambda doc: None, validate=lambda doc: ["bad"])
    assert (target / "system.yaml").read_text() == before


def test_a_replacement_needs_the_version_it_was_edited_from(tmp_path):
    target = _system(tmp_path)
    old = _sha(target)
    new = system_doc.dump({"system": {"slug": "churn", "status": "draft"}})
    assert system_doc.replace(target, new, old) == _sha(target)
    with pytest.raises(system_doc.Conflict):
        system_doc.replace(target, new, old)
    with pytest.raises(system_doc.Conflict):
        system_doc.create(target, {"system": {}})


def test_a_dead_writers_lock_is_cleared_and_a_live_one_waited_for(tmp_path):
    target = _system(tmp_path)
    lock = target / system_doc.WRITE_LOCK
    lock.write_text("dead")
    old = time.time() - system_doc.STALE_WRITE_LOCK - 5
    os.utime(lock, (old, old))
    system_doc.update(target, lambda doc: doc.update(n=1))
    assert not lock.exists()
    lock.write_text("alive")
    with (
        pytest.raises(system_doc.Conflict, match="another writer"),
        mock.patch.object(system_doc.time, "monotonic", side_effect=[0, 0, 100]),
    ):
        system_doc.update(target, lambda doc: doc.update(n=2))


def test_write_doc_exit_codes_and_the_staging_file_is_removed(tmp_path, monkeypatch):
    target = _system(tmp_path)
    monkeypatch.chdir(tmp_path)
    staged = target / ".system.yaml.edit-1"

    def write(text, expected):
        staged.write_text(text)
        args = ["factory", "system", "write-doc", "churn"]
        return CliRunner().invoke(
            cli, [*args, "--expected-sha256", expected, "--from", str(staged)]
        )

    stale = "0" * 64
    conflict = write(system_doc.dump({"system": {"slug": "churn"}}), stale)
    assert conflict.exit_code == 3 and "changed since it was read" in conflict.output
    assert not staged.exists()
    invalid = write("system: [unclosed", _sha(target))
    assert invalid.exit_code == 2 and "not valid YAML" in invalid.output
    done = write(system_doc.dump({"system": {"slug": "churn", "v": 2}}), _sha(target))
    assert done.exit_code == 0, done.output
    assert yaml.safe_load((target / "system.yaml").read_text())["system"]["v"] == 2
    assert not staged.exists()


def test_set_py_goes_through_the_same_helper(tmp_path):
    setter = _load(REFERENCES / "system_template" / "set.py", "set_under_test")
    target = _system(tmp_path, {"schema_version": 1, "system": {"slug": "churn"}})
    setter.ROOT = target
    setter._validator = lambda: SimpleNamespace(validate=lambda doc: [])
    with mock.patch.object(system_doc, "update", wraps=system_doc.update) as update:
        assert setter.main(["system.status=draft", "decisions+=first"]) == 0
    assert update.called
    doc = yaml.safe_load((target / "system.yaml").read_text())
    assert doc["system"]["status"] == "draft" and doc["decisions"] == ["first"]


# endregion

# region The build lease


def test_a_lease_is_held_renewed_released_and_taken_over_when_it_lapses(tmp_path):
    target = _system(tmp_path)
    held = system_doc.acquire(target)
    assert set(held) == {"owner", "token", "acquired", "expires"}
    with pytest.raises(system_doc.LeaseHeld, match="is being built by"):
        system_doc.acquire(target, token="other")
    with pytest.raises(system_doc.LeaseHeld):
        system_doc.check(target)
    # The holder adopts its own lease.
    assert system_doc.acquire(target, token=held["token"])["token"] == held["token"]
    with pytest.raises(system_doc.LeaseHeld, match="not yours"):
        system_doc.renew(target, "other")
    assert system_doc.renew(target, held["token"])["token"] == held["token"]
    assert not system_doc.release(target, "other")
    assert system_doc.release(target, held["token"])
    assert system_doc.lease(target) is None

    lapsed = system_doc.acquire(target, ttl=timedelta(seconds=-1))
    with pytest.raises(system_doc.LeaseHeld, match="expired"):
        system_doc.renew(target, lapsed["token"])
    taken = system_doc.acquire(target)
    assert taken["token"] != lapsed["token"]
    assert sorted(p.name for p in target.iterdir()) == [".hops.lock", "system.yaml"]


def test_an_old_agent_written_lock_counts_until_a_ttl_after_it_was_written(tmp_path):
    target = _system(tmp_path)
    (target / ".hops.lock").write_text(
        "{holder: claude, host: x, since: 2026-01-01T00:00Z}"
    )
    with pytest.raises(system_doc.LeaseHeld):
        system_doc.acquire(target)
    old = time.time() - system_doc.LEASE_TTL.total_seconds() - 60
    os.utime(target / ".hops.lock", (old, old))
    assert system_doc.acquire(target)["token"]


def test_the_lease_commands(tmp_path, monkeypatch):
    target = _system(tmp_path)
    monkeypatch.chdir(target)
    taken = CliRunner().invoke(cli, ["factory", "system", "lease", "acquire", "churn"])
    assert taken.exit_code == 0, taken.output
    token = taken.output.strip()
    again = CliRunner().invoke(cli, ["factory", "system", "lease", "acquire", "churn"])
    assert again.exit_code == 4
    renewed = CliRunner(env={"HOPS_LEASE_TOKEN": token}).invoke(
        cli, ["factory", "system", "lease", "renew", "churn"]
    )
    assert renewed.exit_code == 0, renewed.output
    lost = CliRunner().invoke(
        cli, ["factory", "system", "lease", "renew", "churn", "--token", "x"]
    )
    assert lost.exit_code == 4
    released = CliRunner().invoke(
        cli, ["factory", "system", "lease", "release", "churn", "--token", token]
    )
    assert released.exit_code == 0 and not (target / ".hops.lock").exists()


# endregion

# region Running and registering


def test_a_disabled_factory_blocks_a_resume_too(tmp_path, monkeypatch):
    monkeypatch.setattr(session, "get_project", lambda ctx: mock.Mock())
    monkeypatch.setattr(
        factory_api,
        "_get",
        lambda name, version=None: {"name": name, "version": 1, "enabled": False},
    )
    _system(tmp_path)
    monkeypatch.chdir(tmp_path)
    done = CliRunner().invoke(cli, ["factory", "run", "churn-review", "churn"])
    assert done.exit_code != 0 and "is disabled" in done.output


def test_registration_sends_the_version_and_digest_system_yaml_records(
    tmp_path, monkeypatch
):
    from hopsworks.cli.commands import mlsystem

    target = _system(
        tmp_path,
        {
            "system": {"slug": "churn"},
            "factory": {"name": "fraud-ml", "version": 3, "digest": "ab" * 32},
        },
    )
    monkeypatch.setattr(session, "get_project", lambda ctx: SimpleNamespace(name="p"))
    monkeypatch.setattr(
        mlsystem, "code_location", lambda path, project: "/Projects/p/x"
    )
    sent = []
    real_register = ml_system_api._register
    monkeypatch.setattr(
        ml_system_api, "_register", lambda *a, **kw: sent.append((a, kw)) or {}
    )
    mlsystem.register(None, target, "Churn", "fraud-ml")
    mlsystem.register(None, target, "Churn", "other")
    assert sent[0] == (
        ("/Projects/p/x", "Churn", "fraud-ml"),
        {"factory_version": 3, "factory_digest": "ab" * 32},
    )
    # Registered by hand under another factory, the recorded version is not that factory's.
    assert sent[1][1] == {"factory_version": None, "factory_digest": None}

    client = mock.Mock(_project_id=127)
    monkeypatch.setattr(ml_system_api.client, "_get_instance", lambda: client)
    real_register("/p", factory="f", factory_version=3, factory_digest="d")
    body = json.loads(client._send_request.call_args.kwargs["data"])
    assert body == {
        "pathToCode": "/p",
        "factory": "f",
        "factoryVersion": 3,
        "factoryDigest": "d",
    }


def test_the_registry_is_read_page_by_page_and_one_system_by_id(monkeypatch):
    systems = [{"id": i} for i in range(ml_system_api.PAGE + 30)]
    queries = []

    def send(method, path, query_params=None):
        if path[-1] == "mlsystems":
            queries.append(query_params)
            offset, limit = query_params["offset"], query_params["limit"]
            return {"items": systems[offset : offset + limit], "count": len(systems)}
        return {"id": path[-1]}

    client = mock.Mock(_project_id=127, _send_request=send)
    monkeypatch.setattr(ml_system_api.client, "_get_instance", lambda: client)
    assert ml_system_api._list("f") == systems
    assert queries == [
        {"offset": 0, "limit": ml_system_api.PAGE, "factory": "f"},
        {"offset": ml_system_api.PAGE, "limit": ml_system_api.PAGE, "factory": "f"},
    ]
    assert ml_system_api._get(7) == {"id": 7}


# endregion

# region Teardown ownership


CHURN_EXAMPLE = {
    "system": {"slug": "churn-example"},
    "data": {
        "customers": {"writes": {"feature_group": "customers", "version": 1}},
        "usage_events": {"writes": {"feature_group": "usage_events", "version": 1}},
    },
    "app": {"name": "churn-example-app", "wanted": True},
}


def test_a_system_made_from_an_example_never_deletes_the_examples_assets():
    """The copied example names its assets; another registered system names them too."""
    copied = {**CHURN_EXAMPLE, "system": {"slug": "customer-churn"}}
    assets, kept = teardown.plan(
        copied, "customer-churn", {"churn-example": CHURN_EXAMPLE}
    )
    names = {(a.kind, a.name) for a in assets}
    assert ("app", "churn-example-app") not in names
    assert ("feature group", "customers") not in names
    assert "kept app churn-example-app: churn-example names it too" in kept
    assert ("job", "customer-churn-*") in names
    assert ("directory", "Resources/customer-churn") in names


def test_a_name_without_the_slug_is_deleted_only_with_its_description_naming_it():
    doc = {
        "data": {"c": {"writes": {"feature_group": "customers", "version": 1}}},
        "app": {"name": "churn-console", "wanted": True},
    }
    assets, kept = teardown.plan(doc, "customer-churn", {})
    assert not kept
    app = next(a for a in assets if a.kind == "app")
    assert app.owned_by == "customer-churn"
    deleted = []

    def thing(description):
        return SimpleNamespace(
            description=description,
            stop=lambda: None,
            delete=lambda: deleted.append(description),
        )

    apps = {"churn-console": thing("the example's console")}
    project = SimpleNamespace(
        get_app_api=lambda: SimpleNamespace(get_app=apps.get),
    )
    deleter = teardown.Deleter(project)
    assert (
        deleter.delete(app)
        == "kept app churn-console (if customer-churn made it): not created by customer-churn"
    )
    assert deleted == []
    apps["churn-console"] = thing("Built by customer-churn")
    assert deleter.delete(app) == "deleted" and deleted == ["Built by customer-churn"]


def test_a_systems_own_assets_are_deleted():
    doc = {
        "data": {
            "p": {
                "writes": {"feature_group": "customer_churn_predictions", "version": 2}
            }
        },
        "app": {"name": "customer-churn-app", "wanted": True},
        "inference": {"deployment": "customerchurn"},
    }
    assets, kept = teardown.plan(doc, "customer-churn", {"other": {}})
    assert not kept and not any(a.owned_by for a in assets if a.kind != "feature group")
    assert {str(a) for a in assets} >= {
        "app customer-churn-app",
        "deployment customerchurn",
        "feature group customer_churn_predictions v2",
    }


def test_an_example_copied_into_a_new_system_is_renamed_to_its_slug():
    new_system = _load(REFERENCES / "new_system.py", "new_system_under_test")
    doc = new_system.example_doc("recs-example", "shop-recs")
    text = yaml.safe_dump({k: v for k, v in doc.items() if k != "system"})
    assert "recs-example" not in text and "recsexample" not in text
    assert doc["app"]["name"] == "shop-recs-app"
    assert doc["inference"]["deployment"] == "shoprecs"
    assert doc["system"]["example"] == "recs-example"
    same = new_system.example_doc("recs-example", "recs-example")
    assert same["app"]["name"] == "recs-example-app"


# endregion

# region Parity


@pytest.fixture
def parity():
    return _load(
        REFERENCES / "system_template" / "src" / "slug_pkg" / "parity.py", "parity"
    )


def test_parity_treats_one_sided_nulls_and_infinities_as_mismatches(parity):
    pd = pytest.importorskip("pandas")
    nan, inf = float("nan"), float("inf")
    offline = pd.DataFrame({"a": [nan, nan, 1.0, inf], "b": ["x", None, "y", "z"]})
    online = pd.DataFrame({"a": [12.0, nan, 1.0, inf], "b": ["x", None, "y", "z"]})
    found = parity.feature_problems(offline, online, ["a", "b"], 4, 1e-6, 0)
    # NaN against 12.0 and inf against inf; both-null rows agree.
    assert found == [
        "2 of 4 entities have different transformed features offline and online"
    ]
    assert (
        parity.feature_problems(offline[2:3], online[2:3], ["a", "b"], 1, 1e-6, 0) == []
    )
    strings = pd.DataFrame({"b": ["x"]}), pd.DataFrame({"b": [None]})
    assert parity.feature_problems(*strings, ["b"], 1, 0, 0)


def test_parity_refuses_missing_columns_and_wrong_counts(parity):
    pd = pytest.importorskip("pandas")
    offline = pd.DataFrame({"a": [1.0, 2.0]})
    online = pd.DataFrame({"a": [1.0, 2.0], "b": [0.0, 0.0]})
    assert parity.feature_problems(offline, online, ["a", "b"], 2, 0, 0) == [
        "offline vectors lack ['b']"
    ]
    assert "online has 2 rows for 3 entities" in parity.feature_problems(
        offline, online, ["a"], 3, 0, 0
    )
    assert parity.feature_problems(offline, online, [], 2, 0, 0) == [
        "no feature columns to compare"
    ]


def test_parity_checks_the_prediction_count_shape_and_values(parity):
    nan, inf = float("nan"), float("inf")
    assert parity.prediction_problems([0.1, 0.2], [0.1, 0.2], 2, 1e-6, 0) == []
    assert parity.prediction_problems([0.1], [0.1, 0.2], 2, 1e-6, 0) == [
        "1 local and 2 served predictions for 2 entities"
    ]
    assert parity.prediction_problems([], [], 0, 1e-6, 0)
    assert parity.prediction_problems([[0.1, 0.9]], [[0.1]], 1, 1e-6, 0) == [
        "local predictions have shape (1, 2), served (1, 1)"
    ]
    assert parity.prediction_problems([nan, 1.0], [12.0, 1.0], 2, 1e-6, 0)
    assert parity.prediction_problems([inf], [inf], 1, 1e-6, 0)
    assert parity.prediction_problems([nan], [None], 1, 1e-6, 0) == []
    assert parity.prediction_problems(["yes"], ["no"], 1, 1e-6, 0)


# endregion
