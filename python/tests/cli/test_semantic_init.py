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
"""Tests for ``hops semantic init``: the scaffold it writes, and its folder, new repository and existing repository modes."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import fg as fg_cmd
from hopsworks.cli.commands import git as git_cmd
from hopsworks.cli.commands import semantic
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import GitException, SemanticBuildException
from hopsworks_common.semantic_layer import SemanticBuild, SemanticIssue


yaml = pytest.importorskip("yaml")


def _feature(name, type, description=None):
    return SimpleNamespace(name=name, type=type, description=description)


def _fg(name, version, features, primary_key, event_time=None, **extra):
    return SimpleNamespace(
        name=name,
        version=version,
        features=[_feature(*f) for f in features],
        primary_key=primary_key,
        event_time=event_time,
        description=extra.get("description"),
        feature_store_name=extra.get("feature_store_name", "demo_featurestore"),
    )


ORDERS = _fg(
    "orders",
    1,
    [
        ("order_id", "bigint"),
        ("customer_id", "bigint"),
        ("order_ts", "timestamp"),
        ("status", "string", "Where the order is"),
        ("amount", "decimal(10,2)"),
        ("quantity", "int"),
    ],
    ["order_id"],
    "order_ts",
    description="One row per order.",
)
CUSTOMERS = _fg(
    "customers",
    2,
    [("customer_id", "bigint"), ("country", "string")],
    ["customer_id"],
    feature_store_name="crm_featurestore",
)


def _semantic(files):
    return yaml.safe_load(files["models/semantic.yml"])


def _uncommented_metrics(files):
    text = files["models/semantic.yml"]
    block = text[text.index("# metrics:") :]
    return yaml.safe_load(
        "\n".join(line[2:] for line in block.splitlines() if line.startswith("# "))
    )["metrics"]


def _by_name(items):
    return {item["name"]: item for item in items}


class TestScaffold:
    def test_dbt_project_is_named_after_the_semantic_project(self):
        files = semantic._scaffold("sales", [ORDERS], "demo_featurestore")

        assert set(files) == {"dbt_project.yml", "models/semantic.yml"}
        project = yaml.safe_load(files["dbt_project.yml"])
        assert project["name"] == "sales"
        assert project["model-paths"] == ["models"]

    def test_a_feature_group_becomes_a_semantic_model_pinned_to_its_version(self):
        model = _semantic(semantic._scaffold("sales", [ORDERS], "demo_featurestore"))[
            "semantic_models"
        ][0]

        assert model["name"] == "orders"
        assert model["description"] == "One row per order."
        assert model["model"] == "ref('orders')"
        assert model["config"]["meta"]["hopsworks"] == {
            "feature_group": "orders",
            "version": 1,
        }
        assert model["defaults"] == {"agg_time_dimension": "order_ts"}
        assert model["entities"] == [
            {"name": "order", "type": "primary", "expr": "order_id"}
        ]
        dimensions = _by_name(model["dimensions"])
        assert dimensions["order_ts"]["type"] == "time"
        assert dimensions["order_ts"]["type_params"] == {"time_granularity": "day"}
        assert dimensions["status"] == {
            "name": "status",
            "type": "categorical",
            "expr": "status",
            "description": "Where the order is",
        }
        assert set(dimensions) == {"order_ts", "status"}
        measures = _by_name(model["measures"])
        assert measures["orders_count"]["agg"] == "count"
        assert measures["orders_count"]["expr"] == "order_id"
        assert {name: m["agg"] for name, m in measures.items()} == {
            "orders_count": "count",
            "customer_id": "sum",
            "amount": "sum",
            "quantity": "sum",
        }

    def test_metrics_are_commented_out_one_per_measure(self):
        files = semantic._scaffold("sales", [ORDERS], "demo_featurestore")

        assert "metrics" not in _semantic(files)
        metrics = _uncommented_metrics(files)
        assert [m["name"] for m in metrics] == [
            "orders_count",
            "customer_id",
            "amount",
            "quantity",
        ]
        assert metrics[0] == {
            "name": "orders_count",
            "label": "Orders count",
            "type": "simple",
            "type_params": {"measure": "orders_count"},
        }
        assert "hops semantic compile sales --wait" in files["models/semantic.yml"]

    def test_a_column_named_like_another_models_key_is_a_foreign_entity(self):
        models = _by_name(
            _semantic(
                semantic._scaffold("sales", [ORDERS, CUSTOMERS], "demo_featurestore")
            )["semantic_models"]
        )

        assert models["orders"]["entities"][1] == {
            "name": "customer",
            "type": "foreign",
            "expr": "customer_id",
        }
        # A key is not a measure.
        assert "customer_id" not in _by_name(models["orders"]["measures"])
        assert models["customers"]["entities"] == [
            {"name": "customer", "type": "primary", "expr": "customer_id"}
        ]

    def test_a_shared_feature_group_is_named_with_its_feature_store(self):
        models = _by_name(
            _semantic(
                semantic._scaffold("sales", [ORDERS, CUSTOMERS], "demo_featurestore")
            )["semantic_models"]
        )

        assert "feature_store" not in models["orders"]["config"]["meta"]["hopsworks"]
        assert models["customers"]["config"]["meta"]["hopsworks"] == {
            "feature_group": "customers",
            "version": 2,
            "feature_store": "crm_featurestore",
        }

    def test_without_an_event_time_a_model_has_dimensions_but_no_measures(self):
        files = semantic._scaffold("sales", [CUSTOMERS], "crm_featurestore")
        model = _semantic(files)["semantic_models"][0]

        assert "measures" not in model and "defaults" not in model
        assert _by_name(model["dimensions"])["country"]["type"] == "categorical"
        assert "the feature group has no event time" in files["models/semantic.yml"]
        assert "# metrics:" not in files["models/semantic.yml"]

    def test_an_event_time_that_is_not_a_time_gives_no_measures(self):
        fg = _fg("clicks", 1, [("id", "bigint"), ("ts", "bigint")], ["id"], "ts")

        files = semantic._scaffold("web", [fg])

        assert "measures" not in _semantic(files)["semantic_models"][0]
        assert (
            "event time ts is not a timestamp or date" in files["models/semantic.yml"]
        )

    def test_a_composite_or_missing_key_makes_the_model_its_own_primary_entity(self):
        lines = _fg(
            "order_lines",
            1,
            [
                ("order_id", "bigint"),
                ("line_no", "int"),
                ("sku", "string"),
                ("price", "double"),
                ("line_date", "date"),
            ],
            ["order_id", "line_no"],
            "line_date",
        )
        events = _fg("events", 3, [("ts", "timestamp"), ("kind", "string")], [], "ts")

        models = _by_name(
            _semantic(semantic._scaffold("sales", [ORDERS, lines, events]))[
                "semantic_models"
            ]
        )

        assert models["order_lines"]["primary_entity"] == "order_lines"
        # order_id is the orders model's key, so it joins there.
        assert models["order_lines"]["entities"] == [
            {"name": "order", "type": "foreign", "expr": "order_id"}
        ]
        assert set(_by_name(models["order_lines"]["measures"])) == {
            "order_lines_count",
            "price",
        }
        assert set(_by_name(models["order_lines"]["dimensions"])) == {
            "line_date",
            "sku",
        }
        assert models["events"]["primary_entity"] == "events"
        assert "entities" not in models["events"]
        assert _by_name(models["events"]["measures"])["events_count"]["expr"] == "ts"

    def test_features_named_like_sql_keywords_are_quoted(self):
        fg = _fg(
            "readings",
            1,
            [("id", "bigint"), ("date", "date"), ("value", "double")],
            ["id"],
            "date",
        )

        model = _semantic(semantic._scaffold("iot", [fg]))["semantic_models"][0]

        assert model["defaults"] == {"agg_time_dimension": "date"}
        assert _by_name(model["dimensions"])["date"]["expr"] == '"date"'
        assert _by_name(model["measures"])["value"]["expr"] == '"value"'
        assert model["entities"][0] == {
            "name": "readings",
            "type": "primary",
            "expr": "id",
        }

    def test_names_stay_unique_across_models_and_versions(self):
        orders_v2 = _fg(
            "orders",
            2,
            [("order_id", "bigint"), ("order_ts", "timestamp"), ("amount", "double")],
            ["order_id"],
            "order_ts",
        )

        files = semantic._scaffold("sales", [ORDERS, orders_v2])
        models = _semantic(files)["semantic_models"]

        assert [m["name"] for m in models] == ["orders", "orders_v2"]
        measures = [m["name"] for model in models for m in model["measures"]]
        assert len(measures) == len(set(measures))
        assert "orders_v2_amount" in measures

    def test_no_feature_groups_writes_an_empty_project(self):
        files = semantic._scaffold("blank", [])

        assert _semantic(files) == {"version": 2}
        assert "hops-semantic skill" in files["models/semantic.yml"]


@pytest.fixture
def sl(mock_project):
    api = mock.MagicMock(name="SemanticLayerApi")
    api.get_project.return_value = None
    api._mandatory_tags.return_value = set()
    api._absolute_path.side_effect = lambda path: (
        path if path.startswith("/") else f"/Projects/demo/{path}"
    )
    mock_project.get_semantic_layer_api.return_value = api
    sp = mock.MagicMock(name="SemanticProject")
    sp.name = "sales"
    sp.root_path = "/Projects/demo/Resources/semantic/sales"
    sp.to_dict.return_value = {"name": "sales"}
    sp.compile.return_value = SemanticBuild(id=1, status="VALID", warning_count=0)
    api.create_project.return_value = sp
    return api


@pytest.fixture
def datasets(mock_project):
    """HopsFS with /Projects/demo/Resources and nothing under it; uploads are read while their files exist."""
    api = mock.MagicMock(name="DatasetApi")
    existing = {"/Projects/demo/Resources"}
    uploaded = {}

    def upload(local_path, upload_path):
        root = Path(local_path)
        uploaded["parent"] = upload_path
        uploaded["name"] = root.name
        uploaded["files"] = {
            str(f.relative_to(root)): f.read_text()
            for f in root.rglob("*")
            if f.is_file()
        }

    api.exists.side_effect = lambda path: path in existing
    api.mkdir.side_effect = existing.add
    api.upload.side_effect = upload
    api.uploaded = uploaded
    api.existing = existing
    mock_project.get_dataset_api.return_value = api
    return api


@pytest.fixture
def feature_groups(mocker):
    by_name = {"orders": ORDERS, "customers": CUSTOMERS}
    return mocker.patch.object(
        fg_cmd, "_get_fg", side_effect=lambda ctx, name, version: by_name[name]
    )


def _run(*args, **kwargs):
    return CliRunner().invoke(cli, ["semantic", "init", *args], **kwargs)


class TestInitInAFolder:
    def test_writes_the_scaffold_registers_and_compiles_it(
        self, sl, datasets, feature_groups
    ):
        result = _run("sales", "--from-fg", "orders", "--from-fg", "customers:2")

        assert result.exit_code == 0, result.output
        feature_groups.assert_has_calls(
            [
                mock.call(mock.ANY, "orders", None),
                mock.call(mock.ANY, "customers", 2),
            ]
        )
        datasets.mkdir.assert_called_once_with("/Projects/demo/Resources/semantic")
        assert datasets.uploaded["parent"] == "/Projects/demo/Resources/semantic"
        assert datasets.uploaded["name"] == "sales"
        assert set(datasets.uploaded["files"]) == {
            "dbt_project.yml",
            "models/semantic.yml",
        }
        sl.create_project.assert_called_once_with(
            "sales",
            "/Projects/demo/Resources/semantic/sales",
            git_repository_id=None,
            format="DBT",
            deployment_mode="FOLLOW",
        )
        sl.create_project.return_value.compile.assert_called_once_with(wait=True)
        assert "Build 1: VALID" in result.output
        assert "Uncomment the metrics" in result.output

    def test_a_missing_mandatory_tag_is_refused_before_anything_is_written(
        self, sl, datasets, feature_groups
    ):
        sl._mandatory_tags.return_value = {"owner", "domain"}

        result = _run("sales", "--from-fg", "orders", "--tag", "domain=sales")

        assert result.exit_code == 1
        assert "owner" in result.output
        assert "--tag NAME=VALUE" in result.output
        datasets.mkdir.assert_not_called()
        assert datasets.uploaded == {}
        sl.create_project.assert_not_called()

    def test_a_refused_registration_says_how_to_register_the_files(
        self, sl, datasets, feature_groups
    ):
        sl.create_project.side_effect = RuntimeError("the backend refused it")

        result = _run("sales", "--from-fg", "orders", "--tag", 'quality={"tier": 1}')

        assert result.exit_code == 1
        assert "the backend refused it" in result.output
        assert (
            "hops semantic create sales --path /Projects/demo/Resources/semantic/sales"
            " --format DBT --tag 'quality={\"tier\": 1}'"
        ) in result.output.replace("\n", " ")

    def test_path_mode_and_tags_reach_the_registration(
        self, sl, datasets, feature_groups
    ):
        result = _run(
            "sales",
            "--from-fg",
            "orders",
            "--path",
            "Resources/metrics/sales",
            "--mode",
            "pinned",
            "--tag",
            "domain=sales",
        )

        assert result.exit_code == 0, result.output
        assert datasets.uploaded["parent"] == "/Projects/demo/Resources/metrics"
        args, kwargs = sl.create_project.call_args
        assert args == ("sales", "/Projects/demo/Resources/metrics/sales")
        assert kwargs["deployment_mode"] == "PINNED"
        assert kwargs["tags"] == {"domain": "sales"}

    def test_an_existing_directory_is_never_overwritten(
        self, sl, datasets, feature_groups
    ):
        datasets.existing.add("/Projects/demo/Resources/semantic/sales")

        result = _run("sales", "--from-fg", "orders")

        assert result.exit_code == 1
        assert "already exists" in result.output
        datasets.upload.assert_not_called()
        sl.create_project.assert_not_called()

    def test_an_existing_semantic_project_is_refused_first(
        self, sl, datasets, feature_groups
    ):
        sl.get_project.return_value = mock.MagicMock()

        result = _run("sales", "--from-fg", "orders")

        assert result.exit_code == 1
        assert "already exists" in result.output
        feature_groups.assert_not_called()
        datasets.upload.assert_not_called()

    def test_without_feature_groups_nothing_is_compiled(self, sl, datasets):
        result = _run("--json", "sales")

        assert result.exit_code == 0, result.output
        sl.create_project.return_value.compile.assert_not_called()
        assert json.loads(result.output)["build"] is None
        assert (
            "semantic_models"
            not in datasets.uploaded["files"]["models/semantic.yml"].split("#", 1)[0]
        )

    def test_an_invalid_build_shows_its_issues_and_exits_one(
        self, sl, datasets, feature_groups
    ):
        build = SemanticBuild(
            id=1,
            status="INVALID",
            error_count=1,
            issues=[
                SemanticIssue(
                    level="ERROR",
                    rule="EXPR",
                    message="unknown column",
                    file="models/semantic.yml",
                    line=9,
                )
            ],
        )
        sl.create_project.return_value.compile.side_effect = SemanticBuildException(
            "sales", build
        )

        result = _run("sales", "--from-fg", "orders")

        assert result.exit_code == 1
        assert "unknown column" in result.output

    def test_an_external_feature_group_is_refused(self, sl, datasets, mocker):
        external = mock.MagicMock(name="ExternalFeatureGroup")
        external.__class__ = type("ExternalFeatureGroup", (), {})
        external.name, external.version = "crm_accounts", 1
        mocker.patch.object(fg_cmd, "_get_fg", return_value=external)

        result = _run("sales", "--from-fg", "crm_accounts")

        assert result.exit_code == 1
        assert "crm_accounts version 1" in result.output
        assert "offline table" in result.output
        datasets.upload.assert_not_called()
        sl.create_project.assert_not_called()

    def test_a_bad_version_is_a_usage_error(self, sl, datasets, feature_groups):
        result = _run("sales", "--from-fg", "orders:latest")

        assert result.exit_code == 2
        assert "FG:VERSION" in result.output


class TestInitFromAnExistingRepository:
    @pytest.fixture
    def git_api(self, mock_project, datasets):
        api = mock.MagicMock(name="GitApi")
        api._clone.return_value = SimpleNamespace(
            path="/Projects/demo/Resources/semantic/metrics", id=31
        )
        mock_project.get_git_api.return_value = api
        return api

    def test_clones_into_resources_semantic_and_registers_the_clone(
        self, sl, git_api, datasets
    ):
        result = _run("sales", "--repo", "https://github.com/acme/metrics.git")

        assert result.exit_code == 0, result.output
        git_api._clone.assert_called_once_with(
            "https://github.com/acme/metrics.git",
            "/Projects/demo/Resources/semantic",
            provider="GitHub",
            timeout=semantic._CLONE_TIMEOUT_SECONDS,
        )
        sl.create_project.assert_called_once_with(
            "sales",
            "/Projects/demo/Resources/semantic/metrics",
            git_repository_id=31,
            format="AUTO",
            deployment_mode="FOLLOW",
        )
        datasets.upload.assert_not_called()
        sl.create_project.return_value.compile.assert_called_once_with(wait=True)

    def test_an_unknown_host_leaves_the_provider_to_the_default(self, sl, git_api):
        result = _run("sales", "--repo", "https://git.acme.internal/metrics.git")

        assert result.exit_code == 0, result.output
        assert git_api._clone.call_args.kwargs["provider"] is None

    @pytest.mark.parametrize(
        ("extra", "message"),
        [(["--from-fg", "orders"], "--from-fg"), (["--path", "x"], "--path")],
    )
    def test_scaffold_options_are_refused(self, sl, git_api, extra, message):
        result = _run("sales", "--repo", "https://github.com/acme/metrics.git", *extra)

        assert result.exit_code == 2
        assert message in result.output
        git_api._clone.assert_not_called()

    def test_a_failed_clone_registers_nothing(self, sl, git_api):
        git_api._clone.side_effect = GitException("authentication required")

        result = _run("sales", "--repo", "https://github.com/acme/metrics.git")

        assert result.exit_code == 1
        assert "authentication required" in result.output
        sl.create_project.assert_not_called()


class FakeShell:
    """Answers the gh and git commands init runs, recording each with the files in its working directory."""

    def __init__(self, taken=(), logged_in=True, email=False):
        self.calls = []
        self.taken = set(taken)
        self.logged_in = logged_in
        self.email = email
        self.pushed_files = None

    def __call__(self, args, cwd=None, timeout=120):
        self.calls.append(list(args))
        if args[:3] == ["gh", "auth", "status"]:
            return self._done(args, 0 if self.logged_in else 1, err="not logged in")
        if args[:3] == ["gh", "api", "user"]:
            return self._done(args, 0, out="octo\n")
        if args[:3] == ["gh", "repo", "view"]:
            return self._done(args, 0 if args[3] in self.taken else 1)
        if args[:3] == ["gh", "auth", "token"]:
            return self._done(args, 0, out="gho_secret\n")
        if args[:3] == ["git", "config", "user.email"]:
            return self._done(args, 0 if self.email else 1)
        if args[:3] == ["gh", "repo", "create"]:
            root = Path(cwd)
            self.pushed_files = sorted(
                str(f.relative_to(root)) for f in root.rglob("*") if f.is_file()
            )
        return self._done(args, 0)

    @staticmethod
    def _done(args, code, out="", err=""):
        return subprocess.CompletedProcess(args, code, out, err)

    def ran(self, *prefix):
        return [c for c in self.calls if c[: len(prefix)] == list(prefix)]


class TestInitInANewRepository:
    @pytest.fixture
    def git_api(self, mock_project, datasets):
        api = mock.MagicMock(name="GitApi")
        api._clone.return_value = SimpleNamespace(
            path="/Projects/demo/Resources/semantic/hops-semantic-sales-2", id=44
        )
        mock_project.get_git_api.return_value = api
        return api

    @pytest.fixture
    def providers(self, mocker):
        find = mocker.patch.object(git_cmd, "find_provider", return_value=None)
        register = mocker.patch.object(git_cmd, "register_provider")
        return SimpleNamespace(find=find, register=register)

    def test_creates_pushes_registers_the_token_clones_and_registers(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell(taken={"octo/hops-semantic-sales"})
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", input="y\ny\n")

        assert result.exit_code == 0, result.output
        assert "octo/hops-semantic-sales-2" in result.output
        providers.register.assert_called_once_with(
            "GitHub", "octo", "gho_secret", "github.com"
        )
        assert "gho_secret" not in result.output
        create = shell.ran("gh", "repo", "create")
        assert create == [
            [
                "gh",
                "repo",
                "create",
                "octo/hops-semantic-sales-2",
                "--private",
                "--source",
                ".",
                "--remote",
                "origin",
                "--push",
            ]
        ]
        assert shell.pushed_files is not None
        assert {"dbt_project.yml", "models/semantic.yml"} <= set(shell.pushed_files)
        commit = next(c for c in shell.calls if "commit" in c)
        assert "user.email=octo@users.noreply.github.com" in commit
        git_api._clone.assert_called_once_with(
            "https://github.com/octo/hops-semantic-sales-2.git",
            "/Projects/demo/Resources/semantic",
            provider="GitHub",
            timeout=semantic._CLONE_TIMEOUT_SECONDS,
        )
        sl.create_project.assert_called_once_with(
            "sales",
            "/Projects/demo/Resources/semantic/hops-semantic-sales-2",
            git_repository_id=44,
            format="DBT",
            deployment_mode="FOLLOW",
        )
        datasets.upload.assert_not_called()

    def test_the_token_is_registered_before_the_repository_exists(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell()
        mocker.patch.object(semantic, "_sh", shell)
        order = []
        providers.register.side_effect = lambda *a: order.append("register")
        original = shell.__call__

        def recording(args, cwd=None, timeout=120):
            if args[:3] == ["gh", "repo", "create"]:
                order.append("create")
            return original(args, cwd=cwd, timeout=timeout)

        mocker.patch.object(semantic, "_sh", recording)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", "--yes")

        assert result.exit_code == 0, result.output
        assert order == ["register", "create"]

    def test_declining_creates_nothing(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell()
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", input="n\n")

        assert result.exit_code == 1
        assert not shell.ran("gh", "repo", "create")
        assert not shell.ran("gh", "auth", "token")
        providers.register.assert_not_called()
        git_api._clone.assert_not_called()
        sl.create_project.assert_not_called()

    def test_declining_the_token_creates_nothing(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell()
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", input="y\nn\n")

        assert result.exit_code == 1
        assert not shell.ran("gh", "repo", "create")
        providers.register.assert_not_called()

    def test_yes_skips_both_questions(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        mocker.patch.object(semantic, "_sh", FakeShell())
        confirm = mocker.patch("click.confirm")

        result = _run("sales", "--from-fg", "orders", "--repo", "new", "--yes")

        assert result.exit_code == 0, result.output
        confirm.assert_not_called()
        providers.register.assert_called_once()

    def test_json_mode_without_yes_creates_nothing(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell()
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("--json", "sales", "--from-fg", "orders", "--repo", "new")

        assert result.exit_code == 1
        assert "--yes" in result.output
        assert not shell.ran("gh", "repo", "create")
        assert not shell.ran("gh", "auth", "token")
        providers.register.assert_not_called()
        git_api._clone.assert_not_called()

    def test_a_registered_provider_is_kept_and_not_asked_about(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        providers.find.return_value = SimpleNamespace(username="octo")
        shell = FakeShell(email=True)
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--repo", "new", input="y\n")

        assert result.exit_code == 0, result.output
        providers.register.assert_not_called()
        assert not shell.ran("gh", "auth", "token")
        commit = next(c for c in shell.calls if "commit" in c)
        assert not any(arg.startswith("user.email=") for arg in commit)
        # An empty project has nothing to compile.
        sl.create_project.return_value.compile.assert_not_called()

    def test_a_name_taken_in_hopsfs_is_skipped_too(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        datasets.existing.add("/Projects/demo/Resources/semantic/hops-semantic-sales")
        shell = FakeShell()
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", "--yes")

        assert result.exit_code == 0, result.output
        assert shell.ran("gh", "repo", "create")[0][3] == "octo/hops-semantic-sales-2"

    def test_without_a_github_login_nothing_happens(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        shell = FakeShell(logged_in=False)
        mocker.patch.object(semantic, "_sh", shell)

        result = _run("sales", "--from-fg", "orders", "--repo", "new", "--yes")

        assert result.exit_code == 1
        assert "gh auth login" in result.output
        assert shell.calls == [["gh", "auth", "status"]]
        providers.register.assert_not_called()
        sl.create_project.assert_not_called()

    def test_a_failed_clone_says_how_to_register_the_repository(
        self, sl, datasets, feature_groups, git_api, providers, mocker
    ):
        mocker.patch.object(semantic, "_sh", FakeShell())
        git_api._clone.side_effect = GitException("clone timed out")

        result = _run("sales", "--from-fg", "orders", "--repo", "new", "--yes")

        assert result.exit_code == 1
        assert (
            "hops semantic init sales --repo https://github.com/octo/hops-semantic-sales.git"
            in result.output
        )
        sl.create_project.assert_not_called()


class TestShell:
    def test_a_missing_program_exits_127(self):
        result = semantic._sh(["hops-no-such-program-xyz"])

        assert result.returncode == 127
        assert "not found" in result.stderr

    def test_a_timeout_exits_124(self, mocker):
        mocker.patch("subprocess.run", side_effect=subprocess.TimeoutExpired(["gh"], 1))

        result = semantic._sh(["gh", "repo", "create"], timeout=1)

        assert result.returncode == 124
        assert "timed out" in result.stderr
