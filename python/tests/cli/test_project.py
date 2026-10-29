"""``hops project list`` shows every membership, not just the active project.

The previous implementation feature-detected a ``hopsworks.get_project_api``
function that does not exist and fell back to the logged-in project, so the
table always had exactly one row.
"""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner
from hopsworks.cli import auth, config
from hopsworks.cli.main import cli
from hopsworks_common.core import project_api


TEAMS = [
    {"project": {"id": 124, "name": "linda"}, "teamRole": "Data owner"},
    {"project": {"id": 119, "name": "demo"}, "teamRole": "Data owner"},
    {"project": {"id": 136, "name": "Shared"}, "teamRole": "Data scientist"},
]


def _run_list(teams):
    with (
        mock.patch.object(auth, "login", return_value=mock.MagicMock(name="Project")),
        mock.patch.object(
            project_api.ProjectApi, "_get_project_teams", return_value=teams
        ),
    ):
        return CliRunner().invoke(cli, ["project", "list"])


def test_project_list_shows_every_membership_sorted_with_active_marked(authed_config):
    result = _run_list(TEAMS)

    assert result.exit_code == 0, result.output
    lines = [
        line for line in result.output.splitlines() if line and not line.startswith("-")
    ]
    assert lines[0].split() == ["ID", "NAME", "ROLE", "ACTIVE"]
    assert [line.split()[1] for line in lines[1:]] == ["demo", "linda", "Shared"]
    assert lines[1].split() == ["119", "demo", "Data", "owner", "*"]
    assert lines[2].split() == ["124", "linda", "Data", "owner"]
    assert "Data scientist" in result.output


def test_project_list_json_keeps_name_clean_and_flags_active(authed_config):
    with (
        mock.patch.object(auth, "login", return_value=mock.MagicMock(name="Project")),
        mock.patch.object(
            project_api.ProjectApi, "_get_project_teams", return_value=TEAMS
        ),
    ):
        result = CliRunner().invoke(cli, ["--json", "project", "list"])

    assert result.exit_code == 0, result.output
    assert '"NAME": "demo"' in result.output
    assert '"NAME": "demo *"' not in result.output
    assert '"ID": 124' in result.output
    assert '"ROLE": "Data scientist"' in result.output
    assert result.output.count('"ACTIVE": "*"') == 1


def test_project_list_with_no_memberships_prints_only_the_header(authed_config):
    result = _run_list([])

    assert result.exit_code == 0, result.output
    assert "ID" in result.output and "demo" not in result.output


def _use_project(send_request):
    """Run `hops project use demo` with the SDK login and REST client mocked out."""
    project = mock.MagicMock()
    project.name = "demo"
    project.backing_name = "demo"
    project.id = 119
    fake_client = mock.Mock()
    fake_client._project_id = 119
    fake_client._send_request = send_request
    with (
        mock.patch.object(auth, "login", return_value=project),
        mock.patch("hopsworks_common.client._get_instance", return_value=fake_client),
    ):
        result = CliRunner().invoke(cli, ["project", "use", "demo"])
    return result, project, fake_client


def test_project_use_reads_the_feature_store_id_over_rest(authed_config):
    """Building a FeatureStore imports hsfs (~3s); the id comes from the response."""
    result, project, fake_client = _use_project(
        mock.Mock(return_value={"featurestoreId": 67})
    )

    assert result.exit_code == 0, result.output
    project.get_feature_store.assert_not_called()
    assert fake_client._send_request.call_args.args[0] == "GET"
    assert fake_client._send_request.call_args.args[1] == [
        "project",
        119,
        "featurestores",
        "demo_featurestore",
    ]
    assert config.load().feature_store_id == 67


def test_project_use_survives_a_project_without_a_feature_store(authed_config):
    result, _, _ = _use_project(mock.Mock(side_effect=RuntimeError("no feature store")))

    assert result.exit_code == 0, result.output
    assert config.load().feature_store_id is None
    assert config.load().project == "demo"


def test_get_project_logs_in_by_the_saved_project_id(tmp_home):
    import click
    from hopsworks.cli import session

    cfg = config.HopsConfig(host="h", api_key="k", project="demo", project_id=7)
    ctx = click.Context(cli)
    ctx.obj = {"config": cfg}
    with mock.patch.object(auth, "login", return_value=mock.MagicMock()) as login:
        session.get_project(ctx)

    assert login.call_args.kwargs["project_id"] == 7
    assert login.call_args.kwargs["project"] == "demo"


def test_auth_login_sends_the_id_instead_of_the_name(monkeypatch):
    import sys

    fake_hopsworks = mock.MagicMock()
    monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)

    auth.login(host="h", api_key_value="k", project="demo", project_id=7)

    kwargs = fake_hopsworks.login.call_args.kwargs
    assert kwargs["project_id"] == 7
    assert "project" not in kwargs


def test_project_use_takes_a_numeric_argument_as_an_id(authed_config):
    project = mock.MagicMock()
    project.name = "demo"
    project.id = 9
    project.backing_name = "demo_b"
    with (
        mock.patch.object(auth, "login", return_value=project) as login,
        mock.patch("hopsworks_common.client._get_instance") as get_client,
    ):
        get_client.return_value._send_request.return_value = {"featurestoreId": 3}
        result = CliRunner().invoke(cli, ["project", "use", "9"])

    assert result.exit_code == 0, result.output
    assert login.call_args.kwargs["project_id"] == 9
    assert login.call_args.kwargs["project"] is None
    assert config.load().project_id == 9


def test_project_list_marks_the_active_project_by_id(authed_config):
    teams = [
        {"project": {"id": 7, "name": "demo"}, "teamRole": "Data owner"},
        {"project": {"id": 9, "name": "demo"}, "teamRole": "Data owner"},
    ]
    cfg = config.load()
    cfg.project_id = 9
    config.save(cfg)

    result = _run_list(teams)

    rows = [line.split() for line in result.output.splitlines() if "demo" in line]
    assert [row[0] for row in rows if row[-1] == "*"] == ["9"]


def test_files_share_sends_a_numeric_target_as_an_id_without_a_lookup(mock_project):
    with mock.patch.object(project_api.ProjectApi, "_get_project_by_id") as lookup:
        result = CliRunner().invoke(
            cli, ["files", "share", "Resources/d", "--target", "22"]
        )

    assert result.exit_code == 0, result.output
    lookup.assert_not_called()
    dataset_api = mock_project.get_dataset_api.return_value
    assert dataset_api.share.call_args.kwargs["target_project"] == 22


def test_files_unshare_sends_a_numeric_target_as_an_id(mock_project):
    result = CliRunner().invoke(
        cli, ["files", "unshare", "Resources/d", "--target", "22"]
    )

    assert result.exit_code == 0, result.output
    dataset_api = mock_project.get_dataset_api.return_value
    assert dataset_api.unshare.call_args.kwargs["target_project"] == 22


def test_hops_login_verifies_with_the_saved_project_id(tmp_home):
    config.save(
        config.HopsConfig(host="https://h", api_key="k", project="demo", project_id=22)
    )
    project = mock.MagicMock()
    project.name = "demo"
    project.id = 22
    with mock.patch.object(auth, "verify", return_value=project) as verify:
        result = CliRunner().invoke(cli, ["login"])

    assert result.exit_code == 0, result.output
    assert verify.call_args.kwargs["project_id"] == 22


def test_a_saved_id_of_a_deleted_project_names_the_id_and_the_fix(tmp_home):
    import click
    from hopsworks.cli import session
    from hopsworks_common.client.exceptions import ProjectException

    cfg = config.HopsConfig(host="h", api_key="k", project="demo", project_id=22)
    ctx = click.Context(cli)
    ctx.obj = {"config": cfg}
    gone = ProjectException("Could not find project with id 22")
    with mock.patch.object(auth, "login", side_effect=gone):
        try:
            session.get_project(ctx)
        except click.ClickException as exc:
            assert "22" in exc.message and "hops project use" in exc.message
        else:
            raise AssertionError("expected a ClickException")
