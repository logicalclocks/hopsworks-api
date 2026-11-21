"""``hops project list`` shows every membership, not just the active project.

The previous implementation feature-detected a ``hopsworks.get_project_api``
function that does not exist and fell back to the logged-in project, so the
table always had exactly one row.
"""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner
from hopsworks.cli import auth, config, session
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


def _member(email, role, first="", last=""):
    m = mock.MagicMock()
    m.email, m.role, m.first_name, m.last_name, m.username = (
        email,
        role,
        first,
        last,
        None,
    )
    return m


def _members_project():
    project = mock.MagicMock()
    project.name = "demo"
    project.owner = "Owner@example.com"
    project.get_members.return_value = [
        _member("zed@example.com", "Data scientist", "Zed"),
        _member("owner@example.com", "Data owner", "Ann", "Owner"),
    ]
    return project


def _members(args, project, input=None):
    with mock.patch.object(session, "get_project", return_value=project):
        return CliRunner().invoke(cli, ["project", "members", *args], input=input)


def test_members_list_sorts_by_email_and_marks_the_owner(authed_config):
    result = _members(["list"], _members_project())

    assert result.exit_code == 0, result.output
    rows = [line.split() for line in result.output.splitlines()[2:] if line]
    assert rows[0] == ["owner@example.com", "Ann", "Owner", "Data", "owner", "*"]
    assert rows[1] == ["zed@example.com", "Zed", "Data", "scientist"]


def test_members_add_passes_email_and_role(authed_config):
    project = _members_project()
    project.add_member.return_value = _member("new@example.com", "Data scientist")

    result = _members(["add", "new@example.com", "--role", "data scientist"], project)

    assert result.exit_code == 0, result.output
    project.add_member.assert_called_once_with("new@example.com", "data scientist")
    assert "Added new@example.com as Data scientist" in result.output


def test_members_add_refused_shows_hopsworks_reason(authed_config):
    project = _members_project()
    project.add_member.side_effect = RuntimeError("Your role does not allow it")

    result = _members(["add", "new@example.com", "--role", "Data owner"], project)

    assert result.exit_code == 1
    assert (
        "Could not add member 'new@example.com': Your role does not allow it"
        in result.output
    )


def test_members_set_updates_the_role(authed_config):
    project = _members_project()

    result = _members(["set", "zed@example.com", "--role", "Data owner"], project)

    assert result.exit_code == 0, result.output
    project.get_members_api.return_value.update_role.assert_called_once_with(
        "zed@example.com", "Data owner"
    )


def _job(name, creator_email, enabled=True, fails=False):
    job = mock.MagicMock()
    job.name = name
    job.creator = {"email": creator_email, "username": name}
    job.job_schedule = mock.MagicMock(enabled=enabled) if enabled is not None else None
    if fails:
        job.pause_schedule.side_effect = RuntimeError("refused")
    return job


def test_members_remove_answered_no_changes_nothing(authed_config):
    project = _members_project()

    result = _members(["remove", "zed@example.com"], project, input="n\n")

    assert result.exit_code == 1
    project.remove_member.assert_not_called()


def test_members_remove_keeps_the_home_dir_then_pauses_only_their_enabled_schedules(
    authed_config,
):
    project = _members_project()
    theirs, disabled, unscheduled, others = (
        _job("theirs", "Zed@Example.com"),
        _job("disabled", "zed@example.com", enabled=False),
        _job("unscheduled", "zed@example.com", enabled=None),
        _job("others", "owner@example.com"),
    )
    project.get_job_api.return_value.get_jobs.return_value = [
        theirs,
        disabled,
        unscheduled,
        others,
    ]
    calls = mock.Mock()
    calls.attach_mock(project.remove_member, "remove")
    calls.attach_mock(project.get_job_api.return_value.get_jobs, "jobs")

    result = _members(["remove", "zed@example.com", "--yes"], project)

    assert result.exit_code == 0, result.output
    assert [c[0] for c in calls.mock_calls] == ["remove", "jobs"]
    project.remove_member.assert_called_once_with(
        "zed@example.com", delete_home_dir=False
    )
    theirs.pause_schedule.assert_called_once_with()
    for job in (disabled, unscheduled, others):
        job.pause_schedule.assert_not_called()


def test_members_remove_keep_schedules_reads_no_jobs(authed_config):
    project = _members_project()

    result = _members(
        ["remove", "zed@example.com", "--yes", "--keep-schedules"], project
    )

    assert result.exit_code == 0, result.output
    project.remove_member.assert_called_once()
    project.get_job_api.assert_not_called()


def test_members_remove_names_a_failed_pause_and_exits_1(authed_config):
    project = _members_project()
    project.get_job_api.return_value.get_jobs.return_value = [
        _job("stuck", "zed@example.com", fails=True),
        _job("fine", "zed@example.com"),
    ]

    result = _members(["remove", "zed@example.com", "--yes"], project)

    assert result.exit_code == 1
    project.remove_member.assert_called_once()
    assert "Could not pause the schedule of stuck" in result.output


def test_members_remove_refused_reads_no_jobs(authed_config):
    project = _members_project()
    project.remove_member.side_effect = RuntimeError("not a member")

    result = _members(["remove", "x@example.com", "--yes"], project)

    assert result.exit_code == 1
    assert "Could not remove member 'x@example.com': not a member" in result.output
    project.get_job_api.assert_not_called()


def test_members_transfer_confirms_then_transfers(authed_config):
    project = _members_project()

    declined = _members(["transfer", "zed@example.com"], project, input="n\n")
    assert declined.exit_code == 1
    project.transfer_ownership.assert_not_called()

    result = _members(["transfer", "zed@example.com", "--yes"], project)
    assert result.exit_code == 0, result.output
    project.transfer_ownership.assert_called_once_with("zed@example.com")
    assert "zed@example.com owns demo" in result.output


def test_members_transfer_refused_shows_hopsworks_reason(authed_config):
    project = _members_project()
    project.transfer_ownership.side_effect = RuntimeError("not a data owner")

    result = _members(["transfer", "zed@example.com", "--yes"], project)

    assert result.exit_code == 1
    assert (
        "Could not transfer project 'demo' to 'zed@example.com': not a data owner"
        in result.output
    )
