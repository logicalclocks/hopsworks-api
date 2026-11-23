"""``hops project``: list available projects, switch the active one, show its cost, and manage its members."""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import auth, config, output, session
from hopsworks_common.client.exceptions import RestAPIError


@click.group("project")
def project_group() -> None:
    """List and switch Hopsworks projects."""


@project_group.command("list")
@click.pass_context
def project_list(ctx: click.Context) -> None:
    """Show every project the current user is a member of.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    cfg = _require_auth(ctx)
    auth.login(host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project)
    from hopsworks_common.core import project_api  # noqa: PLC0415

    # One request: the team list already carries each project's id and name,
    # where the SDK's _get_projects() would fetch every project again by name.
    teams = project_api.ProjectApi()._get_project_teams() or []
    rows = []
    for team in sorted(
        teams, key=lambda t: t.get("project", {}).get("name", "").lower()
    ):
        project = team.get("project", {})
        active = "*" if project.get("name") == cfg.project else ""
        rows.append(
            [
                project.get("id", "?"),
                project.get("name", "?"),
                team.get("teamRole", ""),
                active,
            ]
        )
    output.print_table(["ID", "NAME", "ROLE", "ACTIVE"], rows)


@project_group.command("use")
@click.argument("name")
@click.pass_context
def project_use(ctx: click.Context, name: str) -> None:
    """Switch the default project to ``name`` and cache its identifiers.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        name: Project name to activate.
    """
    cfg = _require_auth(ctx)
    try:
        project = auth.login(
            host=cfg.host or "", api_key_value=cfg.api_key, project=name
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not switch to project '{name}': {exc}"
        ) from exc

    cfg.project = getattr(project, "name", name)
    cfg.project_id = getattr(project, "id", None)
    cfg.feature_store_id = _feature_store_id(project)
    config.save(cfg)
    output.success("✓ Active project set to %s", cfg.project)


def _feature_store_id(project: Any) -> int | None:
    """The project's feature store id, without building a feature store object.

    ``project.get_feature_store()`` imports hsfs, and with it pandas, pyarrow and
    the engine: close to three seconds on a laptop, spent on a number the REST
    response already carries and that only ``hops project info`` ever prints.
    ``None`` when the project has no feature store.
    """
    from hopsworks_common import (  # noqa: PLC0415 - keeps `hops --help` light
        client,
        util,
    )

    _client = client._get_instance()
    store = util._append_feature_store_suffix(getattr(project, "name", "") or "")
    try:
        raw = _client._send_request(
            "GET", ["project", _client._project_id, "featurestores", store]
        )
    except Exception:  # noqa: BLE001 - a project need not have a feature store
        return None
    return raw.get("featurestoreId")


@project_group.command("info")
@click.pass_context
def project_info(ctx: click.Context) -> None:
    """Print details about the currently active project.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    cfg = _require_auth(ctx)
    project = auth.login(
        host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project
    )
    rows = [
        ["Host", cfg.host or "?"],
        ["Project", getattr(project, "name", cfg.project or "?")],
        ["Project ID", str(getattr(project, "id", cfg.project_id or "?"))],
        ["Feature store ID", str(cfg.feature_store_id or "?")],
        ["Mode", cfg.mode()],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@project_group.command("costs")
@click.option(
    "--window",
    type=click.Choice(("hour", "day", "week", "month", "year"), case_sensitive=False),
    default="week",
    show_default=True,
    help="Time window ending now.",
)
@click.option(
    "--project",
    "project_name",
    default=None,
    help="Project to show; the active project when not given.",
)
@click.pass_context
def project_costs(ctx: click.Context, window: str, project_name: str | None) -> None:
    """Show the Kubernetes cost of a project over a recent window.

    Only the project's data owners can see it. Costs come from OpenCost; week,
    month and year also read the daily history Hopsworks has recorded so far.
    When some buckets of the window have no data, a coverage line says how
    many do; missing buckets are unknown cost, not zero.

    Args:
        ctx: Click context.
        window: One of hour, day, week, month, year.
        project_name: Project to show, or None for the active project.
    """
    import hopsworks
    from hopsworks.cli.commands.admin import print_costs

    session.get_project(ctx)
    try:
        costs = hopsworks.get_costs_api().get_project_costs(
            window, project=project_name
        )
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not get project costs: {exc}") from exc
    print_costs(costs, "namespace", window, show_idle=False)


@project_group.group("members")
def members_group() -> None:
    """List and change the members of the active project."""


@members_group.command("list")
@click.pass_context
def members_list(ctx: click.Context) -> None:
    """List the members of the active project: role and owner, then pending invites.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
    """
    project = session.get_project(ctx)
    try:
        members = project.get_members()
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not list members: {exc}") from exc
    owner = _owner_email(project)
    rows = []
    for m in sorted(members, key=lambda m: (m.email or "").casefold()):
        name = " ".join(p for p in (m.first_name, m.last_name) if p)
        rows.append(
            [
                m.email or "?",
                name or m.username or "?",
                m.role or "?",
                "*" if owner and (m.email or "").casefold() == owner else "",
            ]
        )
    invites, unavailable = _invites_of(project)
    for invite in invites:
        rows.append(
            [
                invite.get("email") or "?",
                "",
                f"{invite.get('role') or '?'} (invited, until {_day(invite.get('expires'))})",
                "",
            ]
        )
    output.print_table(["EMAIL", "NAME", "ROLE", "OWNER"], rows)
    if unavailable:
        click.echo(f"invites unavailable: {unavailable}", err=True)


@members_group.command("usage")
@click.option(
    "--since",
    default="7d",
    show_default=True,
    help="How far back: hours (36h) or days (7d).",
)
@click.option(
    "--until",
    default=None,
    help="End of the window, an ISO-8601 time; now when not given.",
)
@click.pass_context
def members_usage(ctx: click.Context, since: str, until: str | None) -> None:
    """Each member's CPU and memory use of the active project over a window.

    Use, not the bill: core-hours and GB-hours of the pods each member started.
    A Data owner sees every member and the pods with no member; any other
    member sees their own row. Money per project: `hops project costs`.

    \f
    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        since: Window length back from ``until``.
        until: Window end, or now.
    """
    project = session.get_project(ctx)
    try:
        usage = project.get_member_usage(since=since, until=until)
    except RestAPIError as exc:
        raise click.ClickException(
            f"Could not read member usage: {exc.user_message or exc}"
        ) from exc
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not read member usage: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(usage)
        return
    rows = [
        [
            m.get("email") or "?",
            f"{m.get('cpuCoreHours', 0):.2f}",
            f"{m.get('memoryGbHours', 0):.2f}",
        ]
        for m in sorted(
            usage.get("members") or [], key=lambda m: -(m.get("cpuCoreHours") or 0)
        )
    ]
    other = usage.get("notByMember")
    if other:
        rows.append(
            [
                "(not by a member)",
                f"{other.get('cpuCoreHours', 0):.2f}",
                f"{other.get('memoryGbHours', 0):.2f}",
            ]
        )
    output.print_table(["EMAIL", "CPU CORE-H", "MEMORY GB-H"], rows)
    click.echo(
        f"{usage.get('since')} to {usage.get('until')}, {usage.get('hours')} hours. Use, not the bill. GPU and storage: not split by member."
    )
    if usage.get("hoursMissing"):
        click.echo(
            f"{usage['hoursMissing']} hours missing: not rolled up yet, or Prometheus had no answer.",
            err=True,
        )


def _invites_of(project: Any) -> tuple[list[dict], str | None]:
    """Pending invites and why they could not be read, if so.

    A 403 means the caller may not read invites (an Observer): no invites, no
    reason. Any other failure is returned as the reason, so a backend outage
    never reads as "nothing pending".
    """
    try:
        invites = project.get_invites()
    except RestAPIError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == RestAPIError.STATUS_CODE_FORBIDDEN:
            return [], None
        return [], f"HTTP {status} {exc.user_message or ''}".strip()
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        return [], str(exc) or type(exc).__name__
    return sorted(invites or [], key=lambda i: (i.get("email") or "").casefold()), None


def _day(value: Any) -> str:
    """A date from the backend, epoch milliseconds or ISO text, as YYYY-MM-DD."""
    import datetime  # noqa: PLC0415

    if isinstance(value, (int, float)):
        return (
            datetime.datetime.fromtimestamp(value / 1000, datetime.timezone.utc)
            .date()
            .isoformat()
        )
    return str(value or "?")[:10]


def _owner_email(project: Any) -> str:
    """The project's owner read now: a cached project keeps the owner it was read with."""
    from hopsworks_common import client  # noqa: PLC0415

    try:
        raw = client._get_instance()._send_request("GET", ["project", project.id])
    except Exception:  # noqa: BLE001 - the list still shows without the marker
        return ""
    return (raw.get("owner") or "").casefold()


@members_group.command("add")
@click.argument("email")
@click.option(
    "--role",
    required=True,
    help="Data owner, Data scientist, Observer, Feature store restricted, or a custom project role.",
)
@click.pass_context
def members_add(ctx: click.Context, email: str, role: str) -> None:
    """Add a Hopsworks user to the active project, by email.

    Only a Data owner of the project can add members.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        email: Email address of the Hopsworks user to add.
        role: The project role to grant.
    """
    project = session.get_project(ctx)
    try:
        member = project.add_member(email, role)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not add member '{email}': {exc}") from exc
    output.success("Added %s as %s", email, getattr(member, "role", None) or role)


@members_group.command("invite")
@click.argument("email")
@click.option(
    "--role",
    required=True,
    help="Data owner, Data scientist, Observer or Feature store restricted.",
)
@click.pass_context
def members_invite(ctx: click.Context, email: str, role: str) -> None:
    """Invite an email address with no Hopsworks account to the active project.

    Once an account with that address is activated and its email is
    proven, it becomes a member with the role. The invite lasts 14 days;
    inviting again refreshes it. For an existing user, use add. Only a
    Data owner of the project can invite.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        email: Email address to invite.
        role: The project role it gets.
    """
    project = session.get_project(ctx)
    try:
        invite = project.invite_member(email, role)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not invite '{email}': {exc}") from exc
    output.success(
        "Invited %s as %s, until %s",
        email,
        (invite or {}).get("role") or role,
        _day((invite or {}).get("expires")),
    )


@members_group.command("set")
@click.argument("email")
@click.option("--role", required=True, help="The member's new project role.")
@click.pass_context
def members_set(ctx: click.Context, email: str, role: str) -> None:
    """Change a member's role in the active project.

    Only a Data owner of the project can change roles.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        email: Email address of the member.
        role: The project role to give them.
    """
    project = session.get_project(ctx)
    try:
        project.get_members_api().update_role(email, role)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not set member '{email}': {exc}") from exc
    output.success("%s is %s", email, role)


@members_group.command("remove")
@click.argument("email")
@click.option(
    "--keep-schedules",
    is_flag=True,
    help="Leave the schedules of jobs the member created running.",
)
@click.option("--yes", is_flag=True, help="Remove without asking to confirm.")
@click.pass_context
def members_remove(
    ctx: click.Context, email: str, keep_schedules: bool, yes: bool
) -> None:
    """Remove a member from the active project, or cancel a pending invite.

    Hopsworks stops their runs and notebooks. The schedules of jobs they
    created keep starting runs on the owner's resources, so they are paused
    after the removal unless --keep-schedules is given. Their home directory
    is kept.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        email: Email address of the member.
        keep_schedules: Leave their job schedules as they are.
        yes: Skip the confirmation prompt.
    """
    project = session.get_project(ctx)
    name = getattr(project, "name", "?")
    if _is_invited(project, email):
        try:
            project.cancel_invite(email)
        except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
            raise click.ClickException(
                f"Could not cancel the invite of '{email}': {exc}"
            ) from exc
        output.success("Cancelled the invite of %s to %s", email, name)
        return
    if not yes and not output.JSON_MODE:
        pause = (
            "" if keep_schedules else " The schedules of jobs they created pause too."
        )
        click.confirm(f"Remove {email} from {name}?{pause}", abort=True)
    try:
        project.remove_member(email, delete_home_dir=False)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Could not remove member '{email}': {exc}") from exc
    output.success("Removed %s from %s", email, name)
    if keep_schedules:
        return
    failed = _pause_schedules_of(project, email)
    if failed:
        raise click.ClickException(
            "Could not pause the schedule of " + ", ".join(failed)
        )


def _is_invited(project: Any, email: str) -> bool:
    """Whether the address has a pending invite and no membership: an invite exists only for an address with no account."""
    return any(
        (i.get("email") or "").casefold() == email.casefold()
        for i in _invites_of(project)[0]
    )


def _pause_schedules_of(project: Any, email: str) -> list[str]:
    """Pause each enabled schedule of a job the user created; the jobs whose pause failed."""
    try:
        jobs = project.get_job_api().get_jobs()
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(
            f"Removed {email}, but could not read the project's jobs: {exc}"
        ) from exc
    failed = []
    for job in jobs:
        schedule = getattr(job, "job_schedule", None)
        if not (schedule and getattr(schedule, "enabled", False)):
            continue
        if _email_of(getattr(job, "creator", None)) != email.casefold():
            continue
        try:
            job.pause_schedule()
        except Exception:  # noqa: BLE001 - named in the result
            failed.append(job.name)
            continue
        output.success("Paused the schedule of %s", job.name)
    return failed


def _email_of(user: Any) -> str:
    value = (
        user.get("email") if isinstance(user, dict) else getattr(user, "email", None)
    )
    return (value or "").casefold()


@members_group.command("transfer")
@click.argument("email")
@click.option("--yes", is_flag=True, help="Transfer without asking to confirm.")
@click.pass_context
def members_transfer(ctx: click.Context, email: str, yes: bool) -> None:
    """Make another Data owner of the active project its owner.

    Only the owner can do this. The new owner's resources pay for the
    project from then on; the previous owner stays a Data owner.

    Args:
        ctx: Click context carrying the resolved ``HopsConfig``.
        email: Email address of the Data owner who becomes the owner.
        yes: Skip the confirmation prompt.
    """
    project = session.get_project(ctx)
    name = getattr(project, "name", "?")
    if not yes and not output.JSON_MODE:
        click.confirm(f"Make {email} the owner of {name}?", abort=True)
    try:
        project.transfer_ownership(email)
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(
            f"Could not transfer project '{name}' to '{email}': {exc}"
        ) from exc
    output.success("%s owns %s", email, name)


def _require_auth(ctx: click.Context) -> config.HopsConfig:
    return session.require_auth(ctx)
