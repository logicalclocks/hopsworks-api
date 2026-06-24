"""``hops alert`` — alerts and alert receivers.

Brings the CLI to parity with the SDK's alerting surface
(``project.get_alerts_api()`` →
[`AlertsApi`][hopsworks_common.core.alerts_api.AlertsApi]). Project-level
alerts and triggered alerts live directly under ``hops alert``; the
``receiver``, ``job``, ``fg`` and ``fv`` sub-groups manage alert receivers and
the entity-scoped alerts the SDK exposes per job / feature group / feature
view.

Every alert is wired to a *receiver* (the notification channel). List the
existing ones with ``hops alert receiver list`` and create new ones with
``hops alert receiver create`` before attaching an alert to them.

Note: creating and deleting receivers requires the Hopsworks administrator
role (``HOPS_ADMIN``); a regular project member can list and use receivers
but not manage them. There is no ``receiver delete`` command because the SDK
exposes no ``delete_receiver`` method — receivers must be removed by an
administrator through the cluster admin UI.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


_SEVERITY = ["warning", "critical", "info"]
_SERVICES = ["Featurestore", "Jobs"]
_JOB_STATUS = ["finished", "failed", "killed", "long_running"]
_FG_STATUS = [
    "feature_validation_success",
    "feature_validation_warning",
    "feature_validation_failure",
    "feature_monitor_shift_undetected",
    "feature_monitor_shift_detected",
]
_FV_STATUS = ["feature_monitor_shift_undetected", "feature_monitor_shift_detected"]
# Union of the per-service project alert statuses; the SDK enforces the
# service↔status pairing, so we accept any and let it validate.
_PROJECT_STATUS = [
    "job_finished",
    "job_failed",
    "job_killed",
    "job_long_running",
    "feature_validation_success",
    "feature_validation_warning",
    "feature_validation_failure",
    "feature_monitor_shift_undetected",
    "feature_monitor_shift_detected",
]


def _alerts_api(ctx: click.Context) -> Any:
    """Return the authenticated project's ``AlertsApi`` handle."""
    return session.get_project(ctx).get_alerts_api()


def _render_alerts(alerts: list[Any]) -> None:
    """Print a list of alert objects as a table (or JSON in ``--json`` mode)."""
    dicts = [a.to_dict() for a in alerts or []]
    if output.JSON_MODE:
        output.print_json(dicts)
        return
    if not dicts:
        output.info("No alerts.")
        return
    # Union the keys across rows so per-entity columns (job_name, service, …)
    # all show up; keep ``id`` first and ``created`` last for readability.
    keys: list[str] = []
    for d in dicts:
        for k in d:
            if k not in keys:
                keys.append(k)
    ordered = (
        ["id"]
        + [k for k in keys if k not in ("id", "created")]
        + (["created"] if "created" in keys else [])
    )
    rows = [
        [d.get(k, "") if d.get(k) is not None else "" for k in ordered] for d in dicts
    ]
    output.print_table([k.upper() for k in ordered], rows)


@click.group("alert")
def alert_group() -> None:
    """Alert and alert-receiver commands."""


# region Project alerts


@alert_group.command("list")
@click.pass_context
def alert_list(ctx: click.Context) -> None:
    """List every project-level alert.

    Args:
        ctx: Click context.
    """
    aa = _alerts_api(ctx)
    try:
        alerts = aa.get_alerts()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list alerts: {exc}") from exc
    _render_alerts(alerts)


@alert_group.command("get")
@click.argument("alert_id", type=int)
@click.pass_context
def alert_get(ctx: click.Context, alert_id: int) -> None:
    """Show a single project alert by ID.

    Args:
        ctx: Click context.
        alert_id: The numeric alert ID.
    """
    aa = _alerts_api(ctx)
    try:
        alert = aa.get_alert(alert_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not get alert {alert_id}: {exc}") from exc
    if alert is None:
        raise click.ClickException(f"Alert {alert_id} not found.")
    if output.JSON_MODE:
        output.print_json(alert.to_dict())
        return
    rows = [
        [k.upper(), v if v is not None else "-"] for k, v in alert.to_dict().items()
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@alert_group.command("create")
@click.option(
    "--receiver", required=True, help="Alert receiver (notification channel) name."
)
@click.option(
    "--status",
    required=True,
    type=click.Choice(_PROJECT_STATUS),
    help="Trigger status. job_* statuses require --service Jobs; the rest require Featurestore.",
)
@click.option("--severity", required=True, type=click.Choice(_SEVERITY))
@click.option("--service", required=True, type=click.Choice(_SERVICES))
@click.option(
    "--threshold", type=int, default=0, show_default=True, help="Alert threshold."
)
@click.pass_context
def alert_create(
    ctx: click.Context,
    receiver: str,
    status: str,
    severity: str,
    service: str,
    threshold: int,
) -> None:
    """Create a project-level alert (Jobs or Featurestore service).

    Args:
        ctx: Click context.
        receiver: Receiver name the alert notifies.
        status: Trigger status (must match the service).
        severity: Alert severity.
        service: ``Jobs`` or ``Featurestore``.
        threshold: Alert threshold.
    """
    aa = _alerts_api(ctx)
    try:
        alert = aa.create_project_alert(receiver, status, severity, service, threshold)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create alert: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(alert.to_dict())
        return
    output.success(
        "✓ Created alert %s (%s/%s)", getattr(alert, "id", "?"), service, status
    )


@alert_group.command("delete")
@click.argument("alert_id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def alert_delete(ctx: click.Context, alert_id: int, yes: bool) -> None:
    """Delete a project alert by ID.

    Args:
        ctx: Click context.
        alert_id: The numeric alert ID.
        yes: Skip confirmation when True.
    """
    aa = _alerts_api(ctx)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete alert {alert_id}?", abort=True)
    try:
        aa.delete_alert(alert_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not delete alert {alert_id}: {exc}") from exc
    output.success("✓ Deleted alert %s", alert_id)


@alert_group.command("triggered")
@click.option(
    "--active/--no-active",
    default=True,
    show_default=True,
    help="Include active alerts.",
)
@click.option("--silenced", is_flag=True, help="Include silenced alerts.")
@click.option("--inhibited", is_flag=True, help="Include inhibited alerts.")
@click.pass_context
def alert_triggered(
    ctx: click.Context, active: bool, silenced: bool, inhibited: bool
) -> None:
    """List currently triggered (firing) alerts.

    Args:
        ctx: Click context.
        active: Include active alerts.
        silenced: Include silenced alerts.
        inhibited: Include inhibited alerts.
    """
    aa = _alerts_api(ctx)
    try:
        alerts = aa.get_triggered_alerts(
            active=active, silenced=silenced, inhibited=inhibited
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list triggered alerts: {exc}") from exc
    dicts = [a.to_dict() for a in alerts or []]
    if output.JSON_MODE:
        output.print_json(dicts)
        return
    if not dicts:
        output.info("No triggered alerts.")
        return
    rows = [
        [
            output.format_mapping(_labels_to_map(d.get("labels"))),
            d.get("starts_at", "-") or "-",
            d.get("ends_at", "-") or "-",
        ]
        for d in dicts
    ]
    output.print_table(["LABELS", "STARTS AT", "ENDS AT"], rows)


@alert_group.command("trigger")
@click.option(
    "--receiver", "receiver_name", required=True, help="Receiver name to route to."
)
@click.option("--title", required=True)
@click.option("--summary", required=True)
@click.option("--description", required=True)
@click.option("--severity", required=True, type=click.Choice(_SEVERITY))
@click.option("--status", required=True, help="Free-form alert status label.")
@click.option("--name", required=True, help="Alert name (the alertname label).")
@click.option("--generator-url", "generator_url", help="URL of the alert generator.")
@click.option(
    "--expire-after-sec",
    "expire_after_sec",
    type=int,
    help="Seconds after which the alert auto-resolves.",
)
@click.pass_context
def alert_trigger(
    ctx: click.Context,
    receiver_name: str,
    title: str,
    summary: str,
    description: str,
    severity: str,
    status: str,
    name: str,
    generator_url: str | None,
    expire_after_sec: int | None,
) -> None:
    """Manually fire a one-off alert through a receiver.

    Args:
        ctx: Click context.
        receiver_name: Receiver to route the alert to.
        title: Alert title annotation.
        summary: Alert summary annotation.
        description: Alert description annotation.
        severity: Alert severity.
        status: Free-form status label.
        name: Alert name (alertname label).
        generator_url: Optional generator URL.
        expire_after_sec: Optional auto-resolve timeout.
    """
    aa = _alerts_api(ctx)
    try:
        aa.trigger_alert(
            receiver_name=receiver_name,
            title=title,
            summary=summary,
            description=description,
            severity=severity,
            status=status,
            name=name,
            generator_url=generator_url,
            expire_after_sec=expire_after_sec,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not trigger alert: {exc}") from exc
    output.success("✓ Triggered alert %s via %s", name, receiver_name)


# region Receivers


@alert_group.group("receiver")
def receiver_group() -> None:
    """Alert receiver (notification channel) commands.

    Listing and inspecting receivers is open to any project member. Creating a
    receiver requires the Hopsworks administrator role (``HOPS_ADMIN``). There
    is no delete command: the SDK exposes no ``delete_receiver`` method, so
    receivers must be removed by an administrator through the cluster admin UI.
    """


@receiver_group.command("list")
@click.pass_context
def receiver_list(ctx: click.Context) -> None:
    """List every alert receiver (project and global).

    Args:
        ctx: Click context.
    """
    aa = _alerts_api(ctx)
    try:
        receivers = aa.get_alert_receivers()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list receivers: {exc}") from exc
    dicts = [r.to_dict() for r in receivers or []]
    if output.JSON_MODE:
        output.print_json(dicts)
        return
    if not dicts:
        output.info("No alert receivers.")
        return
    rows = [[d.get("name", "?"), _receiver_channels(d)] for d in dicts]
    output.print_table(["NAME", "CHANNELS"], rows)


@receiver_group.command("get")
@click.argument("name")
@click.pass_context
def receiver_get(ctx: click.Context, name: str) -> None:
    """Show a single alert receiver by name.

    Args:
        ctx: Click context.
        name: Receiver name (with or without the project prefix).
    """
    aa = _alerts_api(ctx)
    try:
        receiver = aa.get_alert_receiver(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not get receiver '{name}': {exc}") from exc
    if receiver is None:
        raise click.ClickException(f"Receiver '{name}' not found.")
    output.print_json(receiver.to_dict()) if output.JSON_MODE else output.print_table(
        ["FIELD", "VALUE"],
        [
            ["NAME", receiver.to_dict().get("name", "?")],
            ["CHANNELS", _receiver_channels(receiver.to_dict())],
        ],
    )


@receiver_group.command("create")
@click.argument("name")
@click.option("--email", multiple=True, help="Email recipient; repeatable.")
@click.option("--slack", multiple=True, help="Slack channel; repeatable.")
@click.option(
    "--pagerduty",
    multiple=True,
    help='PagerDuty "service_key:routing_key"; repeatable.',
)
@click.option("--webhook", multiple=True, help="Webhook URL; repeatable.")
@click.option(
    "--send-resolved",
    is_flag=True,
    help="Also notify when the alert resolves.",
)
@click.pass_context
def receiver_create(
    ctx: click.Context,
    name: str,
    email: tuple[str, ...],
    slack: tuple[str, ...],
    pagerduty: tuple[str, ...],
    webhook: tuple[str, ...],
    send_resolved: bool,
) -> None:
    """Create an alert receiver. Provide exactly one channel type.

    Requires the Hopsworks administrator role (``HOPS_ADMIN``). A non-admin
    project member will get a permission error from the backend. Receivers
    cannot be deleted from the CLI or SDK (no ``delete_receiver`` method);
    an administrator must remove them through the cluster admin UI.

    Args:
        ctx: Click context.
        name: Receiver name.
        email: Email recipients.
        slack: Slack channels.
        pagerduty: ``service_key:routing_key`` pairs.
        webhook: Webhook URLs.
        send_resolved: Notify on resolution too.
    """
    email_configs = [{"to": e, "send_resolved": send_resolved} for e in email] or None
    slack_configs = [
        {"channel": c, "send_resolved": send_resolved} for c in slack
    ] or None
    webhook_configs = [
        {"url": u, "send_resolved": send_resolved} for u in webhook
    ] or None
    pagerduty_configs = None
    if pagerduty:
        pagerduty_configs = []
        for spec in pagerduty:
            if ":" not in spec:
                raise click.BadParameter(
                    f"'{spec}' must be 'service_key:routing_key'.",
                    param_hint="--pagerduty",
                )
            service_key, _, routing_key = spec.partition(":")
            pagerduty_configs.append(
                {
                    "service_key": service_key.strip(),
                    "routing_key": routing_key.strip(),
                    "send_resolved": send_resolved,
                }
            )

    aa = _alerts_api(ctx)
    try:
        receiver = aa.create_alert_receiver(
            name=name,
            email_configs=email_configs,
            slack_configs=slack_configs,
            pagerduty_configs=pagerduty_configs,
            webhook_configs=webhook_configs,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create receiver: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(receiver.to_dict())
        return
    output.success("✓ Created receiver %s", getattr(receiver, "name", name))


# region Job alerts


@alert_group.group("job")
def job_group() -> None:
    """Job-scoped alert commands."""


@job_group.command("list")
@click.argument("job_name")
@click.pass_context
def job_alert_list(ctx: click.Context, job_name: str) -> None:
    """List alerts attached to a job.

    Args:
        ctx: Click context.
        job_name: The job name.
    """
    aa = _alerts_api(ctx)
    try:
        alerts = aa.get_job_alerts(job_name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not list alerts for job '{job_name}': {exc}"
        ) from exc
    _render_alerts(alerts)


@job_group.command("get")
@click.argument("job_name")
@click.argument("alert_id", type=int)
@click.pass_context
def job_alert_get(ctx: click.Context, job_name: str, alert_id: int) -> None:
    """Show a single job alert by ID.

    Args:
        ctx: Click context.
        job_name: The job name.
        alert_id: The numeric alert ID.
    """
    aa = _alerts_api(ctx)
    try:
        alert = aa.get_job_alert(job_name, alert_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not get alert {alert_id}: {exc}") from exc
    if alert is None:
        raise click.ClickException(f"Alert {alert_id} not found for job '{job_name}'.")
    _render_alerts([alert])


@job_group.command("create")
@click.argument("job_name")
@click.option("--receiver", required=True, help="Alert receiver name.")
@click.option("--status", required=True, type=click.Choice(_JOB_STATUS))
@click.option("--severity", required=True, type=click.Choice(_SEVERITY))
@click.pass_context
def job_alert_create(
    ctx: click.Context,
    job_name: str,
    receiver: str,
    status: str,
    severity: str,
) -> None:
    """Create an alert that fires on a job event (e.g. ``failed``).

    Args:
        ctx: Click context.
        job_name: The job name.
        receiver: Receiver name the alert notifies.
        status: Job event that triggers the alert.
        severity: Alert severity.
    """
    aa = _alerts_api(ctx)
    try:
        alert = aa.create_job_alert(job_name, receiver, status, severity)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create job alert: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(alert.to_dict())
        return
    output.success(
        "✓ Created alert %s on job %s (%s)", getattr(alert, "id", "?"), job_name, status
    )


# region Feature group alerts


@alert_group.group("fg")
def fg_group() -> None:
    """Feature-group-scoped alert commands."""


@fg_group.command("list")
@click.argument("name")
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.pass_context
def fg_alert_list(ctx: click.Context, name: str, version: int | None) -> None:
    """List alerts attached to a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        version: Feature group version.
    """
    aa = _alerts_api(ctx)
    fs_id, fg_id = _resolve_fg(ctx, name, version)
    try:
        alerts = aa.get_feature_group_alerts(fs_id, fg_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not list alerts for feature group '{name}': {exc}"
        ) from exc
    _render_alerts(alerts)


@fg_group.command("create")
@click.argument("name")
@click.option("--receiver", required=True, help="Alert receiver name.")
@click.option("--status", required=True, type=click.Choice(_FG_STATUS))
@click.option("--severity", required=True, type=click.Choice(_SEVERITY))
@click.option("--version", type=int, help="Feature group version; defaults to latest.")
@click.pass_context
def fg_alert_create(
    ctx: click.Context,
    name: str,
    receiver: str,
    status: str,
    severity: str,
    version: int | None,
) -> None:
    """Create a validation / monitoring alert on a feature group.

    Args:
        ctx: Click context.
        name: Feature group name.
        receiver: Receiver name the alert notifies.
        status: Validation or monitoring trigger status.
        severity: Alert severity.
        version: Feature group version.
    """
    aa = _alerts_api(ctx)
    fs_id, fg_id = _resolve_fg(ctx, name, version)
    try:
        alert = aa.create_feature_group_alert(fs_id, fg_id, receiver, status, severity)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not create feature group alert: {exc}"
        ) from exc
    if output.JSON_MODE:
        output.print_json(alert.to_dict())
        return
    output.success(
        "✓ Created alert %s on feature group %s", getattr(alert, "id", "?"), name
    )


# region Feature view alerts


@alert_group.group("fv")
def fv_group() -> None:
    """Feature-view-scoped alert commands."""


@fv_group.command("list")
@click.argument("name")
@click.option("--version", type=int, help="Feature view version; defaults to latest.")
@click.pass_context
def fv_alert_list(ctx: click.Context, name: str, version: int | None) -> None:
    """List alerts attached to a feature view.

    Args:
        ctx: Click context.
        name: Feature view name.
        version: Feature view version.
    """
    aa = _alerts_api(ctx)
    fs_id, fv_name, fv_version = _resolve_fv(ctx, name, version)
    try:
        alerts = aa.get_feature_view_alerts(fs_id, fv_name, fv_version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not list alerts for feature view '{name}': {exc}"
        ) from exc
    _render_alerts(alerts)


@fv_group.command("create")
@click.argument("name")
@click.option("--receiver", required=True, help="Alert receiver name.")
@click.option("--status", required=True, type=click.Choice(_FV_STATUS))
@click.option("--severity", required=True, type=click.Choice(_SEVERITY))
@click.option("--version", type=int, help="Feature view version; defaults to latest.")
@click.pass_context
def fv_alert_create(
    ctx: click.Context,
    name: str,
    receiver: str,
    status: str,
    severity: str,
    version: int | None,
) -> None:
    """Create a monitoring alert on a feature view.

    Args:
        ctx: Click context.
        name: Feature view name.
        receiver: Receiver name the alert notifies.
        status: Monitoring trigger status.
        severity: Alert severity.
        version: Feature view version.
    """
    aa = _alerts_api(ctx)
    fs_id, fv_name, fv_version = _resolve_fv(ctx, name, version)
    try:
        alert = aa.create_feature_view_alert(
            fs_id, fv_name, fv_version, receiver, status, severity
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not create feature view alert: {exc}"
        ) from exc
    if output.JSON_MODE:
        output.print_json(alert.to_dict())
        return
    output.success(
        "✓ Created alert %s on feature view %s", getattr(alert, "id", "?"), name
    )


# region Helpers


def _resolve_fg(ctx: click.Context, name: str, version: int | None) -> tuple[int, int]:
    """Resolve a feature group name to ``(feature_store_id, feature_group_id)``."""
    fs = session.get_feature_store(ctx)
    try:
        fg = fs.get_feature_group(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature group '{name}' not found: {exc}") from exc
    return fs.id, fg.id


def _resolve_fv(
    ctx: click.Context, name: str, version: int | None
) -> tuple[int, str, int]:
    """Resolve a feature view to ``(feature_store_id, name, version)``."""
    fs = session.get_feature_store(ctx)
    try:
        fv = fs.get_feature_view(name, version=version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Feature view '{name}' not found: {exc}") from exc
    return fs.id, getattr(fv, "name", name), fv.version


def _labels_to_map(labels: Any) -> dict[str, Any]:
    """Normalize a triggered alert's labels (list of {key,value} or dict) to a map."""
    if isinstance(labels, dict):
        return labels
    out: dict[str, Any] = {}
    for item in labels or []:
        if isinstance(item, dict) and "key" in item:
            out[item["key"]] = item.get("value")
    return out


def _receiver_channels(d: dict[str, Any]) -> str:
    """Summarize the configured channels of a receiver dict for a table cell."""
    parts: list[str] = []
    if d.get("email_configs"):
        parts.append("email:" + ",".join(c.get("to", "?") for c in d["email_configs"]))
    if d.get("slack_configs"):
        parts.append(
            "slack:" + ",".join(c.get("channel", "?") for c in d["slack_configs"])
        )
    if d.get("pager_duty_configs"):
        parts.append("pagerduty")
    if d.get("webhook_configs"):
        parts.append(
            "webhook:" + ",".join(c.get("url", "?") for c in d["webhook_configs"])
        )
    return "; ".join(parts) or "-"
