"""The health report of a built ML system, for ``hops factory system status``.

`collect` gathers the facts: every job the system created (from the same
inventory ``hops factory system delete`` reads in system.yaml) with its executions in
the last hours and the log tail of each failure, and every deployment and app
with its state and its pods (phase, readiness, restarts, the last termination
reason, CPU and memory used against the limits, from kubectl in the project
namespace). `summarize` asks Claude for a short account of what failed and
why; `render` writes one self-contained HTML page with the facts, the summary
and a little JavaScript, which the Factory page shows next to the architecture.
"""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hopsworks.cli import teardown


LOG_TAIL_LINES = 40
_UNITS = {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024, "Ti": 1024 * 1024}


def _when(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return value if isinstance(value, datetime) else None


def _cpu_millis(value: str) -> float | None:
    if not value:
        return None
    if value.endswith("m"):
        return float(value[:-1])
    if value.endswith("n"):
        return float(value[:-1]) / 1e6
    return float(value) * 1000


def _memory_mib(value: str) -> float | None:
    if not value:
        return None
    for unit, factor in _UNITS.items():
        if value.endswith(unit):
            return float(value[: -len(unit)]) * factor
    if value[-1:] in "KMG":
        return float(value[:-1]) * {"K": 1 / 1000, "M": 1, "G": 1000}[value[-1]]
    return float(value) / (1024 * 1024)


def _read(path: str | None) -> str:
    return Path(path).read_text(errors="replace") if path else ""


def _tail(text: str | None, lines: int = LOG_TAIL_LINES) -> str:
    return "\n".join((text or "").strip().splitlines()[-lines:])


def _job_runs(project: Any, name: str, since: datetime) -> dict:
    job = project.get_job_api().get_job(name)
    if job is None:
        return {"name": name, "missing": True, "runs": []}
    runs = []
    for execution in job.get_executions() or []:
        submitted = _when(execution.submission_time)
        if submitted is None or submitted < since:
            continue
        run = {
            "id": execution.id,
            "state": execution.state,
            "final_status": execution.final_status,
            "submitted": submitted.isoformat(),
            "duration_s": round((execution.duration or 0) / 1000),
        }
        if execution.final_status in ("FAILED", "KILLED") or execution.state in (
            "FAILED",
            "KILLED",
            "FRAMEWORK_FAILURE",
            "APP_MASTER_START_FAILED",
            "INITIALIZATION_FAILED",
        ):
            try:
                # A temporary directory, since the report runs in the system's
                # git work tree, where downloaded logs do not belong.
                with tempfile.TemporaryDirectory(prefix="hops-status-logs-") as tmp:
                    stdout, stderr = execution.download_logs(path=tmp)
                    run["log_tail"] = _tail(_read(stderr)) or _tail(_read(stdout))
            except Exception as exc:  # noqa: BLE001 - a missing log is reported, not fatal
                run["log_tail"] = f"(logs unavailable: {exc})"
        runs.append(run)
    schedule = getattr(job, "job_schedule", None)
    return {
        "name": name,
        "type": getattr(job, "job_type", None),
        "scheduled": bool(schedule and getattr(schedule, "enabled", False)),
        "runs": sorted(runs, key=lambda r: r["submitted"], reverse=True),
    }


def _kubectl(*args: str) -> str:
    if not shutil.which("kubectl"):
        return ""
    done = subprocess.run(
        ["kubectl", *args], capture_output=True, text=True, timeout=60, check=False
    )
    return done.stdout if done.returncode == 0 else ""


def _pods(prefixes: dict[str, str]) -> dict[str, list[dict]]:
    """The pods of each named component, by pod-name prefix, with their resource use."""
    listed = _kubectl("get", "pods", "-o", "json")
    if not listed:
        return {}
    usage = {}
    for line in _kubectl("top", "pods", "--no-headers", "--containers").splitlines():
        parts = line.split()
        if len(parts) >= 4:
            pod = usage.setdefault(parts[0], {"cpu_m": 0.0, "memory_mib": 0.0})
            pod["cpu_m"] += _cpu_millis(parts[2]) or 0
            pod["memory_mib"] += _memory_mib(parts[3]) or 0
    found: dict[str, list[dict]] = {name: [] for name in prefixes}
    for pod in json.loads(listed).get("items", []):
        name = pod["metadata"]["name"]
        owner = next((c for c, p in prefixes.items() if name.startswith(p)), None)
        if owner is None:
            continue
        statuses = pod.get("status", {}).get("containerStatuses", [])
        limits_cpu = limits_mem = 0.0
        for container in pod["spec"].get("containers", []):
            limits = container.get("resources", {}).get("limits", {})
            limits_cpu += _cpu_millis(limits.get("cpu", "")) or 0
            limits_mem += _memory_mib(limits.get("memory", "")) or 0
        terminated = [
            {
                "container": s["name"],
                "reason": s["lastState"]["terminated"].get("reason"),
                "exit_code": s["lastState"]["terminated"].get("exitCode"),
            }
            for s in statuses
            if s.get("lastState", {}).get("terminated")
        ]
        found[owner].append(
            {
                "pod": name,
                "phase": pod.get("status", {}).get("phase"),
                "ready": f"{sum(1 for s in statuses if s.get('ready'))}/{len(statuses)}",
                "restarts": sum(s.get("restartCount", 0) for s in statuses),
                "last_terminations": terminated,
                "cpu_m": usage.get(name, {}).get("cpu_m"),
                "memory_mib": usage.get(name, {}).get("memory_mib"),
                "cpu_limit_m": limits_cpu or None,
                "memory_limit_mib": limits_mem or None,
            }
        )
    return found


def collect(project: Any, doc: dict, slug: str, hours: int = 24) -> dict:
    """The facts of the report: jobs over the last `hours`, what the feature pipelines wrote, deployments and apps with their pods."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    assets = teardown.inventory(doc, slug)
    job_names = [a.name for a in assets if a.kind == "job" and not a.name.endswith("*")]
    jobs = []
    for name in job_names:
        try:
            jobs.append(_job_runs(project, name, since))
        except Exception as exc:  # noqa: BLE001
            jobs.append({"name": name, "error": str(exc), "runs": []})

    services = []
    serving = project.get_model_serving()
    for asset in assets:
        if asset.kind == "deployment":
            deployment = serving.get_deployment(asset.name)
            state = None
            if deployment is not None:
                try:
                    state = deployment.get_state().status
                except Exception as exc:  # noqa: BLE001
                    state = f"unknown ({exc})"
            services.append(
                {
                    "kind": "deployment",
                    "name": asset.name,
                    "state": state or "missing",
                    "prefix": f"{asset.name}-predictor",
                }
            )
        elif asset.kind == "app":
            app = project.get_app_api().get_app(asset.name)
            services.append(
                {
                    "kind": "app",
                    "name": asset.name,
                    "state": getattr(app, "state", None) or "missing",
                    "url": getattr(app, "app_url", None),
                    "prefix": f"pythonapp-{getattr(project, 'name', '')}--{asset.name}",
                }
            )
    pods = _pods({s["name"]: s.pop("prefix") for s in services})
    for service in services:
        service["pods"] = pods.get(service["name"], [])
    # Imported here: pipeline_data reads tables through silver_status, which imports this module.
    from hopsworks.cli import pipeline_data

    pipelines = pipeline_data.collect(project, doc, hours)
    troubled = [p["name"] for p in pipelines if p["problems"]]

    failed = sum(
        1
        for j in jobs
        for r in j["runs"]
        if r.get("final_status") in ("FAILED", "KILLED")
    )
    unhealthy = [
        s["name"]
        for s in services
        if str(s["state"]).upper() not in ("RUNNING", "IDLE")
        or any(
            p["restarts"]
            or p["phase"] != "Running"
            or p["ready"].split("/")[0] != p["ready"].split("/")[1]
            for p in s["pods"]
        )
    ]
    return {
        "system": {
            "slug": slug,
            "name": (doc.get("system") or {}).get("name") or slug,
            "type": (doc.get("requirements") or {}).get("system_type"),
        },
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hours": hours,
        "overall": "failing"
        if failed and (unhealthy or troubled)
        else "degraded"
        if failed or unhealthy or troubled
        else "healthy",
        "counts": {
            "jobs": len(jobs),
            "runs": sum(len(j["runs"]) for j in jobs),
            "failed_runs": failed,
            "services": len(services),
            "unhealthy_services": len(unhealthy),
            "pipelines": len(pipelines),
            "pipeline_problems": len(troubled),
        },
        "jobs": jobs,
        "services": services,
        "pipelines": pipelines,
    }


def summarize(facts: dict, timeout: int = 180) -> str | None:
    """Claude's short account of what failed and why, as HTML paragraphs and lists, or None."""
    if not shutil.which("claude"):
        return None
    prompt = (
        "You are writing the summary section of a health report for an ML system or an analytics layer on Hopsworks. "
        "From the JSON facts below, write at most 150 words as an HTML fragment using only <p>, <ul>, "
        "<li>, <strong> and <code>: first one sentence on the overall health, then, for each failed "
        "job run, unhealthy deployment or app, or table with problems (stale, rejects over the gate, small files), "
        "what failed and the likely cause from its log tail "
        "or pod state (OOMKilled means raise the memory), and what to do. Say plainly when everything "
        "is healthy. No headings, no markdown, no preamble.\n\n"
        + json.dumps(facts, default=str)
    )
    try:
        done = subprocess.run(
            ["claude", "-p", "--model", "haiku", prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None
    text = done.stdout.strip() if done.returncode == 0 else ""
    # Only the tags asked for survive; anything else is shown as text.
    text = re.sub(r"```(?:html)?", "", text).strip()
    allowed = re.compile(r"</?(p|ul|li|strong|code)>")
    parts = re.split(r"(</?[a-zA-Z][^>]*>)", text)
    return (
        "".join(
            p if allowed.fullmatch(p) else html.escape(p) if p.startswith("<") else p
            for p in parts
        )
        or None
    )


def render(facts: dict, summary: str | None) -> str:
    """One self-contained HTML page: the facts as JSON, drawn by the script in the page."""
    data = json.dumps({"facts": facts, "summary": summary}, default=str).replace(
        "</", "<\\/"
    )
    return _PAGE.replace("__DATA__", data).replace(
        "__TITLE__", html.escape(f"{facts['system']['name']} status")
    )


_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="hopsworks-design-tokens" content="1">
<title>__TITLE__</title>
<style>
/* The token names and values of hopsworks-front's src/styles/globals.css, so the
   page matches the UI when opened on its own. The hopsworks-design-tokens meta
   tells the UI's status view it may override them with the live values (theme,
   cluster branding) and set data-theme. */
:root {
  --app-font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, Roboto, 'Helvetica Neue', Arial, sans-serif;
  --font-mono: ui-monospace, 'SF Mono', Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
  --background: oklch(1 0 0); --foreground: oklch(0.145 0 0);
  --card: oklch(1 0 0); --card-foreground: oklch(0.145 0 0);
  --muted: oklch(0.97 0 0); --muted-foreground: oklch(0.556 0 0);
  --accent: oklch(0.95 0 0); --border: oklch(0.922 0 0); --input: oklch(0.922 0 0);
  --destructive: oklch(0.577 0.245 27.325); --primary: #1eb182;
  --surface-recessed: oklch(0.95 0 0); --radius: 0.625rem;
  --quartz-primary: #21b182; --quartz-label-orange: #f2994a;
  --quartz-label-blue: #186781; --quartz-gray: #a0a0a0;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --background: oklch(0.145 0 0); --foreground: oklch(0.985 0 0);
    --card: oklch(0.205 0 0); --card-foreground: oklch(0.985 0 0);
    --muted: oklch(0.269 0 0); --muted-foreground: oklch(0.708 0 0);
    --accent: oklch(0.269 0 0); --border: oklch(1 0 0 / 10%); --input: oklch(1 0 0 / 15%);
    --destructive: oklch(0.704 0.191 22.216); --surface-recessed: oklch(0.24 0 0);
    --quartz-primary: #229570; --quartz-label-orange: #c0844e;
    --quartz-label-blue: #2885a4; --quartz-gray: #a6a6a6;
    color-scheme: dark;
  }
}
:root[data-theme="dark"] { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; padding: 16px; font: 14px/1.5 var(--app-font-sans); background: var(--background); color: var(--foreground); }
h1 { font-size: 18px; font-weight: 600; margin: 0; }
h2 { font-size: 14px; font-weight: 600; margin: 0 0 12px; }
.page { display: flex; flex-direction: column; gap: 20px; }
.muted { color: var(--muted-foreground); } .xs { font-size: 12px; } .grow { flex: 1; min-width: 0; }
.head { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; }
/* Card: rounded-lg border border-border bg-card shadow-sm */
.card { background: var(--card); color: var(--card-foreground); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: 0 1px 2px 0 rgb(0 0 0 / 0.05); padding: 16px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }
.stat .v { font-size: 24px; font-weight: 600; line-height: 1.2; } .stat .l { color: var(--muted-foreground); font-size: 12px; }
/* Badge: h-5 rounded-4xl px-2 text-xs font-medium, variants success/notice/fail */
.badge { display: inline-flex; align-items: center; height: 20px; padding: 0 8px; border-radius: 26px; font-size: 12px; font-weight: 500; white-space: nowrap; }
.badge.success, .badge.healthy { background: color-mix(in oklab, var(--quartz-primary) 10%, transparent); color: var(--quartz-primary); }
.badge.notice, .badge.degraded { background: color-mix(in oklab, var(--quartz-label-orange) 10%, transparent); color: var(--quartz-label-orange); }
.badge.fail, .badge.failing { background: color-mix(in oklab, var(--destructive) 10%, transparent); color: var(--destructive); }
.badge.info { background: color-mix(in oklab, var(--quartz-label-blue) 10%, transparent); color: var(--quartz-label-blue); }
/* Table: wrapper rounded-md border; head h-9 px-3 text-xs font-bold muted-foreground on bg-muted/50; rows border-b hover:bg-accent */
.table { width: 100%; overflow: auto; border: 1px solid var(--border); border-radius: calc(var(--radius) - 2px); }
table { width: 100%; border-collapse: collapse; font-size: 14px; }
thead { background: color-mix(in oklab, var(--muted) 50%, transparent); }
th { height: 36px; padding: 0 12px; text-align: left; font-size: 12px; font-weight: 700; color: var(--muted-foreground); white-space: nowrap; }
td { padding: 8px 12px; vertical-align: top; }
tr { border-bottom: 1px solid var(--border); } tbody tr:last-child { border-bottom: 0; }
tbody tr:hover { background: var(--accent); } tr.detail:hover { background: none; }
/* Button outline, size sm: h-7 px-2.5 rounded-lg border-border bg-background hover:bg-muted */
button.outline { height: 28px; padding: 0 10px; border: 1px solid var(--border); border-radius: calc(var(--radius) - 2px); background: var(--background); color: var(--foreground); font: 500 13px var(--app-font-sans); cursor: pointer; white-space: nowrap; }
button.outline:hover { background: var(--muted); }
:root[data-theme="dark"] button.outline { border-color: var(--input); background: color-mix(in oklab, var(--input) 30%, transparent); }
label.filter { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; cursor: pointer; }
label.filter input { accent-color: var(--primary); margin: 0; }
/* One square per run, oldest first */
.runs { display: flex; gap: 3px; flex-wrap: wrap; max-width: 320px; }
.run { width: 10px; height: 10px; border-radius: 2px; background: var(--quartz-primary); }
.run.FAILED, .run.KILLED { background: var(--destructive); } .run.running { background: var(--quartz-label-orange); }
/* Progress: h-2 rounded-full bg-muted-foreground/20, fill quartz-primary or label-orange */
.progress { height: 8px; min-width: 96px; border-radius: 999px; overflow: hidden; background: color-mix(in oklab, var(--muted-foreground) 20%, transparent); margin-bottom: 2px; }
.progress > div { height: 100%; border-radius: 999px; background: var(--quartz-primary); }
.progress.warning > div { background: var(--quartz-label-orange); } .progress.over > div { background: var(--destructive); }
/* Recessed surface for the nested log tails */
pre { margin: 4px 0 8px; padding: 12px; border-radius: var(--radius); background: var(--surface-recessed); max-height: 260px; overflow: auto; font: 12px/1.5 var(--font-mono); white-space: pre-wrap; }
code { font-family: var(--font-mono); font-size: 12px; }
.summary p { margin: 0 0 8px; } .summary p:last-child, .summary ul:last-child { margin-bottom: 0; }
.fail-text { color: var(--destructive); } .service + .service { margin-top: 16px; }
.service-head { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 8px; }
</style>
</head>
<body>
<div id="root" class="page"></div>
<script>
const { facts, summary } = __DATA__;
const el = (tag, attrs = {}, ...kids) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) k === 'class' ? (n.className = v) : k.startsWith('on') ? n.addEventListener(k.slice(2), v) : n.setAttribute(k, v);
  for (const kid of kids.flat()) n.append(kid instanceof Node ? kid : document.createTextNode(kid ?? ''));
  return n;
};
const ago = (iso) => { const s = (Date.now() - new Date(iso)) / 1000;
  return s < 90 ? `${Math.round(s)} s ago` : s < 5400 ? `${Math.round(s / 60)} min ago` : `${Math.round(s / 3600)} h ago`; };
const dur = (s) => s >= 3600 ? `${(s / 3600).toFixed(1)} h` : s >= 60 ? `${Math.round(s / 60)} min` : `${s} s`;
const failedRun = (r) => ['FAILED', 'KILLED'].includes(r.final_status);
const healthyState = (s) => ['RUNNING', 'IDLE'].includes(String(s.state).toUpperCase());
const badge = (variant, text) => el('span', { class: `badge ${variant}` }, text);
const table = (heads, rows) => el('div', { class: 'table' }, el('table', {}, el('thead', {}, el('tr', {}, heads.map((h) => el('th', {}, h)))), el('tbody', {}, rows)));
let onlyProblems = false;

function usage(used, limit, unit) {
  if (used == null) return el('span', { class: 'muted' }, 'no metrics');
  const pct = limit ? Math.min(100, (used / limit) * 100) : 0;
  const bar = el('div', { class: `progress ${pct > 90 ? 'over' : pct > 70 ? 'warning' : ''}` }, el('div', { style: `width:${pct}%` }));
  return el('div', {}, bar, el('span', { class: 'muted xs' }, `${Math.round(used)} ${unit}${limit ? ` of ${Math.round(limit)}` : ''}`));
}

function jobsSection() {
  const rows = facts.jobs.filter((j) => !onlyProblems || j.runs.some(failedRun) || j.missing || j.error);
  const body = [];
  for (const j of rows) {
    const failed = j.runs.filter(failedRun);
    const last = j.runs[0];
    const detail = el('tr', { class: 'detail', style: 'display:none' }, el('td', { colspan: 5 },
      failed.map((r) => el('div', {}, el('div', { class: 'muted xs' }, `Execution ${r.id}, ${r.final_status}, ${ago(r.submitted)}`), el('pre', {}, r.log_tail || '(no log)')))));
    const toggle = failed.length ? el('button', { class: 'outline', onclick: (e) => {
      const open = detail.style.display === 'none';
      detail.style.display = open ? '' : 'none';
      e.target.textContent = `${open ? 'Hide' : 'Show'} ${failed.length} failed`;
    } }, `Show ${failed.length} failed`) : null;
    body.push(el('tr', {},
      el('td', {}, el('div', { style: 'font-weight:500' }, j.name), el('div', { class: 'muted xs' }, j.missing ? 'missing' : `${j.type ?? ''}${j.scheduled ? ' · scheduled' : ''}`)),
      el('td', {}, el('div', { class: 'runs' }, j.runs.slice().reverse().map((r) => el('span', { class: `run ${r.final_status} ${r.final_status === 'UNDEFINED' ? 'running' : ''}`, title: `${r.id} ${r.final_status} ${ago(r.submitted)}` })))),
      el('td', {}, `${j.runs.filter((r) => r.final_status === 'SUCCEEDED').length}/${j.runs.length}`),
      el('td', {}, last ? `${ago(last.submitted)} · ${dur(last.duration_s)}` : el('span', { class: 'muted' }, j.error || 'no runs')),
      el('td', { style: 'text-align:right' }, toggle || (j.missing || j.error ? badge('fail', 'missing') : j.runs.length ? badge('success', 'healthy') : badge('info', 'idle')))));
    body.push(detail);
  }
  return el('section', { class: 'card' }, el('h2', {}, `Jobs, last ${facts.hours} hours`),
    rows.length ? table(['Job', 'Runs', 'Succeeded', 'Last run', ''], body)
      : el('p', { class: 'muted' }, onlyProblems ? 'No job has a failed run.' : 'No jobs.'));
}

function servicesSection() {
  const title = el('h2', {}, 'Deployments and apps');
  if (!facts.services.length) return el('section', { class: 'card' }, title, el('p', { class: 'muted' }, 'This system has no deployment or app.'));
  const rows = facts.services.filter((s) => !onlyProblems || s.pods.some((p) => p.restarts || p.phase !== 'Running') || !healthyState(s));
  if (!rows.length) return el('section', { class: 'card' }, title, el('p', { class: 'muted' }, 'Every deployment and app is healthy.'));
  return el('section', { class: 'card' }, title,
    rows.map((s) => el('div', { class: 'service' },
      el('div', { class: 'service-head' }, el('span', { style: 'font-weight:500' }, s.name), el('span', { class: 'muted' }, s.kind),
        badge(healthyState(s) ? 'success' : 'fail', s.state)),
      s.pods.length ? table(['Pod', 'Ready', 'Restarts', 'CPU (mCPU)', 'Memory (MiB)'],
        s.pods.map((p) => el('tr', {},
          el('td', {}, el('code', {}, p.pod), p.last_terminations.map((t) => el('div', { class: 'fail-text xs' }, `${t.container}: ${t.reason} (exit ${t.exit_code})`))),
          el('td', {}, `${p.phase} ${p.ready}`),
          el('td', { class: p.restarts ? 'fail-text' : '' }, String(p.restarts)),
          el('td', {}, usage(p.cpu_m, p.cpu_limit_m, 'm')),
          el('td', {}, usage(p.memory_mib, p.memory_limit_mib, 'MiB')))))
        : el('p', { class: 'muted' }, 'No pods running.'))));
}

function tablesSection() {
  const all = facts.tables || [];
  const rows = all.filter((t) => !onlyProblems || (t.problems || []).length);
  // An unpartitioned table reads as one partition; only more is worth saying.
  const files = (l) => [`${l.active_files} file${l.active_files === 1 ? '' : 's'}`, l.total_bytes ? size(l.total_bytes) : '', l.partitions > 1 ? `${l.partitions} partitions` : ''].filter(Boolean).join(', ');
  return el('section', { class: 'card' }, el('h2', {}, 'Tables'),
    rows.length ? table(['Table', 'Rows', 'Last write', 'Files', ''],
      rows.map((t) => { const l = t.layout || {}; const p = t.problems || [];
        return el('tr', {},
          el('td', {}, el('div', { style: 'font-weight:500' }, `${t.name} v${t.version}`), el('div', { class: 'muted xs' }, t.kind + (t.reject_pct != null ? ` · ${t.reject_pct}% rejected` : '')),
            p.map((m) => el('div', { class: 'fail-text xs' }, m))),
          el('td', {}, t.rows == null ? el('span', { class: 'muted' }, 'n/a') : t.rows.toLocaleString()),
          el('td', {}, t.last_write ? `${ago(t.last_write)}${t.max_age_hours ? ` · target ${t.max_age_hours} h` : ''}` : el('span', { class: 'muted' }, 'never')),
          el('td', {}, l.active_files != null ? files(l) : el('span', { class: 'muted' }, 'n/a')),
          el('td', { style: 'text-align:right' }, p.length ? badge('fail', 'problem') : badge('success', 'healthy'))); }))
      : el('p', { class: 'muted' }, onlyProblems ? 'No table has a problem.' : 'No tables yet.'));
}

const size = (b) => b >= 1 << 30 ? `${(b / (1 << 30)).toFixed(1)} GB` : b >= 1 << 20 ? `${Math.round(b / (1 << 20))} MB` : `${Math.max(1, Math.round(b / 1024))} KB`;

// What one input or output took in over the window: rows and bytes from a table's commits, or files.
function written(d) {
  if (d.kind === 'data source') return el('span', { class: 'muted' }, 'not measured');
  if (d.missing) return el('span', { class: 'fail-text' }, 'missing');
  if (d.kind === 'files') return d.files == null ? el('span', { class: 'muted' }, 'n/a') : `${d.files} file${d.files === 1 ? '' : 's'}${d.bytes ? `, ${size(d.bytes)}` : ''}`;
  if (d.commits == null) return el('span', { class: 'muted' }, 'n/a');
  return `${(d.rows || 0).toLocaleString()} rows${d.bytes ? `, ${size(d.bytes)}` : ''} in ${d.commits} commit${d.commits === 1 ? '' : 's'}`;
}

// Nulls per column, worst first, and the hours or days with no rows.
function missingData(o) {
  if (o.columns_error) return el('span', { class: 'muted xs' }, `not checked: ${o.columns_error}`);
  if (o.nulls == null) return el('span', { class: 'muted' }, o.kind === 'feature group' ? 'not checked' : '');
  const worst = Object.entries(o.nulls).sort((a, b) => b[1] - a[1]);
  const scope = o.checked === 'window' ? `${(o.checked_rows || 0).toLocaleString()} rows in the window` : `the whole table (${(o.checked_rows || 0).toLocaleString()} rows; no event time)`;
  return el('div', {},
    worst.length ? worst.slice(0, 4).map(([c, pct]) => el('div', { class: pct >= 100 ? 'fail-text xs' : 'xs' }, `${c}: ${pct}% null`)) : el('div', { class: 'xs' }, 'no nulls'),
    worst.length > 4 ? el('div', { class: 'muted xs' }, `and ${worst.length - 4} more columns with nulls`) : '',
    (o.missing || []).length ? el('div', { class: 'fail-text xs' }, `${o.missing.length} empty period${o.missing.length === 1 ? '' : 's'}`) : '',
    el('div', { class: 'muted xs' }, `over ${scope}`));
}

function pipelinesSection() {
  const all = facts.pipelines || [];
  const rows = all.filter((p) => !onlyProblems || p.problems.length);
  if (!rows.length) return [el('section', { class: 'card' }, el('h2', {}, `Data, last ${facts.hours} hours`), el('p', { class: 'muted' }, 'No pipeline has a problem.'))];
  return rows.map((p) => {
    const ratio = p.rows_in && p.rows_out ? ` · ${(p.rows_out / p.rows_in).toFixed(2)} rows out per row in` : '';
    const flow = p.rows_in || p.rows_out
      ? `${p.rows_in.toLocaleString()} rows in, ${p.rows_out.toLocaleString()} rows out${ratio}`
      : `${size(p.bytes_in || 0)} in, ${size(p.bytes_out || 0)} out`;
    const line = (d, dir) => { const probs = d.problems || [];
      return el('tr', {},
        el('td', {}, el('div', { style: 'font-weight:500' }, d.name), el('div', { class: 'muted xs' }, `${dir} · ${d.kind}`), probs.map((m) => el('div', { class: 'fail-text xs' }, m))),
        el('td', {}, written(d)),
        el('td', {}, d.last_write ? ago(d.last_write) : el('span', { class: 'muted' }, d.kind === 'data source' ? '' : 'never')),
        el('td', {}, dir === 'out' ? missingData(d) : ''),
        el('td', { style: 'text-align:right' }, dir === 'in' ? '' : probs.length ? badge('fail', 'problem') : badge('success', 'healthy'))); };
    return el('section', { class: 'card' },
      el('h2', {}, `Data, last ${facts.hours} hours: ${p.name}${p.engine ? ` (${p.engine})` : ''}`),
      el('p', { class: 'muted', style: 'margin:0 0 12px' }, flow),
      (p.flow_problems || []).map((m) => el('p', { class: 'fail-text', style: 'margin:0 0 12px' }, m)),
      table(['Data', 'Written in the window', 'Last write', 'Missing data', ''],
        [...p.inputs.map((d) => line(d, 'in')), ...p.outputs.map((d) => line(d, 'out'))]));
  });
}

function draw() {
  const c = facts.counts;
  document.getElementById('root').replaceChildren(
    el('div', { class: 'head' },
      el('div', { class: 'grow' }, el('h1', {}, facts.system.name), el('div', { class: 'muted xs' }, `${facts.system.type ?? ''} system ${facts.system.slug} · report of ${new Date(facts.generated).toLocaleString()}`)),
      el('label', { class: 'filter' }, el('input', { type: 'checkbox', ...(onlyProblems ? { checked: '' } : {}), onchange: (e) => { onlyProblems = e.target.checked; draw(); } }), 'Only problems'),
      badge(facts.overall, facts.overall)),
    el('div', { class: 'cards' },
      (facts.tables ? [[c.runs, `job runs in ${facts.hours} h`], [c.failed_runs, 'failed runs'], [c.tables, 'tables'], [c.table_problems, 'tables with problems']]
        : (facts.pipelines || []).length && !facts.services.length ? [[c.runs, `job runs in ${facts.hours} h`], [c.failed_runs, 'failed runs'],
          [facts.pipelines.reduce((n, p) => n + p.rows_out, 0).toLocaleString(), `rows written in ${facts.hours} h`], [c.pipeline_problems, 'pipelines with problems']]
        : [[c.runs, `job runs in ${facts.hours} h`], [c.failed_runs, 'failed runs'], [c.services, 'deployments and apps'], [c.unhealthy_services, 'unhealthy']]).map(([v, l], i) =>
        el('div', { class: 'card stat' }, el('div', { class: `v ${(i === 1 || i === 3) && v && v !== '0' ? 'fail-text' : ''}` }, String(v)), el('div', { class: 'l' }, l)))),
    summary ? (() => { const s = el('section', { class: 'card summary' }, el('h2', {}, 'Summary')); const d = el('div'); d.innerHTML = summary; s.append(d); return s; })() : '',
    jobsSection(), ...((facts.pipelines || []).length ? pipelinesSection() : []),
    facts.tables ? tablesSection() : facts.services.length || !(facts.pipelines || []).length ? servicesSection() : '');
}
draw();
</script>
</body>
</html>
"""
