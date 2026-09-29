"""The health report of a built ML system, for ``hops mlsystem status``.

`collect` gathers the facts: every job the system created (from the same
inventory ``hops mlsystem delete`` reads in system.yaml) with its executions in
the last hours and the log tail of each failure, and every deployment and app
with its state and its pods (phase, readiness, restarts, the last termination
reason, CPU and memory used against the limits, from kubectl in the project
namespace). `summarize` asks Claude for a short account of what failed and
why; `render` writes one self-contained HTML page with the facts, the summary
and a little JavaScript, which Brewer shows next to the architecture.
"""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
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
                stdout, stderr = execution.download_logs()
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
    """The facts of the report: jobs over the last `hours`, deployments and apps with their pods."""
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
        if failed and unhealthy
        else "degraded"
        if failed or unhealthy
        else "healthy",
        "counts": {
            "jobs": len(jobs),
            "runs": sum(len(j["runs"]) for j in jobs),
            "failed_runs": failed,
            "services": len(services),
            "unhealthy_services": len(unhealthy),
        },
        "jobs": jobs,
        "services": services,
    }


def summarize(facts: dict, timeout: int = 180) -> str | None:
    """Claude's short account of what failed and why, as HTML paragraphs and lists, or None."""
    if not shutil.which("claude"):
        return None
    prompt = (
        "You are writing the summary section of a health report for an ML system on Hopsworks. "
        "From the JSON facts below, write at most 150 words as an HTML fragment using only <p>, <ul>, "
        "<li>, <strong> and <code>: first one sentence on the overall health, then, for each failed "
        "job run or unhealthy deployment or app, what failed and the likely cause from its log tail "
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
<title>__TITLE__</title>
<style>
:root { --accent:#1eb182; --bg:#f5f7f9; --panel:#fff; --text:#1a1f2b; --muted:#5f6b7a; --line:#e3e8ee;
  --ok:#1eb182; --warn:#e0a100; --bad:#d6455d; --radius:12px; color-scheme: light; }
@media (prefers-color-scheme: dark) { :root { --bg:#0e1117; --panel:#161b25; --text:#eef1f5; --muted:#9aa5b4; --line:#262d3a; color-scheme: dark; } }
* { box-sizing: border-box; }
body { margin:0; padding:20px; font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; background:var(--bg); color:var(--text); }
h1 { font-size:20px; margin:0; } h2 { font-size:15px; margin:0 0 10px; }
.head { display:flex; flex-wrap:wrap; align-items:center; gap:12px; margin-bottom:16px; }
.muted { color:var(--muted); } .grow { flex:1; }
.pill { display:inline-block; padding:2px 10px; border-radius:999px; font-weight:600; font-size:12px; color:#fff; }
.healthy { background:var(--ok); } .degraded { background:var(--warn); } .failing { background:var(--bad); }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:16px; }
.card { background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); padding:14px; }
.stat .v { font-size:24px; font-weight:700; } .stat .l { color:var(--muted); font-size:12px; }
section.card { margin-bottom:16px; }
table { width:100%; border-collapse:collapse; } th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }
th { color:var(--muted); font-weight:600; font-size:12px; }
.dots { display:flex; gap:3px; flex-wrap:wrap; } .dot { width:10px; height:10px; border-radius:2px; background:var(--ok); }
.dot.FAILED, .dot.KILLED { background:var(--bad); } .dot.running { background:var(--warn); }
.bar { height:8px; background:var(--line); border-radius:4px; overflow:hidden; min-width:80px; }
.bar > div { height:100%; background:var(--accent); } .bar.hot > div { background:var(--warn); } .bar.over > div { background:var(--bad); }
button.link { background:none; border:none; color:var(--accent); cursor:pointer; padding:0; font:inherit; }
pre { background:var(--bg); border:1px solid var(--line); border-radius:8px; padding:8px; max-height:260px; overflow:auto; font-size:12px; white-space:pre-wrap; }
.summary p { margin:0 0 8px; } .bad { color:var(--bad); font-weight:600; } .ok { color:var(--ok); font-weight:600; }
label.filter { font-size:12px; color:var(--muted); cursor:pointer; }
</style>
</head>
<body>
<div id="root"></div>
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
let onlyProblems = false;

function usage(used, limit, unit) {
  if (used == null) return el('span', { class: 'muted' }, 'no metrics');
  const pct = limit ? Math.min(100, (used / limit) * 100) : 0;
  const bar = el('div', { class: `bar ${pct > 90 ? 'over' : pct > 70 ? 'hot' : ''}` }, el('div', { style: `width:${pct}%` }));
  return el('div', {}, bar, el('span', { class: 'muted' }, `${Math.round(used)} ${unit}${limit ? ` of ${Math.round(limit)}` : ''}`));
}

function jobsSection() {
  const rows = facts.jobs.filter((j) => !onlyProblems || j.runs.some(failedRun) || j.missing || j.error);
  const body = el('tbody');
  for (const j of rows) {
    const failed = j.runs.filter(failedRun);
    const last = j.runs[0];
    const detail = el('tr', { style: 'display:none' }, el('td', { colspan: 5 },
      failed.map((r) => el('div', {}, el('div', { class: 'muted' }, `execution ${r.id}, ${r.final_status}, ${ago(r.submitted)}`), el('pre', {}, r.log_tail || '(no log)')))));
    body.append(el('tr', {},
      el('td', {}, el('strong', {}, j.name), el('div', { class: 'muted' }, j.missing ? 'missing' : `${j.type ?? ''}${j.scheduled ? ' · scheduled' : ''}`)),
      el('td', {}, el('div', { class: 'dots' }, j.runs.slice().reverse().map((r) => el('span', { class: `dot ${r.final_status} ${r.final_status === 'UNDEFINED' ? 'running' : ''}`, title: `${r.id} ${r.final_status} ${ago(r.submitted)}` })))),
      el('td', {}, `${j.runs.length - failed.length}/${j.runs.length}`),
      el('td', {}, last ? `${ago(last.submitted)} · ${dur(last.duration_s)}` : el('span', { class: 'muted' }, j.error || 'no runs')),
      el('td', {}, failed.length ? el('button', { class: 'link', onclick: () => { detail.style.display = detail.style.display ? '' : 'none'; } }, `${failed.length} failed ▸`) : el('span', { class: 'ok' }, '✓'))));
    body.append(detail);
  }
  return el('section', { class: 'card' }, el('h2', {}, `Jobs, last ${facts.hours} hours`),
    rows.length ? el('table', {}, el('thead', {}, el('tr', {}, ['Job', 'Runs', 'Succeeded', 'Last run', ''].map((h) => el('th', {}, h)))), body)
      : el('p', { class: 'muted' }, onlyProblems ? 'No job has a failed run.' : 'No jobs.'));
}

function servicesSection() {
  const rows = facts.services.filter((s) => !onlyProblems || s.pods.some((p) => p.restarts || p.phase !== 'Running') || !['RUNNING', 'IDLE'].includes(String(s.state).toUpperCase()));
  if (!rows.length && onlyProblems) return el('section', { class: 'card' }, el('h2', {}, 'Deployments and apps'), el('p', { class: 'muted' }, 'Every deployment and app is healthy.'));
  if (!facts.services.length) return el('section', { class: 'card' }, el('h2', {}, 'Deployments and apps'), el('p', { class: 'muted' }, 'This system has no deployment or app.'));
  return el('section', { class: 'card' }, el('h2', {}, 'Deployments and apps'),
    rows.map((s) => el('div', { style: 'margin-bottom:14px' },
      el('div', {}, el('strong', {}, s.name), ' ', el('span', { class: 'muted' }, `${s.kind} · `),
        el('span', { class: ['RUNNING', 'IDLE'].includes(String(s.state).toUpperCase()) ? 'ok' : 'bad' }, s.state)),
      s.pods.length ? el('table', {}, el('thead', {}, el('tr', {}, ['Pod', 'Ready', 'Restarts', 'CPU (mCPU)', 'Memory (MiB)'].map((h) => el('th', {}, h)))),
        el('tbody', {}, s.pods.map((p) => el('tr', {},
          el('td', {}, p.pod, p.last_terminations.map((t) => el('div', { class: 'bad' }, `${t.container}: ${t.reason} (exit ${t.exit_code})`))),
          el('td', {}, `${p.phase} ${p.ready}`),
          el('td', { class: p.restarts ? 'bad' : '' }, String(p.restarts)),
          el('td', {}, usage(p.cpu_m, p.cpu_limit_m, 'm')),
          el('td', {}, usage(p.memory_mib, p.memory_limit_mib, 'MiB'))))))
        : el('p', { class: 'muted' }, 'No pods running.'))));
}

function draw() {
  const c = facts.counts;
  document.getElementById('root').replaceChildren(
    el('div', { class: 'head' },
      el('div', { class: 'grow' }, el('h1', {}, facts.system.name), el('div', { class: 'muted' }, `${facts.system.type ?? ''} system ${facts.system.slug} · report of ${new Date(facts.generated).toLocaleString()}`)),
      el('label', { class: 'filter' }, el('input', { type: 'checkbox', ...(onlyProblems ? { checked: '' } : {}), onchange: (e) => { onlyProblems = e.target.checked; draw(); } }), ' only problems'),
      el('span', { class: `pill ${facts.overall}` }, facts.overall)),
    el('div', { class: 'cards' },
      [[c.runs, `job runs in ${facts.hours} h`], [c.failed_runs, 'failed runs'], [c.services, 'deployments and apps'], [c.unhealthy_services, 'unhealthy']].map(([v, l], i) =>
        el('div', { class: 'card stat' }, el('div', { class: `v ${(i === 1 || i === 3) && v ? 'bad' : ''}` }, String(v)), el('div', { class: 'l' }, l)))),
    summary ? (() => { const s = el('section', { class: 'card summary' }, el('h2', {}, 'Summary')); const d = el('div'); d.innerHTML = summary; s.append(d); return s; })() : '',
    jobsSection(), servicesSection());
}
draw();
</script>
</body>
</html>
"""
