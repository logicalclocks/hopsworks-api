"""`hops mlsystem status`: the facts of a system's health and the page that shows them."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from hopsworks.cli import health


SPEC = {
    "system": {"name": "Churn next month"},
    "requirements": {"system_type": "batch"},
    "features": {"pipelines": [{"job": {"name": "churn-example-features"}}]},
    "inference": {"realtime": {"deployment": "churnpredictor"}},
    "app": {"name": "churn-example-app"},
}


def _project(tmp_path):
    now = datetime.now(timezone.utc)
    log = tmp_path / "stderr.log"
    log.write_text("\n".join(f"line {i}" for i in range(100)) + "\nMemoryError\n")

    def execution(i, status, hours_ago):
        return SimpleNamespace(
            id=i,
            state="FINISHED" if status == "SUCCEEDED" else "FAILED",
            final_status=status,
            submission_time=(now - timedelta(hours=hours_ago)).isoformat(),
            duration=90_000,
            download_logs=lambda: (None, str(log)),
        )

    job = SimpleNamespace(
        job_type="PYTHON",
        job_schedule=SimpleNamespace(enabled=True),
        get_executions=lambda: [
            execution(1, "SUCCEEDED", 30),  # outside the window
            execution(2, "SUCCEEDED", 5),
            execution(3, "FAILED", 1),
        ],
    )
    deployment = SimpleNamespace(get_state=lambda: SimpleNamespace(status="Running"))
    return SimpleNamespace(
        name="churnfresh",
        get_job_api=lambda: SimpleNamespace(get_job=lambda name: job),
        get_model_serving=lambda: SimpleNamespace(
            get_deployment=lambda name: deployment
        ),
        get_app_api=lambda: SimpleNamespace(
            get_app=lambda name: SimpleNamespace(state="RUNNING", app_url="/app")
        ),
    )


def test_the_facts_hold_the_last_day_of_runs_and_the_pods(tmp_path, monkeypatch):
    pods = {
        "items": [
            {
                "metadata": {"name": "churnpredictor-predictor-00001-abc"},
                "spec": {
                    "containers": [
                        {"resources": {"limits": {"cpu": "2", "memory": "1Gi"}}}
                    ]
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {
                            "name": "kserve-container",
                            "ready": True,
                            "restartCount": 2,
                            "lastState": {
                                "terminated": {"reason": "OOMKilled", "exitCode": 137}
                            },
                        }
                    ],
                },
            },
            {"metadata": {"name": "someone-else"}, "spec": {}, "status": {}},
        ]
    }

    def kubectl(*args):
        if args[:2] == ("get", "pods"):
            return json.dumps(pods)
        return "churnpredictor-predictor-00001-abc kserve-container 250m 900Mi\n"

    monkeypatch.setattr(health, "_kubectl", kubectl)
    facts = health.collect(_project(tmp_path), SPEC, "churn-example", hours=24)
    [job] = facts["jobs"]
    assert [r["id"] for r in job["runs"]] == [3, 2]
    assert job["runs"][0]["log_tail"].endswith("MemoryError")
    assert len(job["runs"][0]["log_tail"].splitlines()) == health.LOG_TAIL_LINES
    deployment = next(s for s in facts["services"] if s["kind"] == "deployment")
    [pod] = deployment["pods"]
    assert pod["cpu_m"] == 250 and pod["memory_mib"] == 900
    assert pod["cpu_limit_m"] == 2000 and pod["memory_limit_mib"] == 1024
    assert pod["last_terminations"][0]["reason"] == "OOMKilled"
    assert facts["overall"] == "failing"
    assert facts["counts"] == {
        "jobs": 1,
        "runs": 2,
        "failed_runs": 1,
        "services": 2,
        "unhealthy_services": 1,
    }


def test_the_page_carries_the_facts_and_cannot_be_broken_out_of(tmp_path, monkeypatch):
    monkeypatch.setattr(health, "_kubectl", lambda *a: "")
    facts = health.collect(_project(tmp_path), SPEC, "churn-example")
    facts["jobs"][0]["runs"][0]["log_tail"] = "</script><script>alert(1)</script>"
    page = health.render(facts, "<p>One run failed.</p>")
    assert "<title>Churn next month status</title>" in page
    assert "</script><script>alert(1)" not in page
    assert '"failed_runs": 1' in page and "One run failed." in page


def test_the_summary_keeps_only_the_tags_it_asked_for(monkeypatch):
    monkeypatch.setattr(health.shutil, "which", lambda name: "/usr/bin/claude")
    reply = "```html\n<p>All <strong>good</strong>.</p><script>x()</script><img src=x onerror=y>\n```"
    monkeypatch.setattr(
        health.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout=reply),
    )
    summary = health.summarize({"counts": {}})
    assert summary.startswith("<p>All <strong>good</strong>.</p>")
    assert "<script>" not in summary and "<img" not in summary
