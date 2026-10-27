"""Waits read the reasons in the KubeMonitorDTO bodies the backend sends; only HTTP is fake."""

from __future__ import annotations

from hopsworks.cli import waits

from tests.cli.conftest import PENDING_REASON, backend


PENDING = "Server is in pending state - waiting to be scheduled"


def test_an_app_names_why_its_pod_is_not_scheduled():
    assert waits.kube_reason(backend("app_pending")["kubeMonitor"]) == PENDING_REASON
    assert waits.kube_reason(backend("app_running")["kubeMonitor"]) is None


def test_an_app_names_its_failing_readiness_probe():
    app = backend("app_running")
    probe = {
        "type": "Warning",
        "reason": "Unhealthy",
        "age": "5s",
        "from": "kubelet",
        "message": "Readiness probe failed: HTTP probe failed with statuscode: 503",
    }
    pod = {
        "name": "pythonapp-blah--hopsable-test-echo-67cb65cbd9-q4m8z",
        "status": "Running",
        "ready": False,
        "events": {"items": [probe]},
    }
    app["kubeMonitor"]["deployment"]["pods"] = {"items": [pod], "count": 1}
    assert waits.kube_reason(app["kubeMonitor"]) == (
        "Unhealthy: Readiness probe failed: HTTP probe failed with statuscode: 503"
    )
