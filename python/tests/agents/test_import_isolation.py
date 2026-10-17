"""The serving package must not reach the eval package.

``hopsworks_agents.protocol`` runs synchronously inside every agent request.
``hopsworks_agents.eval`` runs in a job and pulls in judges, detectors, and
dataframe work. Nothing in the second belongs on the first's path, and an
import is how it would get there without anyone deciding to put it there.

The pressure to break this is real and already named in the design: guardrails
and trace-to-eval promotion are meant to share PII/secret detectors. If those
detectors come to live on the eval side, the serving package has to import
them, and the rule inverts. When that day comes the detectors go in the shared
module -- this test is what makes the mistake loud instead of silent.

Run in a subprocess so an import in some other test cannot mask the result.
"""

import subprocess
import sys


def _modules_after_importing(package: str) -> set[str]:
    script = (
        f"import {package}, sys, json;"
        "print(json.dumps(sorted(m for m in sys.modules if 'hopsworks' in m)))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    import json

    return set(json.loads(result.stdout))


def test_serving_package_does_not_pull_in_eval_code():
    loaded = _modules_after_importing("hopsworks_agents.protocol")
    leaked = {m for m in loaded if m.startswith("hopsworks_agents.eval")}
    assert not leaked, (
        "hopsworks_agents.protocol imported eval modules: "
        f"{sorted(leaked)}. Eval code must never be reachable from the "
        "serving path."
    )


def test_importing_agentapp_does_not_pull_in_eval_code():
    # the realistic entry point: an agent constructs AgentApp, not the package
    script = (
        "from hopsworks_agents.protocol import AgentApp;"
        "import sys, json;"
        "print(json.dumps([m for m in sys.modules "
        "if m.startswith('hopsworks_agents.eval')]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    import json

    assert json.loads(result.stdout) == []


def test_eval_package_may_import_the_serving_package():
    # the allowed direction, asserted so that removing the shared conventions
    # module is a deliberate act rather than an accident
    loaded = _modules_after_importing("hopsworks_agents.eval")
    assert "hopsworks_agents.protocol.conventions" in loaded
