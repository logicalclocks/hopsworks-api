"""Run an agent deployment's entry script and serve its AgentApp.

A deployment starts ``main([script, *args])`` through a ``python -c`` bootstrap
that falls back to plain ``runpy.run_path`` where this module is missing (an
environment with an older client). The script runs as ``__main__``, as under
``python <script>``: one that serves in its main block never returns. One that
returns has its single module-level ``AgentApp`` served on port 8080, so a
script written for ``uvicorn my_agent:agent_app`` works deployed too. With no
app or several, the run exits 1 and says why, so the pod log names the cause.

Only the standard library is imported until there is an app to serve: this
module sits on every agent pod's start.
"""

from __future__ import annotations

import os
import runpy
import sys


PORT = 8080


def main(argv: list[str]) -> None:
    if not argv:
        sys.exit("usage: python -m hopsworks_agents.run <script> [args...]")
    script = argv[0]
    sys.argv = list(argv)
    # What `python <script>` puts first on the path, so the script's own imports resolve.
    sys.path[0] = os.path.dirname(os.path.abspath(script))
    namespace = runpy.run_path(script, run_name="__main__")

    # Not imported by the script means no AgentApp was built.
    app_module = sys.modules.get("hopsworks_agents.protocol.app")
    apps = []
    if app_module is not None:
        apps = [
            (name, value)
            for name, value in namespace.items()
            if isinstance(value, app_module.AgentApp)
        ]
    if not apps:
        sys.exit(
            f"{script} returned without serving and defines no AgentApp at module "
            f"level: serve on port {PORT} in its main block, or define one AgentApp."
        )
    if len(apps) > 1:
        names = ", ".join(name for name, _ in apps)
        sys.exit(
            f"{script} returned without serving and defines {len(apps)} AgentApps "
            f"({names}): serve the one to run on port {PORT} in its main block."
        )

    import uvicorn

    uvicorn.run(apps[0][1], host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main(sys.argv[1:])
