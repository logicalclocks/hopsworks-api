# ruff: noqa: INP001
"""Print the Trino connection for this app as shell exports, for `eval`.

hopsworks.login() inside the cluster writes the cluster CA to /tmp/ca_chain.pem.
"""

import contextlib
import os
import shlex
import sys

import hopsworks


# stdout is eval'd, so the login banner goes to stderr.
with contextlib.redirect_stdout(sys.stderr):
    project = hopsworks.login()
trino = project.get_trino_api()
user, password = trino.get_basic_auth()
env = {
    "TRINO_SERVER": f"https://{trino.get_host()}:{trino.get_port()}",
    "TRINO_USER": user,
    "TRINO_PASSWORD": password,
    "TRINO_CA": "/tmp/ca_chain.pem",
    "TRINO_SCHEMA": f"{project.name.lower()}_featurestore",
}
if not os.path.exists(env["TRINO_CA"]):
    raise SystemExit("trino_env.py: no cluster CA at /tmp/ca_chain.pem")
for name, value in env.items():
    print(f"export {name}={shlex.quote(value)}")
