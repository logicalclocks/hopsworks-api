# ruff: noqa: INP001
"""Write this app's Trino connection to a file only its user can read.

The Node server reads the file (TRINO_CONNECTION, default /tmp/trino.json), so
the password is never printed, kept in shell history, or exported to every
process the app starts. hopsworks.login() inside the cluster writes the
cluster CA to /tmp/ca_chain.pem.
"""

import json
import os

import hopsworks


def _main():
    project = hopsworks.login()
    trino = project.get_trino_api()
    user, password = trino.get_basic_auth()
    connection = {
        "server": f"https://{trino.get_host()}:{trino.get_port()}",
        "user": user,
        "password": password,
        "ca": "/tmp/ca_chain.pem",
        "schema": f"{project.name.lower()}_featurestore",
    }
    if not os.path.exists(connection["ca"]):
        raise SystemExit("trino_env.py: no cluster CA at /tmp/ca_chain.pem")
    path = os.environ.get("TRINO_CONNECTION", "/tmp/trino.json")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(connection, f)
    print(f"trino_env.py: connection for {user} written to {path}")


if __name__ == "__main__":
    _main()
