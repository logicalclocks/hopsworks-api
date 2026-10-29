# ruff: noqa: INP001
"""Write this app's Trino connection to a file only its user can read.

The Node server reads the file (TRINO_CONNECTION, default /tmp/trino.json), so
the password is never printed, kept in shell history, or exported to every
process the app starts. The CA is the cluster's, where the SDK keeps it after
login: /tmp/ca_chain.pem in a pod, the session folder outside the cluster.
"""

import json
import os
import tempfile

import hopsworks


def _main():
    project = hopsworks.login()
    trino = project.get_trino_api()
    user, password = trino.get_basic_auth()
    connection = {
        "server": f"https://{trino.get_host()}:{trino.get_port()}",
        "user": user,
        "password": password,
        "ca": trino._get_ca_chain_path(True),
        # Off only where the client turns it off: outside the cluster Trino answers
        # on an IP its certificate does not name.
        "verifyHostname": os.environ.get("HOPSWORKS_HOSTNAME_VERIFICATION", "true").lower() != "false",
        "schema": f"{project.name.lower()}_featurestore",
    }
    if not os.path.exists(connection["ca"]):
        raise SystemExit(f"trino_env.py: no cluster CA at {connection['ca']}")
    path = os.environ.get("TRINO_CONNECTION", "/tmp/trino.json")
    # A new file, created 0600 and renamed over the old one: an existing file
    # keeps its mode under O_TRUNC, and the rename replaces a symlink rather
    # than writing through it.
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", prefix=".trino-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(connection, f)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
    print(f"trino_env.py: connection for {user} written to {path}")


if __name__ == "__main__":
    _main()
