"""Local HTTPS servers that the ``hops deployment`` tests reach through the real SDK and client.

- ``ServingBackend``: the parts of the Hopsworks REST API that login and the
  deployment verbs call (versions, project info, serving reads, save, rollback,
  delete, predict), for any number of projects, kept in memory.
- ``CutProxy``: a TLS proxy in front of a server that forwards every request
  and, for the requests a test names, cuts the client off: it drops the
  connection after forwarding the body (optionally forwarding it only later)
  or holds the connection open and never answers.

Every byte goes over a real socket on 127.0.0.1, so the client's timeouts,
deadlines and transport errors are the real ones.
"""

from __future__ import annotations

import contextlib
import copy
import datetime
import http.client
import ipaddress
import json
import socket
import ssl
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
API = "/hopsworks-api/api/"


def tls_context(directory: Path) -> ssl.SSLContext:
    """A server TLS context with a fresh self-signed certificate for 127.0.0.1."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = directory / "cert.pem", directory / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    return context


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, handler, context: ssl.SSLContext):
        super().__init__(("127.0.0.1", 0), handler)
        self.socket = context.wrap_socket(self.socket, server_side=True)
        threading.Thread(
            target=self.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        ).start()

    @property
    def url(self) -> str:
        return f"https://127.0.0.1:{self.server_port}"

    def stop(self) -> None:
        self.shutdown()
        self.server_close()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send(self, status: int, payload) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Deployment:
    """One deployment: its versions, the active one, and its rollout state."""

    def __init__(self, deployment_id: int, name: str, env: dict[str, str]):
        self.id = deployment_id
        self.name = name
        self.versions = [{"version": 1, "env": dict(env)}]
        self.active = 1
        self.status = "Running"
        self.revision = 1

    def roll(self, version: int) -> None:
        self.active = version
        if self.status != "Stopped":
            self.revision += 1

    def env(self) -> dict[str, str]:
        return next(v["env"] for v in self.versions if v["version"] == self.active)

    def json(self) -> dict:
        body = copy.deepcopy(
            json.loads((_FIXTURES / "predictor_fixtures.json").read_text())[
                "get_deployment_py_kserve_rest_pred"
            ]["response"]
        )
        running = self.status == "Running"
        body.update(
            id=self.id,
            name=self.name,
            version=self.active,
            predictor="predictor.py",
            project_namespace="ns",
            predictor_env_vars=[f"{k}={v}" for k, v in self.env().items()],
            status=self.status,
            revision=f"{self.revision:08x}",
            available_instances=1 if running else 0,
            condition={"type": "READY", "status": running, "reason": "probe failed"},
        )
        return body

    def versions_page(self, offset: int, limit: int) -> dict:
        rows = [
            {
                "version": v["version"],
                "active": v["version"] == self.active,
                "created": "2026-11-20T10:00:00Z",
                "createdBy": "hopsabl2",
                "predictorEnvVars": [f"{k}={x}" for k, x in v["env"].items()],
            }
            for v in sorted(self.versions, key=lambda v: -v["version"])
        ]
        return {"count": len(rows), "items": rows[offset : offset + limit]}


class ServingBackend(_Server):
    """The Hopsworks serving API of some projects, in memory.

    ``projects`` maps a project id to its name and deployments. A key with
    only SERVING is modelled by ``project_readable = False``: reading a
    project by name is then refused with the scope error, as Hopsworks does.
    ``reject_writes`` answers a save or rollback with that (status, error
    code, message) and changes nothing.
    """

    def __init__(self, context: ssl.SSLContext):
        self.lock = threading.Lock()
        self.projects: dict[int, dict] = {}
        self.project_readable = True
        self.reject_writes: tuple[int, int, str] | None = None
        self.rollout_status = "Running"
        self.requests: list[tuple[str, str]] = []
        super().__init__(_BackendHandler, context)

    def add(self, project_id: int, project: str, deployment: Deployment) -> None:
        self.projects.setdefault(project_id, {"name": project, "deployments": {}})
        self.projects[project_id]["deployments"][deployment.name] = deployment

    def deployment(self, project_id: int, name: str) -> Deployment:
        return self.projects[project_id]["deployments"][name]

    def writes(self) -> list[tuple[str, str]]:
        return [(m, p) for m, p in self.requests if m != "GET"]


_NOT_FOUND = (404, {"errorCode": 240000, "usrMsg": "Deployment not found"})


class _BackendHandler(_Handler):
    server: ServingBackend

    def _route(self, method: str) -> None:
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        parts = url.path[len(API) :].split("/") if url.path.startswith(API) else []
        body = self._body()
        backend = self.server
        with backend.lock:
            backend.requests.append((method, url.path))
            status, payload = self._answer(method, parts, query, body)
        self._send(status, payload)

    def _answer(self, method, parts, query, body):
        backend = self.server
        if parts == ["variables", "versions"]:
            from hopsworks_common import version

            return 200, [{"software": "hopsworks", "version": version.__version__}]
        if parts[:1] == ["variables"]:
            return 200, {"successMessage": "false"}
        if parts[:2] == ["project", "getProjectInfo"]:
            if not backend.project_readable:
                return 403, {
                    "errorCode": 320004,
                    "usrMsg": "No valid scope found for this invocation. Allowed scopes: [PROJECT]",
                }
            for pid, project in backend.projects.items():
                if project["name"] == parts[2]:
                    return 200, {
                        "projectId": pid,
                        "projectName": parts[2],
                        "namespace": "ns",
                    }
            return 404, {"errorCode": 150004, "usrMsg": "Project not found"}
        if len(parts) < 3 or parts[0] != "project" or not parts[1].isdigit():
            return 404, {"errorCode": 100000, "usrMsg": f"no route {parts}"}
        project = backend.projects.get(int(parts[1]))
        if project is None:
            return 404, {"errorCode": 150004, "usrMsg": "Project not found"}
        deployments = project["deployments"]
        rest = parts[2:]
        if rest[:2] == ["inference", "models"]:
            return 200, {"predictions": json.loads(body or b"{}").get("instances")}
        if rest != ["serving"] and rest[:1] == ["serving"] and len(rest) >= 2:
            d = next((x for x in deployments.values() if str(x.id) == rest[1]), None)
            if d is None:
                return _NOT_FOUND
            tail = rest[2:]
            if method == "GET" and not tail:
                return 200, d.json()
            if method == "GET" and tail == ["versions"]:
                return 200, d.versions_page(
                    int(query.get("offset", ["0"])[0]),
                    int(query.get("limit", ["100"])[0]),
                )
            if method == "DELETE" and not tail:
                del deployments[d.name]
                return 200, None
            if method == "POST" and tail == ["rollback"]:
                if backend.reject_writes is not None:
                    return self._rejected()
                d.roll(json.loads(body)["version"])
                d.status = backend.rollout_status if d.status != "Stopped" else d.status
                return 200, d.json()
        if rest == ["serving"] and method == "GET":
            d = deployments.get(query.get("name", [""])[0])
            return (200, d.json()) if d is not None else _NOT_FOUND
        if rest == ["serving"] and method == "PUT":
            if backend.reject_writes is not None:
                return self._rejected()
            sent = json.loads(body)
            d = next(x for x in deployments.values() if x.id == sent["id"])
            env = dict(e.split("=", 1) for e in sent.get("predictorEnvVars") or [])
            number = max(v["version"] for v in d.versions) + 1
            d.versions.append({"version": number, "env": env})
            d.roll(number)
            d.status = backend.rollout_status if d.status != "Stopped" else d.status
            return 200, d.json()
        return 404, {"errorCode": 100000, "usrMsg": f"no route {method} {parts}"}

    def _rejected(self):
        status, code, message = self.server.reject_writes
        return status, {"errorCode": code, "usrMsg": message}

    def do_GET(self):
        self._route("GET")

    def do_PUT(self):
        self._route("PUT")

    def do_POST(self):
        self._route("POST")

    def do_DELETE(self):
        self._route("DELETE")


class CutProxy(_Server):
    """A TLS proxy that forwards to ``upstream`` and cuts off the requests ``cut`` picks.

    ``cut(method, path)`` returns None to forward and relay the answer, or:
    - ``"drop"``: forward the whole request, close the client's connection
      without an answer, and let the upstream finish it;
    - ``("delay", seconds)``: close the client's connection at once, and
      forward the request only ``seconds`` later;
    - ``("delay", seconds, status)``: the same, but answer the client with
      an HTML error page of that status first, as a gateway that gave up does;
    - ``"stall"``: keep the client's connection open and never answer;
    - ``"trickle"``: forward, then send the answer's status line, headers and
      first 8 body bytes at once and the rest one byte a second;
    - ``("trickle", seconds)``: the same with a 200 answer the proxy makes
      up, forwarding the request only ``seconds`` later.
    ``cuts`` lists every (method, path, action) it applied; ``forwarded`` is
    set once a delayed request reached the upstream.
    """

    def __init__(self, context: ssl.SSLContext, upstream: str, cut):
        self.upstream = urllib.parse.urlsplit(upstream)
        self.cut = cut
        self.cuts: list[tuple[str, str, object]] = []
        self.forwarded = threading.Event()
        self.release = threading.Event()
        super().__init__(_ProxyHandler, context)

    def stop(self) -> None:
        self.release.set()
        super().stop()

    def send_upstream(self, method: str, path: str, headers, body: bytes):
        connection = http.client.HTTPSConnection(
            self.upstream.hostname,
            self.upstream.port,
            context=ssl._create_unverified_context(),
            timeout=30,
        )
        forward = {
            k: v for k, v in headers.items() if k.lower() not in ("host", "connection")
        }
        connection.request(method, path, body=body or None, headers=forward)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, response.getheaders(), data


class _ProxyHandler(_Handler):
    server: CutProxy

    def _relay(self, method: str) -> None:
        proxy = self.server
        body = self._body()
        path = urllib.parse.urlsplit(self.path).path
        action = proxy.cut(method, path)
        if action is not None:
            proxy.cuts.append((method, path, action))
        if action == "stall":
            proxy.release.wait()
            self.close_connection = True
            return
        if action == "drop":
            proxy.send_upstream(method, self.path, self.headers, body)
            self._cut_off()
            return
        if isinstance(action, tuple) and action[0] == "delay":
            seconds, headers, target = action[1], dict(self.headers), self.path
            status = action[2] if len(action) > 2 else None

            def later():
                time.sleep(seconds)
                proxy.send_upstream(method, target, headers, body)
                proxy.forwarded.set()

            threading.Thread(target=later, daemon=True).start()
            if status is None:
                self._cut_off()
                return
            page = b"<html><body>Bad Gateway</body></html>"
            self.send_response(status)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        if action == "trickle" or (
            isinstance(action, tuple) and action[0] == "trickle"
        ):
            self._trickle(method, body, action)
            return
        status, headers, data = proxy.send_upstream(
            method, self.path, self.headers, body
        )
        self.send_response(status)
        for name, value in headers:
            if name.lower() not in (
                "connection",
                "transfer-encoding",
                "content-length",
            ):
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _trickle(self, method: str, body: bytes, action) -> None:
        """Answer with a body that starts at once and then arrives one byte a second, until the client hangs up."""
        proxy = self.server
        if action == "trickle":
            status, _, data = proxy.send_upstream(method, self.path, self.headers, body)
        else:
            status, data = 200, b'{"id": 7, "status": "Running"}' + b" " * 4096
            seconds, headers, target = action[1], dict(self.headers), self.path

            def later():
                time.sleep(seconds)
                proxy.send_upstream(method, target, headers, body)
                proxy.forwarded.set()

            threading.Thread(target=later, daemon=True).start()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data[:8])
        self.wfile.flush()
        self.close_connection = True
        for i in range(8, len(data)):
            if proxy.release.wait(1):
                return
            try:
                self.wfile.write(data[i : i + 1])
                self.wfile.flush()
            except OSError:
                return

    def _cut_off(self) -> None:
        """Close the client's connection with no answer, as a reset proxy or a dropped link does."""
        self.close_connection = True
        with contextlib.suppress(OSError):
            self.connection.shutdown(socket.SHUT_RDWR)

    def do_GET(self):
        self._relay("GET")

    def do_PUT(self):
        self._relay("PUT")

    def do_POST(self):
        self._relay("POST")

    def do_DELETE(self):
        self._relay("DELETE")
