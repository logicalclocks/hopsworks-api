"""Feature vector reads through the SDK's online store clients.

One user class runs every call shape as a task: single and batch, on the one-group view and the two-group join view, blocking and through the async API.
It does so for each client enabled by the `clients` option: `rest` (RonDB REST server) and `sql` (MySQL).
Locust picks among the tasks for each iteration, so all of them share the run time.
Tasks are named `<client>:<shape>`.
A response whose rows do not match the requested keys is a failure.

Settings come from locust.conf, including the options added below.
"""

import asyncio
import os
import random
import time

import hopsworks
from common import CONFIG, FV_NAME, JOIN_FV_NAME, VERSION, login
from gevent.lock import Semaphore
from locust import User, constant, events
from locust.runners import WorkerRunner
from setup_data import setup


@events.init_command_line_parser.add_listener
def add_options(parser):
    parser.add_argument("--rows", type=int, default=100, help="Rows loaded and read from.")
    parser.add_argument("--batch-size", type=int, default=10, help="Keys per batch read.")
    parser.add_argument(
        "--skip-setup",
        type=lambda value: str(value).lower() in ("1", "true", "yes"),
        default=False,
        help="Reuse the rows already loaded.",
    )
    parser.add_argument(
        "--clients",
        default="rest,sql",
        help="Comma-separated online store clients to read through: rest, sql.",
    )


_login_lock = Semaphore()


def feature_store(environment):
    """The process's feature store, logging in on first use.

    A worker can be handed users before its init listener finishes.
    Users therefore log in through here rather than relying on init having run.
    """
    with _login_lock:
        if getattr(environment, "feature_store", None) is None:
            environment.feature_store = login().get_feature_store()
            print(
                f"hopsworks {hopsworks.__version__} from {hopsworks.__file__}, "
                f"SDK_REF={os.environ.get('SDK_REF')}"
            )
    return environment.feature_store


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    # Workers skip setup.
    # The master or a single process loads the data once, before any user is started.
    options = environment.parsed_options
    clients = [c.strip() for c in options.clients.split(",") if c.strip()]
    unknown = set(clients) - set(CLIENTS)
    if not clients or unknown:
        raise ValueError(f"clients must be a subset of {CLIENTS}, got {options.clients!r}")
    environment.clients = clients
    # Filtered on the class, not per user.
    # Locust computes the report's task ratio from the class, so it then lists only the tasks that run.
    FeatureVectorUser.tasks = [t for t in ALL_TASKS if t.client in clients]
    if not options.skip_setup and not isinstance(environment.runner, WorkerRunner):
        setup(feature_store(environment), options.rows)


@events.quitting.add_listener
def on_locust_quitting(environment, **kwargs):
    hopsworks.logout()


def check_single(vector, entry):
    if vector is None or vector[0] != entry["ip"]:
        raise AssertionError(f"expected ip {entry['ip']}, got {vector}")


def check_batch(vectors, entries):
    got = [vector[0] for vector in vectors]
    expected = [entry["ip"] for entry in entries]
    if got != expected:
        raise AssertionError(f"expected ips {expected}, got {got}")


CLIENTS = ("rest", "sql")
SHAPES = (
    "single",
    "batch",
    "join_single",
    "join_batch",
    "join_async_single",
    "join_async_batch",
)


def read_task(client, shape):
    """A named task for one client and call shape."""

    def run(user):
        user.read(client, shape)

    run.__name__ = f"{client}_{shape}"
    run.client = client
    return run


def rest_config():
    """REST client settings; rdrs_host/rdrs_port pin the server instead of the load balancer."""
    config = {"verify_certs": CONFIG["verify_certs"]}
    if "rdrs_host" in CONFIG:
        config["host"] = CONFIG["rdrs_host"]
        config["port"] = CONFIG["rdrs_port"]
    return config


ALL_TASKS = [read_task(client, shape) for client in CLIENTS for shape in SHAPES]


class FeatureVectorUser(User):
    wait_time = constant(0)
    # Narrowed to the enabled clients by on_locust_init.
    tasks = ALL_TASKS

    def on_start(self):
        options = self.environment.parsed_options
        self.rows = options.rows
        self.batch_size = options.batch_size
        clients = self.environment.clients
        self.fv = self.serving_view(FV_NAME, clients)
        self.join_fv = self.serving_view(JOIN_FV_NAME, clients)
        # One loop per user for the async API, reused across calls.
        # The SQL client keeps a connection pool per loop, so reusing it keeps one pool.
        self.loop = asyncio.new_event_loop()

    def on_stop(self):
        self.loop.close()

    def serving_view(self, name, clients):
        fv = feature_store(self.environment).get_feature_view(name, VERSION)
        fv.init_serving(
            init_rest_client="rest" in clients,
            init_sql_client="sql" in clients,
            default_client=clients[0],
            config_rest_client=rest_config() if "rest" in clients else None,
        )
        return fv

    def random_entry(self):
        return {"ip": random.randrange(self.rows)}

    def random_entries(self):
        return [self.random_entry() for _ in range(self.batch_size)]

    def measure(self, client, name, call, check, keys):
        start = time.perf_counter()
        exception = None
        length = 0
        try:
            result = call()
            check(result, keys)
            length = len(result)
        except Exception as e:
            exception = e
        self.environment.events.request.fire(
            request_type=f"SDK-{client.upper()}",
            name=f"{client}:{name}",
            response_time=(time.perf_counter() - start) * 1000,
            response_length=length,
            exception=exception,
            context={},
        )

    def read(self, client, shape):
        fv = self.join_fv if shape.startswith("join_") else self.fv
        force = {f"force_{client}_client": True}
        if shape.endswith("single"):
            keys, check = self.random_entry(), check_single
        else:
            keys, check = self.random_entries(), check_batch
        if "async" in shape:
            method = fv.get_feature_vector_async if check is check_single else fv.get_feature_vectors_async

            def call():
                return self.loop.run_until_complete(
                    method(entry=keys, return_type="list", **force)
                )
        else:
            method = fv.get_feature_vector if check is check_single else fv.get_feature_vectors

            def call():
                return method(keys, **force)

        self.measure(client, shape, call, check, keys)
