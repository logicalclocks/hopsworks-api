# Online Feature Store Benchmark

Load tests for online feature vector retrieval through the Hopsworks Python SDK, driven by [locust](https://locust.io/).
They run from a local Docker host or as Kubernetes Jobs inside the cluster, and can compare SDK builds, for example a change against `main`.

Each locust user reads through two feature views:

- `locust_fv` over one feature group, `locust_fg` (`ip` primary key and 10 features);
- `locust_join_fv`, which joins `locust_fg` with `locust_ip_meta_fg` (`region`, `risk_score`) on `ip`.

It runs six call shapes as locust tasks: single and batch reads on both views, and single and batch reads on the join view through the async API.
Locust picks a task per iteration, so the shapes share the run time.
Tasks are named `<client>:<shape>`, for example `rest:join_batch`.
A response whose rows do not match the requested keys counts as a failure.

## Layout

| Path | Purpose |
| --- | --- |
| `locustfile.py`, `common.py`, `setup_data.py` | Test code, shared by both targets. |
| `Dockerfile`, `sdk.env`, `build_images.sh` | Image `locust-hsfs:<name>`: locust plus one SDK build per entry in `sdk.env`. |
| `local/` | Runs on a local Docker host: `locust.conf`, `hopsworks_config.json`, `docker-compose.yml`, `run.sh`, `run_single.sh`. |
| `k8s/` | Runs inside the cluster: `locust.conf`, `hopsworks_config.json`, `cluster.env`, `locust-job.yaml`, `push_images.sh`, `run.sh`. |
| `results/` | HTML reports (created by the runs, git-ignored). |
| `logs/` | Run logs and node CPU samples from Kubernetes runs (git-ignored). |
| `Jenkinsfile`, `build-manifest.json`, `KUBE_IMAGE_VERSION` | CI build of `hopsworks/locust-hsfs` from the `Dockerfile` defaults (upstream `main`). |

## Configuration

| File | Holds |
| --- | --- |
| `sdk.env` | SDK builds: `SDKS`, and `<name>_REPO` / `<name>_REF` for each. The default is the upstream `main` branch. |
| `<target>/hopsworks_config.json` | `host`, `port`, `project`, `verify_certs`; optionally `rdrs_host` / `rdrs_port` to pin the REST client to a RonDB REST server address. |
| `<target>/locust.conf` | Locust options, plus the test's own: `rows`, `batch-size`, `clients` (`rest`, `sql`), `skip-setup`. |
| `k8s/cluster.env` | Namespace, image registry, optional pull secret and preferred node, pod CPU and memory. |

Values in `<angle brackets>` are placeholders; the run scripts refuse to start until they are replaced.

The API key is read from `locust_benchmark/.api_key` (git-ignored); `API_KEY_FILE` points elsewhere.

`setup_data.py` creates the feature groups and views, with statistics disabled, and upserts `rows` rows.
Locust runs it at startup unless `skip-setup = true`.
With `skip-setup`, `rows` must not exceed what was loaded, or reads of missing keys count as failures.
It also runs on its own: `python setup_data.py <rows>`.

## Build the images

```bash
./build_images.sh          # every SDK in sdk.env
./build_images.sh main     # one
```

To compare a change against `main`, add it to `sdk.env` and build both:

```bash
SDKS="main change"
change_REPO=https://github.com/<fork>/hopsworks-api.git
change_REF=<commit>
```

Pin commits for comparisons; a branch ref is resolved at build time.

## Run locally

Set `host` and `project` in `local/hopsworks_config.json`, then:

```bash
SDK=main local/run.sh
SDK=change local/run.sh
MASTER_ARGS="--run-time 30" local/run.sh
```

`local/run.sh` starts one locust master and one worker container per `users` in `local/locust.conf`, one user per worker.
The report goes to `results/report_local_<sdk>_u<users>.html`.

From outside the cluster every read crosses the network, so latency mostly measures the network rather than the client.
Use the Kubernetes target to compare client performance.

`local/run_single.sh` runs one locust process; keep `users = 1` with it (see Limitations).

## Run on Kubernetes

Needs `kubectl` with `KUBECONFIG` exported for the cluster, and an image registry the cluster can pull from.
Set `REGISTRY` (and `PULL_SECRET`, `PREFERRED_NODE` if needed) in `k8s/cluster.env` and `project` in `k8s/hopsworks_config.json`.
The Hopsworks and RonDB REST server addresses in `k8s/hopsworks_config.json` are the in-cluster service names of a standard Hopsworks installation.

```bash
k8s/push_images.sh                                    # every SDK in sdk.env
SDK=main USERS=2 DURATION=60 LABEL=smoke k8s/run.sh   # first run loads the data
SDK=main USERS=8 DURATION=300 LABEL=load k8s/run.sh
```

Set `skip-setup = true` in `k8s/locust.conf` after the first run, so later runs do not reload the rows.

Each run creates, in `NAMESPACE`:

- a master Job and its Service;
- a worker Job with one pod per user (one locust worker, one user, `WORKER_CPU`), preferring `PREFERRED_NODE`;
- the ConfigMap `locust-files` (code and settings) and the Secret `locust-api-key`, updated on every run.

The Jobs and the Service are deleted when the run ends; the ConfigMap and the Secret stay for the next run.

Output:

- `results/report_k8s_<label>_<sdk>_u<users>_<duration>s.html`
- `logs/report_k8s_<...>_master.log`, `_workers.log`, and `_nodes.txt` (node CPU and worker placement mid-run)

Clean up:

```bash
kubectl -n <namespace> delete configmap locust-files
kubectl -n <namespace> delete secret locust-api-key
```

Place the workers away from the nodes that run the RonDB REST server and ingress where possible, so the load generator does not compete with the system under test; `_nodes.txt` shows where they ran.

## Limitations

### One user per locust worker

Locust users are gevent greenlets on one OS thread, and asyncio allows one running event loop per thread.
When two users in one process call the async API at the same time, the second fails with `RuntimeError: Cannot run the event loop while another loop is running`.
`local/run.sh` and `k8s/run.sh` therefore give each user its own worker process.
Scale with `users` (local) or `USERS` (Kubernetes), not with more users per worker.
`local/run_single.sh` puts every user in one process; keep `users = 1` with it.

### The MySQL client does not run under locust

Leave `clients = rest` until this is resolved.
The SDK's SQL client runs its queries on an `AsyncTaskThread` with its own event loop.
Under locust, gevent's monkey-patching turns that thread into a greenlet on the same OS thread as everything else.
The first SQL-initialised feature view's loop occupies the thread, and the task thread of every further one fails at startup with `Cannot run the event loop while another loop is running`; nearly every SQL read then fails, even with one user.
`nest_asyncio`, which earlier versions of this benchmark applied, does not help on Python 3.12: `asyncio.wait_for` there is built on `asyncio.timeout`, which needs a current task, and reads fail with `Timeout should be used inside a task`.
Load testing the SQL client needs a driver without gevent, for example plain threads or processes.

### Other notes

- `verify_certs` is off in the Kubernetes configuration because the client does not have the cluster CA; turn it off locally too if the RonDB REST server certificate does not cover the address used.
- Locust rounds response time percentiles to whole milliseconds; in-cluster reads take a few milliseconds, so compare requests per second as well.
- Each configuration is one run; repeat runs, alternating the SDKs, before drawing conclusions from small differences.
