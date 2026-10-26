# ruff: noqa: INP001
"""Measure the inference SLA; exits 0 when it holds and 1 when it does not.

Reads `requirements.sla` and `inference` from system.yaml and follows
`inference.mode`. The protocol (`inference.benchmark.protocol`) is frozen for a
tuning round: an attempt may change the deployment, never how it is measured.

    python benchmarks/benchmark_inference.py --bundle <bundle> --record    # as the <slug>-benchmark job
    python benchmarks/benchmark_inference.py --system system.yaml --duration 10   # a short check

realtime: closed-loop load at `offered_qps` with fixed `concurrency`, after
`warmup_s`, for `duration_s` and at least `min_requests`; a request that times
out or fails counts as an error and as the maximum latency, never dropped.
batch: times reading, scoring and writing one window into a throwaway feature
group it deletes afterwards (`kind: full_window`, or `kind: sample`), for `verify
benchmark`; during the build the inference job's first full-window run is the measurement.
`--record` writes result.json; the orchestrator imports it as one `measured` line.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


# region bundle prelude: identical in every entrypoint, see hops-reqs/references/bundle.md
def _load_bundle(bundle: str) -> tuple[Path, dict, dict]:
    """Fetch and unpack a run bundle, check it against its manifest, load system.yaml.

    `bundle` is a HopsFS path (Resources/<slug>/runs/<run_id>/bundle.tar.gz) or a
    local file. The bundle's src/ goes first on sys.path, so the harness a run
    imports is the one its commit names. Returns (workdir, manifest, system).
    """
    import hashlib
    import json
    import sys
    import tarfile
    import tempfile
    from pathlib import Path

    import yaml

    archive = Path(bundle)
    if not archive.is_file():
        import hopsworks

        download_dir = tempfile.mkdtemp(prefix="bundle-download-")
        archive = Path(
            hopsworks.login().get_dataset_api().download(bundle, download_dir, overwrite=True)
        )
    workdir = Path(tempfile.mkdtemp(prefix="bundle-"))
    with tarfile.open(archive) as tar:
        tar.extractall(workdir, filter="data")
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    present = {p.relative_to(workdir).as_posix() for p in workdir.rglob("*") if p.is_file()} - {
        "manifest.json"
    }
    if present != set(manifest["files"]):
        raise SystemExit(f"bundle {bundle}: its files differ from its manifest")
    for rel, digest in manifest["files"].items():
        if hashlib.sha256((workdir / rel).read_bytes()).hexdigest() != digest:
            raise SystemExit(f"bundle {bundle}: {rel} does not match its manifest")
    sys.path.insert(0, str(workdir / "src"))
    system = yaml.safe_load((workdir / "system.yaml").read_text(encoding="utf-8"))
    return workdir, manifest, system


def _write_result(manifest: dict, result: dict) -> str:
    """Write result.json beside the bundle; the orchestrator imports it into system.yaml.

    The run id and commit are echoed from the manifest so the orchestrator can
    check the result belongs to the row it wrote before submission. With
    HOPS_RESULT_DIR set the file is written there instead of uploaded.
    """
    import json
    import os
    import tempfile
    from pathlib import Path

    payload = {"run_id": manifest["run_id"], "commit": manifest["commit"], **result}
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    local_dir = os.environ.get("HOPS_RESULT_DIR")
    if local_dir:
        path = Path(local_dir) / "result.json"
        path.write_text(text, encoding="utf-8")
        return str(path)
    local = Path(tempfile.mkdtemp(prefix="result-")) / "result.json"
    local.write_text(text, encoding="utf-8")
    target = f"Resources/{manifest['slug']}/runs/{manifest['run_id']}"
    import hopsworks

    hopsworks.login().get_dataset_api().upload(str(local), target, overwrite=True)
    return f"{target}/result.json"


# endregion


def percentile(values: list[float], q: float) -> float:
    """Nearest-rank percentile; `q` in [0, 100]."""
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def closed_loop(
    send: Callable[[Any], Any],
    keys: list[Any],
    *,
    offered_qps: float,
    concurrency: int,
    duration_s: float,
    warmup_s: float,
    timeout_ms: float,
    min_requests: int = 0,
    clock: Callable[[], float] = time.monotonic,
) -> dict:
    """Drive `send(key)` from `concurrency` workers paced to `offered_qps` in total.

    Requests during the warm-up are sent but not measured. A request slower than
    `timeout_ms`, or one that raises, is an error recorded at `timeout_ms`.
    Measurement continues past `duration_s` until `min_requests` were measured.
    """
    lock = threading.Lock()
    latencies: list[float] = []
    errors = 0
    samples: list[str] = []
    interval = concurrency / offered_qps if offered_qps > 0 else 0.0
    start = clock()
    measure_from = start + warmup_s
    stop_at = measure_from + duration_s

    def worker(offset: int) -> None:
        nonlocal errors
        n = offset
        next_send = start + (offset / concurrency) * interval
        while True:
            now = clock()
            with lock:
                enough = len(latencies) + errors >= min_requests
            if now >= stop_at and enough:
                return
            if now < next_send:
                time.sleep(min(next_send - now, 0.05))
                continue
            sent = clock()
            failed = False
            try:
                send(keys[n % len(keys)])
            except Exception as exc:  # noqa: BLE001 - every failure is an error, never dropped
                failed = True
                with lock:
                    if len(samples) < 3:
                        samples.append(f"{type(exc).__name__}: {exc}"[:300])
            elapsed_ms = (clock() - sent) * 1000
            if sent >= measure_from:
                with lock:
                    if failed or elapsed_ms > timeout_ms:
                        errors += 1
                        latencies.append(timeout_ms)
                    else:
                        latencies.append(elapsed_ms)
            n += concurrency
            next_send += interval

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    measured_s = max(clock() - measure_from, 1e-9)
    summary = summarize(latencies, errors, measured_s, offered_qps)
    # The first few failures, so an unmet SLA says why without reading the job logs.
    summary["error_samples"] = samples
    return summary


def summarize(latencies: list[float], errors: int, measured_s: float, offered_qps: float) -> dict:
    """The figures the SLA is judged on."""
    total = len(latencies)
    return {
        "requests": total,
        "errors": errors,
        "error_rate": errors / total if total else 1.0,
        "offered_qps": offered_qps,
        "completed_qps": (total - errors) / measured_s,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
    }


def meets_realtime(summary: dict, sla: dict) -> bool:
    """p99, throughput and error rate against `sla.realtime`."""
    return (
        summary["requests"] > 0
        and summary["p99_ms"] <= sla["p99_ms"]
        and summary["completed_qps"] >= sla["throughput_qps"] * 0.99
        and summary["error_rate"] <= sla.get("error_rate_max", 0.0)
    )


def deployment_config(deployment: Any) -> str:
    """A hash of what the deployment runs, so verify can tell a redeploy from the measured one."""
    config = deployment.to_dict()
    for volatile in ("id", "created", "creator", "revision"):
        config.pop(volatile, None)
    return hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()[:12]


def run_realtime(system: dict, protocol: dict, params: dict) -> dict:
    """Load the deployment with real requests and summarize against the SLA.

    A request is `[{entity: key}]` over real entity keys, unless the system's package
    has a `benchmark_requests` module, whose `requests(project, n)` returns the `inputs`
    of `n` requests in the deployment's own shape.
    """
    import hopsworks

    project = hopsworks.login()
    realtime = system["inference"]["realtime"]
    deployment = project.get_model_serving().get_deployment(realtime["deployment"])
    n = int(params.get("entities", 500))
    pkg = system["system"]["slug"].replace("-", "_")
    if importlib.util.find_spec(f"{pkg}.benchmark_requests"):
        requests = importlib.import_module(f"{pkg}.benchmark_requests").requests(project, n)
    else:
        entity = system["requirements"]["problem"]["entity"]
        source = next(
            s for s in system["requirements"]["data_sources"] if s.get("entity", entity) == entity
        )
        fg = project.get_feature_store().get_feature_group(
            source["name"], version=source.get("version", 1)
        )
        requests = [[{entity: key}] for key in fg.select([entity]).show(n)[entity]]

    summary = closed_loop(
        lambda inputs: deployment.predict(inputs=inputs),
        requests,
        offered_qps=params.get("qps", protocol["offered_qps"]),
        concurrency=protocol["concurrency"],
        duration_s=params.get("duration_s", protocol["duration_s"]),
        warmup_s=protocol["warmup_s"],
        timeout_ms=protocol["timeout_ms"],
        min_requests=protocol.get("min_requests", 0),
    )
    sla = system["requirements"]["sla"]["realtime"]
    return {
        **summary,
        "kind": "full_window",
        "model_version": realtime["model_version"],
        "deployment_config": deployment_config(deployment),
        "met": meets_realtime(summary, sla),
    }


def run_batch(system: dict, params: dict, run_id: str) -> dict:
    """Time one window end to end into a throwaway prediction group, then delete it."""
    import hopsworks

    pkg = system["system"]["slug"].replace("-", "_")
    evaluate = importlib.import_module(f"{pkg}.evaluate")
    pipeline = importlib.import_module(f"{pkg}.inference_pipeline")
    project = hopsworks.login()
    fs = project.get_feature_store()
    training = system["training"]
    fv = fs.get_feature_view(
        training["feature_view"]["name"], version=training["feature_view"]["version"]
    )
    model = project.get_model_registry().get_model(
        training["model"]["name"], version=training["model"]["version"]
    )
    predict = evaluate.load_predictor(model.download())

    started = time.monotonic()
    fv.init_batch_scoring(training_dataset_version=training["harness"]["training_dataset_version"])
    batch = fv.get_batch_data(start_time=params["start"], end_time=params["end"])
    scores = predict(batch)
    import pandas as pd

    frame = pd.DataFrame({"row": range(len(batch)), "score": list(scores)})
    name = f"{pkg}_predictions_test_{run_id.replace('-', '_')}"
    sink = fs.get_or_create_feature_group(
        name=name,
        version=1,
        primary_key=["row"],
        description=f"benchmark {run_id}; deleted after the run",
        statistics_config=False,
    )
    try:
        sink.insert(frame, write_options={"wait_for_job": True})
        window_s = time.monotonic() - started
    finally:
        sink.delete()
    return {
        **pipeline.batch_verdict(system, len(batch), window_s, params.get("kind", "full_window")),
        "model_version": training["model"]["version"],
    }


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark for `inference.mode` and exit 0 when the SLA holds."""
    parser = argparse.ArgumentParser(description="Measure the inference SLA.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle")
    source.add_argument("--system", help="a local system.yaml, for a check that is not recorded")
    parser.add_argument("--duration", type=float, help="override duration_s; never recorded")
    parser.add_argument("--record", action="store_true", help="write result.json")
    args = parser.parse_args(argv)

    if args.bundle:
        _, manifest, system = _load_bundle(args.bundle)
    else:
        import yaml

        system = yaml.safe_load(Path(args.system).read_text(encoding="utf-8"))
        manifest = {
            "run_id": "local",
            "commit": "local",
            "slug": system["system"]["slug"],
        }
    benchmark = system["inference"]["benchmark"]
    params = dict(benchmark.get("params") or {})
    if args.duration:
        params["duration_s"] = args.duration
    mode = system["inference"]["mode"]
    if mode == "realtime":
        result = run_realtime(system, benchmark["protocol"], params)
    elif mode == "batch":
        result = run_batch(system, params, manifest["run_id"])
    else:
        raise SystemExit("agent systems are captured at reqs and not built in v1")
    if args.duration:
        result["kind"] = "sample"
        result["met"] = False
    if args.record:
        if args.duration:
            raise SystemExit("--duration is a one-off check and is never recorded")
        _write_result(manifest, result)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["met"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
