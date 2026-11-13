# ruff: noqa: INP001
"""TPC-H Q8 as a data pipeline: generate TPC-H in DuckDB, transform it with dbt, write feature groups.

    python tpch_pipeline.py [--sf 1] [--memory-limit 300MB] [--threads 1] [--system tpch-duckdb]

Runs as a Python job in the `dbt-pipeline` environment, which ships DuckDB, dbt-core,
dbt-duckdb and dbt_run_graph:

1. `INSTALL tpch; LOAD tpch; CALL dbgen(sf = 1);` writes the TPC-H tables into a
   DuckDB file in the job's scratch directory (about 250 MB at scale factor 1).
2. Unless `--no-tpch-tables`, the eight TPC-H tables are written to offline Delta
   feature groups, tpch_<table>, in chunks of CHUNK_ROWS rows appended one after the other, so
   memory holds one chunk at a time. A table already holding every row dbgen
   wrote is left as it is, and one holding fewer (a run that failed while
   writing it) is written again, so a second run writes nothing twice.
3. `dbt build` runs the dbt project in tpch_q8/: one model per node of the DAG,
   from each table's selected columns to Brazil's share of the American market
   per year, and the project's data tests.
4. The two output models are written to offline Delta feature groups:
   tpch_brazil_market_share (one row per year) and tpch_america_volume_by_nation
   (one row per year and supplier nation, which the dashboard charts).

DuckDB keeps to `--memory-limit` and spills to the scratch directory beyond it,
with one thread and insertion order not preserved, so the job holds about 600 MB
at its peak at scale factor 1. The dbt artifacts and the execution graph the Jobs
UI shows land in Resources/dbt_runs/<job>/<execution>, on failure as well.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


DBT_PROJECT = Path(__file__).resolve().parent / "tpch_q8"
DBT_PROJECT_NAME = "tpch_q8"
# model -> (feature group, primary key): both offline Delta, version 1, event time year_start.
OUTPUTS = {
    "brazil_volume_by_year": ("tpch_brazil_market_share", ["o_year"]),
    "america_volume_by_nation_year": (
        "tpch_america_volume_by_nation",
        ["o_year", "supplier_nation"],
    ),
}
# TPC-H table -> (primary key, event time): written as tpch_<table>, offline Delta, version 1.
SOURCES = {
    "region": (["r_regionkey"], None),
    "nation": (["n_nationkey"], None),
    "customer": (["c_custkey"], None),
    "supplier": (["s_suppkey"], None),
    "part": (["p_partkey"], None),
    "partsupp": (["ps_partkey", "ps_suppkey"], None),
    "orders": (["o_orderkey"], "o_orderdate"),
    "lineitem": (["l_orderkey", "l_linenumber"], "l_shipdate"),
}
CHUNK_ROWS = 250_000
DESCRIPTIONS = {
    "tpch_brazil_market_share": "Brazilian suppliers' share of the American market for a part type, "
    "per year (TPC-H Q8)",
    "tpch_america_volume_by_nation": "the volume each supplier nation sold to American customers "
    "for a part type, per year (TPC-H Q8)",
}


def configure(con, memory_limit: str, threads: int, scratch: Path) -> None:
    """Keep DuckDB within `memory_limit`, spilling to `scratch` beyond it."""
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET threads = {threads}")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{scratch / 'spill'}'")


def generate(path: Path, sf: float, memory_limit: str, threads: int) -> dict[str, int]:
    """Write the TPC-H tables at scale factor `sf` into the DuckDB file `path`; their row counts."""
    import duckdb

    con = duckdb.connect(str(path))
    try:
        configure(con, memory_limit, threads, path.parent)
        con.execute(f"INSTALL tpch; LOAD tpch; CALL dbgen(sf = {sf});")
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        return {
            t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables
        }
    finally:
        con.close()


def dbt_build(
    path: Path, target_dir: Path, memory_limit: str, threads: int, dbt_vars: str | None
) -> bool:
    """Run `dbt build` over the DuckDB file; True when every model and data test passed.

    dbt runs in a subprocess, as hops-dbt says for DuckDB: in process it would keep
    the file open and block reading the outputs back.
    """
    env = {
        **os.environ,
        "TPCH_DUCKDB_PATH": str(path),
        "TPCH_DUCKDB_MEMORY_LIMIT": memory_limit,
        "TPCH_DUCKDB_THREADS": str(threads),
        "TPCH_DUCKDB_TEMP_DIRECTORY": str(path.parent / "spill"),
    }
    # The dbt entry point next to this interpreter, so it is the environment's dbt.
    dbt = Path(sys.executable).with_name("dbt")
    command = [
        str(dbt) if dbt.exists() else "dbt",
        "build",
        "--project-dir",
        str(DBT_PROJECT),
        "--profiles-dir",
        str(DBT_PROJECT),
        "--target-path",
        str(target_dir),
        "--log-path",
        str(target_dir / "logs"),
    ]
    if dbt_vars:
        command += ["--vars", dbt_vars]
    return subprocess.run(command, env=env, check=False).returncode == 0


def read_outputs(path: Path) -> dict:
    """The output models as pandas frames, keyed by model."""
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        return {
            model: con.execute(f"SELECT * FROM main.{model}").fetch_df()
            for model in OUTPUTS
        }
    finally:
        con.close()


def run_dir() -> Path:
    """Where the dbt artifacts go: Resources/dbt_runs/<job>/<execution> in a job, ./artifacts otherwise."""
    job = os.environ.get("HOPSWORKS_JOB_NAME")
    execution = os.environ.get("HOPSWORKS_JOB_EXECUTION_ID")
    if job and execution and Path("/hopsfs").is_dir():
        return Path("/hopsfs/Resources/dbt_runs") / job / execution
    # Absolute: dbt resolves a relative --target-path against the project directory.
    return (
        Path("artifacts") / f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    ).resolve()


def write_graph(target_dir: Path, nodes: list[dict]) -> None:
    """The execution graph the Jobs UI shows, with the generation and the writes as external nodes."""
    try:
        import dbt_run_graph  # preinstalled in dbt-pipeline
    except ImportError:
        return
    dbt_run_graph.write_execution_graph(
        target_dir, external_nodes=nodes, title="TPC-H Q8 · DuckDB"
    )


def rows_in(fg) -> int:
    """The rows a feature group's Delta commits inserted, less those they deleted."""
    return sum(
        int(commit.get("rowsInserted") or 0) - int(commit.get("rowsDeleted") or 0)
        for commit in fg.commit_details().values()
    )


def source_query(con, table: str) -> str:
    """Every column of a TPC-H table, its DECIMAL prices as DOUBLE, which pandas holds as floats."""
    columns = [
        f"CAST({name} AS DOUBLE) AS {name}" if kind.startswith("DECIMAL") else name
        for name, kind, *_ in con.execute(f"DESCRIBE {table}").fetchall()
    ]
    return f"SELECT {', '.join(columns)} FROM {table}"


def write_sources(
    fs,
    path: Path,
    counts: dict[str, int],
    sf: float,
    system: str,
    memory_limit: str,
    threads: int,
) -> dict[str, int]:
    """Write each TPC-H table to its feature group in appended chunks; rows written per group."""
    import duckdb
    import polars as pl

    written = {}
    con = duckdb.connect(str(path), read_only=True)
    try:
        # Without a limit DuckDB takes 80% of the node's memory, not the job's.
        configure(con, memory_limit, threads, path.parent)
        for table, (primary_key, event_time) in SOURCES.items():
            name = f"tpch_{table}"
            fg = fs.get_feature_group(name, version=1)
            if fg is not None:
                if rows_in(fg) == counts[table]:
                    written[name] = 0
                    continue
                fg.delete()
            fg = fs.create_feature_group(
                name=name,
                version=1,
                primary_key=primary_key,
                event_time=event_time,
                online_enabled=False,
                time_travel_format="DELTA",
                statistics_config=False,
                description=f"{system}: the TPC-H {table} table at scale factor {sf:g}, as DuckDB's dbgen wrote it",
            )
            rows = 0
            reader = con.execute(source_query(con, table)).to_arrow_reader(CHUNK_ROWS)
            for batch in reader:
                # Appends: the first chunk creates the Delta table, and nothing is merged.
                # Polars shares the batch's Arrow memory, where pandas would copy it.
                fg.insert(
                    pl.from_arrow(batch),
                    operation="insert",
                    write_options={"wait_for_job": True},
                )
                rows += batch.num_rows
            written[name] = rows
            print(f"{name}: {rows:,} rows; peak memory: {peak_memory()}", flush=True)
    finally:
        con.close()
    return written


def write_feature_groups(fs, frames: dict, system: str) -> dict[str, int]:
    """Write each output model to its offline Delta feature group; rows written per group."""
    written = {}
    for model, frame in frames.items():
        name, primary_key = OUTPUTS[model]
        fg = fs.get_or_create_feature_group(
            name=name,
            version=1,
            primary_key=primary_key,
            event_time="year_start",
            online_enabled=False,
            time_travel_format="DELTA",
            # A DuckDB pipeline computes in its own process and never starts a Spark job.
            statistics_config=False,
            description=f"{system}: {DESCRIPTIONS[name]}",
        )
        fg.insert(frame, write_options={"wait_for_job": True})
        written[name] = len(frame)
    return written


def peak_memory() -> str:
    """The peak resident memory of this process and of dbt's, and the container's peak with page cache."""
    import resource

    runner = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    dbt = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss // 1024
    try:
        container = f"{int(Path('/sys/fs/cgroup/memory.peak').read_text()) // 2**20} MB"
    except (OSError, ValueError):
        container = "unknown"
    return f"runner {runner} MB, dbt {dbt} MB resident; container {container} with page cache"


def node(name: str, label: str, depends_on: list[str]) -> dict:
    """An external node of the execution graph, skipped until it runs."""
    return {
        "id": f"external.hopsworks.{name}",
        "label": label,
        "status": "skipped",
        "execution_time": None,
        "depends_on": depends_on,
        "detail": {"message": "not run"},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--sf", type=float, default=1.0, help="TPC-H scale factor")
    parser.add_argument("--memory-limit", default="300MB", help="DuckDB's memory limit")
    parser.add_argument("--threads", type=int, default=1, help="DuckDB's threads")
    parser.add_argument(
        "--vars", help="dbt vars as YAML, e.g. \"{p_type: 'STANDARD POLISHED TIN'}\""
    )
    parser.add_argument("--system", default="tpch-duckdb", help="the system's slug")
    parser.add_argument(
        "--no-tpch-tables",
        action="store_true",
        help="leave out the TPC-H tables' feature groups: about 5 minutes less, and 1024 MB is enough",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="build and test the models, write no feature group",
    )
    # A scheduled run appends -start_time/-end_time; the pipeline recomputes everything, so they are ignored.
    parser.add_argument("-start_time", dest="_start", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_end", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    target_dir = run_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    generation = node("tpch_dbgen", f"DuckDB dbgen(sf = {args.sf:g})", [])
    sources = node(
        "tpch_tables",
        "feature groups tpch_<table> v1",
        ["external.hopsworks.tpch_dbgen"],
    )
    writes = [
        node(name, f"feature group {name} v1", [f"model.{DBT_PROJECT_NAME}.{model}"])
        for model, (name, _) in OUTPUTS.items()
    ]
    scratch = Path(tempfile.mkdtemp(prefix="tpch-"))
    path = scratch / "tpch.duckdb"
    try:
        started = time.monotonic()
        counts = generate(path, args.sf, args.memory_limit, args.threads)
        generation.update(
            status="success",
            execution_time=round(time.monotonic() - started, 1),
            detail={"rows": counts},
        )
        print(f"dbgen(sf = {args.sf:g}): {counts}")

        fs = None
        if not args.no_write:
            import hopsworks

            fs = hopsworks.login().get_feature_store()
        if fs is not None and not args.no_tpch_tables:
            started = time.monotonic()
            try:
                tables = write_sources(
                    fs,
                    path,
                    counts,
                    args.sf,
                    args.system,
                    args.memory_limit,
                    args.threads,
                )
            except Exception as exc:
                sources.update(status="error", detail={"message": str(exc)})
                raise
            sources.update(
                status="success",
                execution_time=round(time.monotonic() - started, 1),
                detail={"rows_affected": tables},
            )

        if not dbt_build(path, target_dir, args.memory_limit, args.threads, args.vars):
            print("dbt build failed: see the execution graph", file=sys.stderr)
            return 1
        frames = read_outputs(path)
        for model, frame in frames.items():
            print(f"{model}:\n{frame.to_string(index=False)}")
        if args.no_write:
            return 0

        started = time.monotonic()
        try:
            written = write_feature_groups(fs, frames, args.system)
        except Exception as exc:
            for write in writes:
                write.update(status="error", detail={"message": str(exc)})
            raise
        for write, rows in zip(writes, written.values(), strict=True):
            write.update(
                status="success",
                execution_time=round(time.monotonic() - started, 1),
                detail={"rows_affected": rows},
            )
        print(f"written: {written}")
        return 0
    finally:
        steps = [generation, *([] if args.no_tpch_tables else [sources]), *writes]
        write_graph(target_dir, steps)
        shutil.rmtree(scratch, ignore_errors=True)
        print(f"peak memory: {peak_memory()}")


if __name__ == "__main__":
    sys.exit(main())
