"""Batch features: run the dbt models in Trino and land them in feature groups.

    python batch_features.py [--dbt-project-dir DIR] [--target-dir DIR]

Runs in the dbt-pipeline environment, which ships dbt-core, dbt-trino and
dbt_run_graph. dbt compile gives each model's SQL and dbt build runs the data
tests; then each model's rows are streamed from Trino into its feature group:
card_daily_features offline, card_spending_baselines online too, where the
deployment reads a card's baseline. The run's artifacts go to their own
directory, Resources/dbt_runs/<job>/<execution> in a job, where the Jobs UI
reads the execution graph from.
"""

import argparse
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import dbt_run_graph
import pandas as pd

import hopsworks
from hsfs.feature import Feature

DBT_PROJECT = "card_fraud"
DEFAULT_FETCH_SIZE = 25_000
SOURCES = ["card_authorisations", "card_authorisation_signals"]

MODELS = {
    "card_daily_features": {
        "description": "Each card's activity per active day computed by dbt in Trino, its trailing 7 and 30 "
        "active days, and fraud_next_active_day, whether fraud follows on its next active day.",
        "primary_key": ["card_day_id"],
        "event_time": "feature_ts",
        "online": False,
        "features": [
            Feature("card_day_id", "string", description="card_number|feature_date"),
            Feature("card_number", "string"),
            Feature("feature_date", "date"),
            Feature("feature_ts", "timestamp", description="The day's last authorisation"),
            Feature("day_of_week", "int", description="1 Monday to 7 Sunday"),
            Feature("is_weekend", "int"),
            Feature("txn_count", "bigint"),
            Feature("total_amount_eur", "double"),
            Feature("avg_amount_eur", "double"),
            Feature("max_amount_eur", "double"),
            Feature("amount_stddev_eur", "double"),
            Feature("declined_count", "bigint"),
            Feature("decline_ratio", "double"),
            Feature("review_count", "bigint"),
            Feature(
                "high_risk_count",
                "bigint",
                description="Authorisations with a network or issuer score over 500",
            ),
            Feature("high_risk_ratio", "double"),
            Feature("cvc2_failure_count", "bigint"),
            Feature("ecommerce_or_token_count", "bigint"),
            Feature("distinct_merchants", "bigint"),
            Feature("distinct_regions", "bigint"),
            Feature("distinct_card_schemes", "bigint"),
            Feature("avg_merchant_distance_km", "double"),
            Feature("max_merchant_distance_km", "double"),
            Feature("max_cash_withdrawals_48h_eur", "double"),
            Feature("avg_network_risk_score", "double"),
            Feature("avg_issuer_fraud_score", "double"),
            Feature("txn_count_7d", "bigint", description="Over the last 7 active days"),
            Feature("total_amount_eur_7d", "double", description="Over the last 7 active days"),
            Feature("txn_count_30d", "bigint", description="Over the last 30 active days"),
            Feature("declined_count_30d", "bigint", description="Over the last 30 active days"),
            Feature("high_risk_count_30d", "bigint", description="Over the last 30 active days"),
            Feature(
                "avg_daily_amount_eur_30d", "double", description="Over the last 30 active days"
            ),
            Feature(
                "amount_to_30d_avg",
                "double",
                description="The day's amount over avg_daily_amount_eur_30d",
            ),
            Feature(
                "max_merchant_distance_km_30d", "double", description="Over the last 30 active days"
            ),
            Feature("fraud_count_30d", "bigint", description="Over the last 30 active days"),
            Feature(
                "fraud_next_active_day",
                "int",
                description="1 when fraud follows on the next active day; null on the last",
            ),
        ],
        "nullable_ints": ["day_of_week", "is_weekend", "fraud_next_active_day"],
    },
    "card_spending_baselines": {
        "description": "Each card's spending baseline over its last 30 active days, as of its latest active day, "
        "computed by dbt in Trino. Read online by the fraud deployment.",
        "primary_key": ["card_number"],
        "event_time": "baseline_time",
        "online": True,
        "features": [
            Feature("card_number", "string"),
            Feature(
                "baseline_time",
                "timestamp",
                description="The last authorisation the baseline covers",
            ),
            Feature("baseline_date", "date"),
            Feature("avg_daily_amount_eur_30d", "double", description="Mean spend per active day"),
            Feature(
                "avg_daily_txn_count_7d",
                "double",
                description="Mean authorisations per active day over the last 7",
            ),
            Feature("decline_ratio_30d", "double"),
            Feature("high_risk_ratio_30d", "double"),
            Feature("max_merchant_distance_km_30d", "double"),
        ],
        "nullable_ints": [],
    },
}


def default_dbt_project_dir() -> Path:
    return Path(__file__).resolve().parent / "dbt"


def default_target_dir() -> Path:
    job, execution = (
        os.environ.get("HOPSWORKS_JOB_NAME"),
        os.environ.get("HOPSWORKS_JOB_EXECUTION_ID"),
    )
    if job and execution and Path("/hopsfs").is_dir():
        return Path("/hopsfs/Resources/dbt_runs") / job / execution
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path(__file__).resolve().parent / "artifacts" / f"run-{stamp}"


def dbt_environment(project) -> dict[str, str]:
    trino = project.get_trino_api()
    user, password = trino.get_basic_auth()
    return {
        "DBT_TRINO_HOST": trino.get_host(),
        "DBT_TRINO_PORT": str(trino.get_port()),
        "DBT_TRINO_USER": user,
        "DBT_TRINO_PASSWORD": password,
        "DBT_TRINO_SCHEMA": f"{project.name}_featurestore",
        "DBT_TRINO_CA": str(trino._get_ca_chain_path(True)),
    }


def dbt_args(command: str, project_dir: Path, target_dir: Path) -> list[str]:
    return [
        command,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target-path",
        str(target_dir),
        "--log-path",
        str(target_dir / "logs"),
        "--select",
        *MODELS,
    ]


def invoke_dbt(args: list[str]):
    from dbt.cli.main import dbtRunner

    return dbtRunner().invoke(args)


def compile_models(project_dir: Path, target_dir: Path) -> dict[str, str]:
    """Each model's SQL, compiled with its ephemeral parents inlined."""
    result = invoke_dbt(dbt_args("compile", project_dir, target_dir))
    if not result.success:
        raise RuntimeError(f"dbt compile failed: {result.exception}")
    compiled = {}
    for model in MODELS:
        found = list((target_dir / "compiled").rglob(f"{model}.sql"))
        if len(found) != 1:
            raise RuntimeError(f"expected one compiled {model}.sql, found {found}")
        compiled[model] = found[0].read_text()
    return compiled


def feature_group(fs, name: str, parents):
    model = MODELS[name]
    return fs.get_or_create_feature_group(
        name=name,
        version=1,
        description=model["description"],
        primary_key=model["primary_key"],
        event_time=model["event_time"],
        features=model["features"],
        online_enabled=model["online"],
        statistics_config=False,
        parents=parents,
    )


def ingest(project, name: str, query: str, fetch_size: int) -> int:
    """Stream the model's rows from Trino into its feature group; returns the row count."""
    fs = project.get_feature_store()
    parents = [fs.get_feature_group(source, version=1) for source in SOURCES]
    if any(parent is None for parent in parents):
        raise RuntimeError("The source feature groups are missing: run generate_data.py first.")
    fg = feature_group(fs, name, parents)
    model = MODELS[name]
    connection = project.get_trino_api().connect(
        catalog="delta", schema=f"{project.name}_featurestore"
    )
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    expected = [feature.name for feature in model["features"]]
    if columns != expected:
        raise RuntimeError(
            f"{name}: the model returns {columns}, the feature group expects {expected}"
        )
    storage = None if model["online"] else "offline"
    rows = 0
    with fg.multi_part_insert() as writer:
        while batch := cursor.fetchmany(fetch_size):
            frame = pd.DataFrame.from_records(batch, columns=columns)
            frame[model["nullable_ints"]] = frame[model["nullable_ints"]].astype("Int32")
            writer.insert(
                frame,
                operation="upsert",
                storage=storage,
                validation_options={"run_validation": False},
            )
            rows += len(frame)
            print(f"  {name}: {rows:,} rows queued")
    cursor.close()
    connection.close()
    if rows == 0:
        raise RuntimeError(f"{name} returned no rows")
    return rows


def graph_node(name: str, elapsed: float, rows=None, error=None) -> dict:
    """The Trino to feature group ingestion of `name`, as a node of the execution graph."""
    return {
        "id": f"external.hopsworks.{name}_1",
        "label": f"feature group {name} v1",
        "status": "error" if error is not None else "success",
        "execution_time": elapsed,
        "depends_on": [f"model.{DBT_PROJECT}.{name}"],
        "detail": {
            "resource_type": "Hopsworks feature group ingestion",
            "relation": f"{name}_1",
            "rows_affected": rows,
            "message": str(error)
            if error is not None
            else f"Streamed {name} from Trino into its feature group.",
        },
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dbt-project-dir", type=Path, default=default_dbt_project_dir())
    parser.add_argument(
        "--target-dir", type=Path, help="this run's dbt artifacts and execution graph"
    )
    parser.add_argument("--fetch-size", type=int, default=DEFAULT_FETCH_SIZE)
    # A scheduled run appends -start_time <fire time>; the models read all of the history.
    args, _ = parser.parse_known_args()

    project = hopsworks.login()
    target_dir = args.target_dir or default_target_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"dbt project {args.dbt_project_dir}, artifacts in {target_dir}")
    os.environ.update(dbt_environment(project))

    queries = compile_models(args.dbt_project_dir, target_dir)
    build = invoke_dbt(dbt_args("build", args.dbt_project_dir, target_dir))
    nodes, failure = [], None
    if build.success:
        for name, query in queries.items():
            started = time.perf_counter()
            try:
                rows = ingest(project, name, query, args.fetch_size)
                nodes.append(graph_node(name, time.perf_counter() - started, rows=rows))
            except Exception as error:  # the graph is written before the run fails
                nodes.append(graph_node(name, time.perf_counter() - started, error=error))
                failure = error
                break
    graph = dbt_run_graph.write_execution_graph(
        target_dir, external_nodes=nodes, title=f"{project.name} card fraud"
    )
    print(f"Execution graph: {graph}")
    if not build.success:
        raise RuntimeError(
            f"dbt build failed: {build.exception or 'data test failures'}; artifacts in {target_dir}"
        )
    if failure is not None:
        raise failure


if __name__ == "__main__":
    main()
