"""Train the fraud model on the cards' current windows and baselines, and register it.

    python train.py [--contamination 0.02] [--min-cards 100] [--system debitcard-example]

Creates the feature view card_fraud_fv, card_window_aggregates joined with
card_spending_baselines on card_number, which the deployment reads a card's
features from. The model is unsupervised: it trains on the online snapshot of
every card with an open window, joined with its baseline, so run it once the
streaming job has seen at least --min-cards cards. The windows are clock hours,
the first one counted from when the running <system>-transactions execution
started; training waits until the current window is MIN_ELAPSED_SECONDS old,
since a younger one is all noise.
"""

import argparse
import tempfile
import time
from pathlib import Path

import hopsworks
import joblib
import model as fraud_model
import pandas as pd


def feature_view(fs, system: str):
    windows = fs.get_feature_group("card_window_aggregates", version=1)
    baselines = fs.get_feature_group("card_spending_baselines", version=1)
    if windows is None or baselines is None:
        raise RuntimeError("Run batch_features.py and streaming_features.py first.")
    query = windows.select(list(fraud_model.WINDOW_FEATURES + fraud_model.WINDOW_TIME)).join(
        baselines.select(list(fraud_model.BASELINE_FEATURES)), on=["card_number"], join_type="left"
    )
    fv = fs.get_or_create_feature_view(
        name=fraud_model.FEATURE_VIEW,
        version=1,
        query=query,
        description=f"{system}: a card's live window aggregates and its 30-active-day spending "
        "baseline, for the fraud model.",
    )
    return fv, windows, baselines


def stream_started(project, system: str) -> str | None:
    """When the running transaction stream was submitted (ISO 8601, UTC), or None without one.

    Waits until the current window, from the clock hour or the stream's start
    if later, is MIN_ELAPSED_SECONDS old.
    """
    job = project.get_job_api().get_job(f"{system}-transactions")
    running = [e for e in (job.get_executions() if job else []) if e.state == "RUNNING"]
    now = pd.Timestamp.now(tz="UTC")
    started = min((pd.to_datetime(e.submission_time, utc=True) for e in running), default=None)
    if started is None:
        print(f"No running {system}-transactions execution: windows count from their clock hour")
    window_start = max(now.floor("h"), started or now.floor("h"))
    wait = fraud_model.MIN_ELAPSED_SECONDS - (now - window_start).total_seconds()
    if wait > 0:
        print(f"The current window opened at {window_start}; waiting {wait:.0f}s for it to fill")
        time.sleep(wait)
    return started.isoformat() if started is not None else None


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--contamination", type=float, default=0.02, help="expected fraction of anomalous cards"
    )
    parser.add_argument("--min-cards", type=int, default=100)
    # Every asset's description starts with the system's slug, which is how deleting the
    # system with its assets tells its own from another's.
    parser.add_argument("--system", default="debitcard-example", help="the system's slug")
    args = parser.parse_args()

    project = hopsworks.login()
    fs = project.get_feature_store()
    fv, windows, baselines = feature_view(fs, args.system)
    started = stream_started(project, args.system)
    snapshot = windows.read(online=True)
    if len(snapshot) < args.min_cards:
        raise RuntimeError(
            f"card_window_aggregates holds {len(snapshot)} cards online; {args.min_cards} are needed"
        )
    snapshot = snapshot.merge(baselines.read(online=True), on="card_number", how="left")
    with_baseline = snapshot["avg_daily_amount_eur_30d"].notna().mean()
    print(f"{len(snapshot):,} cards, {with_baseline:.0%} with a baseline")

    artifact = fraud_model.train(snapshot, contamination=args.contamination, stream_started=started)
    features, _ = fraud_model.prepare_features(snapshot, artifact["medians"], since=started)
    flagged = float(fraud_model.score(artifact, snapshot)[1].mean())

    with tempfile.TemporaryDirectory() as directory:
        joblib.dump(artifact, Path(directory) / fraud_model.MODEL_FILE)
        # The deployment imports model.py, so it travels with the model.
        Path(directory, "model.py").write_text(Path(fraud_model.__file__).read_text())
        registered = project.get_model_registry().python.create_model(
            name=fraud_model.MODEL_NAME,
            metrics={
                "training_cards": float(len(features)),
                "flagged_fraction": flagged,
                "with_baseline": float(with_baseline),
            },
            description=f"{args.system}: Isolation Forest over a card's live window aggregates and its "
            "spending baseline.",
            input_example=features.head(1),
            feature_view=fv,
        )
        registered.save(directory)
    print(
        f"Registered {fraud_model.MODEL_NAME} v{registered.version}: {flagged:.1%} of the cards flagged"
    )


if __name__ == "__main__":
    main()
