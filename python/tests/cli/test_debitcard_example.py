"""The debit card example's reference code: the generator, the live stream and the dbt and Spark contracts."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pytest


pl = pytest.importorskip("polars")


DEBITCARD = (
    Path(__file__).resolve().parents[2]
    / "hopsworks"
    / "skills"
    / "ml"
    / "hops-reqs"
    / "references"
    / "debitcard"
)


@pytest.fixture(scope="module")
def code():
    sys.path.insert(0, str(DEBITCARD))
    try:
        import generate_data
        import synth
        import transaction_stream

        yield synth, generate_data, transaction_stream
    finally:
        sys.path.remove(str(DEBITCARD))
        for name in ("synth", "generate_data", "transaction_stream"):
            sys.modules.pop(name, None)


def _history(synth, rows=20_000, seed=7):
    rng = np.random.default_rng(seed)
    cards = synth.generate_card_pool(500, rng)
    card_numbers = cards[synth.power_law_indices(rng, len(cards), rows)]
    times = synth.random_timestamps(rng, rows)
    overlay = synth.generate_fraud_plan(card_numbers, times, rng, fraud_rate=0.05)
    pools = (
        synth.generate_merchant_names(200, rng),
        synth.generate_acquirer_ids(10),
        synth.generate_wallet_ids(100),
        synth.generate_token_requestor_ids(5),
    )
    authorisations = synth.generate_card_authorisations(
        card_numbers, times, *pools, rng, overlay
    )
    signals = synth.generate_card_authorisation_signals(
        card_numbers, times, rng, overlay
    )
    return authorisations, signals, overlay


def test_the_tables_match_their_feature_group_schemas(code):
    synth, generate_data, _ = code
    authorisations, signals, overlay = _history(synth)
    rng = np.random.default_rng(1)
    frames = {
        "card_authorisations": authorisations,
        "card_authorisation_signals": signals,
        "card_weekly_spend": synth.generate_card_weekly_spend(
            300, np.unique(authorisations["card_number"]), rng, overlay["fraud_cards"]
        ),
        "account_events": synth.generate_account_events(
            300, synth.generate_account_pool(100), rng
        ),
    }
    for name, frame in frames.items():
        expected = [f.name for f in generate_data.FEATURE_GROUPS[name]["features"]]
        assert frame.columns == expected, name


def test_the_signals_label_the_authorisations_they_share_a_key_with(code):
    synth, _, _ = code
    authorisations, signals, overlay = _history(synth)
    joined = authorisations.join(
        signals, on=["card_number", "transaction_time"], how="inner"
    )
    # The signals drop a duplicate key; every other authorisation keeps its label.
    assert len(joined) >= len(authorisations) * 0.99
    fraud = joined.filter(pl.col("fraud_flag") == 1)
    assert 0.002 < len(fraud) / len(joined) < 0.2
    assert (
        signals["fraud_flag"]
        == signals["approved_fraud_flag"] + signals["declined_fraud_flag"]
    ).all()
    assert len(fraud) <= int(overlay["is_fraud"].sum())


def test_fraud_looks_riskier_than_legitimate_authorisations(code):
    synth, _, _ = code
    authorisations, signals, _ = _history(synth, rows=50_000)
    joined = authorisations.join(signals, on=["card_number", "transaction_time"])
    by_label = (
        joined.group_by("fraud_flag")
        .agg(
            pl.col("issuer_fraud_score").mean(),
            pl.col("amount_eur").median(),
            pl.col("merchant_distance_km").mean(),
        )
        .sort("fraud_flag")
    )
    legit, fraud = by_label.row(0, named=True), by_label.row(1, named=True)
    assert fraud["issuer_fraud_score"] > legit["issuer_fraud_score"] * 1.5
    assert fraud["amount_eur"] > legit["amount_eur"]
    assert fraud["merchant_distance_km"] > legit["merchant_distance_km"]


def test_the_values_are_european(code):
    synth, _, _ = code
    authorisations, _, _ = _history(synth)
    assert set(authorisations["authorisation_region"].unique()) <= set(synth.REGIONS)
    assert set(authorisations["card_scheme"].unique()) <= set(synth.CARD_SCHEMES)
    assert (
        authorisations.filter(pl.col("authorisation_region") == "EEA").height
        > authorisations.height * 0.7
    )
    # Nullable ids stay strings with nulls.
    assert authorisations.schema["wallet_id"] == pl.String
    assert 0 < authorisations["token_requestor_id"].null_count() < authorisations.height


def test_the_default_history_is_a_million_authorisations(code):
    _, generate_data, _ = code
    assert generate_data.DEFAULT_ROWS == 1_000_000


def test_the_live_stream_uses_the_historys_cards(code):
    synth, generate_data, transaction_stream = code
    rows = 50_000
    history = synth.generate_card_pool(
        generate_data.card_count(rows), np.random.default_rng(42)
    )
    universe = transaction_stream.build_universe(rows, 42)
    assert set(universe["cards"]) == set(history)
    frame = transaction_stream.generate_second(universe, 20, 0.5, first_id=1_000)
    assert frame["transaction_id"].to_list() == list(range(1_000, 1_020))
    assert set(frame["card_number"]) <= set(history)
    expected = [f.name for f in generate_data.AUTHORISATIONS["features"]]
    assert frame.columns == expected


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="the system code runs on the cluster's Python 3.13 and imports datetime.UTC",
)
def test_the_dbt_models_return_the_feature_groups_columns_in_order(code, monkeypatch):
    # dbt_run_graph ships only in the dbt-pipeline image.
    monkeypatch.setitem(sys.modules, "dbt_run_graph", type(sys)("dbt_run_graph"))
    monkeypatch.syspath_prepend(str(DEBITCARD))
    import batch_features

    for name, model in batch_features.MODELS.items():
        sql = (DEBITCARD / "dbt" / "models" / f"{name}.sql").read_text()
        final_select = sql.rsplit("\nselect\n", 1)[1].split("\nfrom ")[0]
        columns = [
            re.split(r"\s+as\s+|\s", line.strip().rstrip(","))[-1]
            for line in final_select.splitlines()
            if line.strip()
        ]
        assert columns == [f.name for f in model["features"]], name
    sys.modules.pop("batch_features")


def test_the_model_reads_what_the_streaming_job_and_the_baselines_write():
    model_source = (DEBITCARD / "model.py").read_text()
    streaming = (DEBITCARD / "streaming_features.py").read_text()
    window = re.search(r"WINDOW_FEATURES = \((.*?)\)", model_source, re.S).group(1)
    baseline = re.search(r"BASELINE_FEATURES = \((.*?)\)", model_source, re.S).group(1)
    assert set(re.findall(r'"(\w+)"', window)) <= set(
        re.findall(r'Feature\(\s*"(\w+)"', streaming)
    )
    baselines_sql = (
        DEBITCARD / "dbt" / "models" / "card_spending_baselines.sql"
    ).read_text()
    for name in re.findall(r'"(\w+)"', baseline):
        assert name in baselines_sql


def test_the_first_window_counts_from_when_the_stream_started():
    pytest.importorskip("sklearn")
    pd = pytest.importorskip("pandas")
    sys.path.insert(0, str(DEBITCARD))
    try:
        import model
    finally:
        sys.path.remove(str(DEBITCARD))
        sys.modules.pop("model", None)
    window = pd.DataFrame(
        {"window_end": [pd.Timestamp("2026-10-25 16:00")], "window_seconds": [3600]}
    )
    now = pd.Timestamp("2026-10-25 15:55", tz="UTC")
    assert model.elapsed_hours(window, now)[0] == pytest.approx(55 / 60)
    assert model.elapsed_hours(window, now, "2026-10-25T15:40:00+00:00")[
        0
    ] == pytest.approx(15 / 60)
    # The next hour's window opened after the stream started, so `since` leaves it alone.
    later = pd.DataFrame(
        {"window_end": [pd.Timestamp("2026-10-25 17:00")], "window_seconds": [3600]}
    )
    at = pd.Timestamp("2026-10-25 16:30", tz="UTC")
    assert model.elapsed_hours(later, at, "2026-10-25T15:40:00+00:00")[
        0
    ] == pytest.approx(0.5)
