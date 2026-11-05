"""The leasing example's reference code: the generated history, the transformations and the app's scoring."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import numpy as np
import pytest


pd = pytest.importorskip("pandas")


LEASING = (
    Path(__file__).resolve().parents[2]
    / "hopsworks"
    / "skills"
    / "ml"
    / "hops-reqs"
    / "references"
    / "leasing"
)
MODULES = ("names", "synth", "generate_data", "transformations", "scoring")
END = pd.Timestamp("2026-10-30 06:00:00")


@pytest.fixture(scope="module")
def code():
    sys.path.insert(0, str(LEASING))
    try:
        import generate_data
        import names
        import scoring
        import synth
        import transformations

        yield names, synth, generate_data, transformations, scoring
    finally:
        sys.path.remove(str(LEASING))
        for name in MODULES:
            sys.modules.pop(name, None)


def _columns(out):
    """A multi-output transformation's columns as lists."""
    if isinstance(out, pd.DataFrame):
        return [out[c].tolist() for c in out.columns]
    return [list(c) for c in out]


@pytest.fixture(scope="module")
def history(code):
    _, _, generate_data, _, _ = code
    return generate_data.tables(2_000, 4_000, END)


def test_every_table_is_keyed_and_timed_as_its_feature_group(code, history):
    names, _, generate_data, _, _ = code
    assert set(history) == set(generate_data.GROUPS)
    for name, frame in history.items():
        spec = generate_data.GROUPS[name]
        # Every group has an event time, or the feature view's joins are not point-in-time.
        key = spec["primary_key"] + [spec["event_time"]]
        assert set(key) <= set(frame.columns), name
        # The offline upsert key: a duplicate would silently be one row.
        assert not frame.duplicated(key).any(), name
        assert str(frame[spec["event_time"]].dtype) == "datetime64[us]", name
    assert "damage_risk" not in history[names.EQUIPMENT].columns


def test_the_history_is_one_world_in_time_order(code, history):
    names, _, _, _, _ = code
    orders, outcomes = history[names.ORDERS], history[names.OUTCOMES]
    credit, equipment = history[names.CREDIT], history[names.EQUIPMENT]
    assert len(orders) == 2_000 and len(equipment) == names.EQUIPMENT_ITEMS
    assert set(outcomes["order_id"]) == set(orders["order_id"])
    # Every machine is listed, and every customer reported, before their first order.
    assert equipment["listed_ts"].max() < orders["order_ts"].min()
    first_report = credit.groupby("customer_id")["report_ts"].min()
    assert (
        orders["order_ts"].to_numpy()
        > first_report.loc[orders["customer_id"]].to_numpy()
    ).all()
    # Each lease ends by the end of the history; the online store keeps the newest per customer.
    assert outcomes["outcome_ts"].max() <= END
    assert outcomes["outcome_ts"].is_monotonic_increasing
    joined = orders.merge(outcomes, on=["order_id", "customer_id"])
    declined = joined["outcome"] == "declined"
    assert (declined == (joined[names.LABEL] == 0)).all()
    assert (joined.loc[joined["problem_free"] == 1, "outcome"] == "paid").all()


def test_underwriters_lease_more_to_customers_with_less_risk(code, history):
    names, synth, _, _, _ = code
    customers, _, _ = synth.world(names.SEED, names.CUSTOMERS, names.EQUIPMENT_ITEMS)
    orders = history[names.ORDERS]
    risk = customers.set_index("customer_id").loc[orders["customer_id"], "risk"]
    low = (risk <= risk.median()).to_numpy()
    leased = orders[names.LABEL].to_numpy()
    assert leased[low].mean() > leased[~low].mean() + 0.1
    assert 0.5 < leased.mean() < 0.9


def test_the_world_is_rebuilt_from_its_seed(code):
    names, synth, _, _, _ = code
    a, b = (synth.world(names.SEED, 50, 20) for _ in range(2))
    pd.testing.assert_frame_equal(a[0], b[0])
    pd.testing.assert_frame_equal(a[1], b[1])


def test_the_simulation_settles_every_order_now(code, history):
    names, synth, _, _, _ = code
    customers, equipment, _ = synth.world(
        names.SEED, names.CUSTOMERS, names.EQUIPMENT_ITEMS
    )
    new = pd.DataFrame(
        {
            "order_id": [9001, 9002, 9003],
            "customer_id": [1, 2, 3],
            "equipment_id": [1, 2, 3],
            "lease_months": [1, 3, 6],
            "order_ts": pd.to_datetime([END] * 3).astype("datetime64[us]"),
        }
    )
    decided = synth.decide(
        new,
        customers,
        equipment,
        pd.Series({1: "paid", 2: "defaulted"}),
        np.random.default_rng(3),
        settled_at=END,
    )
    assert list(decided["order_id"]) == [9001, 9002, 9003]
    assert (decided["outcome_ts"] == END).all()
    assert ((decided["outcome"] == "declined") == (decided[names.LABEL] == 0)).all()


def test_the_prior_lease_counts_only_leases_that_were_paid_without_problems(
    code, mocker
):
    mocker.patch("hsfs.engine._get_type", return_value="python")
    transformations = code[3]
    out = transformations.prior_lease.executor().execute(
        pd.Series([None, "declined", "paid", "paid_late", "defaulted"]),
        pd.Series([None, 0, 1, 1, 0]),
        pd.Series([None, 0, 1, 0, 0]),
        pd.Series([None, 0, 0, 12, 90]),
        pd.Series([None, 0.0, 0.0, 0.0, 0.0]),
    )
    leased, paid, problem_free, days_late, _ = _columns(out)
    assert leased == [0, 0, 1, 1, 1]
    assert paid == [0, 0, 1, 1, 0]
    assert problem_free == [0, 0, 1, 0, 0]
    assert days_late == [0.0, 0.0, 0.0, 12.0, 90.0]


def test_affordability_and_the_statistics_based_transformations(code, mocker):
    mocker.patch("hsfs.engine._get_type", return_value="python")
    transformations = code[3]
    out = transformations.affordability.executor().execute(
        pd.Series([100.0, 100.0]), pd.Series([1000.0, 0.0]), pd.Series([12000.0, 0.0])
    )
    payment, debt = _columns(out)
    assert payment == [0.1, 100.0]  # income clipped at 1, never a division by zero
    assert debt == [1.0, 0.0]
    z = transformations.credit_score_z.executor(
        statistics={"credit_score": {"mean": 650.0, "std_dev": 50.0}}
    ).execute(pd.Series([700, 600]))
    assert list(z) == [1.0, -1.0]
    ratio = transformations.price_to_typical.executor(
        statistics={"monthly_price": {"mean": 50.0}}
    ).execute(pd.Series([100.0]))
    assert list(ratio) == [2.0]


def test_the_app_asks_the_feature_view_for_a_new_orders_features(code):
    *_, scoring = code
    fv = mock.Mock()
    orders = pd.DataFrame(
        {"customer_id": [1, 2], "equipment_id": [5, 6], "lease_months": [3, 12]}
    )
    scoring.Scorer.untransformed(fv, orders)
    # The machine and the customer by their keys, the lease length passed in: the order is new.
    fv.get_feature_vectors.assert_called_once_with(
        serving_keys=[
            {"customer_id": 1, "equipment_id": 5},
            {"customer_id": 2, "equipment_id": 6},
        ],
        passed_features=[{"lease_months": 3}, {"lease_months": 12}],
        allow_missing=True,
        transform=False,
        return_type="pandas",
    )


def test_train_selects_what_the_app_reads():
    source = (LEASING / "train.py").read_text()
    for column in (
        "monthly_income",
        "total_debt",
        "credit_score",
        "monthly_price",
        "lease_months",
    ):
        assert f'"{column}"' in source
    assert 'prefix="prev_"' in source
