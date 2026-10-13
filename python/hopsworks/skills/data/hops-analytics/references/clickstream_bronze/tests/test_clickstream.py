"""The clickstream generator, without a cluster: python -m pytest tests in the layer's directory."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


_spec = importlib.util.spec_from_file_location(
    "clickstream", Path(__file__).resolve().parents[1] / "clickstream.py"
)
cs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cs)

END = datetime(2026, 10, 7)
START = END - timedelta(days=cs.HISTORY_DAYS)


@pytest.fixture(scope="module")
def history():
    rng = cs._rng(START, 1)
    signup = pd.concat(
        [
            cs._times(rng, END - timedelta(days=1095), START, 1800),
            cs._times(rng, START, END, 200),
        ],
        ignore_index=True,
    )
    customers = cs.make_customers(rng, 1, 2000, signup)
    products = cs.make_products(
        rng, 1, 200, cs._times(rng, END - timedelta(days=730), START, 200)
    )
    orders = cs.make_orders(
        rng, 1, cs._times(rng, START, END, 3000), customers, products, END
    )
    return customers, products, orders


def _hour(customers, products, at=END):
    return cs.make_clicks(
        cs._rng(at, 2),
        at,
        at + timedelta(hours=1),
        cs.HOURLY_CLICKS,
        customers,
        products,
    )


def test_an_hour_of_clicks_stays_in_its_window_in_ordered_sessions(history):
    customers, products, _ = history
    clicks = _hour(customers, products)
    assert len(clicks) >= cs.HOURLY_CLICKS
    assert (
        clicks["event_ts"]
        .between(END, END + timedelta(hours=1), inclusive="left")
        .all()
    )
    assert (
        clicks.groupby("session_id")["event_ts"]
        .apply(lambda s: s.is_monotonic_increasing)
        .all()
    )
    assert (clicks["ingested_at"] >= clicks["event_ts"]).all()
    assert clicks["ingest_id"].is_unique
    assert set(clicks["event_date"]) == {"2026-10-07"}


def test_a_replayed_window_writes_the_same_rows(history):
    customers, products, orders = history
    assert _hour(customers, products).equals(_hour(customers, products))
    day = (END, END + timedelta(days=1))
    first = cs.daily_changes(cs._rng(END, 3), *day, customers, products, orders)
    again = cs.daily_changes(cs._rng(END, 3), *day, customers, products, orders)
    assert all(first[t].equals(again[t]) for t in first)


def test_about_one_click_in_a_hundred_thousand_arrives_twice(history):
    customers, products, _ = history
    assert len(cs._duplicates(np.random.default_rng(0), cs.N_CLICKS)) == 10
    extra = sum(
        len(_hour(customers, products, END + timedelta(hours=h))) - cs.HOURLY_CLICKS
        for h in range(100)
    )
    # 0.1 a hour expected: 10 in 100 hours, and never anywhere near 1%.
    assert 2 <= extra <= 25
    clicks = cs.make_clicks(cs._rng(START, 9), START, END, 200_000, customers, products)
    twice = clicks[clicks["click_id"].duplicated(keep=False)]
    assert len(twice) == 4 and twice["ingest_id"].is_unique


def test_clicks_reference_known_customers_and_active_products(history):
    customers, products, _ = history
    products = products.copy()
    products.loc[products["product_id"] <= 50, "is_active"] = False
    clicks = _hour(customers, products)
    known = clicks["customer_id"].dropna()
    assert known.isin(customers["customer_id"]).all()
    assert 0.5 < len(known) / len(clicks) < 0.7
    viewed = clicks["product_id"].dropna()
    assert (viewed > 50).all()
    assert (
        clicks.loc[clicks["product_id"].isna(), "event_type"]
        .isin(["page_view", "search", "begin_checkout", "purchase"])
        .all()
    )


def test_orders_are_placed_after_signup_with_lines_that_add_up(history):
    customers, _, orders = history
    signup = customers.set_index("customer_id")["signup_ts"]
    assert (
        orders["order_ts"].to_numpy() >= signup.loc[orders["customer_id"]].to_numpy()
    ).all()
    for _, order in orders.head(200).iterrows():
        lines = json.loads(order["items"])
        assert len(lines) == order["n_items"]
        total = sum(line["quantity"] * line["unit_price"] for line in lines)
        assert order["total_amount"] == pytest.approx(total, abs=0.01)
    # Old orders have settled; the newest are still open.
    old = orders[orders["order_ts"] < END - timedelta(days=5)]
    assert old["status"].isin(["delivered", "cancelled", "returned"]).all()
    assert (
        orders[orders["order_ts"] > END - timedelta(hours=2)]["status"]
        .isin(["placed", "paid"])
        .all()
    )


def test_a_day_adds_rows_after_the_existing_ids_and_changes_some(history):
    customers, products, orders = history
    changes = cs.daily_changes(
        cs._rng(END, 3), END, END + timedelta(days=1), customers, products, orders
    )
    new = changes["customers"][changes["customers"]["customer_id"] > 2000]
    assert 15 < len(new) < 60
    assert new["customer_id"].min() == 2001 and new["customer_id"].is_unique
    assert (
        new["signup_ts"].between(END, END + timedelta(days=1), inclusive="left").all()
    )
    changed = changes["customers"][changes["customers"]["customer_id"] <= 2000]
    assert changed["updated_at"].min() >= END
    prices = changes["products"]
    assert (prices["product_id"] > 200).sum() <= 10
    assert changes["orders"]["order_id"].is_unique
    new_orders = changes["orders"][changes["orders"]["order_id"] > 3000]
    assert 550 < len(new_orders) < 750 and new_orders["order_id"].min() == 3001
    moved = changes["orders"][changes["orders"]["order_id"] <= 3000]
    before = orders.set_index("order_id").loc[moved["order_id"], "status"]
    assert (before.to_numpy() != moved["status"].to_numpy()).all()


def test_a_scheduled_run_takes_its_window_from_the_variables(monkeypatch):
    args = cs.parse_args(["--mode", "clicks", "-start_time", "2026-10-07T02:00:00Z"])
    assert args.mode == "clicks"
    monkeypatch.setenv("HOPS_START_TIME", "2026-10-07T01:00:00Z")
    monkeypatch.setenv("HOPS_END_TIME", "2026-10-07T02:00:00Z")
    assert cs._window(1) == (datetime(2026, 10, 7, 1), datetime(2026, 10, 7, 2))
    with pytest.raises(SystemExit):
        cs.parse_args(["--mode", "clicks", "--stat_time", "x"])
