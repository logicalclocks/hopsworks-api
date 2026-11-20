"""The ecommerce generator, without a cluster: python -m pytest tests in the system's directory."""

from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest


_spec = importlib.util.spec_from_file_location(
    "ecommerce", Path(__file__).resolve().parents[1] / "ecommerce.py"
)
ec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ec)

END = date(2026, 11, 14)
DAYS = 60


@pytest.fixture(scope="module")
def tables():
    return ec.history(END, DAYS)


def test_keys_are_unique(tables):
    for table, spec in ec.TABLES.items():
        assert tables[table][spec["primary_key"]].duplicated().sum() == 0, table


def test_columns_are_the_documented_features(tables):
    for table, spec in ec.TABLES.items():
        assert list(tables[table].columns) == list(spec["features"]), table


def test_lines_reference_their_order_customer_and_product(tables):
    items, orders = tables["order_items"], tables["orders"]
    assert set(items["order_id"]) == set(orders["order_id"])
    assert set(items["customer_id"]) <= set(tables["customers"]["customer_id"])
    assert set(items["product_id"]) <= set(tables["products"]["product_id"])
    merged = items.merge(orders, on="order_id", suffixes=("", "_order"))
    assert (merged["customer_id"] == merged["customer_id_order"]).all()
    assert (merged["status"] == merged["status_order"]).all()
    assert (merged["ordered_at"] == merged["ordered_at_order"]).all()


def test_amounts_add_up(tables):
    items, orders = tables["order_items"], tables["orders"]
    assert (
        abs(items["gross_amount"] - items["quantity"] * items["unit_price"]) < 0.011
    ).all()
    assert (
        abs(items["net_amount"] - (items["gross_amount"] - items["discount_amount"]))
        < 0.011
    ).all()
    assert (
        abs(items["profit_amount"] - (items["net_amount"] - items["cost_amount"]))
        < 0.011
    ).all()
    totals = (
        items.groupby("order_id")[["net_amount", "quantity"]]
        .sum()
        .reindex(orders["order_id"])
    )
    assert (
        abs(totals["net_amount"].to_numpy() - orders["net_amount"].to_numpy()) < 0.05
    ).all()
    assert (totals["quantity"].to_numpy() == orders["units"].to_numpy()).all()
    assert (
        orders.loc[orders["net_amount"] >= ec.FREE_SHIPPING_FROM, "shipping_fee"] == 0
    ).all()


def test_values_are_the_documented_ones(tables):
    orders, customers = tables["orders"], tables["customers"]
    assert set(orders["status"]) == {"completed", "returned", "cancelled"}
    assert set(orders["channel"]) == {"Web", "Mobile app", "Marketplace"}
    assert set(orders["customer_type"]) == {"new", "returning"}
    assert set(customers["region"]) == set(ec.REGIONS)
    assert set(customers["segment"]) == set(ec.SEGMENTS)
    assert set(tables["products"]["category"]) == set(ec.CATEGORIES)


def test_orders_fall_in_the_history(tables):
    ordered = tables["orders"]["ordered_at"]
    assert ordered.min() >= pd.Timestamp(END - timedelta(days=DAYS))
    assert ordered.max() < pd.Timestamp(END)
    signup = tables["customers"].set_index("customer_id")["signup_ts"]
    placed = tables["orders"].set_index("customer_id")["ordered_at"]
    assert (signup.reindex(placed.index).to_numpy() <= placed.to_numpy()).all()


def test_a_new_customer_orders_on_the_day_they_sign_up(tables):
    orders = tables["orders"]
    signup = tables["customers"].set_index("customer_id")["signup_ts"]
    new = orders[orders["customer_type"] == "new"]
    assert len(new) > 0
    assert (
        signup.loc[new["customer_id"]].dt.date.to_numpy()
        == new["ordered_at"].dt.date.to_numpy()
    ).all()


def test_a_day_is_the_same_on_every_run():
    day = date(2026, 3, 3)
    customers, products = (
        ec.customers_until(day + timedelta(days=1)),
        ec.make_products(),
    )
    first = ec.make_orders(day, customers, products)
    second = ec.make_orders(day, customers, products)
    pd.testing.assert_frame_equal(first[0], second[0])
    pd.testing.assert_frame_equal(first[1], second[1])


def test_the_daily_job_writes_what_the_backfill_wrote_for_the_same_day():
    day = date(2026, 11, 10)
    daily = ec.days_of(day, day + timedelta(days=1))
    backfill = ec.history(day + timedelta(days=1), 1)
    pd.testing.assert_frame_equal(daily["orders"], backfill["orders"])
    pd.testing.assert_frame_equal(daily["order_items"], backfill["order_items"])
    signed_up = backfill["customers"][backfill["customers"]["signup_ts"].dt.date == day]
    pd.testing.assert_frame_equal(daily["customers"], signed_up.reset_index(drop=True))


def test_black_friday_is_the_busiest_day():
    friday = ec._black_friday(2025)
    assert friday == date(2025, 11, 28)
    days = [friday + timedelta(days=i) for i in range(-14, 15)]
    assert max(days, key=ec.demand) == friday
    assert ec.promotion(friday)[0] == "black_friday"


def test_volume_grows_and_asia_pacific_grows_fastest():
    early, late = date(2024, 3, 1), date(2026, 3, 1)
    assert sum(ec.region_weights(late).values()) > 1.4 * sum(
        ec.region_weights(early).values()
    )
    growth = {
        r: ec.region_weights(late)[r] / ec.region_weights(early)[r] for r in ec.REGIONS
    }
    assert max(growth, key=growth.get) == "Asia Pacific"
