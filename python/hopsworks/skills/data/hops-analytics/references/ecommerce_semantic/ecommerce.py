"""Generated orders of an online shop: the tables of the ecommerce semantic layer blueprint.

One program for the system's two jobs, picked with --mode:

- backfill: create the four feature groups and fill them with two years of history up to the last
  midnight (UTC): every customer who signed up before the end, the product catalog, and every order
  and order line of the 730 days.
- daily: write one day of new customers, orders and order lines.

The tables are a star schema around order lines, modeled for consumption: ecommerce_order_items (one
row per order line, the fact), ecommerce_orders, ecommerce_customers and ecommerce_products.

Every row is a function of its day: a day's customers, orders and lines are drawn from a generator
seeded with the day, and their keys are derived from it, so a replayed day rewrites the same rows and
the daily job reads nothing. Volume grows about 30% a year from 2023, faster in Asia Pacific and Latin
America; weekends are busier, January is quiet, November and December peak, with Black Friday and Cyber
Monday spikes, and site-wide sales discount most orders.

A scheduled run writes the days of the window Hopsworks passes in HOPS_START_TIME and HOPS_END_TIME;
without them, daily writes the last full day.
Every table is an offline Delta feature group tagged analytics_table {"layer": "gold"}.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd


# Growth, seasonality and keys count days from these dates, so every day is generated the same way
# whenever, and by whichever job, it is written.
EPOCH = date(2020, 1, 1)
ANCHOR = date(2023, 1, 1)
POOL_SIZE = 12_000  # customers who signed up between EPOCH and ANCHOR
HISTORY_DAYS = 730
BASE_ORDERS = 95  # orders a day at ANCHOR, before seasonality
BASE_SIGNUPS = 22  # new customers a day at ANCHOR
FIRST_ORDER_SHARE = (
    0.7  # of a day's new customers, those who order on the day they sign up
)
PREFIX = "ecommerce_"
TAG = "analytics_table"
SEED = 20230101

# Each region: its share of orders at ANCHOR, its yearly growth, and its countries with their share
# of the region and their cities.
REGIONS = {
    "North America": (
        0.44,
        1.22,
        {
            "United States": (
                0.82,
                ["New York", "Chicago", "Austin", "Seattle", "Atlanta"],
            ),
            "Canada": (0.18, ["Toronto", "Vancouver", "Montreal"]),
        },
    ),
    "Europe": (
        0.34,
        1.28,
        {
            "United Kingdom": (0.26, ["London", "Manchester", "Leeds"]),
            "Germany": (0.24, ["Berlin", "Munich", "Hamburg"]),
            "France": (0.18, ["Paris", "Lyon", "Lille"]),
            "Spain": (0.12, ["Madrid", "Barcelona", "Valencia"]),
            "Sweden": (0.10, ["Stockholm", "Gothenburg", "Malmo"]),
            "Netherlands": (0.10, ["Amsterdam", "Rotterdam", "Utrecht"]),
        },
    ),
    "Asia Pacific": (
        0.15,
        1.55,
        {
            "Japan": (0.34, ["Tokyo", "Osaka", "Nagoya"]),
            "Australia": (0.28, ["Sydney", "Melbourne", "Brisbane"]),
            "India": (0.24, ["Mumbai", "Bengaluru", "Delhi"]),
            "Singapore": (0.14, ["Singapore"]),
        },
    ),
    "Latin America": (
        0.07,
        1.42,
        {
            "Brazil": (0.62, ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte"]),
            "Mexico": (0.38, ["Mexico City", "Guadalajara", "Monterrey"]),
        },
    ),
}
# Each segment: its share of customers, how often its customers order relative to a consumer, and
# the mean extra lines per order and extra units per line.
SEGMENTS = {
    "Consumer": (0.74, 1.0, 0.5, 0.1),
    "Small Business": (0.19, 2.2, 1.0, 0.4),
    "Enterprise": (0.07, 3.5, 2.0, 1.5),
}
LOYALTY = {"Bronze": (0.62, 1.0), "Silver": (0.27, 2.0), "Gold": (0.11, 3.5)}
ACQUISITION = {
    "Organic search": 0.29,
    "Paid search": 0.21,
    "Social": 0.17,
    "Referral": 0.13,
    "Marketplace": 0.11,
    "Email": 0.09,
}
PAYMENTS = {
    "Card": 0.52,
    "PayPal": 0.20,
    "Apple Pay": 0.12,
    "Klarna": 0.10,
    "Bank transfer": 0.06,
}
# Each category: subcategories, brands, list price range, gross margin, the share of its orders
# returned, its share of order lines, and how much more it sells in November and December.
CATEGORIES = {
    "Electronics": (
        ["Phones", "Laptops", "Audio", "Accessories"],
        ["Voltix", "Nordcell", "Arcwave"],
        (15, 1200),
        0.18,
        0.07,
        0.18,
        1.5,
    ),
    "Home & Kitchen": (
        ["Cookware", "Furniture", "Decor", "Appliances"],
        ["Hearthly", "Oakline", "Kitchora"],
        (10, 600),
        0.34,
        0.05,
        0.17,
        1.25,
    ),
    "Apparel": (
        ["Women", "Men", "Kids", "Shoes"],
        ["Northwind Wear", "Loom & Lark", "Stride"],
        (10, 160),
        0.52,
        0.16,
        0.22,
        1.2,
    ),
    "Beauty": (
        ["Skincare", "Makeup", "Fragrance", "Hair"],
        ["Lumen", "Petalis", "Verde"],
        (5, 90),
        0.56,
        0.03,
        0.13,
        1.35,
    ),
    "Sports & Outdoors": (
        ["Fitness", "Camping", "Cycling", "Running"],
        ["Summitline", "Kinetiq", "Trailmark"],
        (12, 700),
        0.31,
        0.06,
        0.12,
        1.0,
    ),
    "Books & Media": (
        ["Fiction", "Non-fiction", "Games", "Music"],
        ["Paperlane", "Inkwell", "Playforge"],
        (5, 60),
        0.24,
        0.02,
        0.10,
        1.3,
    ),
    "Toys": (
        ["Building sets", "Puzzles", "Dolls", "Outdoor play"],
        ["Brickly", "Puzzleworks", "Funhaus"],
        (8, 140),
        0.41,
        0.04,
        0.08,
        1.9,
    ),
}
PRODUCTS_PER_CATEGORY = 60
# Demand by weekday (Monday first) and by month.
WEEKDAY = [0.95, 0.93, 0.96, 0.98, 1.04, 1.12, 1.10]
MONTH = [0.82, 0.86, 0.95, 0.97, 1.00, 0.98, 1.05, 0.96, 0.98, 1.02, 1.30, 1.55]
CANCELLED = {"Web": 0.025, "Mobile app": 0.022, "Marketplace": 0.045}
FREE_SHIPPING_FROM = 50.0
SHIPPING_FEE = {
    "North America": 4.99,
    "Europe": 4.99,
    "Asia Pacific": 7.99,
    "Latin America": 7.99,
}

TABLES = {
    "customers": {
        "primary_key": ["customer_id"],
        "event_time": "signup_ts",
        "description": "Customers of the online shop, one row per customer, written when they sign up.",
        "features": {
            "customer_id": "The customer's id.",
            "region": "Sales region of the customer's address: North America, Europe, Asia Pacific or Latin America.",
            "country": "Country of the customer's address.",
            "city": "City of the customer's address.",
            "segment": "Customer segment: Consumer, Small Business or Enterprise.",
            "loyalty_tier": "Loyalty program tier: Bronze, Silver or Gold.",
            "acquisition_channel": "How the customer found the shop: Organic search, Paid search, Social, Referral, Marketplace or Email.",
            "signup_ts": "When the customer signed up (UTC).",
            "signup_month": "The month the customer signed up, as YYYY-MM: the customer's cohort.",
        },
    },
    "products": {
        "primary_key": ["product_id"],
        "event_time": "launched_at",
        "description": "The product catalog, one row per product.",
        "features": {
            "product_id": "The product's id.",
            "product_name": "The product's name.",
            "category": "Product category, such as Electronics or Apparel.",
            "subcategory": "Product subcategory within its category.",
            "brand": "The product's brand.",
            "price_band": "budget (list price under 25), mid (under 100) or premium.",
            "list_price": "List price of one unit, in USD.",
            "unit_cost": "What one unit costs the shop, in USD.",
            "launched_at": "When the product was added to the catalog (UTC).",
        },
    },
    "orders": {
        "primary_key": ["order_id"],
        "event_time": "ordered_at",
        "description": "Orders, one row per order, with the order's totals in USD.",
        "features": {
            "order_id": "The order's id.",
            "customer_id": "The customer who placed the order.",
            "ordered_at": "When the order was placed (UTC).",
            "channel": "Where the order was placed: Web, Mobile app or Marketplace.",
            "payment_method": "How the order was paid: Card, PayPal, Apple Pay, Klarna or Bank transfer.",
            "promotion": "The discount the order had: black_friday, summer_sale, holiday_sale, coupon or none.",
            "status": "completed, returned (refunded in full) or cancelled (never shipped).",
            "customer_type": "new for a customer's first order, placed the day they signed up; returning otherwise.",
            "item_count": "Number of order lines.",
            "units": "Units ordered, over all lines.",
            "gross_amount": "List value of the units ordered, before discounts, in USD.",
            "discount_amount": "Discount given on the order, in USD.",
            "net_amount": "What the customer paid for the items, gross_amount minus discount_amount, in USD.",
            "shipping_fee": "Shipping charged, in USD; free from 50 USD of net amount.",
        },
    },
    "order_items": {
        "primary_key": ["order_item_id"],
        "event_time": "ordered_at",
        "description": "Order lines, one row per product in an order: the fact table of the ecommerce semantic layer. Amounts in USD.",
        "features": {
            "order_item_id": "The order line's id.",
            "order_id": "The order the line belongs to.",
            "customer_id": "The customer who placed the order.",
            "product_id": "The product ordered.",
            "ordered_at": "When the order was placed (UTC).",
            "status": "The order's status: completed, returned or cancelled.",
            "quantity": "Units ordered.",
            "unit_price": "List price of one unit when ordered.",
            "gross_amount": "quantity times unit_price.",
            "discount_amount": "Discount on the line.",
            "net_amount": "gross_amount minus discount_amount: what the customer paid for the line.",
            "cost_amount": "What the line's units cost the shop.",
            "profit_amount": "net_amount minus cost_amount.",
        },
    },
}


def _day_no(day: date) -> int:
    return (day - EPOCH).days


def _rng(day: date, salt: int) -> np.random.Generator:
    """A generator seeded with the day, so a replayed day draws the same rows."""
    return np.random.default_rng([SEED, _day_no(day), salt])


def _pick(rng: np.random.Generator, shares: dict, n: int) -> np.ndarray:
    names = list(shares)
    p = np.array(
        [shares[k] if not isinstance(shares[k], tuple) else shares[k][0] for k in names]
    )
    return np.array(names, dtype=object)[rng.choice(len(names), n, p=p / p.sum())]


def _years(day: date) -> float:
    return (day - ANCHOR).days / 365.25


def region_weights(day: date) -> dict[str, float]:
    """Each region's orders relative to all orders at ANCHOR: its share grown at its own rate."""
    years = _years(day)
    return {r: share * growth**years for r, (share, growth, _) in REGIONS.items()}


def _black_friday(year: int) -> date:
    november = date(year, 11, 1)
    return november + timedelta(days=(4 - november.weekday()) % 7 + 21)


def demand(day: date) -> float:
    """How busy a day is relative to an ordinary weekday at the same point of the trend."""
    factor = WEEKDAY[day.weekday()] * MONTH[day.month - 1]
    friday = _black_friday(day.year)
    spikes = {
        friday: 3.2,
        friday + timedelta(days=1): 2.0,
        friday + timedelta(days=2): 1.6,
        friday + timedelta(days=3): 2.4,  # Cyber Monday
        date(day.year, 12, 26): 1.4,
    }
    return factor * spikes.get(day, 1.0)


def promotion(day: date) -> tuple[str, float, float]:
    """The site-wide sale on a day: its name, the share of orders that use it, and its discount."""
    friday = _black_friday(day.year)
    if friday <= day <= friday + timedelta(days=3):
        return "black_friday", 0.85, 0.25
    if day.month == 7 and 10 <= day.day <= 24:
        return "summer_sale", 0.60, 0.15
    if day.month == 12 and day.day >= 26:
        return "holiday_sale", 0.50, 0.20
    return "none", 0.0, 0.0


def make_products() -> pd.DataFrame:
    """The catalog: the same products on every run, launched between EPOCH and ANCHOR."""
    rng = np.random.default_rng([SEED, 1])
    rows = []
    for c, (
        category,
        (subcategories, brands, (low, high), margin, _, _, _),
    ) in enumerate(CATEGORIES.items()):
        for i in range(PRODUCTS_PER_CATEGORY):
            product_id = c * 1000 + i + 1
            subcategory = subcategories[i % len(subcategories)]
            brand = brands[rng.integers(len(brands))]
            # Skewed to the low end of the range: most products are cheap, a few expensive.
            price = float(low * (high / low) ** (rng.uniform() ** 2.5))
            price = round(max(low, price) - 0.01, 2) if price > 10 else round(price, 2)
            cost = round(price * (1 - margin * rng.uniform(0.8, 1.2)), 2)
            rows.append(
                {
                    "product_id": product_id,
                    "product_name": f"{brand} {subcategory} {chr(65 + i % 26)}{100 + i}",
                    "category": category,
                    "subcategory": subcategory,
                    "brand": brand,
                    "price_band": "budget"
                    if price < 25
                    else "mid"
                    if price < 100
                    else "premium",
                    "list_price": price,
                    "unit_cost": cost,
                    "launched_at": pd.Timestamp(EPOCH)
                    + pd.Timedelta(
                        seconds=int(rng.integers(0, (ANCHOR - EPOCH).days * 86400))
                    ),
                }
            )
    return pd.DataFrame(rows)


def _customers(
    rng: np.random.Generator, ids: np.ndarray, signup: pd.Series, regions: np.ndarray
) -> pd.DataFrame:
    n = len(ids)
    countries = np.empty(n, dtype=object)
    cities = np.empty(n, dtype=object)
    for region, (_, _, places) in REGIONS.items():
        rows = np.flatnonzero(regions == region)
        if len(rows) == 0:
            continue
        picked = _pick(rng, places, len(rows))
        countries[rows] = picked
        cities[rows] = [places[c][1][rng.integers(len(places[c][1]))] for c in picked]
    signup = pd.Series(pd.to_datetime(signup).to_numpy())
    return pd.DataFrame(
        {
            "customer_id": ids.astype("int64"),
            "region": regions,
            "country": countries,
            "city": cities,
            "segment": _pick(rng, SEGMENTS, n),
            "loyalty_tier": _pick(rng, LOYALTY, n),
            "acquisition_channel": _pick(rng, ACQUISITION, n),
            "signup_ts": signup,
            "signup_month": signup.dt.strftime("%Y-%m"),
        }
    )


def _regions(rng: np.random.Generator, day: date, n: int) -> np.ndarray:
    return _pick(rng, region_weights(day), n)


def make_pool() -> pd.DataFrame:
    """The customers who signed up between EPOCH and ANCHOR, the same on every run."""
    rng = np.random.default_rng([SEED, 2])
    seconds = np.sort(rng.integers(0, (ANCHOR - EPOCH).days * 86400, POOL_SIZE))
    signup = pd.Timestamp(EPOCH) + pd.to_timedelta(seconds, unit="s")
    return _customers(
        rng, np.arange(1, POOL_SIZE + 1), signup, _regions(rng, ANCHOR, POOL_SIZE)
    )


def make_signups(day: date) -> pd.DataFrame:
    """The customers who sign up on a day from ANCHOR on; none before it."""
    if day < ANCHOR:
        return _customers(
            np.random.default_rng(0),
            np.array([], "int64"),
            pd.Series([], dtype="datetime64[ns]"),
            np.array([], object),
        )
    rng = _rng(day, 3)
    total = sum(region_weights(day).values())
    n = int(rng.poisson(BASE_SIGNUPS * total * (0.5 + 0.5 * demand(day))))
    ids = 10_000_000 + _day_no(day) * 1000 + np.arange(n)
    signup = pd.Timestamp(day) + pd.to_timedelta(
        np.sort(rng.integers(0, 86400, n)), unit="s"
    )
    return _customers(rng, ids, signup, _regions(rng, day, n))


def customers_until(end: date) -> pd.DataFrame:
    """Every customer who signed up before `end`."""
    days = [ANCHOR + timedelta(days=i) for i in range(max(0, (end - ANCHOR).days))]
    frames = [make_pool(), *(make_signups(d) for d in days)]
    return pd.concat([f for f in frames if len(f)], ignore_index=True)


def _order_times(rng: np.random.Generator, day: date, n: int) -> np.ndarray:
    """Times on a day, more of them in the evening."""
    hours = np.arange(24)
    shape = 1 + 0.8 * np.sin((hours - 14) / 24 * 2 * np.pi)
    hour = rng.choice(hours, n, p=shape / shape.sum())
    seconds = hour * 3600 + rng.integers(0, 3600, n)
    return (pd.Timestamp(day) + pd.to_timedelta(np.sort(seconds), unit="s")).to_numpy()


def _channel_shares(day: date) -> dict[str, float]:
    # The mobile app gains four points a year from the web.
    shift = float(np.clip(0.04 * _years(day), -0.2, 0.3))
    return {"Web": 0.55 - shift, "Mobile app": 0.33 + shift, "Marketplace": 0.12}


def _popularity(products: pd.DataFrame, day: date) -> np.ndarray:
    """Each product's share of a day's order lines: its category's share, boosted in Q4, Zipf within it."""
    weights = np.zeros(len(products))
    for category, (_, _, _, _, _, share, q4) in CATEGORIES.items():
        rows = np.flatnonzero(products["category"].to_numpy() == category)
        rank = (
            np.random.default_rng([SEED, 4, len(category)]).permutation(len(rows)) + 1
        )
        boost = q4 if day.month in (11, 12) else 1.0
        weights[rows] = share * boost * (1 / rank**0.8) / (1 / rank**0.8).sum()
    return weights / weights.sum()


def make_orders(
    day: date, customers: pd.DataFrame, products: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """A day's orders and order lines. `customers` holds at least everyone who signed up by the day's end."""
    rng = _rng(day, 5)
    weights = region_weights(day)
    expected = BASE_ORDERS * sum(weights.values()) * demand(day)
    n_orders = int(rng.poisson(expected * float(rng.lognormal(0, 0.08))))

    start, end = pd.Timestamp(day), pd.Timestamp(day + timedelta(days=1))
    signup = customers["signup_ts"].to_numpy()
    todays = customers[
        (signup >= start.to_datetime64()) & (signup < end.to_datetime64())
    ]
    # A share of today's new customers place their first order shortly after signing up.
    first = todays[rng.random(len(todays)) < FIRST_ORDER_SHARE]
    n_returning = max(0, n_orders - len(first))

    known = customers[signup < start.to_datetime64()]
    weight = (
        known["segment"].map({s: v[1] for s, v in SEGMENTS.items()}).to_numpy()
        * known["loyalty_tier"].map({t: v[1] for t, v in LOYALTY.items()}).to_numpy()
    )
    regions = _pick(rng, weights, n_returning)
    buyers = np.empty(n_returning, dtype="int64")
    for region in weights:
        rows = np.flatnonzero(regions == region)
        pool = np.flatnonzero(known["region"].to_numpy() == region)
        if len(rows) == 0 or len(pool) == 0:
            continue
        p = weight[pool] / weight[pool].sum()
        buyers[rows] = known["customer_id"].to_numpy()[
            pool[rng.choice(len(pool), len(rows), p=p)]
        ]
    buyers = buyers[: len(regions)]
    returning_times = _order_times(rng, day, n_returning)
    first_times = (
        first["signup_ts"]
        + pd.to_timedelta(
            np.minimum(rng.exponential(1200, len(first)), 3000).astype("int64"),
            unit="s",
        )
    ).clip(upper=end - pd.Timedelta(seconds=1))

    orders = pd.DataFrame(
        {
            "customer_id": np.concatenate(
                [first["customer_id"].to_numpy(), buyers]
            ).astype("int64"),
            "ordered_at": np.concatenate([first_times.to_numpy(), returning_times]),
            "customer_type": ["new"] * len(first) + ["returning"] * n_returning,
        }
    ).sort_values("ordered_at", kind="stable", ignore_index=True)
    n = len(orders)
    orders.insert(
        0, "order_id", (_day_no(day) * 100_000 + np.arange(n)).astype("int64")
    )
    by_id = customers.set_index("customer_id")
    segment = by_id.loc[orders["customer_id"], "segment"].to_numpy()
    region = by_id.loc[orders["customer_id"], "region"].to_numpy()

    orders["channel"] = _pick(rng, _channel_shares(day), n)
    orders["payment_method"] = _pick(rng, PAYMENTS, n)
    sale, sale_share, sale_pct = promotion(day)
    uses_sale = rng.random(n) < sale_share
    coupon = ~uses_sale & (rng.random(n) < 0.22)
    pct = np.where(
        uses_sale, sale_pct, np.where(coupon, rng.choice([0.05, 0.10, 0.15], n), 0.0)
    )
    orders["promotion"] = np.where(uses_sale, sale, np.where(coupon, "coupon", "none"))

    extra_lines = np.array([SEGMENTS[s][2] for s in segment])
    extra_units = np.array([SEGMENTS[s][3] for s in segment])
    lines = 1 + rng.poisson(extra_lines)
    line_order = np.repeat(np.arange(n), lines)
    line_no = (
        np.concatenate([np.arange(k) for k in lines]) if n else np.array([], "int64")
    )
    product = products.iloc[
        rng.choice(len(products), len(line_order), p=_popularity(products, day))
    ]
    quantity = 1 + rng.poisson(extra_units[line_order])
    unit_price = product["list_price"].to_numpy()
    gross = np.round(quantity * unit_price, 2)
    discount = np.round(gross * pct[line_order], 2)
    net = np.round(gross - discount, 2)
    cost = np.round(quantity * product["unit_cost"].to_numpy(), 2)

    # An order is returned in full with the mean return rate of its lines' categories, more often
    # when it was bought on sale; cancellations depend on the channel.
    returns = np.array([CATEGORIES[c][4] for c in product["category"]])
    return_rate = np.bincount(line_order, returns, n) / np.maximum(lines, 1)
    return_rate = np.minimum(
        return_rate * np.where(orders["promotion"] != "none", 1.4, 1.0), 0.4
    )
    cancel_rate = orders["channel"].map(CANCELLED).to_numpy()
    draw = rng.random(n)
    orders["status"] = np.where(
        draw < cancel_rate,
        "cancelled",
        np.where(draw < cancel_rate + return_rate, "returned", "completed"),
    )

    items = pd.DataFrame(
        {
            "order_item_id": orders["order_id"].to_numpy()[line_order] * 100 + line_no,
            "order_id": orders["order_id"].to_numpy()[line_order],
            "customer_id": orders["customer_id"].to_numpy()[line_order],
            "product_id": product["product_id"].to_numpy().astype("int64"),
            "ordered_at": orders["ordered_at"].to_numpy()[line_order],
            "status": orders["status"].to_numpy()[line_order],
            "quantity": quantity.astype("int64"),
            "unit_price": unit_price,
            "gross_amount": gross,
            "discount_amount": discount,
            "net_amount": net,
            "cost_amount": cost,
            "profit_amount": np.round(net - cost, 2),
        }
    )
    totals = items.groupby("order_id")[
        ["quantity", "gross_amount", "discount_amount", "net_amount"]
    ].sum()
    orders["item_count"] = lines.astype("int64")
    orders["units"] = (
        totals["quantity"].reindex(orders["order_id"]).to_numpy().astype("int64")
    )
    for column in ("gross_amount", "discount_amount", "net_amount"):
        orders[column] = np.round(
            totals[column].reindex(orders["order_id"]).to_numpy(), 2
        )
    fee = np.array([SHIPPING_FEE[r] for r in region])
    orders["shipping_fee"] = np.where(
        orders["net_amount"] >= FREE_SHIPPING_FROM, 0.0, fee
    )
    columns = list(TABLES["orders"]["features"])
    return orders[columns], items[list(TABLES["order_items"]["features"])]


def history(end: date, days: int = HISTORY_DAYS) -> dict[str, pd.DataFrame]:
    """Every table as the backfill writes it: the days [end - days, end)."""
    customers = customers_until(end)
    products = make_products()
    orders, items = [], []
    for i in range(days, 0, -1):
        day_orders, day_items = make_orders(
            end - timedelta(days=i), customers, products
        )
        orders.append(day_orders)
        items.append(day_items)
    return {
        "customers": customers,
        "products": products,
        "orders": pd.concat(orders, ignore_index=True),
        "order_items": pd.concat(items, ignore_index=True),
    }


def days_of(start: date, end: date) -> dict[str, pd.DataFrame]:
    """The rows the daily job writes for the days [start, end): their new customers, orders and lines."""
    customers = customers_until(end)
    products = make_products()
    days = [start + timedelta(days=i) for i in range((end - start).days)]
    made = [make_orders(d, customers, products) for d in days]
    signups = [make_signups(d) for d in days]
    return {
        "customers": pd.concat(
            [f for f in signups if len(f)] or [signups[0]], ignore_index=True
        ),
        "orders": pd.concat([m[0] for m in made], ignore_index=True),
        "order_items": pd.concat([m[1] for m in made], ignore_index=True),
    }


# region Feature store


def _feature_group(fs, table: str):
    """The feature group of a table, created on first use."""
    spec = TABLES[table]
    return fs.get_or_create_feature_group(
        name=PREFIX + table,
        version=1,
        description=spec["description"],
        primary_key=spec["primary_key"],
        event_time=spec["event_time"],
        online_enabled=False,
        time_travel_format="DELTA",
        statistics_config=False,
    )


def _insert(fs, table: str, df: pd.DataFrame, lifecycle: str) -> None:
    """Upsert rows on the primary key; a new feature group gets its feature descriptions and tag."""
    if df.empty:
        print(f"{PREFIX}{table}: nothing to write", flush=True)
        return
    fg = _feature_group(fs, table)
    created = fg.id is None
    fg.insert(df, write_options={"wait_for_job": True})
    print(f"{fg.name}: wrote {len(df)} rows", flush=True)
    if created:
        for feature, description in TABLES[table]["features"].items():
            fg.update_feature_description(feature, description)
        fg.add_tag(TAG, {"layer": "gold", "lifecycle": lifecycle})


# endregion


def _utc_day(text: str) -> date:
    when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if when.tzinfo:
        when = when.astimezone(timezone.utc)
    return when.date()


def _window() -> tuple[date, date]:
    """The scheduled window's days, else the last full day."""
    start, end = os.environ.get("HOPS_START_TIME"), os.environ.get("HOPS_END_TIME")
    if start and end:
        return _utc_day(start), _utc_day(end)
    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=1), today


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    parser.add_argument(
        "--lifecycle", default="dev", choices=["dev", "staging", "prod"]
    )
    parser.add_argument(
        "--end",
        help="backfill only: the day after the last day of history (UTC, YYYY-MM-DD); by default today",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=HISTORY_DAYS,
        help="backfill only: how many days of history",
    )
    # A scheduled run also gets the instant it fired appended; the window comes
    # from HOPS_START_TIME and HOPS_END_TIME instead.
    parser.add_argument("-start_time", dest="_fired_at", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_fired_end", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    import hopsworks

    fs = hopsworks.login().get_feature_store()
    if args.mode == "backfill":
        end = _utc_day(args.end) if args.end else datetime.now(timezone.utc).date()
        print(
            f"backfill: {args.days} days up to {end.isoformat()} (exclusive)",
            flush=True,
        )
        tables = history(end, args.days)
    else:
        start, end = _window()
        print(f"daily: days [{start.isoformat()}, {end.isoformat()})", flush=True)
        tables = days_of(start, end)
    for table, df in tables.items():
        _insert(fs, table, df, args.lifecycle)


if __name__ == "__main__":
    main()
