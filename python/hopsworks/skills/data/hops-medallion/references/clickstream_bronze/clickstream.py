"""Synthetic clickstream bronze tables for a web shop: clicks, customers, products and orders.

One program for the layer's three jobs, picked with --mode:

- backfill: create the four bronze feature groups and fill them with the 30 days of history
  up to the last midnight (UTC): 10,000 customers, 1,000 products, 20,000 orders and 1,000,000
  clicks.
- clicks: write 10,000 clicks for one hour.
- daily: write one day of new customers, products and orders, and the day's updates to them:
  profile changes, price changes, discontinued products and order status changes.

A scheduled run writes the window Hopsworks passes in HOPS_START_TIME and HOPS_END_TIME;
without them, clicks writes the last full hour and daily the last full day.
The rows of a window are drawn from a generator seeded with the window's start, and every key
is derived from the window, so a replayed window rewrites the same rows.

Bronze holds the data as it arrived: about 0.001% of clicks arrive twice (the same click_id
under a new ingest_id, as an at-least-once collector delivers them), and an order's lines are
a JSON array in its items column.
Every table is an offline Delta feature group tagged medallion_table {"layer": "bronze"}.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


N_CUSTOMERS = 10_000
N_PRODUCTS = 1_000
N_ORDERS = 20_000
N_CLICKS = 1_000_000
HISTORY_DAYS = 30
HOURLY_CLICKS = 10_000
DUPLICATE_RATE = 0.00001  # 0.001% of clicks arrive twice
# A day's changes, as Poisson means.
DAILY = {
    "new_customers": 35,
    "customer_updates": 15,
    "new_products": 2,
    "price_changes": 25,
    "discontinued": 1,
    "new_orders": 650,
}
PREFIX = "clickstream_"
TAG = "medallion_table"

COUNTRIES = {
    "SE": ["Stockholm", "Gothenburg", "Malmo", "Uppsala"],
    "DE": ["Berlin", "Munich", "Hamburg", "Cologne"],
    "GB": ["London", "Manchester", "Leeds", "Bristol"],
    "US": ["New York", "Chicago", "Seattle", "Austin"],
    "FR": ["Paris", "Lyon", "Marseille", "Lille"],
    "IE": ["Dublin", "Cork", "Galway", "Limerick"],
}
COUNTRY_WEIGHTS = [0.25, 0.2, 0.2, 0.2, 0.1, 0.05]
CURRENCY = {
    "SE": "SEK",
    "DE": "EUR",
    "GB": "GBP",
    "US": "USD",
    "FR": "EUR",
    "IE": "EUR",
}
FIRST_NAMES = (
    "Alex Anna Ben Clara David Elin Emma Erik Felix Hanna Isak Jonas Julia Karin Leo Lina "
    "Liam Maja Marco Mia Noah Nora Olivia Oscar Paula Sara Sofia Tom Vera William"
).split()
LAST_NAMES = (
    "Andersson Berg Brown Dubois Fischer Garcia Johansson Keane Larsen Martin Meyer Moreau "
    "Murphy Nilsson Novak Olsen Petit Rossi Schmidt Smith Taylor Walsh Weber Wilson"
).split()
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "proton.me", "icloud.com"]
SEGMENTS = ["new", "regular", "loyal", "vip"]
CATEGORIES = {
    "shoes": ["Nike", "Adidas", "Puma", "Asics"],
    "apparel": ["Uniqlo", "H&M", "Levi's", "Patagonia"],
    "electronics": ["Sony", "Samsung", "Apple", "Anker"],
    "home": ["IKEA", "Muji", "Bodum", "Philips"],
    "beauty": ["Nivea", "L'Oreal", "Clinique", "Weleda"],
    "sports": ["Wilson", "Garmin", "Decathlon", "Osprey"],
}
PRICE_RANGE = {
    "shoes": (40, 180),
    "apparel": (10, 120),
    "electronics": (20, 900),
    "home": (5, 250),
    "beauty": (4, 60),
    "sports": (15, 400),
}
NOUNS = {
    "shoes": ["Runner", "Sneaker", "Trail Shoe", "Boot"],
    "apparel": ["T-Shirt", "Hoodie", "Jeans", "Jacket"],
    "electronics": ["Headphones", "Charger", "Speaker", "Smartwatch"],
    "home": ["Mug", "Lamp", "Kettle", "Throw"],
    "beauty": ["Moisturizer", "Serum", "Lip Balm", "Cleanser"],
    "sports": ["Racket", "Backpack", "Bottle", "Yoga Mat"],
}
ADJECTIVES = ["Classic", "Pro", "Lite", "Eco", "Ultra", "Urban", "Studio", "Max"]
EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "purchase",
]
EVENT_WEIGHTS = [0.32, 0.36, 0.1, 0.1, 0.03, 0.05, 0.04]
SEARCH_TERMS = [
    "running shoes",
    "hoodie",
    "headphones",
    "lamp",
    "serum",
    "backpack",
    "gift",
]
REFERRERS = ["direct", "google", "instagram", "newsletter", "tiktok", "affiliate"]
REFERRER_WEIGHTS = [0.3, 0.35, 0.12, 0.1, 0.08, 0.05]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.62, 0.32, 0.06]
PAYMENTS = ["card", "paypal", "klarna", "apple_pay"]


def _epoch(when: datetime) -> int:
    return int(when.replace(tzinfo=timezone.utc).timestamp())


def _rng(start: datetime, salt: int) -> np.random.Generator:
    """A generator seeded with the window's start, so a replayed window draws the same rows."""
    return np.random.default_rng([_epoch(start), salt])


def _times(
    rng: np.random.Generator, start: datetime, end: datetime, n: int
) -> pd.Series:
    """N sorted timestamps in [start, end); over whole days, more of them in the evening, as shop traffic is."""
    seconds = (end - start).total_seconds()
    days = int(seconds // 86400)
    if days < 1:
        offsets = rng.uniform(0, seconds, n)
    else:
        hours = np.arange(24)
        shape = 1 + 0.8 * np.sin((hours - 13) / 24 * 2 * np.pi)
        hour = rng.choice(hours, n, p=shape / shape.sum())
        offsets = (
            rng.integers(0, days, n) * 86400.0 + hour * 3600.0 + rng.uniform(0, 3600, n)
        )
        offsets = np.minimum(offsets, seconds - 1)
    return pd.Series(pd.Timestamp(start) + pd.to_timedelta(np.sort(offsets), unit="s"))


def make_customers(
    rng: np.random.Generator, first_id: int, n: int, signup: pd.Series
) -> pd.DataFrame:
    """N customers with ids from first_id, signed up at the given times."""
    countries = rng.choice(list(COUNTRIES), n, p=COUNTRY_WEIGHTS)
    first = rng.choice(FIRST_NAMES, n)
    last = rng.choice(LAST_NAMES, n)
    ids = np.arange(first_id, first_id + n)
    return pd.DataFrame(
        {
            "customer_id": ids,
            "email": [
                f"{f.lower()}.{s.lower()}{i % 1000}@{d}"
                for f, s, i, d in zip(
                    first, last, ids, rng.choice(EMAIL_DOMAINS, n), strict=True
                )
            ],
            "first_name": first,
            "last_name": last,
            "country": countries,
            "city": [rng.choice(COUNTRIES[c]) for c in countries],
            "segment": rng.choice(SEGMENTS, n, p=[0.4, 0.35, 0.2, 0.05]),
            "marketing_opt_in": rng.random(n) < 0.55,
            "signup_ts": signup.values,
            "updated_at": signup.values,
        }
    )


def make_products(
    rng: np.random.Generator, first_id: int, n: int, created: pd.Series
) -> pd.DataFrame:
    """N active products with ids from first_id, created at the given times."""
    categories = rng.choice(list(CATEGORIES), n)
    low = np.array([PRICE_RANGE[c][0] for c in categories])
    high = np.array([PRICE_RANGE[c][1] for c in categories])
    # Prices skew low within each category's range, and end in .99 or .49.
    price = np.floor(low + (high - low) * rng.beta(1.5, 4, n)) + rng.choice(
        [0.49, 0.99], n
    )
    ids = np.arange(first_id, first_id + n)
    return pd.DataFrame(
        {
            "product_id": ids,
            "sku": [
                f"SKU-{c[:3].upper()}-{i:06d}"
                for c, i in zip(categories, ids, strict=True)
            ],
            "name": [
                f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS[c])}" for c in categories
            ],
            "category": categories,
            "brand": [rng.choice(CATEGORIES[c]) for c in categories],
            "price": price.round(2),
            "currency": "EUR",
            "is_active": True,
            "created_at": created.values,
            "updated_at": created.values,
        }
    )


def _popularity(rng: np.random.Generator, n: int) -> np.ndarray:
    """Zipf-like weights over n items in random order: a few best sellers, a long tail."""
    weights = 1 / np.arange(1, n + 1) ** 1.1
    rng.shuffle(weights)
    return weights / weights.sum()


def _status(age_days: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """An order's status from its age: placed, paid, shipped, then delivered, cancelled or returned."""
    status = np.where(
        age_days < 0.1, "placed", np.where(age_days < 1, "paid", "shipped")
    ).astype(object)
    settled = age_days >= 4
    outcome = rng.choice(
        ["delivered", "cancelled", "returned"], len(age_days), p=[0.9, 0.06, 0.04]
    )
    status[settled] = outcome[settled]
    # An order is cancelled before it ships.
    early = (age_days >= 1) & (age_days < 4) & (rng.random(len(age_days)) < 0.03)
    status[early] = "cancelled"
    return status


def make_orders(
    rng: np.random.Generator,
    first_id: int,
    order_ts: pd.Series,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    now: datetime,
) -> pd.DataFrame:
    """Orders with ids from first_id at the given times, by customers signed up before each, of active products."""
    n = len(order_ts)
    order_ts = order_ts.reset_index(drop=True)
    signup = customers["signup_ts"].to_numpy()
    buyer_weights = _popularity(rng, len(customers))
    buyers = rng.choice(len(customers), n, p=buyer_weights)
    # A customer who signed up after the order's time cannot have placed it: pick the earliest.
    too_new = signup[buyers] > order_ts.to_numpy()
    buyers[too_new] = int(np.argmin(signup))
    active = products[products["is_active"]].reset_index(drop=True)
    product_weights = _popularity(rng, len(active))
    lines = rng.choice([1, 1, 1, 2, 2, 3, 4], n)
    items, totals = [], []
    for k in lines:
        picked = active.iloc[
            rng.choice(len(active), k, replace=False, p=product_weights)
        ]
        quantity = rng.choice([1, 1, 1, 2, 3], k)
        line_items = [
            {"product_id": int(p), "quantity": int(q), "unit_price": float(u)}
            for p, q, u in zip(
                picked["product_id"], quantity, picked["price"], strict=True
            )
        ]
        items.append(json.dumps(line_items))
        totals.append(round(float((quantity * picked["price"].to_numpy()).sum()), 2))
    age_days = (pd.Timestamp(now) - order_ts).dt.total_seconds().to_numpy() / 86400
    status = _status(age_days, rng)
    chosen = customers.iloc[buyers].reset_index(drop=True)
    updated = order_ts + pd.to_timedelta(
        np.where(status == "placed", 0, np.minimum(age_days, 6) * 86400 * 0.8), unit="s"
    )
    return pd.DataFrame(
        {
            "order_id": np.arange(first_id, first_id + n),
            "customer_id": chosen["customer_id"].to_numpy(),
            "order_ts": order_ts.values,
            "status": status,
            "items": items,
            "n_items": lines,
            "total_amount": totals,
            "currency": "EUR",
            "payment_method": rng.choice(PAYMENTS, n, p=[0.5, 0.2, 0.2, 0.1]),
            "shipping_country": chosen["country"].to_numpy(),
            "updated_at": updated.values,
        }
    )


def make_clicks(
    rng: np.random.Generator,
    start: datetime,
    end: datetime,
    n: int,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    source: str = "h",
) -> pd.DataFrame:
    """N click events in [start, end), in sessions, about 0.001% of them delivered twice.

    `source` starts every key, so the backfill's keys never meet an hourly run's.
    """
    # Sessions of about eight events each, 60% of them by a logged-in customer.
    n_sessions = max(1, n // 8)
    session_start = _times(rng, start, end, n_sessions)
    session_of = np.sort(rng.integers(0, n_sessions, n))
    logged_in = rng.random(n_sessions) < 0.6
    session_customer = np.where(
        logged_in, rng.choice(customers["customer_id"].to_numpy(), n_sessions), -1
    )
    session_country = rng.choice(list(COUNTRIES), n_sessions, p=COUNTRY_WEIGHTS)
    session_device = rng.choice(DEVICES, n_sessions, p=DEVICE_WEIGHTS)
    session_referrer = rng.choice(REFERRERS, n_sessions, p=REFERRER_WEIGHTS)
    window = _epoch(start)
    # A click lands a few seconds to a minute after the previous one in its session.
    first = np.arange(n) == np.searchsorted(session_of, session_of)
    elapsed = (
        pd.Series(np.where(first, 0.0, rng.exponential(25, n)))
        .groupby(session_of)
        .cumsum()
    )
    # A session that would run past the window starts early enough to end inside it.
    duration = (
        elapsed.groupby(session_of).max().reindex(range(n_sessions), fill_value=0.0)
    )
    latest = pd.Timestamp(end) - pd.to_timedelta(duration.to_numpy() + 1, unit="s")
    session_start = np.minimum(session_start.to_numpy(), latest.to_numpy())
    event_ts = pd.Series(session_start[session_of]) + pd.to_timedelta(
        elapsed.to_numpy(), unit="s"
    )
    event_type = rng.choice(EVENT_TYPES, n, p=EVENT_WEIGHTS)
    active = products[products["is_active"]]["product_id"].to_numpy()
    product = rng.choice(active, n, p=_popularity(rng, len(active)))
    has_product = np.isin(
        event_type, ["product_view", "add_to_cart", "remove_from_cart"]
    )
    terms = rng.choice(SEARCH_TERMS, n)
    page = np.select(
        [
            has_product,
            event_type == "search",
            event_type == "begin_checkout",
            event_type == "purchase",
        ],
        [
            np.char.add("/product/", product.astype(str)),
            np.char.add("/search?q=", np.char.replace(terms.astype(str), " ", "+")),
            "/checkout",
            "/checkout/confirmation",
        ],
        default=rng.choice(["/", "/category/new", "/sale", "/cart"], n),
    )
    customer = session_customer[session_of]
    clicks = pd.DataFrame(
        {
            "ingest_id": [f"{source}{window}-{i:07d}" for i in range(n)],
            "click_id": [f"c{source}{window:x}{i:07x}" for i in range(n)],
            "session_id": [f"s{source}{window:x}{s:06x}" for s in session_of],
            "customer_id": pd.array(
                np.where(customer < 0, None, customer), dtype="Int64"
            ),
            "anonymous_id": [f"a{source}{window:x}{s:06x}" for s in session_of],
            "event_type": event_type,
            "product_id": pd.array(np.where(has_product, product, None), dtype="Int64"),
            "page_url": page,
            "referrer": session_referrer[session_of],
            "device": session_device[session_of],
            "country": session_country[session_of],
            "event_ts": event_ts.values,
        }
    )
    # The collector stamps a click when it lands, up to a few seconds later.
    clicks["ingested_at"] = clicks["event_ts"] + pd.to_timedelta(
        rng.exponential(2, n), unit="s"
    )
    duplicates = clicks.iloc[_duplicates(rng, n)].copy()
    duplicates["ingest_id"] = duplicates["ingest_id"] + "-r"
    duplicates["ingested_at"] = duplicates["ingested_at"] + pd.Timedelta(seconds=30)
    clicks = pd.concat([clicks, duplicates], ignore_index=True)
    clicks["event_date"] = clicks["event_ts"].dt.strftime("%Y-%m-%d")
    return clicks


def _duplicates(rng: np.random.Generator, n: int) -> np.ndarray:
    """The rows delivered twice: DUPLICATE_RATE of n, drawn so an hour of 10,000 clicks gets one about every ten hours."""
    expected = n * DUPLICATE_RATE
    count = (
        int(round(expected)) if expected >= 1 else int(rng.binomial(n, DUPLICATE_RATE))
    )
    return rng.choice(n, count, replace=False)


def daily_changes(
    rng: np.random.Generator,
    start: datetime,
    end: datetime,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """One day's new and changed rows of customers, products and orders.

    Drawn from the rows that existed before the day, so a replayed day draws the same ids.
    """
    before = pd.Timestamp(start)
    customers = customers[customers["signup_ts"] < before]
    products = products[products["created_at"] < before]
    orders = orders[orders["order_ts"] < before]

    signups = _times(rng, start, end, int(rng.poisson(DAILY["new_customers"])))
    new_customers = make_customers(
        rng, int(customers["customer_id"].max()) + 1, len(signups), signups
    )

    changed = customers.sample(
        n=min(len(customers), int(rng.poisson(DAILY["customer_updates"]))),
        random_state=rng.integers(1 << 31),
    ).copy()
    moved = rng.random(len(changed)) < 0.5
    changed.loc[moved, "city"] = [
        rng.choice(COUNTRIES[c]) for c in changed.loc[moved, "country"]
    ]
    changed.loc[~moved, "marketing_opt_in"] = ~changed.loc[~moved, "marketing_opt_in"]
    changed["updated_at"] = _times(rng, start, end, len(changed)).values

    created = _times(rng, start, end, int(rng.poisson(DAILY["new_products"])))
    new_products = make_products(
        rng, int(products["product_id"].max()) + 1, len(created), created
    )
    active = products[products["is_active"]]
    repriced = active.sample(
        n=min(len(active), int(rng.poisson(DAILY["price_changes"]))),
        random_state=rng.integers(1 << 31),
    ).copy()
    factor = 1 + rng.choice([-1, 1], len(repriced), p=[0.6, 0.4]) * rng.uniform(
        0.05, 0.2, len(repriced)
    )
    repriced["price"] = (np.floor(repriced["price"] * factor) + 0.99).round(2)
    repriced["updated_at"] = _times(rng, start, end, len(repriced)).values
    kept = active[~active["product_id"].isin(repriced["product_id"])]
    gone = kept.sample(
        n=min(len(kept), int(rng.poisson(DAILY["discontinued"]))),
        random_state=rng.integers(1 << 31),
    ).copy()
    gone["is_active"] = False
    gone["updated_at"] = _times(rng, start, end, len(gone)).values

    catalog = pd.concat(
        [products[~products["product_id"].isin(gone["product_id"])], new_products]
    )
    everyone = pd.concat([customers, new_customers], ignore_index=True)
    new_orders = make_orders(
        rng,
        int(orders["order_id"].max()) + 1,
        _times(rng, start, end, int(rng.poisson(DAILY["new_orders"]))),
        everyone,
        catalog,
        end,
    )

    # Open orders move on as they age, judged at the end of the day.
    open_orders = orders[orders["status"].isin(["placed", "paid", "shipped"])].copy()
    age = (
        pd.Timestamp(end) - open_orders["order_ts"]
    ).dt.total_seconds().to_numpy() / 86400
    moved_on = _status(age, rng)
    progressed = open_orders[moved_on != open_orders["status"].to_numpy()].copy()
    progressed["status"] = moved_on[moved_on != open_orders["status"].to_numpy()]
    progressed["updated_at"] = _times(rng, start, end, len(progressed)).values

    return {
        "customers": pd.concat([new_customers, changed], ignore_index=True),
        "products": pd.concat([new_products, repriced, gone], ignore_index=True),
        "orders": pd.concat([new_orders, progressed], ignore_index=True),
    }


# region Feature groups

TABLES = {
    "customers": {
        "primary_key": ["customer_id"],
        "event_time": "updated_at",
        "description": "Bronze: shop customers as the CRM exports them; a changed profile arrives as the whole row again with a new updated_at.",
        "features": {
            "customer_id": "The customer's id in the shop.",
            "email": "Email address, as the customer typed it.",
            "first_name": "First name.",
            "last_name": "Last name.",
            "country": "ISO 3166 country code of the customer's address.",
            "city": "City of the customer's address.",
            "segment": "CRM segment: new, regular, loyal or vip.",
            "marketing_opt_in": "Whether the customer accepts marketing email.",
            "signup_ts": "When the customer signed up (UTC).",
            "updated_at": "When the row last changed in the CRM (UTC).",
        },
    },
    "products": {
        "primary_key": ["product_id"],
        "event_time": "updated_at",
        "description": "Bronze: the shop's product catalog; a price change or a discontinued product arrives as the whole row again.",
        "features": {
            "product_id": "The product's id in the shop.",
            "sku": "Stock keeping unit.",
            "name": "Product name.",
            "category": "Catalog category.",
            "brand": "Brand.",
            "price": "List price, in currency.",
            "currency": "ISO 4217 currency of price.",
            "is_active": "False once the product is discontinued.",
            "created_at": "When the product was added to the catalog (UTC).",
            "updated_at": "When the row last changed in the catalog (UTC).",
        },
    },
    "orders": {
        "primary_key": ["order_id"],
        "event_time": "updated_at",
        "description": "Bronze: shop orders as the order system exports them; a status change arrives as the whole row again. items is the order's lines as a JSON array.",
        "features": {
            "order_id": "The order's id.",
            "customer_id": "The customer who placed it.",
            "order_ts": "When it was placed (UTC).",
            "status": "placed, paid, shipped, delivered, cancelled or returned.",
            "items": "The order lines, a JSON array of {product_id, quantity, unit_price}.",
            "n_items": "Number of lines.",
            "total_amount": "Sum of quantity times unit_price over the lines.",
            "currency": "ISO 4217 currency of the amounts.",
            "payment_method": "card, paypal, klarna or apple_pay.",
            "shipping_country": "ISO 3166 country code it ships to.",
            "updated_at": "When the row last changed in the order system (UTC).",
        },
    },
    "clicks": {
        "primary_key": ["ingest_id"],
        "event_time": "event_ts",
        "partition_key": ["event_date"],
        "description": "Bronze: web shop click events as the collector delivered them; about 0.001% arrive twice, the same click_id under a new ingest_id.",
        "features": {
            "ingest_id": "The collector's id of this delivery; a click delivered twice has two.",
            "click_id": "The click's id from the browser; the same in a duplicate delivery.",
            "session_id": "The browsing session.",
            "customer_id": "The logged-in customer, null for an anonymous visitor.",
            "anonymous_id": "The browser's cookie id.",
            "event_type": "page_view, product_view, search, add_to_cart, remove_from_cart, begin_checkout or purchase.",
            "product_id": "The product of a product_view, add_to_cart or remove_from_cart, else null.",
            "page_url": "Path of the page.",
            "referrer": "Where the session came from.",
            "device": "mobile, desktop or tablet.",
            "country": "ISO 3166 country code from the visitor's IP address.",
            "event_ts": "When the click happened in the browser (UTC).",
            "ingested_at": "When the collector received it (UTC).",
            "event_date": "The UTC date of event_ts, the partition.",
        },
    },
}


def _feature_group(fs, table: str, lifecycle: str):
    """The bronze feature group of a table, created and tagged on first use."""
    spec = TABLES[table]
    return fs.get_or_create_feature_group(
        name=PREFIX + table,
        version=1,
        description=spec["description"],
        primary_key=spec["primary_key"],
        event_time=spec["event_time"],
        partition_key=spec.get("partition_key", []),
        online_enabled=False,
        time_travel_format="DELTA",
        statistics_config=False,
    )


def _insert(fg, df: pd.DataFrame, lifecycle: str) -> None:
    """Upsert rows on the primary key; a new feature group gets its feature descriptions and bronze tag."""
    created = fg.id is None
    fg.insert(df, write_options={"wait_for_job": True})
    print(f"{fg.name}: wrote {len(df)} rows", flush=True)
    if created:
        table = fg.name[len(PREFIX) :]
        for feature, description in TABLES[table]["features"].items():
            fg.update_feature_description(feature, description)
        fg.add_tag(TAG, {"layer": "bronze", "lifecycle": lifecycle})


def _read(fg, columns: list[str]) -> pd.DataFrame:
    return fg.select(columns).read(dataframe_type="pandas")


# endregion


def _utc(text: str) -> datetime:
    """An ISO 8601 time as naive UTC, the way every timestamp here is kept."""
    when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if when.tzinfo:
        when = when.astimezone(timezone.utc).replace(tzinfo=None)
    return when


def _window(hours: int) -> tuple[datetime, datetime]:
    """The scheduled window, else the last full one of the given length ending now."""
    start, end = os.environ.get("HOPS_START_TIME"), os.environ.get("HOPS_END_TIME")
    if start and end:
        return _utc(start), _utc(end)
    now = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0, tzinfo=None
    )
    if hours == 24:
        now = now.replace(hour=0)
    return now - timedelta(hours=hours), now


def _naive(df: pd.DataFrame) -> pd.DataFrame:
    """Timestamps as naive UTC, as the feature store may read them back with a zone."""
    for column in df.columns:
        if isinstance(df[column].dtype, pd.DatetimeTZDtype):
            df[column] = df[column].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def backfill(fs, lifecycle: str, end: datetime) -> None:
    start = end - timedelta(days=HISTORY_DAYS)
    rng = _rng(start, 1)
    # Customers signed up over three years, a tenth of them within the history window.
    signup = pd.concat(
        [
            _times(rng, end - timedelta(days=3 * 365), start, int(N_CUSTOMERS * 0.9)),
            _times(rng, start, end, N_CUSTOMERS - int(N_CUSTOMERS * 0.9)),
        ],
        ignore_index=True,
    )
    customers = make_customers(rng, 1, N_CUSTOMERS, signup)
    products = make_products(
        rng,
        1,
        N_PRODUCTS,
        _times(rng, end - timedelta(days=2 * 365), start, N_PRODUCTS),
    )
    orders = make_orders(
        rng, 1, _times(rng, start, end, N_ORDERS), customers, products, end
    )
    clicks = make_clicks(rng, start, end, N_CLICKS, customers, products, source="b")
    for table, df in (
        ("customers", customers),
        ("products", products),
        ("orders", orders),
        ("clicks", clicks),
    ):
        _insert(_feature_group(fs, table, lifecycle), _naive(df), lifecycle)


def hourly_clicks(fs, lifecycle: str, start: datetime, end: datetime) -> None:
    customers = _naive(
        _read(_feature_group(fs, "customers", lifecycle), ["customer_id", "signup_ts"])
    )
    products = _read(
        _feature_group(fs, "products", lifecycle), ["product_id", "is_active"]
    )
    customers = customers[customers["signup_ts"] < pd.Timestamp(end)]
    hours = max(1, round((end - start).total_seconds() / 3600))
    clicks = make_clicks(
        _rng(start, 2), start, end, HOURLY_CLICKS * hours, customers, products
    )
    _insert(_feature_group(fs, "clicks", lifecycle), _naive(clicks), lifecycle)


def daily_update(fs, lifecycle: str, start: datetime, end: datetime) -> None:
    groups = {
        t: _feature_group(fs, t, lifecycle) for t in ("customers", "products", "orders")
    }
    current = {t: _read(fg, list(TABLES[t]["features"])) for t, fg in groups.items()}
    for frame in current.values():
        _naive(frame)
    changes = daily_changes(
        _rng(start, 3),
        start,
        end,
        current["customers"],
        current["products"],
        current["orders"],
    )
    for table, fg in groups.items():
        _insert(fg, _naive(changes[table]), lifecycle)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--mode", choices=["backfill", "clicks", "daily"], required=True
    )
    parser.add_argument(
        "--lifecycle", default="dev", choices=["dev", "staging", "prod"]
    )
    parser.add_argument(
        "--end",
        help="backfill only: the end of the history (UTC, ISO 8601); by default the last midnight",
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
        # Ending at midnight keeps the backfill out of every daily window, so a day's new
        # ids follow on from the rows written before it.
        end = _utc(args.end) if args.end else _window(24)[1]
        print(f"backfill: {HISTORY_DAYS} days to {end.isoformat()}Z", flush=True)
        backfill(fs, args.lifecycle, end)
    elif args.mode == "clicks":
        start, end = _window(1)
        print(f"clicks: window [{start.isoformat()}Z, {end.isoformat()}Z)", flush=True)
        hourly_clicks(fs, args.lifecycle, start, end)
    else:
        start, end = _window(24)
        print(f"daily: window [{start.isoformat()}Z, {end.isoformat()}Z)", flush=True)
        daily_update(fs, args.lifecycle, start, end)


if __name__ == "__main__":
    main()
