# ruff: noqa: INP001
"""The synthetic leasing world: gardening machines, the customers who lease them and how leases end.

Every customer has a hidden risk that the data never shows directly: it lowers their
credit score, raises their debt and delinquencies, makes underwriters decline them
more often and makes their leases end badly more often. Underwriters decide from what
they know of the customer (that risk, their income and how their previous order ended);
a model sees the credit reports and the last settled lease through the feature view,
which measure the same things with noise, so it learns their decisions well but not
perfectly. Nothing here joins tables by time: that is the feature view's job. The same
functions write the history and the outcomes the app simulates, so the two are drawn
from one world.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


LEASE_MONTHS = (1, 3, 6, 12)
OUTCOMES = ("paid", "paid_late", "damaged", "defaulted", "declined")

# category: (models, monthly price range in EUR, replacement value per monthly EUR, damage risk)
CATEGORIES = {
    "lawn mower": (["Push 46", "Self-propelled 53", "Mulching 51"], (25, 60), 16, 0.04),
    "riding mower": (
        ["Rider 92", "Rider 107 HD", "Zero-turn 122"],
        (120, 260),
        22,
        0.06,
    ),
    "robotic mower": (["Robo 450", "Robo 1200", "Robo 5000 AWD"], (45, 140), 20, 0.03),
    "chainsaw": (["Arborist 25", "Farm 45", "Pro 60"], (30, 90), 14, 0.12),
    "hedge trimmer": (["Hedge 55", "Hedge 70 pole"], (15, 40), 14, 0.05),
    "leaf blower": (
        ["Blower 2-stroke", "Backpack 580", "Battery 36V"],
        (15, 50),
        13,
        0.03,
    ),
    "tiller": (["Mini tiller", "Rear-tine 20", "Front-tine 24"], (40, 110), 15, 0.08),
    "wood chipper": (["Chipper 76", "Chipper 125 tow"], (110, 320), 18, 0.14),
    "stump grinder": (["Grinder 14 HP", "Grinder 25 HP"], (150, 360), 18, 0.13),
    "log splitter": (["Splitter 7t", "Splitter 22t"], (45, 150), 15, 0.07),
    "pressure washer": (["Washer 140 bar", "Washer 210 bar"], (20, 70), 13, 0.05),
    "garden tractor": (["Compact 23", "Compact 38 4WD"], (220, 480), 24, 0.07),
    "brush cutter": (["Brush 35", "Brush 52 harness"], (25, 75), 14, 0.09),
    "lawn aerator": (["Aerator 19", "Aerator 26"], (55, 140), 15, 0.05),
}
BRANDS = (
    "Husqvarna",
    "Stihl",
    "Honda",
    "Makita",
    "Echo",
    "Greenworks",
    "Toro",
    "Cub Cadet",
)
FIRST = (
    "Anna",
    "Erik",
    "Maria",
    "Lars",
    "Karin",
    "Johan",
    "Eva",
    "Nils",
    "Sara",
    "Olof",
    "Ingrid",
    "Per",
    "Lena",
    "Anders",
    "Elin",
    "Gustav",
    "Frida",
    "Oskar",
    "Maja",
    "Hugo",
)
LAST = (
    "Lindqvist",
    "Berg",
    "Holm",
    "Sandberg",
    "Ek",
    "Lund",
    "Nyberg",
    "Dahl",
    "Wikström",
    "Forsberg",
    "Hedlund",
    "Ström",
    "Åberg",
    "Björk",
    "Strand",
)
TRADES = (
    "Gardens",
    "Landscaping",
    "Tree Care",
    "Grounds",
    "Green Services",
    "Park & Lawn",
)


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def generate_customers(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """The customers, with the hidden risk the other tables are drawn from (never written)."""
    business = rng.random(n) < 0.35
    first = rng.choice(FIRST, n)
    last = rng.choice(LAST, n)
    names = np.where(
        business,
        [f"{a} {b}" for a, b in zip(last, rng.choice(TRADES, n), strict=True)],
        [f"{a} {b}" for a, b in zip(first, last, strict=True)],
    )
    income = np.where(
        business,
        rng.lognormal(np.log(9_000), 0.45, n),
        rng.lognormal(np.log(3_600), 0.35, n),
    )
    return pd.DataFrame(
        {
            "customer_id": np.arange(1, n + 1, dtype=np.int64),
            "customer_name": names,
            "customer_type": np.where(business, "business", "household"),
            "risk": rng.beta(2.0, 5.0, n),
            "base_income": income,
        }
    )


def generate_equipment(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """`n` machines across the categories, priced per month, each with its damage risk."""
    categories = list(CATEGORIES)
    rows = []
    for i in range(n):
        category = categories[i % len(categories)]
        models, (low, high), value_factor, damage = CATEGORIES[category]
        price = round(float(rng.uniform(low, high)), 0)
        # Robotic mowers run on batteries; the big machines on petrol or diesel.
        if category == "robotic mower":
            power = "battery"
        elif price > 200:
            power = str(rng.choice(["petrol", "diesel"]))
        else:
            power = str(rng.choice(["petrol", "battery", "electric"]))
        rows.append(
            {
                "equipment_id": i + 1,
                "name": f"{rng.choice(BRANDS)} {models[(i // len(categories)) % len(models)]}",
                "category": category,
                "power_source": power,
                "monthly_price": price,
                "replacement_value": round(
                    price * value_factor * float(rng.uniform(0.85, 1.15)), 0
                ),
                "damage_risk": damage,
            }
        )
    return pd.DataFrame(rows)


def world(
    seed: int, customers: int, equipment: int
) -> tuple[pd.DataFrame, pd.DataFrame, np.random.Generator]:
    """The customers (with their hidden risk) and the machines, the same for the same arguments.

    The app rebuilds them from the seed to simulate how its leases end, so it never reads
    the hidden risk from a feature group. The generator is returned for the rest of the history.
    """
    rng = np.random.default_rng(seed)
    return generate_customers(customers, rng), generate_equipment(equipment, rng), rng


def generate_credit_history(
    customers: pd.DataFrame,
    rows: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """`rows` credit reports between `start` and `end`; every customer's first one is at `start`.

    Each report draws the score, debt and delinquencies from the customer's hidden risk with
    noise, so a customer's reports drift around their risk rather than repeat it.
    """
    n = len(customers)
    extra = rng.integers(0, n, max(rows - n, 0))
    index = np.concatenate([np.arange(n), extra])
    span = (end - start).total_seconds()
    offsets = np.where(np.arange(len(index)) < n, 0.0, rng.random(len(index)) * span)
    risk = customers["risk"].to_numpy()[index]
    noise = rng.normal(0, 0.04, len(index))
    r = np.clip(risk + noise, 0, 1)
    income = customers["base_income"].to_numpy()[index] * rng.lognormal(
        0, 0.08, len(index)
    )
    return pd.DataFrame(
        {
            "customer_id": customers["customer_id"].to_numpy()[index],
            "report_ts": start + pd.to_timedelta(offsets, unit="s"),
            "customer_name": customers["customer_name"].to_numpy()[index],
            "customer_type": customers["customer_type"].to_numpy()[index],
            "credit_score": np.clip(
                np.round(820 - 430 * r + rng.normal(0, 18, len(index))), 300, 850
            ).astype(np.int64),
            "monthly_income": np.round(income, 0),
            "total_debt": np.round(
                income * (0.5 + 9 * r) * rng.lognormal(0, 0.3, len(index)), 0
            ),
            "delinquencies_12m": rng.poisson(0.1 + 3.5 * r**2).astype(np.int64),
            "open_credit_lines": rng.poisson(2 + 5 * r).astype(np.int64),
            "bankruptcies": (rng.random(len(index)) < 0.25 * r**3).astype(np.int64),
        }
    ).sort_values(["customer_id", "report_ts"], ignore_index=True)


def underwrite(
    risk: np.ndarray,
    income: np.ndarray,
    monthly_price: np.ndarray,
    lease_months: np.ndarray,
    prior_outcome: np.ndarray,
    rng: np.random.Generator,
) -> np.ndarray:
    """1 where an underwriter leases the machine, from what they know of the customer.

    The underwriter knows the customer: their hidden risk (which their credit reports
    measure with noise), their income, and how their previous order ended
    (`prior_outcome`, "" without one). A model sees only the reports and the last
    settled lease through the feature view, so it learns the decisions well but not
    perfectly.
    """
    payment_to_income = monthly_price / np.maximum(income, 1)
    prior = np.select(
        [
            prior_outcome == "paid",
            prior_outcome == "paid_late",
            np.isin(prior_outcome, ["damaged", "defaulted"]),
        ],
        [0.9, -0.2, -1.6],
        default=0.0,
    )
    score = 5.6 - 13.0 * risk - 16 * payment_to_income - 0.06 * lease_months + 1.2 * prior
    return (rng.random(len(score)) < _sigmoid(score)).astype(np.int64)


def settle(
    leased: np.ndarray,
    risk: np.ndarray,
    damage_risk: np.ndarray,
    lease_months: np.ndarray,
    monthly_price: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """How each lease ended: declined when not leased, else paid, paid late, damaged or defaulted."""
    n = len(leased)
    u = rng.random(n)
    p_default = 0.01 + 0.30 * risk**2 + 0.004 * lease_months
    p_damage = damage_risk * (0.6 + 1.2 * risk)
    p_late = 0.05 + 0.45 * risk
    outcome = np.select(
        [u < p_default, u < p_default + p_damage, u < p_default + p_damage + p_late],
        ["defaulted", "damaged", "paid_late"],
        default="paid",
    )
    outcome = np.where(leased == 1, outcome, "declined")
    days_late = np.where(outcome == "paid_late", rng.integers(3, 60, n), 0)
    days_late = np.where(outcome == "defaulted", rng.integers(60, 180, n), days_late)
    damage_cost = np.where(
        outcome == "damaged", np.round(monthly_price * rng.uniform(1.5, 8, n), 0), 0.0
    )
    return pd.DataFrame(
        {
            "outcome": outcome,
            "paid": np.isin(outcome, ["paid", "paid_late", "damaged"]).astype(np.int64),
            "problem_free": (outcome == "paid").astype(np.int64),
            "days_late": days_late.astype(np.int64),
            "damage_cost": damage_cost.astype(float),
        }
    )


def generate_orders(
    customers: pd.DataFrame,
    equipment: pd.DataFrame,
    rows: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """`rows` orders between `start` and `end` and their outcomes, decided a month at a time.

    Every order is placed after the customers' first credit reports (at `start`) and
    its lease ends before `end`. Each month's orders are decided knowing how each
    customer's orders of the earlier months ended.
    """
    customer_index = rng.integers(0, len(customers), rows)
    months = rng.choice(LEASE_MONTHS, rows, p=[0.3, 0.3, 0.25, 0.15])
    latest = end - pd.to_timedelta(months * 30, unit="D")
    first = start + pd.Timedelta(days=1)
    order_ts = first + (latest - first) * rng.random(rows)
    orders = pd.DataFrame(
        {
            "customer_id": customers["customer_id"].to_numpy()[customer_index],
            "equipment_id": equipment["equipment_id"].to_numpy()[
                rng.integers(0, len(equipment), rows)
            ],
            "lease_months": months.astype(np.int64),
            "order_ts": pd.to_datetime(order_ts),
        }
    ).sort_values("order_ts", ignore_index=True)
    orders.insert(0, "order_id", np.arange(1, rows + 1, dtype=np.int64))
    orders["leased"] = 0
    outcomes = []
    last = pd.Series(dtype=object)  # each customer's latest outcome so far
    for _, batch in orders.groupby(orders["order_ts"].dt.to_period("M")):
        frame = decide(batch, customers, equipment, last, rng)
        orders.loc[batch.index, "leased"] = frame["leased"].to_numpy()
        outcomes.append(frame)
        newest = frame.groupby("customer_id")["outcome"].last()
        last = pd.concat([last[~last.index.isin(newest.index)], newest])
    outcome = pd.concat(outcomes).loc[orders.index]
    return orders, outcome[
        [
            "order_id",
            "customer_id",
            "outcome_ts",
            "outcome",
            "paid",
            "problem_free",
            "days_late",
            "damage_cost",
        ]
    ].reset_index(drop=True)


def decide(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    equipment: pd.DataFrame,
    last: pd.Series,
    rng: np.random.Generator,
    settled_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """The underwriter's decision on each order and how the lease ended, indexed like `orders`.

    `last` is each customer's latest outcome, indexed by customer_id. Outcomes settle at
    the end of the lease, or at `settled_at` when given (the app's simulation, which
    fast-forwards every lease to its end now).
    """
    who = customers.set_index("customer_id").loc[orders["customer_id"]]
    what = equipment.set_index("equipment_id").loc[orders["equipment_id"]]
    prior = last.reindex(orders["customer_id"]).fillna("").astype(str).to_numpy()
    leased = underwrite(
        who["risk"].to_numpy(),
        who["base_income"].to_numpy(),
        what["monthly_price"].to_numpy(),
        orders["lease_months"].to_numpy(),
        prior,
        rng,
    )
    ended = settle(
        leased,
        who["risk"].to_numpy(),
        what["damage_risk"].to_numpy(),
        orders["lease_months"].to_numpy(),
        what["monthly_price"].to_numpy(),
        rng,
    )
    ended.index = orders.index
    if settled_at is not None:
        outcome_ts = pd.Series(settled_at, index=orders.index)
    else:
        # A declined order settles the next day, a lease at its end.
        days = np.where(leased == 1, orders["lease_months"].to_numpy() * 30, 1)
        outcome_ts = orders["order_ts"] + pd.to_timedelta(days, unit="D")
    return pd.concat(
        [
            orders[["order_id", "customer_id"]].assign(
                leased=leased, outcome_ts=outcome_ts.to_numpy()
            ),
            ended,
        ],
        axis=1,
    )
