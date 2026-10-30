# ruff: noqa: INP001
"""Write the leasing history: the machines, the customers' credit reports, the orders and how they ended.

    python generate_data.py [--orders 10000] [--credit-reports 20000] [--system leasing-example]

Writes four feature groups as they are generated, with no feature engineering:
leasing_equipment (100 gardening machines), customer_credit_history (credit reports of
5,000 customers over two years), leasing_orders (whether each order was leased) and
leasing_outcomes (how each one ended: paid, paid late, damaged, defaulted or declined).
Every feature group is online, so the app reads the newest report and outcome of a
customer, and the feature view's transformations run in the app.

The history ends now, so a second run would write a second history beside the first:
it leaves feature groups that exist alone, and refuses when only some exist. Delete
all four to write a new history.
"""

import argparse

import hopsworks
import names
import pandas as pd
import synth


DAYS = 760


def to_seconds(times: pd.Series) -> pd.Series:
    """Timestamps to the second, in the microseconds the feature store keeps."""
    return times.dt.floor("s").astype("datetime64[us]")


def tables(
    orders: int, credit_reports: int, end: pd.Timestamp
) -> dict[str, pd.DataFrame]:
    """The four tables, keyed by feature group name, with timestamps to the second."""
    customers, equipment, rng = synth.world(
        names.SEED, names.CUSTOMERS, names.EQUIPMENT_ITEMS
    )
    start = end - pd.Timedelta(days=DAYS)
    credit = synth.generate_credit_history(customers, credit_reports, start, end, rng)
    order_rows, outcomes = synth.generate_orders(
        customers, equipment, orders, start, end, rng
    )
    credit["report_ts"] = to_seconds(credit["report_ts"])
    order_rows["order_ts"] = to_seconds(order_rows["order_ts"])
    outcomes["outcome_ts"] = to_seconds(outcomes["outcome_ts"])
    # The upsert key is the customer and the event time: two of a customer's rows in one
    # second would be one row.
    credit = credit.drop_duplicates(["customer_id", "report_ts"])
    # In time order: the online store keeps the row written last for each customer,
    # which must be their newest, as the point-in-time join at "now" would pick.
    outcomes = outcomes.drop_duplicates(["customer_id", "outcome_ts"]).sort_values(
        "outcome_ts", ignore_index=True
    )
    return {
        # Listed before the first order: the feature view's point-in-time join finds every machine.
        names.EQUIPMENT: equipment.drop(columns=["damage_risk"]).assign(
            listed_ts=to_seconds(
                pd.Series(start - pd.Timedelta(days=30), index=equipment.index)
            )
        ),
        names.CREDIT: credit,
        names.ORDERS: order_rows,
        names.OUTCOMES: outcomes,
    }


# The feature view's joins are point-in-time only when every group has an event time,
# so the machines carry when they were listed. It joins the credit reports and the
# outcomes on customer_id, and a join must cover the joined group's whole primary key,
# so both are keyed by the customer with an event time. The offline store keeps every row (its upsert key includes the
# event time), which the point-in-time joins read; the online store keeps each
# customer's newest report and outcome, which the app reads.
GROUPS = {
    names.EQUIPMENT: {
        "primary_key": ["equipment_id"],
        "event_time": "listed_ts",
        "description": "gardening machines for lease, with their monthly price and replacement value",
    },
    names.CREDIT: {
        "primary_key": ["customer_id"],
        "event_time": "report_ts",
        "description": "credit reports of the customers who lease machines",
    },
    names.ORDERS: {
        "primary_key": ["order_id"],
        "event_time": "order_ts",
        "description": "lease orders and whether each was leased (the label)",
    },
    names.OUTCOMES: {
        "primary_key": ["customer_id"],
        "event_time": "outcome_ts",
        "description": "how each lease order ended: paid, paid_late, damaged, defaulted or declined",
    },
}


def write(fs, name: str, frame: pd.DataFrame, system: str):
    spec = dict(GROUPS[name])
    description = spec.pop("description")
    fg = fs.get_or_create_feature_group(
        name=name,
        version=1,
        online_enabled=True,
        statistics_config=False,
        description=f"{system}: {description}",
        **spec,
    )
    fg.insert(frame, write_options={"wait_for_job": True})
    return fg


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--credit-reports", type=int, default=20_000)
    # Every asset's description starts with the system's slug, which is how deleting the
    # system with its assets tells its own from another's.
    parser.add_argument("--system", default="leasing-example", help="the system's slug")
    args = parser.parse_args()

    fs = hopsworks.login().get_feature_store()
    existing = [
        name for name in GROUPS if fs.get_feature_group(name, version=1) is not None
    ]
    if len(existing) == len(GROUPS):
        print(
            "The history is already written: delete the four feature groups to write a new one"
        )
        return
    if existing:
        raise SystemExit(
            f"Only some of the history exists ({', '.join(existing)}): delete it and run again"
        )
    frames = tables(
        args.orders,
        args.credit_reports,
        pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s"),
    )
    for name, frame in frames.items():
        write(fs, name, frame, args.system)
        print(f"{name}: {len(frame):,} rows")
    orders = frames[names.ORDERS]
    print(f"{orders[names.LABEL].mean():.0%} of the orders were leased")


if __name__ == "__main__":
    main()
