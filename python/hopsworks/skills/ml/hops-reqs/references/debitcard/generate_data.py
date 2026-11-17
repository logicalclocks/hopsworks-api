"""Generate a year of synthetic debit card authorisations into offline feature groups.

    python generate_data.py                       # 1,000,000 authorisations
    python generate_data.py --rows 100000
    python generate_data.py --tables card_authorisations,card_authorisation_signals
    python generate_data.py --generate-only       # print samples, write nothing

card_authorisations and card_authorisation_signals describe the same
transactions, 1:1 on (card_number, transaction_time), so their keys are drawn
once and sliced batch by batch for both. The fraud schemes move some of those
times to cluster a card's fraudulent transactions.
"""

import argparse
import gc
import time

import numpy as np
import synth

import hopsworks
from hsfs.feature import Feature

DEFAULT_ROWS = 1_000_000
DEFAULT_BATCH_SIZE = 500_000
TABLES = [
    "card_authorisations",
    "card_authorisation_signals",
    "card_weekly_spend",
    "account_events",
]


def card_count(rows: int) -> int:
    """The cards of a history of `rows` authorisations; transaction_stream.py draws its cards from the same pool."""
    return max(10_000, rows // 100)


AUTHORISATIONS = {
    "description": "Debit card authorisations with risk scores, channel, merchant and velocity signals; amounts in EUR",
    "primary_key": ["transaction_id"],
    "event_time": "transaction_time",
    "features": [
        Feature("transaction_id", "bigint", description="Unique authorisation identifier"),
        Feature("card_number", "string", description="16-digit card number"),
        Feature(
            "transaction_time", "timestamp", description="When the authorisation was processed"
        ),
        Feature("reporting_month", "date", description="First day of the authorisation's month"),
        Feature(
            "network_risk_score",
            "double",
            description="Card scheme's real-time risk score, 0-999, higher is riskier",
        ),
        Feature("amount_eur", "double", description="Authorised amount in EUR"),
        Feature(
            "fraud_rule_set_id",
            "string",
            description="Issuer fraud rule set that evaluated the authorisation",
        ),
        Feature(
            "authorisation_reference", "string", description="12-character authorisation reference"
        ),
        Feature(
            "transaction_type_code",
            "string",
            description="Transaction type: 00 purchase, 01 cash withdrawal, 51 reversal and others",
        ),
        Feature(
            "card_txn_count_24h",
            "int",
            description="Authorisations on the card in the previous 24 hours",
        ),
        Feature(
            "mcc_amount_24h_eur",
            "double",
            description="Amount at this merchant category in the previous 24 hours, EUR",
        ),
        Feature(
            "mcc_txn_count_24h",
            "int",
            description="Authorisations at this merchant category in the previous 24 hours",
        ),
        Feature(
            "card_scheme",
            "string",
            description="Card scheme that routed the authorisation: VISA, MASTERCARD, MAESTRO, GIROCARD, CB",
        ),
        Feature("acquirer_id", "string", description="Acquiring bank that sent the authorisation"),
        Feature(
            "merchant_distance_km",
            "double",
            description="Distance from the cardholder's home to the merchant, km",
        ),
        Feature(
            "cash_withdrawals_48h_eur",
            "double",
            description="Cash withdrawn with the card in the previous 48 hours, EUR",
        ),
        Feature(
            "account_change_risk_code",
            "string",
            description="Risk level of recent non-monetary account changes, 0 none to 3 high",
        ),
        Feature(
            "cvc2_check_result",
            "string",
            description="CVC2 check: M match, N no match, P not processed, S should be present, U unavailable",
        ),
        Feature(
            "cvc_check_result",
            "string",
            description="Chip or stripe CVC check: M match, N no match, P not processed, U unavailable",
        ),
        Feature("account_age_days", "int", description="Days since the account was opened"),
        Feature(
            "days_since_card_issued", "int", description="Days since the physical card was issued"
        ),
        Feature(
            "days_since_wallet_added",
            "int",
            description="Days since the card was last added to a mobile wallet",
        ),
        Feature(
            "ecommerce_indicator",
            "string",
            description="E-commerce indicator (ECI): 05 authenticated, 06 attempted, 07 not authenticated, blank in person",
        ),
        Feature(
            "pos_entry_mode",
            "string",
            description="POS entry mode: 01 manual, 02 stripe, 05 chip, 07 contactless, 09 e-commerce, 80/81 token, 90 stripe fallback",
        ),
        Feature(
            "authorisation_decision",
            "string",
            description="A approved, D declined, R referred for review",
        ),
        Feature(
            "issuer_rule_tag_1", "string", description="First risk tag the issuer's rules attached"
        ),
        Feature(
            "issuer_rule_tag_2", "string", description="Second risk tag the issuer's rules attached"
        ),
        Feature("merchant_id", "string", description="Merchant identifier"),
        Feature(
            "wallet_id",
            "string",
            description="Digital wallet identifier, null when no wallet was used",
        ),
        Feature("mcc", "string", description="Merchant category code, ISO 18245"),
        Feature("merchant_name", "string", description="Merchant name and city"),
        Feature(
            "issuer_fraud_score",
            "int",
            description="Issuer's fraud model score, 0-999, higher is likelier fraud",
        ),
        Feature(
            "queued_for_review",
            "string",
            description="Y when the authorisation was queued for an analyst",
        ),
        Feature(
            "authorisation_region",
            "string",
            description="Where the card was used: EEA (home), UK, NA, AP, LA",
        ),
        Feature(
            "three_ds_version",
            "string",
            description="EMV 3-D Secure version of the strong customer authentication, blank without",
        ),
        Feature(
            "token_requestor_id",
            "string",
            description="Token requestor of a tokenised payment, null when not tokenised",
        ),
    ],
}

FEATURE_GROUPS = {
    "card_authorisations": AUTHORISATIONS,
    "card_authorisation_signals": {
        "description": "Per-authorisation review counts, spend ratio, disputes and the confirmed fraud labels",
        "primary_key": ["card_number", "transaction_time"],
        "event_time": "transaction_time",
        "features": [
            Feature("card_number", "string", description="16-digit card number"),
            Feature(
                "transaction_time", "timestamp", description="When the authorisation was processed"
            ),
            Feature(
                "reporting_month", "date", description="First day of the authorisation's month"
            ),
            Feature(
                "queued_txn_count_6h",
                "int",
                description="The card's authorisations queued for review in the previous 6 hours",
            ),
            Feature(
                "queued_txn_count_24h",
                "int",
                description="The card's authorisations queued for review in the previous 24 hours",
            ),
            Feature(
                "declined_amount_1h_eur",
                "double",
                description="Amount declined on the card in the last hour, EUR",
            ),
            Feature(
                "spend_ratio", "double", description="This amount over the cardholder's usual spend"
            ),
            Feature(
                "dispute_count_3m",
                "int",
                description="Disputes the cardholder raised in the previous 3 months",
            ),
            Feature(
                "dispute_amount_3m_eur",
                "double",
                description="Amount disputed in the previous 3 months, EUR",
            ),
            Feature(
                "approved_fraud_flag",
                "int",
                description="1 when the authorisation was approved and later confirmed as fraud",
            ),
            Feature(
                "declined_fraud_flag",
                "int",
                description="1 when the authorisation was declined and confirmed as fraud",
            ),
            Feature(
                "fraud_flag", "int", description="1 when the authorisation was confirmed as fraud"
            ),
            Feature(
                "card_token_device_count", "int", description="Devices holding a token of the card"
            ),
            Feature(
                "account_token_device_count",
                "int",
                description="Devices holding a token of any card of the account",
            ),
            Feature(
                "fraud_report_date", "date", description="When fraud was reported, null when never"
            ),
        ],
    },
    "card_weekly_spend": {
        "description": "Each card's trailing 7-day spend by day",
        "primary_key": ["card_number", "day"],
        "event_time": "day",
        "features": [
            Feature("card_number", "string", description="16-digit card number"),
            Feature("day", "date", description="Last day of the 7 days"),
            Feature("amount_7d_eur", "double", description="Spend over the 7 days, EUR"),
            Feature(
                "spend_ratio_7d",
                "double",
                description="The 7 days' spend over the cardholder's usual week",
            ),
        ],
    },
    "account_events": {
        "description": "Non-monetary account changes: phone and address changes, lost cards, PIN and limit changes",
        "primary_key": ["event_id"],
        "event_time": "event_date",
        "features": [
            Feature("event_id", "bigint", description="Unique event identifier"),
            Feature("account_id", "string", description="Account identifier"),
            Feature(
                "event_type",
                "string",
                description="PHONE_CHANGE, ADDRESS_CHANGE, CARD_LOST, PIN_CHANGE, LIMIT_CHANGE, CONTACT_UPDATE or OTHER",
            ),
            Feature("event_date", "date", description="When the change was made"),
        ],
    },
}


def insert_in_batches(fs, name: str, generate, total: int, batch_size: int):
    """Create `name` offline and insert `total` rows in batches, waiting for each to land."""
    config = FEATURE_GROUPS[name]
    fg = fs.get_or_create_feature_group(
        name=name,
        version=1,
        description=config["description"],
        primary_key=config["primary_key"],
        event_time=config["event_time"],
        features=config["features"],
        online_enabled=False,
        statistics_config=False,
    )
    batches = (total + batch_size - 1) // batch_size
    started = time.time()
    for i in range(batches):
        rows = min(batch_size, total - i * batch_size)
        frame = generate(i * batch_size, rows)
        print(f"  {name}: batch {i + 1}/{batches}, {len(frame):,} rows")
        fg.insert(frame, write_options={"wait_for_job": True})
        del frame
        gc.collect()
    print(f"  {name}: {total:,} rows in {time.time() - started:.0f}s")
    return fg


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="authorisations to generate")
    parser.add_argument("--tables", help="comma-separated feature groups; default all")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--fraud-rate", type=float, default=0.05, help="fraction of cards with a fraud scheme"
    )
    parser.add_argument("--generate-only", action="store_true")
    args = parser.parse_args()

    tables = args.tables.split(",") if args.tables else TABLES
    unknown = set(tables) - set(TABLES)
    if unknown:
        parser.error(f"unknown tables {sorted(unknown)}; one of {TABLES}")

    rows = args.rows
    counts = {
        "card_authorisations": rows,
        "card_authorisation_signals": rows,
        "card_weekly_spend": max(200, rows // 50),
        "account_events": max(1000, rows // 10),
    }
    rng = np.random.default_rng(args.seed)
    # The card pool is the generator's first draw: transaction_stream.py rebuilds it from the seed.
    cards = synth.generate_card_pool(card_count(rows), rng)
    accounts = synth.generate_account_pool(max(10_000, rows // 100))
    merchants = synth.generate_merchant_names(5_000, rng)
    acquirers = synth.generate_acquirer_ids(50)
    wallets = synth.generate_wallet_ids(20_000)
    token_requestors = synth.generate_token_requestor_ids(30)

    card_numbers = cards[synth.power_law_indices(rng, len(cards), rows)]
    times = synth.random_timestamps(rng, rows)
    overlay = synth.generate_fraud_plan(card_numbers, times, rng, fraud_rate=args.fraud_rate)

    generators = {
        "card_authorisations": lambda start, n: synth.generate_card_authorisations(
            card_numbers[start : start + n],
            times[start : start + n],
            merchants,
            acquirers,
            wallets,
            token_requestors,
            rng,
            synth.slice_overlay(overlay, start, start + n),
        ).with_columns(transaction_id=np.arange(start + 1, start + n + 1, dtype=np.int64)),
        "card_authorisation_signals": lambda start, n: synth.generate_card_authorisation_signals(
            card_numbers[start : start + n],
            times[start : start + n],
            rng,
            synth.slice_overlay(overlay, start, start + n),
        ),
        "card_weekly_spend": lambda start, n: synth.generate_card_weekly_spend(
            n, np.unique(card_numbers), rng, overlay["fraud_cards"]
        ),
        "account_events": lambda start, n: synth.generate_account_events(n, accounts, rng),
    }

    if args.generate_only:
        for table in tables:
            frame = generators[table](0, min(1000, counts[table]))
            print(f"\n{table}: {frame.shape}")
            print(frame.head(3))
        return

    project = hopsworks.login()
    fs = project.get_feature_store()
    print(f"Project {project.name}: {', '.join(f'{t} {counts[t]:,}' for t in tables)}")
    for table in tables:
        # The daily and account tables are generated whole, not batch by batch.
        batch = (
            args.batch_size
            if table in ("card_authorisations", "card_authorisation_signals")
            else counts[table]
        )
        insert_in_batches(fs, table, generators[table], counts[table], batch)


if __name__ == "__main__":
    main()
