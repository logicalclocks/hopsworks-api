"""Synthetic debit card authorisations with injected fraud schemes.

Shared by the history generator (generate_data.py) and the live transaction
stream (transaction_stream.py). Amounts are in euros and distances in
kilometres. Every table is generated with numpy and Polars, never row by row,
except where a nullable string column must be a Python list for Polars to infer
a string type with nulls.
"""

import datetime
import string
from enum import Enum

import numpy as np
import polars as pl

MCC_CODES = [
    "5411",
    "5812",
    "5541",
    "5311",
    "5999",
    "5912",
    "5814",
    "7011",
    "4829",
    "5310",
    "5691",
    "5651",
    "5200",
    "5732",
    "5331",
    "7523",
    "5542",
    "8011",
    "8021",
    "5047",
]

MERCHANT_PREFIXES = [
    "CARREFOUR",
    "TESCO",
    "LIDL",
    "ALDI",
    "REWE",
    "EDEKA",
    "AUCHAN",
    "LECLERC",
    "MERCADONA",
    "SAINSBURYS",
    "MONOPRIX",
    "DM DROGERIE",
    "ROSSMANN",
    "BOOTS",
    "IKEA",
    "MEDIAMARKT",
    "FNAC",
    "DECATHLON",
    "ZARA",
    "PRIMARK",
    "SHELL",
    "TOTALENERGIES",
    "ESSO",
    "REPSOL",
    "ENI",
    "MCDONALDS",
    "BURGER KING",
    "SUBWAY",
    "STARBUCKS",
    "PRET A MANGER",
    "COSTA COFFEE",
    "PAUL BOULANGERIE",
    "DOMINOS",
    "DEUTSCHE BAHN",
    "SNCF",
    "RENFE",
    "TRENITALIA",
    "RYANAIR",
    "EASYJET",
    "BOLT",
    "ICA",
    "COOP",
    "SPAR",
    "JUMBO",
    "ALBERT HEIJN",
    "ESSELUNGA",
    "CONAD",
    "BILLA",
    "KAUFLAND",
    "NETTO",
]

CITIES = [
    "PARIS",
    "BERLIN",
    "MADRID",
    "ROME",
    "AMSTERDAM",
    "BRUSSELS",
    "VIENNA",
    "STOCKHOLM",
    "COPENHAGEN",
    "DUBLIN",
    "LISBON",
    "MILAN",
    "MUNICH",
    "HAMBURG",
    "LYON",
    "BARCELONA",
    "WARSAW",
    "PRAGUE",
    "HELSINKI",
    "ATHENS",
]

CARD_SCHEMES = ["VISA", "MASTERCARD", "MAESTRO", "GIROCARD", "CB"]
CARD_SCHEME_WEIGHTS = [0.40, 0.32, 0.12, 0.08, 0.08]

# The card's home region is the EEA; the rest are authorisations abroad.
REGIONS = ["EEA", "UK", "NA", "AP", "LA"]
REGION_WEIGHTS = [0.82, 0.06, 0.05, 0.05, 0.02]


class FraudScheme(Enum):
    VELOCITY_SPIKE = "velocity_spike"
    CARD_TESTING = "card_testing"
    GEO_INCONSISTENCY = "geo_inconsistency"
    MERCHANT_ANOMALY = "merchant_anomaly"
    AMOUNT_ANOMALY = "amount_anomaly"
    ODD_HOUR = "odd_hour"
    HIGH_DECLINE_RATIO = "high_decline_ratio"
    CROSS_ENTITY = "cross_entity"


FRAUD_SCHEMES = list(FraudScheme)
FRAUD_SCHEME_WEIGHTS = [0.20, 0.15, 0.15, 0.10, 0.15, 0.10, 0.08, 0.07]

HIGH_RISK_MCC_CODES = ["5944", "5094", "7995", "5045", "5065", "5946", "5964"]

SUSPICIOUS_MERCHANT_NAMES = [
    "CRYPTO EXCHANGE INTL",
    "GOLD BUYERS EXPRESS",
    "ONLINE GAMING SVCS",
    "MONEY TRANSFER DEPOT",
    "PREPAID CARDS PLUS",
    "DIGITAL GOODS STORE",
    "OVERSEAS ELECTRONICS",
    "GIFT CARD WAREHOUSE",
    "QUICK CASH ATM",
    "UNTRACEABLE PAYMENTS",
    "FOREIGN EXCHANGE HUB",
    "VIRTUAL CARD SHOP",
]

GEO_SETS = [
    ["EEA", "NA", "AP"],
    ["EEA", "LA", "UK"],
    ["UK", "AP", "LA"],
]

# The history ends here; generate_data.py's transactions fall in the year before.
HISTORY_END = datetime.datetime(2026, 3, 31)


# region Entity pools


def generate_card_pool(n: int, rng: np.random.Generator) -> np.ndarray:
    """n 16-digit card numbers. Drawn first from a fresh generator, the pool of
    a smaller n is a prefix of a larger one's, so the live stream's cards are
    cards of the history."""
    prefixes = np.array(["4000", "4111", "4222", "4333", "5100", "5200", "5500", "6759"])
    pfx = prefixes[np.arange(n) % len(prefixes)]
    suffixes = rng.integers(0, 10**12, size=n)
    return np.char.add(pfx, np.char.zfill(suffixes.astype(str), 12))


def generate_account_pool(n: int) -> np.ndarray:
    return np.char.add("ACC", np.char.zfill(np.arange(1, n + 1).astype(str), 9))


def generate_merchant_names(n: int, rng: np.random.Generator) -> np.ndarray:
    prefixes = np.array(MERCHANT_PREFIXES)[np.arange(n) % len(MERCHANT_PREFIXES)]
    cities = np.array(CITIES)[rng.integers(0, len(CITIES), size=n)]
    stores = rng.integers(100, 9999, size=n).astype(str)
    return np.char.add(np.char.add(np.char.add(prefixes, " #"), np.char.add(stores, " ")), cities)


def generate_acquirer_ids(n: int) -> np.ndarray:
    return np.array([f"ACQ{i:04d}" for i in range(1, n + 1)])


def generate_token_requestor_ids(n: int) -> np.ndarray:
    return np.array([f"TR{i:06d}" for i in range(1, n + 1)])


def generate_wallet_ids(n: int) -> np.ndarray:
    return np.array([f"WLT{i:08d}" for i in range(1, n + 1)])


def weighted_choice(rng: np.random.Generator, choices: list, weights: list, n: int) -> np.ndarray:
    p = np.array(weights, dtype=float)
    return rng.choice(choices, size=n, p=p / p.sum())


def random_timestamps(rng: np.random.Generator, n: int, days_back: int = 365) -> np.ndarray:
    """n times in the `days_back` days before HISTORY_END, most of them in the day."""
    start_ts = (HISTORY_END - datetime.timedelta(days=days_back)).timestamp()
    seconds = rng.uniform(start_ts, HISTORY_END.timestamp(), size=n)
    hours = ((seconds - start_ts) % 86400) / 3600
    accept = np.where(
        (hours >= 10) & (hours <= 20),
        0.9,
        np.where(
            (hours >= 7) & (hours < 10), 0.5, np.where((hours > 20) & (hours <= 23), 0.4, 0.1)
        ),
    )
    reject = rng.random(n) > accept
    seconds = np.where(reject, seconds - hours * 3600 + rng.uniform(10, 20, size=n) * 3600, seconds)
    return (seconds * 1_000_000).astype("datetime64[us]")


def power_law_indices(
    rng: np.random.Generator, pool_size: int, n: int, alpha: float = 0.5
) -> np.ndarray:
    """n indices into a pool, a few of them far more often than the rest."""
    weights = np.arange(1, pool_size + 1, dtype=float) ** (-alpha)
    return rng.choice(pool_size, size=n, p=weights / weights.sum())


# endregion

# region Fraud overlay
#
# An overlay holds, per transaction, the values a fraud scheme forces; unset
# slots are NaN, -1 or "". `is_fraud` is the label; `signal_mask` says which
# rows get the forced values. They differ on purpose: stealth fraud is labelled
# but looks normal, and hard negatives look like fraud but are not labelled.


_OVERLAY_FLOAT_KEYS = (
    "amount",
    "network_score",
    "issuer_score",
    "mcc_amount_24h",
    "distance_km",
    "cash_48h",
    "signal_declined_amount",
    "signal_spend_ratio",
)
_OVERLAY_INT_KEYS = (
    "txn_count_24h",
    "mcc_txn_count_24h",
    "signal_queued_6h",
    "signal_queued_24h",
    "signal_card_devices",
    "signal_account_devices",
)
_OVERLAY_STR_KEYS = (
    "region",
    "entry_mode",
    "decision",
    "mcc",
    "merchant_name",
    "cvc2",
    "cvc",
    "queued",
    "eci",
    "wallet_id",
    "token_requestor_id",
)


def make_empty_overlay(n: int) -> dict:
    overlay = {
        "is_fraud": np.zeros(n, dtype=bool),
        "signal_mask": np.zeros(n, dtype=bool),
        "scheme": np.full(n, "", dtype="U30"),
    }
    overlay.update({k: np.full(n, np.nan) for k in _OVERLAY_FLOAT_KEYS})
    overlay.update({k: np.full(n, -1, dtype=np.int32) for k in _OVERLAY_INT_KEYS})
    overlay.update({k: np.full(n, "", dtype="U60") for k in _OVERLAY_STR_KEYS})
    return overlay


def _cluster_timestamps(
    times: np.ndarray, indices: np.ndarray, base_ts: np.int64, window_minutes: int, rng
):
    offsets = rng.integers(0, window_minutes * 60 * 1_000_000, size=len(indices))
    times[indices] = (base_ts + offsets + np.arange(len(indices)) * 1_000).astype("datetime64[us]")


def _ints(rng, low, high, n):
    return rng.integers(low, high, size=n).astype(np.int32)


def apply_velocity_spike(card_indices, times, overlay, rng):
    n = min(rng.integers(6, 14), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    _cluster_timestamps(times, chosen, times[card_indices[0]].astype(np.int64), 60, rng)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.VELOCITY_SPIKE.value
    overlay["txn_count_24h"][chosen] = _ints(rng, 8, 22, n)
    overlay["mcc_txn_count_24h"][chosen] = _ints(rng, 3, 10, n)
    overlay["issuer_score"][chosen] = rng.integers(400, 850, size=n)
    overlay["network_score"][chosen] = rng.uniform(350, 800, size=n)
    overlay["signal_queued_6h"][chosen] = _ints(rng, 2, 10, n)
    overlay["signal_queued_24h"][chosen] = _ints(rng, 5, 18, n)
    overlay["signal_spend_ratio"][chosen] = np.round(rng.uniform(2.0, 7.0, size=n), 3)


def apply_card_testing(card_indices, times, overlay, rng):
    n = min(rng.integers(3, 10), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    _cluster_timestamps(times, chosen, times[card_indices[0]].astype(np.int64), 240, rng)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.CARD_TESTING.value
    micro = rng.random(n) < 0.50
    overlay["amount"][chosen] = np.where(
        micro,
        np.round(rng.uniform(0.50, 5.00, size=n), 2),
        np.round(rng.lognormal(2.5, 0.6, size=n), 2),
    )
    overlay["mcc"][chosen] = rng.choice(["5999", "4829", "5732"], size=n)
    overlay["entry_mode"][chosen] = rng.choice(
        ["09", "05", "07", ""], size=n, p=[0.50, 0.20, 0.20, 0.10]
    )
    overlay["txn_count_24h"][chosen] = _ints(rng, 6, 18, n)
    overlay["issuer_score"][chosen] = rng.integers(350, 800, size=n)
    overlay["network_score"][chosen] = rng.uniform(300, 750, size=n)
    decisions = rng.choice(["A", "D"], size=n, p=[0.40, 0.60])
    overlay["decision"][chosen] = decisions
    overlay["signal_declined_amount"][chosen] = np.where(
        decisions == "D", np.round(rng.uniform(0.50, 5.00, size=n), 2), 0.0
    )
    overlay["mcc_txn_count_24h"][chosen] = _ints(rng, 4, 12, n)


def apply_geo_inconsistency(card_indices, times, overlay, rng):
    n = min(rng.integers(3, 8), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    _cluster_timestamps(times, chosen, times[card_indices[0]].astype(np.int64), 240, rng)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.GEO_INCONSISTENCY.value
    regions = GEO_SETS[rng.integers(0, len(GEO_SETS))]
    overlay["region"][chosen] = [regions[i % len(regions)] for i in range(n)]
    overlay["entry_mode"][chosen] = rng.choice(
        ["09", "80", "81", "05", "07"], size=n, p=[0.30, 0.15, 0.15, 0.20, 0.20]
    )
    # Overlaps the legitimate travellers' 1,300-4,000 km.
    overlay["distance_km"][chosen] = np.round(rng.uniform(800, 4000, size=n), 1)
    overlay["issuer_score"][chosen] = rng.integers(400, 850, size=n)
    overlay["network_score"][chosen] = rng.uniform(350, 800, size=n)
    overlay["amount"][chosen] = np.round(rng.lognormal(5.0, 0.9, size=n), 2)


def apply_merchant_anomaly(card_indices, times, overlay, rng):
    n = min(rng.integers(2, 6), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.MERCHANT_ANOMALY.value
    overlay["mcc"][chosen] = np.where(
        rng.random(n) < 0.60, rng.choice(HIGH_RISK_MCC_CODES, size=n), rng.choice(MCC_CODES, size=n)
    )
    overlay["merchant_name"][chosen] = np.where(
        rng.random(n) < 0.50, rng.choice(SUSPICIOUS_MERCHANT_NAMES, size=n), ""
    )
    overlay["issuer_score"][chosen] = rng.integers(400, 850, size=n)
    overlay["network_score"][chosen] = rng.uniform(300, 800, size=n)
    overlay["mcc_amount_24h"][chosen] = np.round(rng.uniform(500, 4000, size=n), 2)
    overlay["amount"][chosen] = np.round(rng.lognormal(5.0, 0.8, size=n), 2)
    overlay["mcc_txn_count_24h"][chosen] = _ints(rng, 2, 7, n)


def apply_amount_anomaly(card_indices, times, overlay, rng):
    n = min(rng.integers(2, 6), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.AMOUNT_ANOMALY.value
    pattern = rng.integers(0, 3, size=n)
    overlay["amount"][chosen] = np.select(
        [pattern == 0, pattern == 1],
        [
            np.round(rng.uniform(600, 3500, size=n), 2),
            rng.choice([200.0, 300.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0], size=n),
        ],
        rng.choice([199.99, 299.99, 499.99, 999.99, 1499.99, 1999.99], size=n),
    )
    overlay["issuer_score"][chosen] = rng.integers(400, 850, size=n)
    overlay["network_score"][chosen] = rng.uniform(350, 800, size=n)
    overlay["signal_spend_ratio"][chosen] = np.round(rng.uniform(3.0, 9.0, size=n), 3)
    overlay["cash_48h"][chosen] = np.round(rng.uniform(300, 2500, size=n), 2)


def apply_odd_hour(card_indices, times, overlay, rng):
    n = min(rng.integers(2, 6), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    midnight = times[chosen].astype("datetime64[D]").astype("datetime64[us]").astype(np.int64)
    hour = (rng.uniform(1.0, 6.0, size=n) * 3600 * 1_000_000).astype(np.int64)
    times[chosen] = (midnight + hour + rng.integers(0, 3600 * 1_000_000, size=n)).astype(
        "datetime64[us]"
    )
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.ODD_HOUR.value
    overlay["entry_mode"][chosen] = rng.choice(
        ["09", "80", "81", "05", "07"], size=n, p=[0.30, 0.15, 0.15, 0.20, 0.20]
    )
    overlay["issuer_score"][chosen] = rng.integers(350, 800, size=n)
    overlay["network_score"][chosen] = rng.uniform(300, 750, size=n)
    overlay["amount"][chosen] = np.round(rng.lognormal(4.5, 1.0, size=n), 2)
    overlay["eci"][chosen] = np.where(
        rng.random(n) < 0.50, rng.choice(["05", "06", "07"], size=n), ""
    )


def apply_high_decline_ratio(card_indices, times, overlay, rng):
    n = min(rng.integers(3, 8), len(card_indices))
    chosen = rng.choice(card_indices, size=n, replace=False)
    _cluster_timestamps(times, chosen, times[card_indices[0]].astype(np.int64), 180, rng)
    overlay["is_fraud"][chosen] = True
    overlay["scheme"][chosen] = FraudScheme.HIGH_DECLINE_RATIO.value
    # Mostly declines, ending with the approval the fraudster was after.
    decisions = np.where(rng.random(n) < 0.60, "D", "A")
    decisions[-1] = "A"
    overlay["decision"][chosen] = decisions
    for key in ("cvc2", "cvc"):
        codes = np.where(rng.random(n) < 0.60, "N", "M")
        codes[-1] = "M"
        overlay[key][chosen] = codes
    overlay["issuer_score"][chosen] = np.clip(
        rng.integers(350, 600) + np.linspace(0, 150, n), 0, 999
    )
    overlay["network_score"][chosen] = rng.uniform(300, 750, size=n)
    overlay["signal_declined_amount"][chosen] = np.where(
        decisions == "D", np.round(rng.lognormal(4.5, 1, size=n), 2), 0.0
    )
    overlay["signal_queued_6h"][chosen] = _ints(rng, 1, 7, n)
    overlay["signal_queued_24h"][chosen] = _ints(rng, 3, 12, n)


def apply_cross_entity(card_indices_list, times, overlay, rng):
    """A ring of cards sharing a wallet or token requestor, each some of the time."""
    shared_wallet = f"WLT_RING_{rng.integers(10000, 99999)}"
    shared_token = f"TR_RING_{rng.integers(100000, 999999)}"
    for card_indices in card_indices_list:
        n = min(rng.integers(2, 6), len(card_indices))
        chosen = rng.choice(card_indices, size=n, replace=False)
        overlay["is_fraud"][chosen] = True
        overlay["scheme"][chosen] = FraudScheme.CROSS_ENTITY.value
        if rng.random() < 0.60:
            overlay["wallet_id"][chosen] = shared_wallet
        if rng.random() < 0.60:
            overlay["token_requestor_id"][chosen] = shared_token
        overlay["issuer_score"][chosen] = rng.integers(400, 850, size=n)
        overlay["network_score"][chosen] = rng.uniform(350, 800, size=n)
        overlay["signal_card_devices"][chosen] = _ints(rng, 3, 10, n)
        overlay["signal_account_devices"][chosen] = _ints(rng, 6, 18, n)
        overlay["entry_mode"][chosen] = rng.choice(
            ["80", "81", "05", "07"], size=n, p=[0.30, 0.30, 0.20, 0.20]
        )


SINGLE_CARD_SCHEMES = {
    FraudScheme.VELOCITY_SPIKE: apply_velocity_spike,
    FraudScheme.CARD_TESTING: apply_card_testing,
    FraudScheme.GEO_INCONSISTENCY: apply_geo_inconsistency,
    FraudScheme.MERCHANT_ANOMALY: apply_merchant_anomaly,
    FraudScheme.AMOUNT_ANOMALY: apply_amount_anomaly,
    FraudScheme.ODD_HOUR: apply_odd_hour,
    FraudScheme.HIGH_DECLINE_RATIO: apply_high_decline_ratio,
}


def apply_universal_fraud_signature(overlay: dict, rng: np.random.Generator):
    """Give each signal row a random quarter of the fraud tells it does not have yet.

    Fraud rings vary their fingerprint, so most fraud looks normal on any single
    feature; the magnitudes overlap the legitimate upper tail.
    """
    fm = overlay["signal_mask"] if overlay["signal_mask"].any() else overlay["is_fraud"]
    if not fm.any():
        return

    def some(unset: np.ndarray) -> np.ndarray:
        rows = fm & unset
        return rows & (rng.random(len(rows)) < 0.25)

    def fill(key, unset, values):
        rows = some(unset)
        if rows.any():
            overlay[key][rows] = values(int(rows.sum()))

    def nan(key):
        return np.isnan(overlay[key])

    def blank(key):
        return overlay[key] == ""

    def negative(key):
        return overlay[key] < 0

    fill("network_score", nan("network_score"), lambda n: rng.uniform(250, 700, size=n))
    fill("issuer_score", nan("issuer_score"), lambda n: rng.integers(250, 700, size=n))
    fill("cvc2", blank("cvc2"), lambda n: np.where(rng.random(n) < 0.35, "N", "M"))
    fill("cvc", blank("cvc"), lambda n: np.where(rng.random(n) < 0.35, "N", "M"))
    fill("queued", blank("queued"), lambda n: np.where(rng.random(n) < 0.40, "Y", "N"))
    fill(
        "eci",
        blank("eci"),
        lambda n: np.where(rng.random(n) < 0.30, rng.choice(["05", "06", "07"], size=n), ""),
    )
    fill(
        "entry_mode",
        blank("entry_mode"),
        lambda n: rng.choice(
            ["09", "80", "81", "05", "07"], size=n, p=[0.30, 0.15, 0.15, 0.20, 0.20]
        ),
    )
    fill("distance_km", nan("distance_km"), lambda n: np.round(rng.uniform(80, 1300, size=n), 1))
    fill("cash_48h", nan("cash_48h"), lambda n: np.round(rng.uniform(100, 1500, size=n), 2))
    fill("amount", nan("amount"), lambda n: np.round(rng.lognormal(4.5, 1.0, size=n), 2))
    fill("txn_count_24h", negative("txn_count_24h"), lambda n: _ints(rng, 3, 10, n))
    fill("mcc_txn_count_24h", negative("mcc_txn_count_24h"), lambda n: _ints(rng, 2, 6, n))
    fill(
        "mcc_amount_24h",
        nan("mcc_amount_24h"),
        lambda n: np.round(rng.uniform(300, 3000, size=n), 2),
    )
    fill("signal_queued_6h", negative("signal_queued_6h"), lambda n: _ints(rng, 1, 5, n))
    fill("signal_queued_24h", negative("signal_queued_24h"), lambda n: _ints(rng, 2, 8, n))
    fill(
        "signal_spend_ratio",
        nan("signal_spend_ratio"),
        lambda n: np.round(rng.uniform(1.5, 5.0, size=n), 3),
    )
    fill("signal_card_devices", negative("signal_card_devices"), lambda n: _ints(rng, 2, 7, n))
    fill(
        "signal_account_devices", negative("signal_account_devices"), lambda n: _ints(rng, 3, 10, n)
    )
    fill(
        "signal_declined_amount",
        nan("signal_declined_amount"),
        lambda n: np.round(rng.lognormal(4, 1, size=n), 2),
    )


def clear_overlay(overlay: dict, idx: np.ndarray):
    for k in _OVERLAY_FLOAT_KEYS:
        overlay[k][idx] = np.nan
    for k in _OVERLAY_INT_KEYS:
        overlay[k][idx] = -1
    for k in _OVERLAY_STR_KEYS:
        overlay[k][idx] = ""


def generate_fraud_plan(
    card_numbers: np.ndarray,
    times: np.ndarray,
    rng: np.random.Generator,
    fraud_rate: float = 0.05,
    signal_dropout: float = 0.35,
    hard_negative_rate: float = 0.012,
) -> dict:
    """The fraud overlay for transactions on `card_numbers` at `times`.

    `fraud_rate` of the cards with 3 or more authorisations get a scheme; the schemes move some of `times`
    in place to cluster their transactions. `signal_dropout` of the fraud rows
    lose their forced values (stealth fraud) and `hard_negative_rate` of the
    legitimate rows get them without the label.
    """
    n = len(card_numbers)
    overlay = make_empty_overlay(n)

    unique_cards, inverse = np.unique(card_numbers, return_inverse=True)
    order = np.argsort(inverse, kind="mergesort")
    bounds = np.concatenate([[0], np.cumsum(np.bincount(inverse, minlength=len(unique_cards)))])
    card_rows = {
        unique_cards[i]: order[bounds[i] : bounds[i + 1]] for i in range(len(unique_cards))
    }

    # A scheme is a sequence of at least 3 authorisations, so only cards with as many can get one.
    eligible = unique_cards[np.diff(bounds) >= 3]
    n_fraud_cards = max(1, int(len(eligible) * fraud_rate))
    fraud_cards = eligible[rng.choice(len(eligible), size=n_fraud_cards, replace=False)]

    rings, used = [], 0
    while used < max(1, int(n_fraud_cards * 0.07)) and used + 3 <= n_fraud_cards:
        size = min(rng.integers(3, 6), n_fraud_cards - used)
        rings.append([card_rows[c] for c in fraud_cards[used : used + size]])
        used += size

    weights = np.array(FRAUD_SCHEME_WEIGHTS[:-1], dtype=float)
    schemes = FRAUD_SCHEMES[:-1]
    counts = {s.value: 0 for s in FraudScheme}
    for card in fraud_cards[used:]:
        rows = card_rows[card]
        scheme = schemes[rng.choice(len(schemes), p=weights / weights.sum())]
        SINGLE_CARD_SCHEMES[scheme](rows, times, overlay, rng)
        counts[scheme.value] += 1
    for ring in rings:
        apply_cross_entity(ring, times, overlay, rng)
        counts[FraudScheme.CROSS_ENTITY.value] += 1

    fraud_idx = np.where(overlay["is_fraud"])[0]
    stealth = rng.choice(fraud_idx, size=int(len(fraud_idx) * signal_dropout), replace=False)
    clear_overlay(overlay, stealth)
    legit_idx = np.where(~overlay["is_fraud"])[0]
    hard_negatives = rng.choice(
        legit_idx, size=int(len(legit_idx) * hard_negative_rate), replace=False
    )
    overlay["signal_mask"][:] = overlay["is_fraud"]
    overlay["signal_mask"][stealth] = False
    overlay["signal_mask"][hard_negatives] = True
    apply_universal_fraud_signature(overlay, rng)

    fraud = int(overlay["is_fraud"].sum())
    print(
        f"  Fraud transactions: {fraud:,} ({100 * fraud / max(n, 1):.2f}%) on {n_fraud_cards:,} cards"
    )
    print(f"  Stealth fraud: {len(stealth):,}; hard negatives: {len(hard_negatives):,}")
    print("  Schemes: " + ", ".join(f"{k} {v}" for k, v in counts.items() if v))

    overlay["fraud_cards"] = set(fraud_cards.tolist())
    return overlay


def slice_overlay(overlay: dict, start: int, end: int) -> dict:
    return {k: v[start:end] if isinstance(v, np.ndarray) else v for k, v in overlay.items()}


def _override(values: np.ndarray, overlay: dict, key: str, rows: np.ndarray):
    forced = overlay[key]
    if forced.dtype.kind == "f":
        keep = rows & ~np.isnan(forced)
    elif forced.dtype.kind in "iu":
        keep = rows & (forced >= 0)
    else:
        keep = rows & (forced != "")
    values[keep] = forced[keep].astype(values.dtype)


def _nullable(values: np.ndarray, null_mask: np.ndarray) -> list:
    out = values.tolist()
    for i in np.where(null_mask)[0]:
        out[i] = None
    return out


# endregion

# region Tables


def generate_card_authorisations(
    card_numbers: np.ndarray,
    times: np.ndarray,
    merchant_names: np.ndarray,
    acquirer_ids: np.ndarray,
    wallet_ids: np.ndarray,
    token_requestor_ids: np.ndarray,
    rng: np.random.Generator,
    fraud_overlay: dict | None = None,
) -> pl.DataFrame:
    """One authorisation per (card_numbers, times) pair; transaction ids 1..n."""
    n = len(card_numbers)
    amount = np.round(np.clip(rng.lognormal(mean=3.5, sigma=1.1, size=n), 0.50, 3000.0), 2)
    # 5% of legitimate scores are mid-range, so a high score alone is no fraud tell.
    network_score = np.round(
        np.where(
            rng.random(n) < 0.05, rng.uniform(150, 600, size=n), rng.beta(1.5, 12, size=n) * 999
        ),
        1,
    )
    issuer_score = np.where(
        rng.random(n) < 0.05, rng.uniform(150, 600, size=n), rng.beta(1.5, 12, size=n) * 999
    ).astype(np.int32)

    chars = np.array(list(string.ascii_uppercase + string.digits))
    reference = chars[rng.integers(0, len(chars), size=(n, 12))].view("<U12").ravel()

    rule_set = weighted_choice(
        rng, [f"RS{i:03d}" for i in range(1, 21)], rng.dirichlet(np.ones(20)).tolist(), n
    )
    txn_type = weighted_choice(
        rng,
        ["00", "01", "05", "10", "28", "51", "54", "76"],
        [0.40, 0.15, 0.10, 0.10, 0.05, 0.08, 0.07, 0.05],
        n,
    )

    # 5% of legitimate cards are heavy shoppers.
    heavy = rng.random(n) < 0.05
    txn_count_24h = np.where(heavy, rng.integers(8, 25, n), rng.poisson(2, n)).astype(np.int32)
    mcc_amount_24h = np.round(np.clip(rng.lognormal(3.0, 0.9, n), 0, 4000), 2)
    mcc_txn_count_24h = np.where(heavy, rng.integers(3, 10, n), rng.poisson(1, n)).astype(np.int32)

    scheme = weighted_choice(rng, CARD_SCHEMES, CARD_SCHEME_WEIGHTS, n)
    acquirer = acquirer_ids[rng.integers(0, len(acquirer_ids), n)]
    # 95% within 320 km of home, 4% regional, 1% travellers.
    travel = rng.random(n)
    distance_km = np.round(
        np.where(
            travel < 0.01,
            rng.uniform(1300, 4000, n),
            np.where(
                travel < 0.05, rng.uniform(320, 1300, n), np.clip(rng.exponential(25, n), 0, 320)
            ),
        ),
        1,
    )
    cash = rng.random(n)
    cash_48h = np.where(
        cash < 0.75,
        0.0,
        np.where(
            cash < 0.98,
            np.round(np.clip(rng.lognormal(3, 0.6, n), 0, 1000), 2),
            np.round(np.clip(rng.lognormal(5, 0.7, n), 0, 3000), 2),
        ),
    )
    change_risk = weighted_choice(rng, ["0", "1", "2", "3"], [0.90, 0.05, 0.03, 0.02], n)
    cvc2 = weighted_choice(
        rng, ["M", "N", "P", "S", "U", ""], [0.88, 0.04, 0.03, 0.02, 0.02, 0.01], n
    )
    cvc = weighted_choice(rng, ["M", "N", "P", "U", ""], [0.89, 0.04, 0.03, 0.02, 0.02], n)
    account_age = rng.integers(180, 7300, n).astype(np.int32)
    card_issued = np.clip(rng.exponential(500, n), 30, 2500).astype(np.int32)
    wallet_added = np.clip(rng.exponential(300, n), 30, 2500).astype(np.int32)
    # ECI 05/06/07: authenticated, attempted, unauthenticated; blank in person.
    eci = weighted_choice(rng, ["05", "06", "07", ""], [0.12, 0.05, 0.03, 0.80], n)
    entry_mode = weighted_choice(
        rng,
        ["01", "02", "05", "07", "09", "80", "81", "90"],
        [0.05, 0.10, 0.40, 0.20, 0.02, 0.08, 0.08, 0.07],
        n,
    )
    decision = weighted_choice(rng, ["A", "D", "R"], [0.95, 0.03, 0.02], n)
    rule_tag_1 = rng.choice([f"T1_{i:02d}" for i in range(10)], n)
    rule_tag_2 = rng.choice([f"T2_{i:02d}" for i in range(10)], n)
    merchant_id = np.char.add("MID", np.char.zfill(rng.integers(1, 100000, n).astype(str), 10))
    merchant_name = merchant_names[rng.integers(0, len(merchant_names), n)].astype("U60")
    wallet = wallet_ids[rng.integers(0, len(wallet_ids), n)].astype("U60")
    mcc = rng.choice(MCC_CODES, n).astype("U5")
    queued = weighted_choice(rng, ["Y", "N"], [0.04, 0.96], n)
    region = weighted_choice(rng, REGIONS, REGION_WEIGHTS, n).astype("U5")
    three_ds = weighted_choice(rng, ["2.1", "2.2", "2.3", ""], [0.15, 0.50, 0.15, 0.20], n)
    token_requestor = token_requestor_ids[rng.integers(0, len(token_requestor_ids), n)].astype(
        "U60"
    )
    wallet_null = rng.random(n) < 0.15
    token_null = rng.random(n) < 0.40

    if fraud_overlay is not None:
        rows = (
            fraud_overlay["signal_mask"]
            if fraud_overlay["signal_mask"].any()
            else fraud_overlay["is_fraud"]
        )
        for values, key in (
            (amount, "amount"),
            (network_score, "network_score"),
            (issuer_score, "issuer_score"),
            (txn_count_24h, "txn_count_24h"),
            (mcc_txn_count_24h, "mcc_txn_count_24h"),
            (mcc_amount_24h, "mcc_amount_24h"),
            (distance_km, "distance_km"),
            (cash_48h, "cash_48h"),
            (region, "region"),
            (entry_mode, "entry_mode"),
            (decision, "decision"),
            (mcc, "mcc"),
            (merchant_name, "merchant_name"),
            (cvc2, "cvc2"),
            (cvc, "cvc"),
            (queued, "queued"),
            (eci, "eci"),
            (wallet, "wallet_id"),
            (token_requestor, "token_requestor_id"),
        ):
            _override(values, fraud_overlay, key, rows)
        wallet_null &= ~(rows & (fraud_overlay["wallet_id"] != ""))
        token_null &= ~(rows & (fraud_overlay["token_requestor_id"] != ""))

    return pl.DataFrame(
        {
            "transaction_id": np.arange(1, n + 1, dtype=np.int64),
            "card_number": card_numbers,
            "transaction_time": times,
            "reporting_month": pl.Series(times).cast(pl.Date).dt.truncate("1mo"),
            "network_risk_score": network_score,
            "amount_eur": amount,
            "fraud_rule_set_id": rule_set,
            "authorisation_reference": reference,
            "transaction_type_code": txn_type,
            "card_txn_count_24h": txn_count_24h,
            "mcc_amount_24h_eur": mcc_amount_24h,
            "mcc_txn_count_24h": mcc_txn_count_24h,
            "card_scheme": scheme,
            "acquirer_id": acquirer,
            "merchant_distance_km": distance_km,
            "cash_withdrawals_48h_eur": cash_48h,
            "account_change_risk_code": change_risk,
            "cvc2_check_result": cvc2,
            "cvc_check_result": cvc,
            "account_age_days": account_age,
            "days_since_card_issued": card_issued,
            "days_since_wallet_added": wallet_added,
            "ecommerce_indicator": eci,
            "pos_entry_mode": entry_mode,
            "authorisation_decision": decision,
            "issuer_rule_tag_1": rule_tag_1,
            "issuer_rule_tag_2": rule_tag_2,
            "merchant_id": merchant_id,
            "wallet_id": pl.Series(_nullable(wallet, wallet_null), dtype=pl.String),
            "mcc": mcc,
            "merchant_name": merchant_name,
            "issuer_fraud_score": issuer_score,
            "queued_for_review": queued,
            "authorisation_region": region,
            "three_ds_version": three_ds,
            "token_requestor_id": pl.Series(
                _nullable(token_requestor, token_null), dtype=pl.String
            ),
        }
    )


def generate_card_authorisation_signals(
    card_numbers: np.ndarray,
    times: np.ndarray,
    rng: np.random.Generator,
    fraud_overlay: dict | None = None,
) -> pl.DataFrame:
    """Per-authorisation review counts, spend ratio, disputes and the fraud labels, 1:1 with the authorisations."""
    n = len(card_numbers)
    heavy = rng.random(n) < 0.04
    queued_6h = np.where(heavy, rng.integers(2, 8, n), rng.poisson(0.3, n)).astype(np.int32)
    queued_24h = np.where(heavy, rng.integers(3, 12, n), rng.poisson(0.5, n)).astype(np.int32)
    declined_1h = np.where(
        rng.random(n) < 0.90, 0.0, np.round(np.clip(rng.lognormal(2.5, 0.8, n), 0, 1500), 2)
    )
    spend_ratio = np.round(
        np.clip(
            np.where(rng.random(n) < 0.04, rng.uniform(2.0, 5.0, n), rng.normal(1.0, 0.25, n)),
            0.2,
            6.0,
        ),
        3,
    )
    no_dispute = rng.random(n) < 0.99
    dispute_count = np.where(no_dispute, 0, rng.poisson(1, n) + 1).astype(np.int32)
    dispute_amount = np.where(
        no_dispute, 0.0, np.round(np.clip(rng.lognormal(3, 0.5, n), 0, 2000), 2)
    )
    approved_fraud = np.zeros(n, dtype=np.int32)
    declined_fraud = np.zeros(n, dtype=np.int32)
    fraud = np.zeros(n, dtype=np.int32)
    card_devices = np.where(heavy, rng.integers(3, 10, n), rng.poisson(1, n) + 1).astype(np.int32)
    account_devices = np.where(heavy, rng.integers(5, 18, n), rng.poisson(1.5, n) + 1).astype(
        np.int32
    )
    report_dates = times.astype("datetime64[D]") + rng.integers(1, 30, n).astype("timedelta64[D]")
    reported = rng.random(n) < 0.01

    if fraud_overlay is not None:
        is_fraud = fraud_overlay["is_fraud"]
        decision = fraud_overlay["decision"]
        fraud[is_fraud] = 1
        approved_fraud[is_fraud & (decision != "D")] = 1
        declined_fraud[is_fraud & (decision == "D")] = 1
        rows = fraud_overlay["signal_mask"] if fraud_overlay["signal_mask"].any() else is_fraud
        for values, key in (
            (queued_6h, "signal_queued_6h"),
            (queued_24h, "signal_queued_24h"),
            (declined_1h, "signal_declined_amount"),
            (spend_ratio, "signal_spend_ratio"),
            (card_devices, "signal_card_devices"),
            (account_devices, "signal_account_devices"),
        ):
            _override(values, fraud_overlay, key, rows)
        reported |= rows

    return pl.DataFrame(
        {
            "card_number": card_numbers,
            "transaction_time": times,
            "reporting_month": pl.Series(times).cast(pl.Date).dt.truncate("1mo"),
            "queued_txn_count_6h": queued_6h,
            "queued_txn_count_24h": queued_24h,
            "declined_amount_1h_eur": declined_1h,
            "spend_ratio": spend_ratio,
            "dispute_count_3m": dispute_count,
            "dispute_amount_3m_eur": dispute_amount,
            "approved_fraud_flag": approved_fraud,
            "declined_fraud_flag": declined_fraud,
            "fraud_flag": fraud,
            "card_token_device_count": card_devices,
            "account_token_device_count": account_devices,
            "fraud_report_date": pl.Series(report_dates)
            .cast(pl.Date)
            .set(pl.Series(~reported), None),
        }
    ).unique(subset=["card_number", "transaction_time"], keep="first", maintain_order=True)


def generate_card_weekly_spend(
    n: int, cards: np.ndarray, rng: np.random.Generator, fraud_cards: set | None = None
) -> pl.DataFrame:
    """n daily rows of trailing 7-day spend for `cards`, over the history's year."""
    card_numbers = cards[rng.integers(0, len(cards), n)]
    days = np.datetime64(HISTORY_END.date()) - rng.integers(0, 365, n).astype("timedelta64[D]")
    amount_7d = np.round(np.clip(rng.lognormal(5, 0.5, n), 10, 10000), 2)
    spend_ratio_7d = np.round(np.clip(rng.normal(1.0, 0.15, n), 0.3, 2.0), 3)
    if fraud_cards:
        on_fraud_card = np.isin(card_numbers, list(fraud_cards))
        k = int(on_fraud_card.sum())
        amount_7d[on_fraud_card] = np.round(
            np.clip(amount_7d[on_fraud_card] * rng.uniform(5.0, 15.0, k), 0, 500000), 2
        )
        spend_ratio_7d[on_fraud_card] = np.round(rng.uniform(5.0, 20.0, k), 3)
    return pl.DataFrame(
        {
            "card_number": card_numbers,
            "day": days,
            "amount_7d_eur": amount_7d,
            "spend_ratio_7d": spend_ratio_7d,
        }
    ).unique(subset=["card_number", "day"], keep="first", maintain_order=True)


def generate_account_events(n: int, accounts: np.ndarray, rng: np.random.Generator) -> pl.DataFrame:
    """n non-monetary account changes: phone, address, lost card, PIN and maintenance."""
    accounts = accounts[power_law_indices(rng, min(len(accounts), 30_000), n, alpha=1.2)]
    codes = weighted_choice(
        rng,
        [
            "PHONE_CHANGE",
            "ADDRESS_CHANGE",
            "CARD_LOST",
            "PIN_CHANGE",
            "LIMIT_CHANGE",
            "CONTACT_UPDATE",
            "OTHER",
        ],
        [0.15, 0.15, 0.10, 0.10, 0.20, 0.15, 0.15],
        n,
    )
    days = np.datetime64(HISTORY_END.date()) - rng.integers(0, 730, n).astype("timedelta64[D]")
    return pl.DataFrame(
        {
            "event_id": np.arange(1, n + 1, dtype=np.int64),
            "account_id": accounts,
            "event_type": codes,
            "event_date": days,
        }
    )


# endregion
