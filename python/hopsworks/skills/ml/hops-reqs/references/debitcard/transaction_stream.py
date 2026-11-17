"""Live debit card authorisations, written to an online stream feature group until stopped.

    python transaction_stream.py                     # 5 a second, forever
    python transaction_stream.py --rate 20 --duration-secs 600
    python transaction_stream.py --dry-run           # 3 seconds' worth, printed, nothing written

The cards are generate_data.py's: the pool is rebuilt from the same seed and
history size, so each card's live windows meet its batch baseline. Each
second's authorisations are stamped to that second; now and then one card gets
a burst from a fraud scheme. Writes go through a multi-part insert, which never
starts an offline materialisation job; streaming_features.py reads them from
the feature group's Kafka topic.
"""

import argparse
import time

import numpy as np
import polars as pl
import synth
from generate_data import AUTHORISATIONS, DEFAULT_ROWS, card_count

FEATURE_GROUP = "card_authorisations_live"
# Mean burst size, to turn the fraud rate into a burst probability per second.
_MEAN_BURST = 4.5


def build_universe(history_rows: int, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    return {
        "rng": rng,
        "cards": synth.generate_card_pool(card_count(history_rows), rng),
        "merchants": synth.generate_merchant_names(5_000, rng),
        "acquirers": synth.generate_acquirer_ids(50),
        "wallets": synth.generate_wallet_ids(20_000),
        "token_requestors": synth.generate_token_requestor_ids(30),
    }


def _now(n: int, rng: np.random.Generator) -> np.ndarray:
    now_us = int(time.time() * 1_000_000)
    return (now_us - rng.integers(0, 1_000_000, size=n)).astype("datetime64[us]")


def _burst(card_numbers: np.ndarray, fraud_rate: float, rng: np.random.Generator):
    """Sometimes, an overlay turning the first 3 to 6 rows into one card's fraud burst; else None."""
    n = len(card_numbers)
    if n < 3 or rng.random() >= min(1.0, fraud_rate * n / _MEAN_BURST):
        return None
    k = int(min(rng.integers(3, 7), n))
    card_numbers[:k] = card_numbers[0]
    overlay = synth.make_empty_overlay(n)
    schemes = list(synth.SINGLE_CARD_SCHEMES.values())
    # The schemes move timestamps; these are thrown away, the rows are stamped now.
    schemes[rng.integers(0, len(schemes))](np.arange(k), _now(n, rng), overlay, rng)
    overlay["signal_mask"][:] = overlay["is_fraud"]
    synth.apply_universal_fraud_signature(overlay, rng)
    return overlay


def generate_second(universe: dict, n: int, fraud_rate: float, first_id: int) -> pl.DataFrame:
    rng = universe["rng"]
    cards = universe["cards"]
    card_numbers = cards[synth.power_law_indices(rng, len(cards), n)].copy()
    overlay = _burst(card_numbers, fraud_rate, rng)
    frame = synth.generate_card_authorisations(
        card_numbers,
        _now(n, rng),
        universe["merchants"],
        universe["acquirers"],
        universe["wallets"],
        universe["token_requestors"],
        rng,
        overlay,
    )
    return frame.with_columns(transaction_id=np.arange(first_id, first_id + n, dtype=np.int64))


def connect():
    import hopsworks

    project = hopsworks.login()
    fg = project.get_feature_store().get_or_create_feature_group(
        name=FEATURE_GROUP,
        version=1,
        description="Live debit card authorisations, the schema of card_authorisations, written by transaction_stream.py.",
        primary_key=AUTHORISATIONS["primary_key"],
        event_time=AUTHORISATIONS["event_time"],
        features=AUTHORISATIONS["features"],
        online_enabled=True,
        stream=True,
        statistics_config=False,
    )
    return fg, fg.multi_part_insert()


def reconnect(fg):
    try:
        fg.finalize_multi_part_insert()
    except Exception as error:  # the old writer is discarded either way
        print(f"  finalize failed on reconnect: {type(error).__name__}")
    return connect()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rate", type=int, default=5, help="authorisations a second")
    parser.add_argument("--fraud-rate", type=float, default=0.02, help="target fraction of fraud")
    parser.add_argument("--duration-secs", type=int, default=0, help="0 runs until stopped")
    parser.add_argument(
        "--history-rows", type=int, default=DEFAULT_ROWS, help="generate_data.py's --rows"
    )
    parser.add_argument("--seed", type=int, default=42, help="generate_data.py's --seed")
    parser.add_argument(
        "--relogin-mins", type=int, default=30, help="refresh the connection this often"
    )
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    universe = build_universe(args.history_rows, args.seed)
    # Ids from the clock, so a restarted stream never reuses one.
    next_id = int(time.time() * 1000) * 1000

    if args.dry_run:
        for _ in range(3):
            frame = generate_second(universe, args.rate, args.fraud_rate, next_id)
            next_id += args.rate
            print(
                frame.select(
                    [
                        "transaction_id",
                        "card_number",
                        "transaction_time",
                        "amount_eur",
                        "issuer_fraud_score",
                        "authorisation_region",
                    ]
                )
            )
        return

    fg, writer = connect()
    print(
        f"Writing {args.rate} authorisations a second to {FEATURE_GROUP} (topic {fg._online_topic_name})"
    )
    deadline = time.time() + args.duration_secs if args.duration_secs > 0 else None
    started = last_login = last_log = time.time()
    written = dropped = 0
    while deadline is None or time.time() < deadline:
        tick = time.time()
        if tick - last_login >= args.relogin_mins * 60:
            fg, writer = reconnect(fg)
            last_login = time.time()
        frame = generate_second(universe, args.rate, args.fraud_rate, next_id)
        next_id += args.rate
        for attempt in range(args.max_retries):
            try:
                writer.insert(frame)
                written += len(frame)
                break
            except (
                Exception
            ) as error:  # a Kafka or metadata blip must not end a stream that runs for days
                backoff = min(2**attempt, 30)
                print(
                    f"  insert failed ({type(error).__name__}: {str(error)[:120]}); reconnecting in {backoff}s"
                )
                time.sleep(backoff)
                try:
                    fg, writer = reconnect(fg)
                    last_login = time.time()
                except Exception as reconnect_error:
                    print(f"  reconnect failed: {type(reconnect_error).__name__}")
        else:
            dropped += 1
        if time.time() - last_log >= 10:
            elapsed = time.time() - started
            print(
                f"  {elapsed:6.0f}s: {written:,} written ({written / elapsed:.1f}/s), {dropped} seconds dropped"
            )
            last_log = time.time()
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    fg.finalize_multi_part_insert()


if __name__ == "__main__":
    main()
