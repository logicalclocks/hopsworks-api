"""The debit card deployment's benchmark requests, for benchmarks/benchmark_inference.py.

Copied into the system as src/<slug_pkg>/benchmark_requests.py. A request is
`[[card_number]]`, over the cards with an open window, the ones the console asks about.
"""

import random


def requests(project, n: int) -> list[list[list[str]]]:
    windows = project.get_feature_store().get_feature_group("card_window_aggregates", version=1)
    cards = windows.read(online=True)["card_number"].astype(str).tolist()
    rng = random.Random(20261025)
    return [[[rng.choice(cards)]] for _ in range(n)]
