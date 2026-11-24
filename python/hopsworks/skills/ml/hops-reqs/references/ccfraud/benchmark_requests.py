"""The fraud deployment's benchmark requests, for benchmarks/benchmark_inference.py.

Copied into the system as src/<slug_pkg>/benchmark_requests.py. A request is
`[[cc_num, amount, merchant_id, ip_address, card_present, t_id]]`: a real card
and merchant, with a request-time amount, IP address and card presence.
"""

import random


def requests(project, n: int) -> list[list[list]]:
    fs = project.get_feature_store()
    cards = list(fs.get_feature_group("card_details", version=1).read()["cc_num"])
    merchants = list(
        fs.get_feature_group("merchant_details", version=1).read()["merchant_id"]
    )
    rng = random.Random(20261025)
    return [
        [
            [
                rng.choice(cards),
                round(rng.lognormvariate(3.5, 1.2), 2),
                rng.choice(merchants),
                f"81.2.69.{rng.randint(1, 254)}",
                rng.random() < 0.3,
                i + 1,
            ]
        ]
        for i in range(n)
    ]
