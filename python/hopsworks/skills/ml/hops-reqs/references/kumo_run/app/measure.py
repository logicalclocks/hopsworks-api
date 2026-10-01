# ruff: noqa: INP001
"""Measure the Kumo Tabular pilot: how often it picks the rules' move, and how fast.

Run in the Terminal from the app directory, once the deployment runs:

    python measure.py [--deployment runexample] [--requests 50]

Accuracy is over the situations held out of the context, which the model has never seen
labelled; latency is the round trip of single-state decisions, as the app makes them, so
its p99 is the number to hold against `requirements.sla.realtime.p99_ms`.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import time

import game_rules


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]


def _answers(reply: dict) -> list:
    predictions = reply["predictions"]
    return predictions["predictions"] if isinstance(predictions, dict) else predictions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Measure the Kumo Tabular pilot.")
    parser.add_argument("--deployment", default="runexample")
    parser.add_argument("--requests", type=int, default=50)
    args = parser.parse_args(argv)

    import hopsworks

    deployment = (
        hopsworks.login(engine="python")
        .get_model_serving()
        .get_deployment(args.deployment)
    )
    context, held_out = game_rules.split()

    def ask(rows: list[dict]) -> dict:
        return _answers(
            deployment.predict(
                inputs=[
                    {"context": context, "query": rows, "target": game_rules.TARGET}
                ]
            )
        )[0]

    # All held-out situations in one request: the rules' move against the model's.
    rows = [game_rules.features(lane, near) for lane, near in held_out]
    labels = ask(rows)["labels"]
    right = sum(
        label == game_rules.rule_move(lane, near)
        for label, (lane, near) in zip(labels, held_out, strict=True)
    )

    rnd = random.Random(1)
    every = game_rules.situations()
    round_trips, model_ms = [], []
    for _ in range(args.requests):
        lane, near = rnd.choice(every)
        started = time.perf_counter()
        answer = ask([game_rules.features(lane, near)])
        round_trips.append((time.perf_counter() - started) * 1000)
        model_ms.append(answer["seconds"] * 1000)

    print(
        json.dumps(
            {
                "context_rows": len(context),
                "held_out": len(held_out),
                "held_out_right": right,
                "accuracy": round(right / len(held_out), 3),
                "requests": args.requests,
                "p50_ms": round(_percentile(round_trips, 0.5), 1),
                "p99_ms": round(_percentile(round_trips, 0.99), 1),
                "model_p50_ms": round(_percentile(model_ms, 0.5), 1),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
