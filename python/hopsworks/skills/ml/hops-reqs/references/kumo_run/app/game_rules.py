# ruff: noqa: INP001
"""The game situations Kumo Tabular learns from, and how a game state becomes a query row.

A situation is the lane the hops is in and what each lane holds in the next row of
obstacles: a wall, a low block, a bar, or nothing. The rules label each one with its
move: fly an open lane, jump a low block, duck a bar, otherwise change lane towards the
nearest open one. The context Kumo reads is those labelled situations, a share of them
held out so the model must decide rows it has never seen; the held-out ones measure how
well it generalises.
"""

from __future__ import annotations

import itertools
import random

LANES = ["left", "centre", "right"]
KINDS = ["wall", "low", "bar"]
MOVES = ["left", "hold", "right", "up", "down"]
TARGET = "move"


def rule_move(lane: str, near: dict[str, str]) -> str:
    """The move the rules call for in `lane`, with `near` the obstacles of the next row."""
    here = near.get(lane)
    if here is None:
        return "hold"
    if here == "low":
        return "up"
    if here == "bar":
        return "down"
    i = LANES.index(lane)
    target = min(
        (j for j, name in enumerate(LANES) if name not in near),
        key=lambda j: (abs(j - i), j),
    )
    return "left" if target < i else "right"


def situations() -> list[tuple[str, dict[str, str]]]:
    """Every lane with an empty row ahead or a row of one or two obstacles; a row never has three."""
    found = []
    for lane in LANES:
        found.append((lane, {}))
        for n in (1, 2):
            for taken in itertools.combinations(LANES, n):
                for kinds in itertools.product(KINDS, repeat=n):
                    found.append((lane, dict(zip(taken, kinds, strict=True))))
    return found


def features(lane: str, near: dict[str, str]) -> dict:
    """A situation as the row Kumo reads: the hops' lane and each lane's obstacle.

    The distance to the row is left out: the rules do not use it, and as a column it only
    adds noise the model must learn to ignore, which costs accuracy and time per request.
    """
    return {"lane": lane, **{f"{name}_lane": near.get(name, "open") for name in LANES}}


def split(holdout: float = 0.3, seed: int = 7) -> tuple[list[dict], list[tuple[str, dict]]]:
    """The labelled context rows, and the situations held out of them."""
    rnd = random.Random(seed)
    every = situations()
    rnd.shuffle(every)
    cut = round(len(every) * (1 - holdout))
    context = [
        {**features(lane, near), TARGET: rule_move(lane, near)} for lane, near in every[:cut]
    ]
    return context, every[cut:]


def query(state: dict) -> tuple[dict, list[str]]:
    """A game state, as the page sends it, as a query row and the moves the hops can make.

    `state` is `{lane, airborne, ahead: [{distance, lanes: {lane: kind}}, ...]}`, nearest row first.
    """
    lane = state.get("lane")
    if lane not in LANES:
        raise ValueError("lane must be left, centre or right")
    ahead = state.get("ahead") or []
    nearest = ahead[0] if ahead else {"lanes": {}}
    near = {k: v for k, v in (nearest.get("lanes") or {}).items() if v in KINDS}
    allowed = [
        m
        for m in MOVES
        if not (m == "left" and lane == "left")
        and not (m == "right" and lane == "right")
        and not (m == "up" and state.get("airborne"))
    ]
    return features(lane, near), allowed
