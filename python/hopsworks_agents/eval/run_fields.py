"""Fields every runner reads off a run row, spelled once.

A module of its own so the three runners can share it without importing each
other: the suite runner imports the sample runner, so a helper in either is a
cycle for the third.
"""

from __future__ import annotations

from typing import Any


def run_trials(run: dict[str, Any], default: int) -> int:
    """The run's trial budget, however the server spelled it.

    The field is ``nTrials``; a backend before the wire name was pinned sent it as
    ``ntrials`` (Jackson's reading of a getter with two leading capitals), and a
    review that read neither ran with its ceiling instead of its budget.
    """
    for key in ("nTrials", "ntrials", "n_trials"):
        if run.get(key) not in (None, ""):
            try:
                return int(run[key])
            except (TypeError, ValueError):
                return default
    return default
