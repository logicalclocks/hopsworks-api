"""Smoke test for hops job deploy. Prints a heartbeat and exits.

Replaced by a real feature pipeline once the deploy flow is validated.
"""

from __future__ import annotations

import datetime
import os
import sys


def main() -> int:
    start = os.environ.get("HOPS_START_TIME") or "n/a"
    end = os.environ.get("HOPS_END_TIME") or "n/a"
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(f"[onboarding_pipeline] now={now} window=[{start},{end}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
