"""Generate the shared prediction-time fixtures from the SDK, the authority at run time."""

import json
from datetime import date

from hsfs.constructor.prediction_times import PredictionTimes as PT


CASES = [
    {
        "name": "daily at 08:00",
        "cron": "0 8 * * *",
        "start": "2026-09-14",
        "count": 3,
        "tz": "UTC",
    },
    {
        "name": "weekdays at 08:00",
        "cron": "0 8 * * MON-FRI",
        "start": "2026-09-12",
        "count": 5,
        "tz": "UTC",
    },
    {
        "name": "every 15 minutes",
        "cron": "*/15 * * * *",
        "start": "2026-09-14",
        "count": 5,
        "tz": "UTC",
    },
    {
        "name": "sunday as 0 and 7",
        "cron": "0 0 * * 0,7",
        "start": "2026-09-01",
        "count": 3,
        "tz": "UTC",
    },
    {
        "name": "day-of-month OR day-of-week",
        "cron": "0 0 1 * SUN",
        "start": "2026-09-01",
        "count": 6,
        "tz": "UTC",
    },
    {
        "name": "monthly on the first",
        "cron": "0 8 1 * *",
        "start": "2026-09-14",
        "count": 3,
        "tz": "UTC",
    },
    {
        "name": "weekly on monday",
        "cron": "0 8 * * MON",
        "start": "2026-09-14",
        "count": 3,
        "tz": "UTC",
    },
    {
        "name": "hourly at :15",
        "cron": "15 * * * *",
        "start": "2026-09-14",
        "count": 3,
        "tz": "UTC",
    },
    {
        "name": "DST spring: 08:00 keeps its local hour",
        "cron": "0 8 * * *",
        "start": "2026-03-28",
        "count": 3,
        "tz": "Europe/Stockholm",
    },
    {
        "name": "DST spring: 02:30 is skipped on the transition day",
        "cron": "30 2 * * *",
        "start": "2026-03-28",
        "count": 3,
        "tz": "Europe/Stockholm",
    },
    {
        "name": "DST autumn: 02:30 takes its first occurrence",
        "cron": "30 2 * * *",
        "start": "2026-10-24",
        "count": 3,
        "tz": "Europe/Stockholm",
    },
    {
        "name": "month names",
        "cron": "0 0 1 JAN,JUL *",
        "start": "2026-02-01",
        "count": 3,
        "tz": "UTC",
    },
]

out = []
for c in CASES:
    y, m, d = (int(x) for x in c["start"].split("-"))
    pt = PT.cron(c["cron"], start=date(y, m, d), count=c["count"], timezone=c["tz"])
    out.append(
        {**c, "expected": [t.isoformat().replace("+00:00", "Z") for t in pt.timestamps]}
    )

INTERVALS = [
    {
        "name": "hourly at :15",
        "interval": "hourly",
        "offset": "15",
        "cron": "15 * * * *",
    },
    {
        "name": "daily at 08:00",
        "interval": "daily",
        "offset": "08:00",
        "cron": "0 8 * * *",
    },
    {
        "name": "weekly on monday",
        "interval": "weekly",
        "offset": "MON:08:00",
        "cron": "0 8 * * MON",
    },
    {
        "name": "monthly on the first",
        "interval": "monthly",
        "offset": "1:08:00",
        "cron": "0 8 1 * *",
    },
    {
        "name": "hourly default offset",
        "interval": "hourly",
        "offset": None,
        "cron": "0 * * * *",
    },
    {
        "name": "daily default offset",
        "interval": "daily",
        "offset": None,
        "cron": "0 0 * * *",
    },
    {
        "name": "weekly default offset",
        "interval": "weekly",
        "offset": None,
        "cron": "0 0 * * MON",
    },
    {
        "name": "monthly default offset",
        "interval": "monthly",
        "offset": None,
        "cron": "0 0 1 * *",
    },
]
print(json.dumps({"schedules": out, "intervals": INTERVALS}, indent=2))
