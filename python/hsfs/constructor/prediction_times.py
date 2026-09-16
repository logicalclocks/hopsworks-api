#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from hopsworks_apigen import public
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.decorators import typechecked


DEFAULT_MAX_HORIZON_DAYS = 3660

_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ],
        start=1,
    )
}
_DAYS = {d: i for i, d in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}

_HOURLY_OFFSET = re.compile(r"^(\d{1,2})$")
_DAILY_OFFSET = re.compile(r"^(\d{1,2}):(\d{2})$")
_WEEKLY_OFFSET = re.compile(r"^([A-Za-z]{3}):(\d{1,2}):(\d{2})$")
_MONTHLY_OFFSET = re.compile(r"^(\d{1,2}):(\d{1,2}):(\d{2})$")


def _parse_field(
    spec: str, low: int, high: int, names: dict[str, int], field: str
) -> set[int]:
    """Expand one cron field into the set of values it matches."""
    values: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"Empty {field} value in cron expression.")
        step = 1
        if "/" in part:
            part, _, step_str = part.partition("/")
            if not step_str.isdigit() or int(step_str) < 1:
                raise ValueError(
                    f"Step in {field} must be a positive integer; got {step_str!r}."
                )
            step = int(step_str)
        if part == "*":
            lo, hi = low, high
        elif "-" in part.lstrip("-"):
            lo_str, _, hi_str = part.partition("-")
            lo, hi = (
                _parse_value(lo_str, names, field),
                _parse_value(hi_str, names, field),
            )
        else:
            lo = hi = _parse_value(part, names, field)
        if lo < low or hi > high or lo > hi:
            raise ValueError(f"{field} range {lo}-{hi} is outside {low}-{high}.")
        values.update(range(lo, hi + 1, step))
    return values


def _parse_value(token: str, names: dict[str, int], field: str) -> int:
    token = token.strip().lower()
    if token in names:
        return names[token]
    if not token.isdigit():
        raise ValueError(f"Unrecognized {field} value {token!r} in cron expression.")
    return int(token)


def _localize(naive: datetime, tz: ZoneInfo) -> datetime | None:
    """Attach `tz` to a naive local time, or return None when that local time does not exist.

    An ambiguous local time (the repeated hour when clocks go back) resolves to its first,
    earlier occurrence. A non-existent one (the skipped hour when clocks go forward) does not
    round-trip through UTC, which is how it is detected.
    """
    aware = naive.replace(tzinfo=tz, fold=0)
    if aware.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != naive:
        return None
    return aware


def _to_local_naive(value: date | datetime | str | int, tz: ZoneInfo) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    elif isinstance(value, int):
        value = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, datetime):
        pass
    elif isinstance(value, date):
        value = datetime(value.year, value.month, value.day)
    else:
        raise TypeError(
            f"Expected a datetime, date, ISO-8601 string or epoch seconds; got {type(value)!r}."
        )
    if value.tzinfo is not None:
        value = value.astimezone(tz)
    return value.replace(tzinfo=None, microsecond=0, second=0)


@public
@typechecked
class PredictionTimes:
    """The timestamps a batch-inference read is anchored on.

    Each timestamp becomes the prediction time of one row per entity: the feature values
    returned for it are the newest at or before it in every feature group of the view.
    The timestamps may be in the future, which is the point of the class.

    Build one with [`cron`][hsfs.constructor.prediction_times.PredictionTimes.cron],
    [`every`][hsfs.constructor.prediction_times.PredictionTimes.every], or
    [`of`][hsfs.constructor.prediction_times.PredictionTimes.of].

    Example:
        ```python
        fv.get_batch_data(
            spine_df=pd.DataFrame([{"country": "SE", "city": "Stockholm", "street": "Sveavagen"}]),
            prediction_times=PredictionTimes.every("daily", offset="08:00", count=7),
        )
        ```
    """

    def __init__(self, timestamps: list[datetime]) -> None:
        self._timestamps = timestamps

    @public
    @classmethod
    def cron(
        cls,
        expression: str,
        start: date | datetime | str | int | None = None,
        end: date | datetime | str | int | None = None,
        count: int | None = None,
        timezone: str = "UTC",
        max_horizon_days: int = DEFAULT_MAX_HORIZON_DAYS,
    ) -> PredictionTimes:
        """Prediction times from a five-field cron expression.

        The fields are `minute hour day-of-month month day-of-week`, in the Vixie cron dialect:
        `*`, an integer, `a-b`, `a-b/n`, `*/n`, comma-separated lists, and the three-letter month
        and weekday names.
        Both `0` and `7` mean Sunday.
        When both day-of-month and day-of-week are restricted, a day matches if either matches.
        This is not the Quartz dialect used by Hopsworks job schedules, which has a seconds field
        and numbers Sunday as 1.

        `start` is inclusive and defaults to now truncated to the minute; `end` is exclusive.
        Exactly one of `end` and `count` is required.
        A naive `datetime` and a `date` are read in `timezone`; an aware one is converted from its own.
        Expansion happens in `timezone`, so a daily schedule keeps its local time across a DST change.

        Example:
            ```python
            PredictionTimes.cron("0 8 * * MON-FRI", count=5)
            ```

        Parameters:
            expression: The cron expression.
            start: First instant the schedule may fire at, inclusive.
            end: Instant the schedule stops before, exclusive.
            count: How many timestamps to produce.
            timezone: IANA time zone name the schedule is expressed in.
            max_horizon_days: How far past `start` to search before giving up on `count`.

        Returns:
            The resolved prediction times.

        Raises:
            ValueError: If the expression is malformed, neither or both of `end` and `count` are
                given, or the schedule yields fewer than `count` timestamps within the horizon.
        """
        return cls(_expand(expression, start, end, count, timezone, max_horizon_days))

    @public
    @classmethod
    def every(
        cls,
        interval: str,
        offset: str | None = None,
        start: date | datetime | str | int | None = None,
        end: date | datetime | str | int | None = None,
        count: int | None = None,
        timezone: str = "UTC",
        max_horizon_days: int = DEFAULT_MAX_HORIZON_DAYS,
    ) -> PredictionTimes:
        """Prediction times from a named interval, which compiles to a cron expression.

        `interval` is one of `"hourly"`, `"daily"`, `"weekly"` or `"monthly"`.
        `offset` places the firing within the period and defaults to its start: a minute (`"15"`)
        for hourly, `"HH:MM"` for daily, `"DDD:HH:MM"` with a three-letter weekday for weekly, and
        `"D:HH:MM"` with a day of the month for monthly.
        Every other parameter behaves as in
        [`cron`][hsfs.constructor.prediction_times.PredictionTimes.cron].

        Example:
            ```python
            PredictionTimes.every("daily", offset="08:00", start=tomorrow, count=7)
            ```

        Parameters:
            interval: The named interval.
            offset: Where in the period the schedule fires.
            start: First instant the schedule may fire at, inclusive.
            end: Instant the schedule stops before, exclusive.
            count: How many timestamps to produce.
            timezone: IANA time zone name the schedule is expressed in.
            max_horizon_days: How far past `start` to search before giving up on `count`.

        Returns:
            The resolved prediction times.

        Raises:
            ValueError: If the interval or the offset is not recognized.
        """
        return cls.cron(
            _interval_to_cron(interval, offset),
            start,
            end,
            count,
            timezone,
            max_horizon_days,
        )

    @public
    @classmethod
    def of(
        cls,
        timestamps: list[date | datetime | str | int],
        timezone: str = "UTC",
    ) -> PredictionTimes:
        """Prediction times from an explicit list.

        Accepts `datetime`, `date`, ISO-8601 strings and epoch seconds.
        Naive values are read in `timezone`.
        Use this for anything a schedule cannot express.

        Example:
            ```python
            PredictionTimes.of([datetime(2026, 9, 14, 8), datetime(2026, 9, 21, 8)])
            ```

        Parameters:
            timestamps: The prediction times.
            timezone: IANA time zone name naive values are read in.

        Returns:
            The resolved prediction times, sorted ascending and deduplicated.

        Raises:
            ValueError: If the list is empty or contains a null.
        """
        if not timestamps:
            raise ValueError("PredictionTimes.of requires at least one timestamp.")
        tz = ZoneInfo(timezone)
        resolved = []
        for value in timestamps:
            if value is None or (isinstance(value, float) and value != value):
                raise ValueError("PredictionTimes cannot contain a null timestamp.")
            naive = _to_local_naive(value, tz)
            aware = _localize(naive, tz)
            if aware is None:
                raise ValueError(
                    f"{naive} does not exist in {timezone} (the clocks go forward over it)."
                )
            resolved.append(aware.astimezone(_utc()))
        return cls(sorted(set(resolved)))

    @public
    def cross(self, spine_df: Any, event_time: str) -> Any:
        """Cross a frame of entities with these times, one row per entity per time.

        The ordering is the contract: entities in the order given, and within an entity ascending
        in time. A batch read returns its rows in that same order, so predictions zip back onto
        the frame positionally.

        Args:
            spine_df: The entities, one row each.
            event_time: Name of the feature view's event time column, which the result carries
                the times under.

        Returns:
            A pandas DataFrame ready to pass as `spine_df`.
        """
        import pandas as pd
        from hopsworks_common import spark_connect_utils

        if spark_connect_utils._is_spark_dataframe(spine_df):
            raise FeatureStoreException(
                "`cross` builds a pandas frame and cannot cross a Spark DataFrame; pandas would"
                " reject it with a constructor error. Read the entities as pandas, for example"
                ' `fg.read_primary_keys(dataframe_type="pandas")`. A spine is collected to the'
                " driver to be registered anyway, so nothing is gained by keeping it in Spark."
            )

        times = self.timestamps
        if not times:
            raise FeatureStoreException(
                "These prediction times resolve to no timestamps."
            )
        frame = (
            spine_df if isinstance(spine_df, pd.DataFrame) else pd.DataFrame(spine_df)
        )
        if event_time in frame.columns:
            raise FeatureStoreException(
                f"`spine_df` already carries `{event_time}`; it would be overwritten by these"
                " prediction times. Pass the frame without it, or do not cross at all."
            )
        frame = frame.reset_index(drop=True)
        crossed = frame.loc[frame.index.repeat(len(times))].reset_index(drop=True)
        crossed[event_time] = pd.to_datetime(
            [t for _ in range(len(frame)) for t in times], utc=True
        )
        return crossed

    @property
    def timestamps(self) -> list[datetime]:
        """The resolved prediction times, timezone-aware in UTC, ascending and unique."""
        return list(self._timestamps)

    @classmethod
    def _from_user_input(
        cls, value: PredictionTimes | list[Any] | None
    ) -> PredictionTimes | None:
        """Accept a `PredictionTimes`, a bare list of timestamps, or None."""
        if value is None or isinstance(value, cls):
            return value
        if isinstance(value, list):
            return cls.of(value)
        raise TypeError(
            f"prediction_times expects a PredictionTimes or a list of timestamps; got {type(value)!r}."
        )

    def __len__(self) -> int:
        return len(self._timestamps)

    def __repr__(self) -> str:
        if not self._timestamps:
            return "PredictionTimes([])"
        return (
            f"PredictionTimes({len(self._timestamps)} times, "
            f"{self._timestamps[0].isoformat()} .. {self._timestamps[-1].isoformat()})"
        )


def _utc() -> timezone:
    return timezone.utc


def _interval_to_cron(interval: str, offset: str | None) -> str:
    key = interval.strip().lower()
    if key == "hourly":
        minute = 0
        if offset is not None:
            match = _HOURLY_OFFSET.match(offset.strip())
            if not match:
                raise ValueError(
                    f'An hourly offset is a minute, such as "15"; got {offset!r}.'
                )
            minute = int(match.group(1))
        return f"{minute} * * * *"
    if key == "daily":
        hour, minute = 0, 0
        if offset is not None:
            match = _DAILY_OFFSET.match(offset.strip())
            if not match:
                raise ValueError(f'A daily offset is "HH:MM"; got {offset!r}.')
            hour, minute = int(match.group(1)), int(match.group(2))
        return f"{minute} {hour} * * *"
    if key == "weekly":
        day, hour, minute = "MON", 0, 0
        if offset is not None:
            match = _WEEKLY_OFFSET.match(offset.strip())
            if not match:
                raise ValueError(
                    f'A weekly offset is "DDD:HH:MM", such as "MON:08:00"; got {offset!r}.'
                )
            day, hour, minute = match.group(1), int(match.group(2)), int(match.group(3))
        return f"{minute} {hour} * * {day.upper()}"
    if key == "monthly":
        dom, hour, minute = 1, 0, 0
        if offset is not None:
            match = _MONTHLY_OFFSET.match(offset.strip())
            if not match:
                raise ValueError(
                    f'A monthly offset is "D:HH:MM", such as "1:08:00"; got {offset!r}.'
                )
            dom, hour, minute = (
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            )
        return f"{minute} {hour} {dom} * *"
    raise ValueError(
        f"interval must be one of 'hourly', 'daily', 'weekly', 'monthly'; got {interval!r}."
    )


def _expand(
    expression: str,
    start: date | datetime | str | int | None,
    end: date | datetime | str | int | None,
    count: int | None,
    tz_name: str,
    max_horizon_days: int,
) -> list[datetime]:
    if (end is None) == (count is None):
        raise ValueError("Exactly one of `end` and `count` is required.")
    if count is not None and count < 1:
        raise ValueError(f"`count` must be positive; got {count}.")

    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(
            f"A cron expression has five fields (minute hour day-of-month month day-of-week); "
            f"got {len(fields)} in {expression!r}."
        )
    minutes = sorted(_parse_field(fields[0], 0, 59, {}, "minute"))
    hours = sorted(_parse_field(fields[1], 0, 23, {}, "hour"))
    doms = _parse_field(fields[2], 1, 31, {}, "day-of-month")
    months = _parse_field(fields[3], 1, 12, _MONTHS, "month")
    dows = {d % 7 for d in _parse_field(fields[4], 0, 7, _DAYS, "day-of-week")}
    # Vixie semantics: with both day fields restricted a day matches if either does.
    dom_restricted = fields[2].strip() != "*"
    dow_restricted = fields[4].strip() != "*"

    tz = ZoneInfo(tz_name)
    start_local = (
        _to_local_naive(start, tz)
        if start is not None
        else datetime.now(tz).replace(tzinfo=None, second=0, microsecond=0)
    )
    end_local = _to_local_naive(end, tz) if end is not None else None
    if end_local is not None and end_local <= start_local:
        raise ValueError(f"`end` ({end_local}) must be after `start` ({start_local}).")

    out: list[datetime] = []
    day = start_local.date()
    last_day = day + timedelta(days=max_horizon_days)
    if end_local is not None:
        last_day = min(last_day, end_local.date())
    while day <= last_day:
        if _day_matches(day, doms, months, dows, dom_restricted, dow_restricted):
            for hour in hours:
                for minute in minutes:
                    naive = datetime.combine(day, time(hour, minute))
                    if naive < start_local:
                        continue
                    if end_local is not None and naive >= end_local:
                        return out
                    aware = _localize(naive, tz)
                    if aware is None:
                        continue
                    out.append(aware.astimezone(timezone.utc))
                    if count is not None and len(out) == count:
                        return out
        day += timedelta(days=1)

    if count is not None:
        raise ValueError(
            f"Cron expression {expression!r} yields only {len(out)} of the {count} requested "
            f"prediction times within {max_horizon_days} days of {start_local}."
        )
    return out


def _day_matches(
    day: date,
    doms: set[int],
    months: set[int],
    dows: set[int],
    dom_restricted: bool,
    dow_restricted: bool,
) -> bool:
    if day.month not in months:
        return False
    # date.weekday() is Monday=0; cron is Sunday=0.
    dow = (day.weekday() + 1) % 7
    if dom_restricted and dow_restricted:
        return day.day in doms or dow in dows
    return day.day in doms and dow in dows
