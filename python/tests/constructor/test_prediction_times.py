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
from datetime import date, datetime, timedelta, timezone

import pytest
from hsfs.constructor.prediction_times import PredictionTimes


def iso(pt):
    return [t.isoformat() for t in pt.timestamps]


class TestCron:
    def test_daily_at_a_fixed_hour(self):
        pt = PredictionTimes.cron("0 8 * * *", start=date(2026, 9, 14), count=3)
        assert iso(pt) == [
            "2026-09-14T08:00:00+00:00",
            "2026-09-15T08:00:00+00:00",
            "2026-09-16T08:00:00+00:00",
        ]

    def test_weekday_names_and_ranges(self):
        pt = PredictionTimes.cron("0 8 * * MON-FRI", start=date(2026, 9, 12), count=3)
        # 2026-09-12 is a Saturday, so the first three firings are Mon, Tue, Wed.
        assert iso(pt) == [
            "2026-09-14T08:00:00+00:00",
            "2026-09-15T08:00:00+00:00",
            "2026-09-16T08:00:00+00:00",
        ]

    def test_step_over_a_wildcard(self):
        pt = PredictionTimes.cron("*/15 * * * *", start=datetime(2026, 9, 14), count=5)
        assert [t.minute for t in pt.timestamps] == [0, 15, 30, 45, 0]

    @pytest.mark.parametrize("field", ["0", "7"])
    def test_zero_and_seven_are_both_sunday(self, field):
        pt = PredictionTimes.cron(f"0 0 * * {field}", start=date(2026, 9, 1), count=2)
        assert all(t.weekday() == 6 for t in pt.timestamps)

    def test_restricted_day_fields_are_or_not_and(self):
        """Vixie semantics: with both day fields restricted, either matching is enough."""
        pt = PredictionTimes.cron("0 0 1 * SUN", start=date(2026, 9, 1), count=3)
        days = [t.date() for t in pt.timestamps]
        assert days == [date(2026, 9, 1), date(2026, 9, 6), date(2026, 9, 13)]

    def test_end_is_exclusive_and_start_inclusive(self):
        pt = PredictionTimes.cron(
            "0 0 * * *", start=date(2026, 9, 14), end=date(2026, 9, 17)
        )
        assert [t.day for t in pt.timestamps] == [14, 15, 16]

    def test_an_unsatisfiable_schedule_is_an_error_not_a_hang(self):
        with pytest.raises(ValueError, match="yields only 0 of the 1"):
            PredictionTimes.cron("0 0 31 2 *", start=date(2026, 1, 1), count=1)

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"expression": "0 8 * *", "count": 1}, "five fields"),
            ({"expression": "0 99 * * *", "count": 1}, "outside 0-23"),
            ({"expression": "0 8 * * NOTADAY", "count": 1}, "Unrecognized"),
            ({"expression": "0 8 * * *", "count": 0}, "must be positive"),
            ({"expression": "0 8 * * *"}, "Exactly one of"),
            (
                {"expression": "0 8 * * *", "count": 1, "end": date(2026, 1, 1)},
                "Exactly one of",
            ),
            (
                {
                    "expression": "0 8 * * *",
                    "start": date(2026, 2, 1),
                    "end": date(2026, 1, 1),
                },
                "must be after",
            ),
        ],
    )
    def test_rejects_malformed_input(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            PredictionTimes.cron(**kwargs)


class TestDaylightSaving:
    """Europe/Stockholm goes forward 2026-03-29 02:00 and back 2026-10-25 03:00."""

    TZ = "Europe/Stockholm"

    def test_a_daily_time_keeps_its_local_hour_across_the_transition(self):
        pt = PredictionTimes.every(
            "daily", offset="08:00", start=date(2026, 3, 28), count=2, timezone=self.TZ
        )
        # 07:00Z is 08:00 CET, 06:00Z is 08:00 CEST.
        assert iso(pt) == ["2026-03-28T07:00:00+00:00", "2026-03-29T06:00:00+00:00"]

    def test_a_local_time_that_does_not_exist_is_skipped(self):
        pt = PredictionTimes.every(
            "daily", offset="02:30", start=date(2026, 3, 28), count=2, timezone=self.TZ
        )
        assert [t.date().day for t in pt.timestamps] == [28, 30]

    def test_a_repeated_local_time_takes_its_first_occurrence(self):
        pt = PredictionTimes.every(
            "daily", offset="02:30", start=date(2026, 10, 25), count=1, timezone=self.TZ
        )
        # 02:30 happens at 00:30Z (CEST) and again at 01:30Z (CET); the earlier one wins.
        assert iso(pt) == ["2026-10-25T00:30:00+00:00"]


class TestEvery:
    @pytest.mark.parametrize(
        "interval, offset, expected_first",
        [
            ("hourly", "15", "2026-09-14T00:15:00+00:00"),
            ("daily", "08:00", "2026-09-14T08:00:00+00:00"),
            ("weekly", "MON:08:00", "2026-09-14T08:00:00+00:00"),
            ("monthly", "1:08:00", "2026-10-01T08:00:00+00:00"),
        ],
    )
    def test_named_intervals(self, interval, offset, expected_first):
        pt = PredictionTimes.every(
            interval, offset=offset, start=date(2026, 9, 14), count=1
        )
        assert iso(pt) == [expected_first]

    def test_offset_defaults_to_the_start_of_the_period(self):
        pt = PredictionTimes.every("daily", start=date(2026, 9, 14), count=1)
        assert iso(pt) == ["2026-09-14T00:00:00+00:00"]

    @pytest.mark.parametrize(
        "interval, offset, message",
        [
            ("fortnightly", None, "must be one of"),
            ("daily", "8am", 'A daily offset is "HH:MM"'),
            ("weekly", "08:00", 'A weekly offset is "DDD:HH:MM"'),
        ],
    )
    def test_rejects_malformed_input(self, interval, offset, message):
        with pytest.raises(ValueError, match=message):
            PredictionTimes.every(interval, offset=offset, count=1)


class TestOf:
    def test_mixed_input_types_normalize_to_utc(self):
        pt = PredictionTimes.of(
            [datetime(2026, 9, 15, 8), "2026-09-14T08:00:00", date(2026, 9, 16)]
        )
        assert iso(pt) == [
            "2026-09-14T08:00:00+00:00",
            "2026-09-15T08:00:00+00:00",
            "2026-09-16T00:00:00+00:00",
        ]

    def test_an_aware_datetime_is_converted_from_its_own_zone(self):
        pt = PredictionTimes.of([datetime(2026, 9, 14, 10, tzinfo=timezone.utc)])
        assert iso(pt) == ["2026-09-14T10:00:00+00:00"]

    def test_duplicates_collapse_and_order_is_ascending(self):
        pt = PredictionTimes.of(
            [datetime(2026, 9, 16), datetime(2026, 9, 14), datetime(2026, 9, 16)]
        )
        assert [t.day for t in pt.timestamps] == [14, 16]

    def test_rejects_an_empty_list(self):
        with pytest.raises(ValueError, match="at least one timestamp"):
            PredictionTimes.of([])

    def test_rejects_a_null(self):
        with pytest.raises(ValueError, match="null timestamp"):
            PredictionTimes.of([datetime(2026, 9, 14), None])


class TestSharedFixtures:
    """The same cases the hopsworks-front batch-inference card checks its preview against.

    Generated by `python/tests/fixtures/generate_prediction_times_fixtures.py` and copied to
    `hopsworks-front/src/pages/project/feature-views/overview/prediction-times.fixtures.json`.
    Regenerate both together; the two implementations drift silently otherwise.
    """

    @pytest.fixture(scope="class")
    def fixtures(self):
        import json
        import pathlib

        path = (
            pathlib.Path(__file__).parent.parent
            / "fixtures"
            / "prediction_times_fixtures.json"
        )
        return json.loads(path.read_text())

    def test_every_schedule_case(self, fixtures):
        for case in fixtures["schedules"]:
            y, m, d = (int(x) for x in case["start"].split("-"))
            pt = PredictionTimes.cron(
                case["cron"],
                start=date(y, m, d),
                count=case["count"],
                timezone=case["tz"],
            )
            got = [t.isoformat().replace("+00:00", "Z") for t in pt.timestamps]
            assert got == case["expected"], case["name"]

    def test_every_interval_case(self, fixtures):
        from hsfs.constructor.prediction_times import _interval_to_cron

        for case in fixtures["intervals"]:
            assert (
                _interval_to_cron(case["interval"], case["offset"]) == case["cron"]
            ), case["name"]


class TestPrecisionAndUnits:
    """An explicit instant keeps its seconds, and an integer is epoch milliseconds."""

    def test_an_explicit_instant_keeps_its_seconds(self):
        # The documented latest-values recipe is PredictionTimes.of([now]). Truncating to the
        # minute made it read as of the start of the current minute and miss the last writes.
        now = datetime(2026, 9, 16, 8, 30, 45, 123456, tzinfo=timezone.utc)
        assert PredictionTimes.of([now]).timestamps == [now]

    def test_an_integer_is_epoch_milliseconds(self):
        # The same unit spine_df reads an integer column in, and the unit the feature store
        # keeps event times in.
        moment = datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone.utc)
        epoch_ms = int(moment.timestamp() * 1000) + 250
        assert PredictionTimes.of([epoch_ms]).timestamps == [
            moment + timedelta(milliseconds=250)
        ]

    def test_a_schedule_bound_keeps_its_seconds(self):
        # A start inside a scheduled minute excludes that minute rather than including it.
        times = PredictionTimes.every(
            "hourly", offset="0", start=datetime(2026, 9, 16, 8, 0, 30), count=1
        ).timestamps
        assert times == [datetime(2026, 9, 16, 9, 0, tzinfo=timezone.utc)]
