#
#   Copyright 2023 Hopsworks AB
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

import json
from datetime import datetime, timezone

import humps
from hopsworks_apigen import public
from hopsworks_common import util


def _ms_to_dt(v):
    if isinstance(v, int):
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    return v


def _dt_to_ms(v):
    if v is None:
        return None
    return int(v.timestamp() * 1000.0)


@public("hopsworks.job_schedule.JobSchedule", "hsfs.core.job_schedule.JobSchedule")
class JobSchedule:
    def __init__(
        self,
        start_date_time,
        enabled,
        cron_expression,
        next_execution_date_time=None,
        id=None,
        end_date_time=None,
        catchup=False,
        max_active_runs=1,
        start_time_offset_seconds=None,
        end_time_offset_seconds=None,
        skip_to_date=None,
        max_catchup_runs=None,
        **kwargs,
    ):
        self._id = id
        self._start_date_time = _ms_to_dt(start_date_time)
        self._end_date_time = _ms_to_dt(end_date_time)
        self._enabled = enabled
        self._cron_expression = cron_expression
        self._next_execution_date_time = _ms_to_dt(next_execution_date_time)
        self._catchup = bool(catchup) if catchup is not None else False
        self._max_active_runs = (
            int(max_active_runs) if max_active_runs is not None else 1
        )
        # None preserved: means "last execution time" for start, "cron fire time" for end.
        self._start_time_offset_seconds = (
            int(start_time_offset_seconds)
            if start_time_offset_seconds is not None
            else None
        )
        self._end_time_offset_seconds = (
            int(end_time_offset_seconds)
            if end_time_offset_seconds is not None
            else None
        )
        self._skip_to_date = _ms_to_dt(skip_to_date)
        self._max_catchup_runs = (
            int(max_catchup_runs) if max_catchup_runs is not None else None
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "startDateTime": _dt_to_ms(self._start_date_time),
            "endDateTime": _dt_to_ms(self._end_date_time),
            "cronExpression": self._cron_expression,
            "enabled": self._enabled,
            "catchup": self._catchup,
            "maxActiveRuns": self._max_active_runs,
            "startTimeOffsetSeconds": self._start_time_offset_seconds,
            "endTimeOffsetSeconds": self._end_time_offset_seconds,
            "skipToDate": _dt_to_ms(self._skip_to_date),
            "maxCatchupRuns": self._max_catchup_runs,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @public
    @property
    def id(self):
        """Return the schedule id."""
        return self._id

    @public
    @property
    def start_date_time(self):
        """Return the schedule start time."""
        return self._start_date_time

    @public
    @property
    def end_date_time(self):
        """Return the schedule end time."""
        return self._end_date_time

    @public
    @property
    def enabled(self):
        """Return whether the schedule is enabled or not."""
        return self._enabled

    @public
    @property
    def cron_expression(self):
        """Return the schedule cron expression."""
        return self._cron_expression

    @public
    @property
    def next_execution_date_time(self):
        """Return the next execution time."""
        return self._next_execution_date_time

    @public
    @property
    def catchup(self):
        """If True, backfill all missed intervals on scheduler recovery; if False, only the most recent."""
        return self._catchup

    @public
    @property
    def max_active_runs(self):
        """Maximum number of concurrent executions allowed for this job (default 1)."""
        return self._max_active_runs

    @public
    @property
    def start_time_offset_seconds(self):
        """Controls HOPS_START_TIME. `None` = previous cron fire (last execution time);
        otherwise `cron_fire + seconds` (negative = before, positive = after)."""
        return self._start_time_offset_seconds

    @public
    @property
    def end_time_offset_seconds(self):
        """Controls HOPS_END_TIME. `None` = cron fire time; otherwise `cron_fire + seconds`."""
        return self._end_time_offset_seconds

    @public
    @property
    def skip_to_date(self):
        """If set, reconciliation skips all missed intervals before this date."""
        return self._skip_to_date

    @public
    @property
    def max_catchup_runs(self):
        """Upper bound on missed intervals created during reconciliation (keeps most recent)."""
        return self._max_catchup_runs
