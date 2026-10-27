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

import pytest
from hopsworks_common.client.exceptions import JobException
from hopsworks_common.job import Job
from hopsworks_common.job_schedule import JobSchedule


class TestJobSchedulePauseResume:
    """`enabled` is what the backend acts on: true runs the schedule, false holds it."""

    def _job(self, mocker, schedule):
        job = Job(
            id=1,
            name="a_job",
            creation_time=None,
            config={},
            job_type="PYTHON",
            creator=None,
        )
        job._job_schedule = schedule
        return job, mocker.patch.object(job, "_update_schedule")

    def _schedule(self, enabled):
        return JobSchedule(
            start_date_time=0, enabled=enabled, cron_expression="0 0 * * * ? *", id=7
        )

    def test_pause_disables_and_resume_enables(self, mocker):
        job, update = self._job(mocker, self._schedule(True))
        job.pause_schedule()
        assert update.call_args.args[0].enabled is False

        job, update = self._job(mocker, self._schedule(False))
        job.resume_schedule()
        assert update.call_args.args[0].enabled is True

    def test_neither_works_without_a_schedule(self, mocker):
        job, _ = self._job(mocker, None)
        with pytest.raises(JobException, match="No schedule found"):
            job.pause_schedule()
        with pytest.raises(JobException, match="No schedule found"):
            job.resume_schedule()
