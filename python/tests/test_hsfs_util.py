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

from unittest.mock import patch

from hsfs import util


class TestApplyDataIntervalDefaults:
    """hsfs.util.apply_scheduler_time_defaults falls back to HOPS_* env vars."""

    def test_explicit_values_win(self):
        # Even if the env vars are set, explicit args take precedence.
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults(
                "2024-06-01", "2024-07-01"
            )
        assert start == "2024-06-01"
        assert end == "2024-07-01"

    def test_fills_both_from_env_when_unset(self):
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start == "2026-01-01T00:00:00Z"
        assert end == "2026-02-01T00:00:00Z"

    def test_fills_only_missing_side(self):
        # Only end_time is missing — start_time passes through, end_time is filled.
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults("2024-06-01", None)
        assert start == "2024-06-01"
        assert end == "2026-02-01T00:00:00Z"

    def test_returns_none_when_no_env_and_no_args(self):
        # Neither env vars nor explicit args: stay None so the read is unconstrained.
        env = {"HOPS_START_TIME": "", "HOPS_END_TIME": ""}
        with patch.dict("os.environ", env, clear=False):
            # Remove the vars entirely too, in case the test environment has them set.
            import os

            os.environ.pop("HOPS_START_TIME", None)
            os.environ.pop("HOPS_END_TIME", None)
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start is None
        assert end is None

    def test_empty_env_string_is_ignored(self):
        # Empty env var must not override None — treat it as unset.
        env = {"HOPS_START_TIME": "", "HOPS_END_TIME": ""}
        with patch.dict("os.environ", env, clear=False):
            start, end = util.apply_scheduler_time_defaults(None, None)
        assert start is None
        assert end is None
