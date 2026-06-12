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

"""Unit tests for the shared `partitioned_by` grain materializer.

`materialize_grains_arrow` is the Arrow twin used by both the delta-rs and
PyIceberg write paths, so testing it here covers both engines.
"""

from datetime import datetime
from types import SimpleNamespace

import pyarrow as pa
from hsfs.core import partition_grains


def _fg(partitioned_by=None, event_time="event_ts"):
    return SimpleNamespace(partitioned_by=partitioned_by, event_time=event_time)


def test_arrow_materializes_grains_from_timestamp():
    fg = _fg(["year", "month"])
    table = pa.table(
        {
            "id": [1, 2],
            "event_ts": pa.array(
                [datetime(2026, 1, 15), datetime(2026, 3, 2)],
                type=pa.timestamp("us"),
            ),
        }
    )
    out = partition_grains.materialize_grains_arrow(fg, table)
    assert out.column_names == ["id", "event_ts", "year", "month"]
    assert out.column("year").to_pylist() == [2026, 2026]
    assert out.column("month").to_pylist() == [1, 3]


def test_arrow_idempotent_when_grain_present():
    fg = _fg(["year"])
    table = pa.table(
        {
            "event_ts": pa.array([datetime(2026, 5, 1)], type=pa.timestamp("us")),
            "year": pa.array([1999], type=pa.int32()),
        }
    )
    out = partition_grains.materialize_grains_arrow(fg, table)
    # Already present: not recomputed, not duplicated.
    assert out.column_names == ["event_ts", "year"]
    assert out.column("year").to_pylist() == [1999]


def test_arrow_integer_seconds_event_time():
    fg = _fg(["year"], event_time="ts")
    # 1736899200 = 2025-01-15 00:00:00 UTC (10-digit -> seconds)
    table = pa.table({"ts": pa.array([1736899200], type=pa.int64())})
    out = partition_grains.materialize_grains_arrow(fg, table)
    assert out.column("year").to_pylist() == [2025]


def test_arrow_noop_without_partitioned_by():
    fg = _fg(None)
    table = pa.table({"event_ts": pa.array([datetime(2026, 1, 1)])})
    assert partition_grains.materialize_grains_arrow(fg, table) is table


def test_arrow_noop_when_event_time_missing():
    fg = _fg(["year"], event_time="event_ts")
    table = pa.table({"id": [1]})
    assert partition_grains.materialize_grains_arrow(fg, table) is table
