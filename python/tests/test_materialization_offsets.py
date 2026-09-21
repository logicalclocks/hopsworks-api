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
"""Where the offline materialization job starts reading its topic from.

The job lives in `utils/python/hsfs_utils.py`, which ships in the spark-feature-pipeline
image rather than on PyPI, and binds a Hadoop filesystem at import time — hence the
fixture below rather than a plain import.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from unittest import mock

import pytest


HSFS_UTILS = Path(__file__).parents[2] / "utils" / "python"
# 2026-09-21T10:00:00Z, and one hour before it
CREATED = "2026-09-21T10:00:00Z"
CREATED_MS = 1789984800000
HOUR_MS = 60 * 60 * 1000


@pytest.fixture(scope="module")
def hsfs_utils():
    with (
        mock.patch("fsspec.implementations.arrow.HadoopFileSystem"),
        mock.patch.dict(os.environ, {"HADOOP_USER_NAME": "test"}, clear=False),
    ):
        sys.path.insert(0, str(HSFS_UTILS))
        try:
            yield importlib.import_module("hsfs_utils")
        finally:
            sys.path.remove(str(HSFS_UTILS))


@pytest.fixture
def entity(mocker):
    feature_group = mocker.Mock()
    feature_group.created = CREATED
    feature_group._online_topic_name = "test_project_onlinefs"
    feature_group.feature_store_id = 99
    return feature_group


class TestOffsetsSinceCreation:
    """Where a first materialization run starts when nothing handed it offsets.

    Not the low watermark of the topic, which by default it shares with every other online-enabled feature group in the project, but the feature group's own creation time.
    """

    def test_creation_time_offsets_replace_the_low_watermark(
        self, mocker, hsfs_utils, entity
    ):
        # Arrange
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine,
            "_kafka_get_offsets_for_times",
            return_value="test_project_onlinefs,0:4200,1:3900",
        )

        # Act
        offsets = hsfs_utils._offsets_since_creation(
            entity, {"test_project_onlinefs": {"0": 0, "1": 0}}, {}
        )

        # Assert
        assert offsets == {"test_project_onlinefs": {"0": 4200, "1": 3900}}
        assert lookup.call_args.kwargs["topic_name"] == "test_project_onlinefs"
        assert lookup.call_args.kwargs["feature_store_id"] == 99

    def test_default_margin_is_one_hour_before_creation(
        self, mocker, hsfs_utils, entity
    ):
        # Kafka stamps a record with the producing client's clock, so the floor is moved
        # back to absorb a client whose clock lags the backend's.
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine,
            "_kafka_get_offsets_for_times",
            return_value="test_project_onlinefs,0:4200",
        )

        hsfs_utils._offsets_since_creation(
            entity, {"test_project_onlinefs": {"0": 0}}, {}
        )

        assert lookup.call_args.kwargs["timestamp"] == CREATED_MS - HOUR_MS

    def test_margin_is_configurable(self, mocker, hsfs_utils, entity):
        # A cluster with looser clocks buys safety with a longer read.
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine,
            "_kafka_get_offsets_for_times",
            return_value="test_project_onlinefs,0:4200",
        )

        hsfs_utils._offsets_since_creation(
            entity,
            {"test_project_onlinefs": {"0": 0}},
            {"initial_offset_margin_hours": 24},
        )

        assert lookup.call_args.kwargs["timestamp"] == CREATED_MS - 24 * HOUR_MS

    def test_a_failed_lookup_keeps_the_low_watermark(self, mocker, hsfs_utils, entity):
        # Reading the whole topic is what this exists to avoid, but re-reading records is
        # recoverable where skipping them is not.
        mocker.patch.object(
            hsfs_utils.kafka_engine,
            "_kafka_get_offsets_for_times",
            side_effect=Exception("broker did not answer"),
        )
        low = {"test_project_onlinefs": {"0": 17}}

        assert hsfs_utils._offsets_since_creation(entity, low, {}) == low

    def test_an_unparseable_margin_keeps_the_low_watermark(
        self, mocker, hsfs_utils, entity
    ):
        # The margin arrives from the job configuration as a string a user set. A bad one
        # must not take the materialization job down.
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine, "_kafka_get_offsets_for_times"
        )
        low = {"test_project_onlinefs": {"0": 17}}

        assert (
            hsfs_utils._offsets_since_creation(
                entity, low, {"initial_offset_margin_hours": "soon"}
            )
            == low
        )
        lookup.assert_not_called()

    def test_an_unparseable_creation_time_keeps_the_low_watermark(
        self, mocker, hsfs_utils, entity
    ):
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine, "_kafka_get_offsets_for_times"
        )
        entity.created = "not a timestamp"
        low = {"test_project_onlinefs": {"0": 17}}

        assert hsfs_utils._offsets_since_creation(entity, low, {}) == low
        lookup.assert_not_called()

    def test_a_vanished_topic_keeps_the_low_watermark(self, mocker, hsfs_utils, entity):
        mocker.patch.object(
            hsfs_utils.kafka_engine,
            "_kafka_get_offsets_for_times",
            return_value="",
        )
        low = {"test_project_onlinefs": {"0": 17}}

        assert hsfs_utils._offsets_since_creation(entity, low, {}) == low

    def test_a_feature_group_without_a_creation_time_keeps_the_low_watermark(
        self, mocker, hsfs_utils, entity
    ):
        # Older backends may not report one, and there is no floor to derive without it.
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine, "_kafka_get_offsets_for_times"
        )
        entity.created = None
        low = {"test_project_onlinefs": {"0": 17}}

        assert hsfs_utils._offsets_since_creation(entity, low, {}) == low
        lookup.assert_not_called()

    def test_an_absent_topic_keeps_the_empty_low_watermark(
        self, mocker, hsfs_utils, entity
    ):
        # `_kafka_get_offsets` returns "" for a topic that does not exist, which
        # `_build_offsets` turns into "". There is nothing to floor.
        lookup = mocker.patch.object(
            hsfs_utils.kafka_engine, "_kafka_get_offsets_for_times"
        )

        assert hsfs_utils._offsets_since_creation(entity, "", {}) == ""
        lookup.assert_not_called()
