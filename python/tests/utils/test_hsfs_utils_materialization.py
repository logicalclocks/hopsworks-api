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
"""The offline materialization job reads its write options from the job configuration."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def entity(hsfs_utils, monkeypatch):
    entity = MagicMock()
    entity.id = 7
    entity._id = 7
    entity._online_topic_name = "topic"
    entity.primary_key = []
    entity.prepare_spark_location.return_value = "hdfs:///fg"
    fs = MagicMock()
    fs.get_feature_group.return_value = entity
    monkeypatch.setattr(hsfs_utils, "get_feature_store_handle", lambda _: fs)
    monkeypatch.setattr(hsfs_utils.kafka_engine, "_get_kafka_config", MagicMock())
    monkeypatch.setattr(
        hsfs_utils.kafka_engine, "_kafka_get_offsets", lambda **_: "topic,0:0"
    )
    monkeypatch.setattr(hsfs_utils, "_pending_offsets", lambda *_: None)
    monkeypatch.setattr(hsfs_utils.engine, "_get_instance", MagicMock())
    # Column expressions need a live SparkContext.
    for name in ("col", "expr", "max", "row_number"):
        monkeypatch.setattr(hsfs_utils, name, MagicMock())
    return entity


@pytest.mark.parametrize("job_conf", [{}, {"write_options": None}])
def test_a_job_without_write_options_upserts(hsfs_utils, entity, job_conf):
    spark = MagicMock()
    spark.read.json.return_value.toJSON.return_value.first.return_value = None
    frame = MagicMock()
    frame.filter.return_value = frame
    frame.limit.return_value = frame
    frame.count.return_value = 0
    frame.groupBy.return_value.agg.return_value.collect.return_value = []
    reader = spark.read.format.return_value.options.return_value
    reader.option.return_value = reader
    reader.load.return_value = frame

    hsfs_utils.offline_fg_materialization(
        spark, {"feature_store": "fs", "name": "fg", "version": 1, **job_conf}, None
    )

    assert entity.insert.call_args.kwargs["operation"] == "upsert"
    assert entity.insert.call_args.kwargs["write_options"] == {}
