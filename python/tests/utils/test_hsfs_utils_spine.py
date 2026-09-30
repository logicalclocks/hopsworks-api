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
"""The training dataset job rebuilds its query from the feature view; the spine is what it cannot rebuild."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException


def _spark_reading(frame):
    spark = MagicMock()
    spark.read.parquet.return_value = frame
    return spark


def test_the_staged_spine_is_read_from_the_projects_spine_directory_in_spine_order(
    hsfs_utils, monkeypatch
):
    frame = MagicMock()
    spark = _spark_reading(frame)
    monkeypatch.setattr(hsfs_utils, "setup_spark", lambda: spark)
    fs = MagicMock()
    fs.project_name = "air"

    result = hsfs_utils.read_staged_spine(
        fs, {"parquetBasename": "0123456789abcdef.parquet"}
    )

    spark.read.parquet.assert_called_once_with(
        "hdfs:///Projects/air/Resources/.hopsworks_spine/0123456789abcdef.parquet"
    )
    frame.orderBy.assert_called_once_with("__hopsworks_spine_row_id")
    frame.orderBy.return_value.drop.assert_called_once_with("__hopsworks_spine_row_id")
    assert result is frame.orderBy.return_value.drop.return_value


def test_a_spine_that_names_no_file_is_an_error(hsfs_utils):
    with pytest.raises(FeatureStoreException, match="names no staged file"):
        hsfs_utils.read_staged_spine(MagicMock(), {"parquetBasename": None})


def test_a_missing_file_fails_the_job_rather_than_building_another_population(
    hsfs_utils, monkeypatch
):
    spark = MagicMock()
    spark.read.parquet.side_effect = RuntimeError("PATH_NOT_FOUND")
    monkeypatch.setattr(hsfs_utils, "setup_spark", lambda: spark)
    with pytest.raises(FeatureStoreException, match="could not be read"):
        hsfs_utils.read_staged_spine(
            MagicMock(), {"parquetBasename": "0123456789abcdef.parquet"}
        )


def test_create_fv_td_passes_the_spine_to_the_compute(hsfs_utils, monkeypatch):
    fs = MagicMock()
    monkeypatch.setattr(hsfs_utils, "get_feature_store_handle", lambda name: fs)
    engine = MagicMock()
    monkeypatch.setattr(
        hsfs_utils.feature_view_engine, "FeatureViewEngine", lambda fs_id: engine
    )
    frame = MagicMock()
    monkeypatch.setattr(hsfs_utils, "read_staged_spine", lambda fs_, spine: frame)

    hsfs_utils.create_fv_td(
        {
            "feature_store": "air_featurestore",
            "name": "air_quality_fv",
            "version": 1,
            "td_version": 3,
            "spine": {"parquetBasename": "0123456789abcdef.parquet"},
        }
    )

    assert engine._compute_training_dataset.call_args.kwargs["spine_df"] is frame


def test_create_fv_td_without_a_spine_computes_the_historical_population(
    hsfs_utils, monkeypatch
):
    monkeypatch.setattr(
        hsfs_utils, "get_feature_store_handle", lambda name: MagicMock()
    )
    engine = MagicMock()
    monkeypatch.setattr(
        hsfs_utils.feature_view_engine, "FeatureViewEngine", lambda fs_id: engine
    )

    hsfs_utils.create_fv_td(
        {
            "feature_store": "air_featurestore",
            "name": "fv",
            "version": 1,
            "td_version": 3,
        }
    )

    assert engine._compute_training_dataset.call_args.kwargs["spine_df"] is None
