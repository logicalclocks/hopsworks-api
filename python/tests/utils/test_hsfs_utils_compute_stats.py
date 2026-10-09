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

from unittest.mock import MagicMock, create_autospec

from hsfs.core.statistics_engine import StatisticsEngine


def _feature_store_returning(monkeypatch, hsfs_utils, feature_group):
    fs = MagicMock()
    fs.get_feature_group.return_value = feature_group
    monkeypatch.setattr(hsfs_utils, "get_feature_store_handle", lambda _: fs)


def test_a_commit_scoped_job_persists_statistics_against_that_commit(
    hsfs_utils, monkeypatch
):
    feature_group = MagicMock()
    feature_group._statistics_engine = create_autospec(StatisticsEngine)
    _feature_store_returning(monkeypatch, hsfs_utils, feature_group)

    hsfs_utils.compute_stats(
        {
            "feature_store": "fs",
            "type": "fg",
            "name": "fg",
            "version": 1,
            "end_commit_time": "1700000000000",
        }
    )

    feature_group._statistics_engine._compute_and_save_statistics.assert_called_once_with(
        feature_group, feature_group_commit_id=1700000000000
    )
    feature_group.compute_statistics.assert_not_called()


def test_a_job_without_a_commit_computes_statistics_over_head(hsfs_utils, monkeypatch):
    feature_group = MagicMock()
    _feature_store_returning(monkeypatch, hsfs_utils, feature_group)

    hsfs_utils.compute_stats(
        {"feature_store": "fs", "type": "fg", "name": "fg", "version": 1}
    )

    feature_group.compute_statistics.assert_called_once_with()
