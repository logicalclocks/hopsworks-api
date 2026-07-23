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

import warnings
from types import SimpleNamespace

import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs.feature_view import FeatureView


def _fg(partitioned_by=None, online_partition_columns=False, online_enabled=False):
    return SimpleNamespace(
        partitioned_by=partitioned_by or [],
        online_partition_columns=online_partition_columns,
        online_enabled=online_enabled,
    )


def _query(left_fg, left_feature_names, joins=None):
    return SimpleNamespace(
        _left_feature_group=left_fg,
        _left_features=[SimpleNamespace(name=n) for n in left_feature_names],
        _joins=joins or [],
    )


def _fv(query):
    # Bypass __init__ (which needs a real query / engines); the helpers under
    # test only read self._query.
    fv = FeatureView.__new__(FeatureView)
    fv._query = query
    return fv


class TestFeatureViewPartitionedBy:
    def test_offline_only_grain_selected_is_detected(self):
        fg = _fg(["year", "month"], online_partition_columns=False, online_enabled=True)
        fv = _fv(_query(fg, ["year", "amount"]))
        assert fv._offline_only_partition_features() == ["year"]
        assert fv._has_online_feature_group() is True

    def test_get_feature_vector_helpers_raise_for_offline_only_grain(self):
        fg = _fg(["year"], online_partition_columns=False, online_enabled=True)
        fv = _fv(_query(fg, ["year", "amount"]))
        with pytest.raises(FeatureStoreException, match="year"):
            fv._assert_no_offline_only_partition_features()

    def test_online_partition_columns_true_is_servable(self):
        fg = _fg(["year"], online_partition_columns=True, online_enabled=True)
        fv = _fv(_query(fg, ["year"]))
        assert fv._offline_only_partition_features() == []
        # Must not raise.
        fv._assert_no_offline_only_partition_features()

    def test_grain_not_selected_is_not_flagged(self):
        fg = _fg(["year", "month"], online_partition_columns=False, online_enabled=True)
        fv = _fv(_query(fg, ["amount"]))
        assert fv._offline_only_partition_features() == []

    def test_grain_in_joined_feature_group_is_detected(self):
        left = _fg([], online_enabled=True)
        joined = _fg(["day"], online_partition_columns=False)
        join = SimpleNamespace(query=_query(joined, ["day"]))
        fv = _fv(_query(left, ["x"], joins=[join]))
        assert fv._offline_only_partition_features() == ["day"]

    def test_feature_group_without_partition_attrs_is_safe(self):
        # e.g. a SpineGroup / external FG that does not expose the attributes.
        fv = _fv(_query(SimpleNamespace(), ["x"]))
        assert fv._offline_only_partition_features() == []
        assert fv._has_online_feature_group() is False

    def test_no_warning_path_when_grain_is_online(self):
        # _has_online_feature_group true but nothing offline-only -> no offenders.
        fg = _fg(["year"], online_partition_columns=True, online_enabled=True)
        fv = _fv(_query(fg, ["year"]))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert fv._offline_only_partition_features() == []
