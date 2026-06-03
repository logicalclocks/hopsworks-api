#
#   Copyright 2024 Hopsworks AB
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

from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult


class TestFeatureMonitoringResult:
    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"]["get_list"][
            "detection_and_reference_statistics_ids"
        ]["response"]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        self.assert_fm_result_with_det_and_ref_stats(result, with_ids=True)

    def test_from_response_json_list_with_det_stats_id(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"]["get_list"][
            "detection_statistics_id"
        ]["response"]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        self.assert_fm_result_with_det_stats(result, with_ids=True)

    def test_from_response_json_list_with_det_and_ref_stats(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_list_with_statistics"
        ]["detection_and_reference_statistics"]["response"]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        self.assert_fm_result_with_det_and_ref_stats(result)

    def test_from_response_json_list_with_det_stats(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"][
            "get_list_with_statistics"
        ]["detection_statistics"]["response"]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)
        result = result_list[0]

        # Assert
        self.assert_fm_result_with_det_stats(result)

    def test_from_response_json_empty_list(self, backend_fixtures):
        # Arrange
        result_json = backend_fixtures["feature_monitoring_result"]["get_list_empty"][
            "response"
        ]

        # Act
        result_list = FeatureMonitoringResult.from_response_json(result_json)

        # Assert
        assert isinstance(result_list, list)
        assert len(result_list) == 0

    def assert_fm_result_with_det_stats(self, result, with_ids=False):
        self.assert_fm_result(result)

        assert len(result.feature_statistics_results) == 1
        fsr = result.feature_statistics_results[0]

        self.assert_fs_result_with_det_stats(fsr, with_ids=with_ids)

    def assert_fm_result_with_det_and_ref_stats(self, result, with_ids=False):
        self.assert_fm_result(result, with_empty_ref_window=False)

        assert len(result.feature_statistics_results) == 1
        fsr = result.feature_statistics_results[0]

        self.assert_fs_result_with_det_and_ref_stats(fsr, with_ids=with_ids)

    def assert_fm_result(
        self, result, with_empty_det_window=False, with_empty_ref_window=None
    ):
        assert isinstance(result, FeatureMonitoringResult)
        assert result._id == 42
        assert result._feature_monitoring_config_id == 32
        assert result._feature_store_id == 67
        assert result._execution_id == 123
        assert result._monitoring_time == 1676457000000
        assert result._raised_exception is False
        assert result._empty_detection_window is with_empty_det_window
        assert result._empty_reference_window is with_empty_ref_window
        assert result._href[-2:] == "32"

    def assert_fs_result_with_det_stats(self, fsr, with_ids=False):
        if with_ids:
            assert fsr._detection_statistics_id == 52
            assert fsr._reference_statistics_id is None
            assert fsr._detection_statistics is None
            assert fsr._reference_statistics is None
        else:
            assert fsr._detection_statistics_id is None
            assert fsr._reference_statistics_id is None
            assert isinstance(fsr._detection_statistics, FeatureDescriptiveStatistics)
            assert fsr._detection_statistics._id == 52
            assert fsr._reference_statistics is None

        assert len(fsr.statistics_comparison_results) == 1
        scr = fsr.statistics_comparison_results[0]

        self.assert_sc_result(scr)

    def assert_fs_result_with_det_and_ref_stats(self, fsr, with_ids=False):
        if with_ids:
            assert fsr._detection_statistics_id == 52
            assert fsr._reference_statistics_id == 53
            assert fsr._detection_statistics is None
            assert fsr._reference_statistics is None
        else:
            assert fsr._detection_statistics_id is None
            assert fsr._reference_statistics_id is None
            assert isinstance(fsr._detection_statistics, FeatureDescriptiveStatistics)
            assert fsr._detection_statistics._id == 52
            assert isinstance(fsr._reference_statistics, FeatureDescriptiveStatistics)
            assert fsr._reference_statistics._id == 53

        assert len(fsr.statistics_comparison_results) == 1
        scr = fsr.statistics_comparison_results[0]

        self.assert_sc_result(scr)

    def assert_sc_result(self, scr):
        assert scr._difference == 0.3
        assert scr._shift_detected is True
