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

"""Tests for the alert-status backward-compat normalization layer (added in ~=3.8.1).

Coverage:
- _normalize_alert_status (response deserialization path)
- _normalize_status_input (input/create path)
- Alert.from_response_json normalizes old wire values
- alerts_api constants include both old and new monitoring status names
"""

import warnings

from hopsworks_common.alert import Alert, _normalize_alert_status
from hopsworks_common.core.alerts_api import (
    _MONITORING_STATUS,
    _PROJECT_FS_STATUS,
    _normalize_status_input,
)


class TestNormalizeAlertStatus:
    """Tests for _normalize_alert_status (backend → SDK deserialization path)."""

    def test_old_success_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("SUCCESS")
        assert result == "VALIDATION_SUCCESS"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)
        assert "SUCCESS" in str(caught[0].message)
        assert "VALIDATION_SUCCESS" in str(caught[0].message)

    def test_old_warning_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("WARNING")
        assert result == "VALIDATION_WARNING"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_old_failure_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("FAILURE")
        assert result == "VALIDATION_FAILURE"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_old_shift_undetected_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("FEATURE_MONITOR_SHIFT_UNDETECTED")
        assert result == "MONITORING_SHIFT_UNDETECTED"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_old_shift_detected_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("FEATURE_MONITOR_SHIFT_DETECTED")
        assert result == "MONITORING_SHIFT_DETECTED"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_new_validation_success_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("VALIDATION_SUCCESS")
        assert result == "VALIDATION_SUCCESS"
        assert len(caught) == 0

    def test_new_monitoring_shift_detected_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("MONITORING_SHIFT_DETECTED")
        assert result == "MONITORING_SHIFT_DETECTED"
        assert len(caught) == 0

    def test_new_monitoring_empty_detection_window_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("MONITORING_EMPTY_DETECTION_WINDOW")
        assert result == "MONITORING_EMPTY_DETECTION_WINDOW"
        assert len(caught) == 0

    def test_none_returns_none(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status(None)
        assert result is None
        assert len(caught) == 0

    def test_unrelated_string_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_alert_status("JOB_FINISHED")
        assert result == "JOB_FINISHED"
        assert len(caught) == 0


class TestAlertFromResponseJsonNormalizes:
    """Tests that Alert.from_response_json normalizes old wire values."""

    def test_old_success_wire_value_is_normalized_in_alert(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            a = Alert.from_response_json({"status": "SUCCESS"})
        assert a.status == "VALIDATION_SUCCESS"
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    def test_old_shift_detected_wire_value_is_normalized_in_alert(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            a = Alert.from_response_json({"status": "FEATURE_MONITOR_SHIFT_DETECTED"})
        assert a.status == "MONITORING_SHIFT_DETECTED"
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    def test_new_monitoring_empty_detection_window_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            a = Alert.from_response_json(
                {"status": "MONITORING_EMPTY_DETECTION_WINDOW"}
            )
        assert a.status == "MONITORING_EMPTY_DETECTION_WINDOW"
        assert len(caught) == 0

    def test_none_status_stays_none(self):
        a = Alert.from_response_json({})
        assert a.status is None


class TestNormalizeStatusInput:
    """Tests for _normalize_status_input (user → SDK input path)."""

    def test_old_shift_detected_input_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_status_input("feature_monitor_shift_detected")
        assert result == "monitoring_shift_detected"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_old_shift_undetected_input_is_normalized(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_status_input("feature_monitor_shift_undetected")
        assert result == "monitoring_shift_undetected"
        assert len(caught) == 1
        assert issubclass(caught[0].category, DeprecationWarning)

    def test_new_monitoring_shift_detected_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_status_input("monitoring_shift_detected")
        assert result == "monitoring_shift_detected"
        assert len(caught) == 0

    def test_new_monitoring_empty_detection_window_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_status_input("monitoring_empty_detection_window")
        assert result == "monitoring_empty_detection_window"
        assert len(caught) == 0

    def test_validation_success_passes_through(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _normalize_status_input("feature_validation_success")
        assert result == "feature_validation_success"
        assert len(caught) == 0


class TestStatusConstantsContainNewValues:
    """Tests that the module-level status constant tuples expose the new names."""

    def test_monitoring_status_contains_new_names(self):
        assert "monitoring_shift_undetected" in _MONITORING_STATUS
        assert "monitoring_shift_detected" in _MONITORING_STATUS
        assert "monitoring_empty_detection_window" in _MONITORING_STATUS

    def test_monitoring_status_retains_old_names_for_compat(self):
        assert "feature_monitor_shift_undetected" in _MONITORING_STATUS
        assert "feature_monitor_shift_detected" in _MONITORING_STATUS

    def test_project_fs_status_contains_new_names(self):
        assert "monitoring_shift_undetected" in _PROJECT_FS_STATUS
        assert "monitoring_shift_detected" in _PROJECT_FS_STATUS
        assert "monitoring_empty_detection_window" in _PROJECT_FS_STATUS

    def test_project_fs_status_retains_old_names_for_compat(self):
        assert "feature_monitor_shift_undetected" in _PROJECT_FS_STATUS
        assert "feature_monitor_shift_detected" in _PROJECT_FS_STATUS
