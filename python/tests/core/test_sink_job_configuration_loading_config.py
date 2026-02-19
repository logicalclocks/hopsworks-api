#
#   Copyright 2025 Hopsworks AB
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

import pytest
from hopsworks_common.core import sink_job_configuration


class TestLoadingConfig:
    def test_to_dict_full_load_defaults(self):
        config = sink_job_configuration.LoadingConfig()

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
            "incrementalLoadingConfig": None,
        }

    def test_to_dict_incremental_id(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy="INCREMENTAL_ID",
            source_cursor_field="id",
            initial_value="42",
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_ID.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "id",
                "initialValue": "42",
            },
        }

    def test_to_dict_incremental_date(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy=sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE,
            source_cursor_field="event_date",
            initial_value="2023-01-01",
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "event_date",
                "initialIngestionDate": 1672531200000,
            },
        }

    def test_to_dict_incremental_date_iso_timestamp(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy=sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE,
            source_cursor_field="event_date",
            initial_value="2024-01-01T00:00:00Z",
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "event_date",
                "initialIngestionDate": 1704067200000,
            },
        }

    def test_from_response_json_incremental_date_timestamp(self):
        config = sink_job_configuration.LoadingConfig.from_response_json(
            {
                "loadingStrategy": "INCREMENTAL_DATE",
                "incrementalLoadingConfig": {
                    "sourceCursorField": "event_date",
                    "initialIngestionDate": 1704067200000,
                },
            }
        )

        assert (
            config._initial_value == "2024-01-01T00:00:00Z"
        ), "Expected timestamp to be normalized to ISO UTC string"

    def test_invalid_loading_strategy(self):
        with pytest.raises(ValueError, match="Invalid loading_strategy"):
            sink_job_configuration.LoadingConfig(loading_strategy="NOT_A_STRATEGY")
