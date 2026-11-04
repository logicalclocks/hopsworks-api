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

import json

import pytest
from hopsworks_common.core import sink_job_configuration


class TestLoadingConfig:
    def test_loading_config_incremental_with_rest_filter_param(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy="INCREMENTAL_ID",
            source_cursor_field="id",
            initial_value=42,
            rest_filter_param="id_gt",
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_ID.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "id",
                "restFilterParam": "id_gt",
                "initialValue": 42,
            },
            "fullLoadConfig": None,
        }

        parsed = sink_job_configuration.LoadingConfig.from_response_json(
            {
                "loadingStrategy": "INCREMENTAL_ID",
                "incrementalLoadingConfig": {
                    "sourceCursorField": "id",
                    "restFilterParam": "id_gt",
                    "initialValue": 42,
                },
            }
        )
        assert (
            parsed.to_dict()["incrementalLoadingConfig"]["restFilterParam"] == "id_gt"
        )

    def test_to_dict_full_load_defaults(self):
        config = sink_job_configuration.LoadingConfig()

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
            "incrementalLoadingConfig": None,
            "fullLoadConfig": None,
        }

    def test_to_dict_incremental_id(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy="INCREMENTAL_ID",
            source_cursor_field="id",
            initial_value=42,
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_ID.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "id",
                "initialValue": 42,
            },
            "fullLoadConfig": None,
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
            "fullLoadConfig": None,
        }

    def test_to_dict_full_load(self):
        config = sink_job_configuration.LoadingConfig(
            loading_strategy=sink_job_configuration.LoadingStrategy.FULL_LOAD,
            source_cursor_field="event_date",
            initial_value="2024-01-01T00:00:00Z",
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
            "incrementalLoadingConfig": None,
            "fullLoadConfig": {
                "sourceCursorField": "event_date",
                "initialValue": "2024-01-01T00:00:00Z",
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

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "event_date",
                "initialIngestionDate": 1704067200000,
            },
            "fullLoadConfig": None,
        }

    def test_invalid_loading_strategy(self):
        with pytest.raises(ValueError, match="Invalid loading_strategy"):
            sink_job_configuration.LoadingConfig(loading_strategy="NOT_A_STRATEGY")

    def test_from_response_json_full_load(self):
        config = sink_job_configuration.LoadingConfig.from_response_json(
            {
                "loadingStrategy": "FULL_LOAD",
                "fullLoadConfig": {
                    "sourceCursorField": "id",
                    "initialValue": "100",
                },
            }
        )

        assert config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
            "incrementalLoadingConfig": None,
            "fullLoadConfig": {
                "sourceCursorField": "id",
                "initialValue": "100",
            },
        }


@pytest.mark.parametrize(
    "start, expected",
    [
        ("2026-11-01T00:00:00Z", 1793491200000),
        ("2026-11-01T01:00:00+01:00", 1793491200000),
        ("2026-11-01", 1793491200000),
        ("1793491200000", "1793491200000"),
        (1793491200000, 1793491200000),
    ],
)
def test_incremental_timestamp_start_is_epoch_millis(start, expected):
    config = sink_job_configuration.LoadingConfig(
        loading_strategy="INCREMENTAL_TIMESTAMP",
        source_cursor_field="updated_at",
        initial_value=start,
    )
    assert config.to_dict()["incrementalLoadingConfig"]["initialValue"] == expected


def test_incremental_id_start_is_sent_as_given():
    config = sink_job_configuration.LoadingConfig(
        loading_strategy="INCREMENTAL_ID", source_cursor_field="id", initial_value="10"
    )
    assert config.to_dict()["incrementalLoadingConfig"]["initialValue"] == "10"


def test_a_flat_loading_key_is_refused():
    with pytest.raises(ValueError, match="belong in loading_config"):
        sink_job_configuration.SinkJobConfiguration.from_response_json(
            {"loading_strategy": "INCREMENTAL_ID", "source_cursor_field": "id"}
        )


@pytest.mark.parametrize("start", ["20261101", "2026-11-01", "2026-11-01T00:00:00Z"])
def test_incremental_date_start_keeps_its_date(start):
    config = sink_job_configuration.LoadingConfig(
        loading_strategy="INCREMENTAL_DATE",
        source_cursor_field="day",
        initial_value=start,
    )
    assert config.to_dict()["incrementalLoadingConfig"]["initialIngestionDate"] == (
        1793491200000
    )


def test_the_refusal_example_round_trips():
    with pytest.raises(ValueError) as refused:
        sink_job_configuration.SinkJobConfiguration.from_response_json(
            {"loading_strategy": "INCREMENTAL_ID"}
        )
    example = json.loads(str(refused.value).split("for example ")[1].split("; at")[0])
    config = sink_job_configuration.SinkJobConfiguration.from_response_json(example)
    assert config.loading_config.to_dict() == {
        "loadingStrategy": "INCREMENTAL_ID",
        "incrementalLoadingConfig": {"sourceCursorField": "id", "initialValue": "0"},
        "fullLoadConfig": None,
    }
