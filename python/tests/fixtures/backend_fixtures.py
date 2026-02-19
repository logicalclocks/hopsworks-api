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

import copy
import json
import os

import pytest


FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))

FIXTURES = [
    "alert_receiver",
    "alert_route",
    "execution",
    "expectation_suite",
    "external_feature_group",
    "external_feature_group_alias",
    "feature",
    "feature_descriptive_statistics",
    "feature_group",
    "feature_group_commit",
    "feature_monitoring_config",
    "feature_monitoring_result",
    "feature_store",
    "feature_view",
    "filter",
    "fs_query",
    "ge_expectation",
    "ge_validation_result",
    "hudi_feature_group_alias",
    "inference_batcher",
    "inference_endpoint",
    "inference_logger",
    "ingestion_job",
    "inode",
    "job",
    "join",
    "kafka_topic",
    "logic",
    "model",
    "predictor",
    "prepared_statement_parameter",
    "query",
    "resources",
    "rondb_server",
    "serving_keys",
    "serving_prepared_statement",
    "spine_group",
    "split_statistics",
    "statistics",
    "statistics_config",
    "storage_connector",
    "tag",
    "training_dataset",
    "training_dataset_feature",
    "training_dataset_split",
    "transformation_function",
    "transformer",
    "user",
    "validation_report",
    "online_ingestion",
]

backend_fixtures_json = {}
for fixture in FIXTURES:
    with open(os.path.join(FIXTURES_DIR, f"{fixture}_fixtures.json")) as json_file:
        backend_fixtures_json[fixture] = json.load(json_file)


@pytest.fixture
def backend_fixtures():
    return copy.deepcopy(backend_fixtures_json)
