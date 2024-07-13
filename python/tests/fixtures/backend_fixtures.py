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

import json
import os

import pytest


FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))

FIXTURES = [
    "tag",
    "model",
    "resources",
    "transformer",
    "predictor",
    "kafka_topic",
    "inference_logger",
    "inference_batcher",
    "inference_endpoint",
]

backend_fixtures_json = {}
for fixture in FIXTURES:
    with open(os.path.join(FIXTURES_DIR, f"{fixture}_fixtures.json"), "r") as json_file:
        backend_fixtures_json[fixture] = json.load(json_file)


@pytest.fixture
def backend_fixtures():
    return backend_fixtures_json
