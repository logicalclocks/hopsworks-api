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

from hopsworks_common.core import sink_job_configuration


class TestFeatureColumnMapping:
    def test_to_dict_and_from_response_json(self):
        mapping = sink_job_configuration.FeatureColumnMapping(
            source_column="source_a", feature_name="feature_a"
        )

        assert mapping.to_dict() == {
            "sourceColumn": "source_a",
            "featureName": "feature_a",
        }

        parsed = sink_job_configuration.FeatureColumnMapping.from_response_json(
            {"sourceColumn": "source_b", "featureName": "feature_b"}
        )
        assert parsed.source_column == "source_b"
        assert parsed.feature_name == "feature_b"
