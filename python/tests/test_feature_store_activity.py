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

from hsfs.feature_store_activity import (
    FeatureStoreActivity,
    FeatureStoreActivityType,
)


class TestFeatureStoreActivity:
    def test_from_response_json_with_an_online_ingestion(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_store_activity"]["get_list"]["response"]

        # Act
        activities = FeatureStoreActivity.from_response_json(json)

        # Assert
        assert [a.type for a in activities] == [
            FeatureStoreActivityType.ONLINE_INGESTION,
            FeatureStoreActivityType.COMMIT,
            FeatureStoreActivityType.METADATA,
        ]
        assert activities[0].online_ingestion == {
            "id": 1092,
            "num_entries": 5,
            "results": [{"online_ingestion_id": 1092, "status": "UPSERTED", "rows": 5}],
        }
        assert activities[1].online_ingestion is None

    def test_to_dict_keeps_the_online_ingestion(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_store_activity"]["get_list"]["response"]
        activity = FeatureStoreActivity.from_response_json(json)[0]

        # Act
        activity_dict = activity.to_dict()

        # Assert
        assert activity_dict["type"] == "ONLINE_INGESTION"
        assert activity_dict["online_ingestion"]["num_entries"] == 5
