/*
 *  Copyright (c) 2026. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.metadata;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFeatureDescriptiveStatistics {

  @Test
  void theMergeableStateIsKeptInTheExtendedStatistics() {
    JSONObject column = new JSONObject("{\"column\": \"x\", \"dataType\": \"Integral\", \"numRecordsNonNull\": 3,"
        + " \"numRecordsNull\": 0, \"mergeable\": {\"format\": \"datasketches-native-v1\", \"hll\": \"AA==\","
        + " \"moments\": {\"n\": 3}}}");

    FeatureDescriptiveStatistics fds = FeatureDescriptiveStatistics.fromDeequStatisticsJson(column);

    JSONObject extended = new JSONObject(fds.getExtendedStatistics());
    Assertions.assertEquals("datasketches-native-v1", extended.getJSONObject("mergeable").getString("format"));
    Assertions.assertEquals(3, extended.getJSONObject("mergeable").getJSONObject("moments").getInt("n"));
  }

  @Test
  void aProfileWithoutStateHasNoExtendedStatistics() {
    JSONObject column = new JSONObject("{\"column\": \"x\", \"dataType\": \"Integral\", \"numRecordsNonNull\": 3,"
        + " \"numRecordsNull\": 0}");

    Assertions.assertNull(FeatureDescriptiveStatistics.fromDeequStatisticsJson(column).getExtendedStatistics());
  }
}
