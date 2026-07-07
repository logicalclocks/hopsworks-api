/*
 *  Copyright (c) 2024. Hopsworks AB
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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFeatureDescriptiveStatistics {

  @Test
  public void testFromDeequStatisticsJsonPreservesEmbedding() {
    // Arrange: a profiler column-stats JSON for an embedding/vector feature
    JSONObject kll = new JSONObject();
    kll.put("kllFormat", "datasketches-native-v1");
    kll.put("bytes", "AQIDBA==");
    kll.put("buckets", new JSONArray().put(new JSONObject().put("count", 3)));

    JSONObject norm = new JSONObject();
    norm.put("histogram", new JSONArray().put(1).put(2).put(3));
    norm.put("kll", kll);

    JSONObject embedding = new JSONObject();
    embedding.put("dimension", 4);
    embedding.put("count", 100L);
    embedding.put("norm", norm);
    embedding.put("centroid", new JSONArray().put(0.1).put(0.2).put(0.3).put(0.4));

    JSONObject statsJson = new JSONObject();
    statsJson.put("column", "embedding_feature");
    statsJson.put("dataType", "Embedding");
    statsJson.put("count", 100L);
    statsJson.put("numRecordsNull", 0);
    statsJson.put("numRecordsNonNull", 100);
    statsJson.put("embedding", embedding);

    // Act
    FeatureDescriptiveStatistics fds = FeatureDescriptiveStatistics.fromDeequStatisticsJson(statsJson);

    // Assert: the Embedding data type is preserved, not coerced or dropped
    Assertions.assertEquals("Embedding", fds.getFeatureType());
    Assertions.assertEquals("embedding_feature", fds.getFeatureName());

    // Assert: the embedding block round-trips into extendedStatistics
    Assertions.assertNotNull(fds.getExtendedStatistics());
    JSONObject extended = new JSONObject(fds.getExtendedStatistics());
    Assertions.assertTrue(extended.has("embedding"));

    JSONObject parsedEmbedding = extended.getJSONObject("embedding");
    Assertions.assertEquals(4, parsedEmbedding.getInt("dimension"));
    Assertions.assertEquals(100L, parsedEmbedding.getLong("count"));
    Assertions.assertEquals(4, parsedEmbedding.getJSONArray("centroid").length());

    // Assert: the nested norm.kll block is preserved
    JSONObject parsedNorm = parsedEmbedding.getJSONObject("norm");
    Assertions.assertEquals(3, parsedNorm.getJSONArray("histogram").length());
    JSONObject parsedKll = parsedNorm.getJSONObject("kll");
    Assertions.assertEquals("datasketches-native-v1", parsedKll.getString("kllFormat"));
    Assertions.assertEquals("AQIDBA==", parsedKll.getString("bytes"));
    Assertions.assertEquals(1, parsedKll.getJSONArray("buckets").length());
  }

  @Test
  public void testFromDeequStatisticsJsonWithoutEmbedding() {
    // Arrange: a numerical feature with no embedding key
    JSONObject statsJson = new JSONObject();
    statsJson.put("column", "numeric_feature");
    statsJson.put("dataType", "Fractional");
    statsJson.put("count", 10L);
    statsJson.put("mean", 5.0);

    // Act
    FeatureDescriptiveStatistics fds = FeatureDescriptiveStatistics.fromDeequStatisticsJson(statsJson);

    // Assert: no extendedStatistics is produced when only base fields are present
    Assertions.assertNull(fds.getExtendedStatistics());
    Assertions.assertEquals(5.0, fds.getMean());
  }
}
