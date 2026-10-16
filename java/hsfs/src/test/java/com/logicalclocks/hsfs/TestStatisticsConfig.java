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
package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class TestStatisticsConfig {

  // the client's mapper leaves out null fields
  private final ObjectMapper objectMapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @Test
  void testIncrementalIsSentWhenNull() throws Exception {
    StatisticsConfig config = new StatisticsConfig();

    JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(config));

    // an explicit null resets the feature group to the cluster default; leaving it out would keep its setting
    Assert.assertTrue(json.has("incremental"));
    Assert.assertTrue(json.get("incremental").isNull());
  }

  @Test
  void testIncrementalIsSentWhenSet() throws Exception {
    StatisticsConfig config = new StatisticsConfig();
    config.setIncremental(true);

    JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(config));

    Assert.assertTrue(json.get("incremental").asBoolean());
  }
}
