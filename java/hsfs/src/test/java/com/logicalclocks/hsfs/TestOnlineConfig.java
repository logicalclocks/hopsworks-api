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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class TestOnlineConfig {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void testHashIndexTypeSerializedAndDeserialized() throws Exception {
    // Arrange
    OnlineConfig config = new OnlineConfig(null, null, PrimaryKeyIndexType.HASH);

    // Act – round-trip through Jackson
    String json = objectMapper.writeValueAsString(config);
    OnlineConfig result = objectMapper.readValue(json, OnlineConfig.class);

    // Assert
    Assert.assertEquals(PrimaryKeyIndexType.HASH, result.getPrimaryKeyIndexType());
  }

  @Test
  void testOrderedIndexTypeSerializedAndDeserialized() throws Exception {
    // Arrange
    OnlineConfig config = new OnlineConfig(null, null, PrimaryKeyIndexType.ORDERED);

    // Act
    String json = objectMapper.writeValueAsString(config);
    OnlineConfig result = objectMapper.readValue(json, OnlineConfig.class);

    // Assert
    Assert.assertEquals(PrimaryKeyIndexType.ORDERED, result.getPrimaryKeyIndexType());
  }

  @Test
  void testNoConfigPassedDefaultsToHash() throws Exception {
    // When no primaryKeyIndexType is set the server applies a TTL-driven default
    // and returns HASH for feature groups without TTL. Simulate that server response.
    String serverResponse = "{\"primaryKeyIndexType\":\"HASH\"}";

    // Act
    OnlineConfig result = objectMapper.readValue(serverResponse, OnlineConfig.class);

    // Assert
    Assert.assertEquals(PrimaryKeyIndexType.HASH, result.getPrimaryKeyIndexType());
  }

  @Test
  void testSecondaryIndexesSerializedAndDeserialized() throws Exception {
    // Arrange
    OnlineConfig config = new OnlineConfig();
    config.setSecondaryIndexes(Arrays.asList(
        Arrays.asList("user_id"),
        Arrays.asList("country", "city")));

    // Act – round-trip through Jackson
    String json = objectMapper.writeValueAsString(config);
    OnlineConfig result = objectMapper.readValue(json, OnlineConfig.class);

    // Assert
    List<List<String>> expected = Arrays.asList(
        Arrays.asList("user_id"),
        Arrays.asList("country", "city"));
    Assert.assertEquals(expected, result.getSecondaryIndexes());
  }

  @Test
  void testUnknownJsonPropertyIgnored() throws Exception {
    // @JsonIgnoreProperties(ignoreUnknown = true) must not throw on extra fields,
    // which is required for forward compatibility when a newer backend adds fields.
    String json = "{\"primaryKeyIndexType\":\"HASH\",\"unknownFutureField\":\"value\"}";

    // Act – must not throw
    OnlineConfig result = objectMapper.readValue(json, OnlineConfig.class);

    // Assert the known field was still parsed correctly
    Assert.assertEquals(PrimaryKeyIndexType.HASH, result.getPrimaryKeyIndexType());
  }
}
