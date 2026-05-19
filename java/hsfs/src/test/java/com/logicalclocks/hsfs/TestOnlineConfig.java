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
}
