/*
 *  Copyright (c) 2023. Hopsworks AB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class TestFeatureGroupBaseForApi {
  @Test
  void testParsingJson() throws JsonProcessingException {
    try (MockedStatic<LoggerFactory> staticLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
      // Arrange
      Logger logger = Mockito.mock(Logger.class);
      staticLoggerFactory.when(() -> LoggerFactory.getLogger(Mockito.any(Class.class))).thenReturn(logger);

      ObjectMapper objectMapper = new ObjectMapper();
      String json = "{\"name\":\"test_fg\",\"version\":1,\"deprecated\":false}";

      // Act
      FeatureGroupBaseForApi fg = objectMapper.readValue(json, FeatureGroupBaseForApi.class);

      // Assert
      Mockito.verify(logger, Mockito.times(0)).warn(Mockito.anyString());
    }
  }

  @Test
  void testParsingJsonWhenDeprecated() throws JsonProcessingException {
    try (MockedStatic<LoggerFactory> staticLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
      // Arrange
      Logger logger = Mockito.mock(Logger.class);
      staticLoggerFactory.when(() -> LoggerFactory.getLogger(Mockito.any(Class.class))).thenReturn(logger);

      ObjectMapper objectMapper = new ObjectMapper();
      String json = "{\"name\":\"test_fg\",\"version\":1,\"deprecated\":true}";

      // Act
      FeatureGroupBaseForApi fg = objectMapper.readValue(json, FeatureGroupBaseForApi.class);

      // Assert
      Mockito.verify(logger, Mockito.times(1)).warn("Feature Group `test_fg`, version `1` is deprecated");
    }
  }
}
