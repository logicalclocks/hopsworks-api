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

package com.logicalclocks.hsfs.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.FeatureViewBase;
import com.logicalclocks.hsfs.metadata.FeatureViewApi;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestFeatureView {

  private FeatureViewApi injectMockApi(FeatureView featureView) throws Exception {
    FeatureViewApi mockApi = Mockito.mock(FeatureViewApi.class);
    Field field = FeatureViewBase.class.getDeclaredField("featureViewApi");
    field.setAccessible(true);
    field.set(featureView, mockApi);
    return mockApi;
  }

  @Test
  void testParsingJson() throws JsonProcessingException {
    // Arrange
    Logger logger = Logger.getRootLogger();
    Appender appender = Mockito.mock(Appender.class);
    logger.addAppender(appender);

    ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);

    ObjectMapper objectMapper = new ObjectMapper();
    String json = "{\"name\":\"test_name\",\"query\":{\"leftFeatureGroup\":{\"name\":\"test_fg\",\"version\":1,\"deprecated\":false}}}";

    // Act
    FeatureView fv = objectMapper.readValue(json, FeatureView.class);

    // Assert
    Mockito.verify(appender, Mockito.times(0)).doAppend(argument.capture());
  }

  @Test
  void testParsingJsonWhenDeprecatedFeatureGroup() throws JsonProcessingException {
    // Arrange
    Logger logger = Logger.getRootLogger();
    Appender appender = Mockito.mock(Appender.class);
    logger.addAppender(appender);

    ArgumentCaptor<LoggingEvent> argument = ArgumentCaptor.forClass(LoggingEvent.class);

    ObjectMapper objectMapper = new ObjectMapper();
    String json = "{\"name\":\"test_name\",\"query\":{\"leftFeatureGroup\":{\"name\":\"test_fg\",\"version\":1,\"deprecated\":true}}}";

    // Act
    FeatureView fv = objectMapper.readValue(json, FeatureView.class);

    // Assert
    Mockito.verify(appender, Mockito.times(1)).doAppend(argument.capture());
    Assert.assertEquals(Level.WARN, argument.getValue().getLevel());
    Assert.assertEquals("Feature Group `test_fg`, version `1` is deprecated",
        argument.getValue().getMessage().toString());
    Assert.assertEquals("com.logicalclocks.hsfs.FeatureGroupBase", argument.getValue().getLoggerName());
  }

  @Test
  void testDeleteDefaultPassesForceFalse() throws Exception {
    // Arrange
    FeatureStore mockStore = Mockito.mock(FeatureStore.class);
    FeatureView featureView = new FeatureView();
    featureView.setName("test_fv");
    featureView.setVersion(1);
    featureView.setFeatureStore(mockStore);
    FeatureViewApi mockApi = injectMockApi(featureView);

    // Act
    featureView.delete();

    // Assert
    Mockito.verify(mockApi).delete(mockStore, "test_fv", 1, false);
  }

  @Test
  void testDeleteWithForceFalse() throws Exception {
    // Arrange
    FeatureStore mockStore = Mockito.mock(FeatureStore.class);
    FeatureView featureView = new FeatureView();
    featureView.setName("test_fv");
    featureView.setVersion(1);
    featureView.setFeatureStore(mockStore);
    FeatureViewApi mockApi = injectMockApi(featureView);

    // Act
    featureView.delete(false);

    // Assert
    Mockito.verify(mockApi).delete(mockStore, "test_fv", 1, false);
  }

  @Test
  void testDeleteWithForceTrue() throws Exception {
    // Arrange
    FeatureStore mockStore = Mockito.mock(FeatureStore.class);
    FeatureView featureView = new FeatureView();
    featureView.setName("test_fv");
    featureView.setVersion(1);
    featureView.setFeatureStore(mockStore);
    FeatureViewApi mockApi = injectMockApi(featureView);

    // Act
    featureView.delete(true);

    // Assert
    Mockito.verify(mockApi).delete(mockStore, "test_fv", 1, true);
  }
}
