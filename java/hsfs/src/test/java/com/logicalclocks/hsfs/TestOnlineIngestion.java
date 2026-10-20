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

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestOnlineIngestion {

  private Appender appender;

  @BeforeEach
  void captureLogs() {
    Logger logger = Logger.getRootLogger();
    logger.removeAllAppenders();
    appender = Mockito.mock(Appender.class);
    logger.addAppender(appender);
  }

  private List<String> warnings() {
    ArgumentCaptor<LoggingEvent> events = ArgumentCaptor.forClass(LoggingEvent.class);
    Mockito.verify(appender, Mockito.atLeast(0)).doAppend(events.capture());
    return events.getAllValues().stream()
        .filter(event -> event.getLevel() == Level.WARN)
        .map(LoggingEvent::getRenderedMessage)
        .collect(Collectors.toList());
  }

  @Test
  void testIndefiniteWaitWithoutEntryCountWarnsAndKeepsPolling() throws Exception {
    // Arrange
    FeatureGroupBase featureGroup = Mockito.mock(FeatureGroupBase.class);
    // the wait cannot end on its own, so the first poll stops it
    Mockito.when(featureGroup.getOnlineIngestion(1)).thenThrow(new FeatureStoreException("stop"));
    OnlineIngestion onlineIngestion = new OnlineIngestion(1, null, Collections.emptyList(), featureGroup);

    // Act
    Assertions.assertThrows(FeatureStoreException.class, () -> onlineIngestion.waitForCompletion(0, 0));

    // Assert
    List<String> warnings = warnings();
    Assert.assertEquals(1, warnings.size());
    Assert.assertTrue(warnings.get(0).contains("Online ingestion 1 has no entry count"));
  }

  @Test
  void testExplicitTimeoutWithoutEntryCountEndsAfterTimeoutSeconds() throws Exception {
    // Arrange
    FeatureGroupBase featureGroup = Mockito.mock(FeatureGroupBase.class);
    Mockito.when(featureGroup.getOnlineIngestion(1))
        .thenReturn(new OnlineIngestion(1, null, Collections.emptyList(), featureGroup));
    OnlineIngestion onlineIngestion = new OnlineIngestion(1, null, Collections.emptyList(), featureGroup);

    // Act
    long start = System.currentTimeMillis();
    onlineIngestion.waitForCompletion(1, 1);
    long elapsed = System.currentTimeMillis() - start;

    // Assert
    Assert.assertTrue("waited " + elapsed + " ms", elapsed >= 1000 && elapsed < 10000);
    Assert.assertEquals(
        Collections.singletonList(
            "Timeout of 1 seconds was exceeded while waiting for online ingestion completion."),
        warnings());
  }

  @Test
  void testZeroEntriesCompleteAtOnce() throws Exception {
    // Arrange
    OnlineIngestion onlineIngestion = new OnlineIngestion(1, 0L, Collections.emptyList(), null);

    // Act
    // a poll would dereference the null feature group, so returning is itself the assertion
    onlineIngestion.waitForCompletion(0, 1);

    // Assert
    Assert.assertEquals(Collections.emptyList(), warnings());
  }
}
