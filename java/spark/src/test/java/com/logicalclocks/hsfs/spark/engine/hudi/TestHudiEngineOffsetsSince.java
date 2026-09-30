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

package com.logicalclocks.hsfs.spark.engine.hudi;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestHudiEngineOffsetsSince {

  private static final long SINCE = 1789984800000L;

  @Test
  @SuppressWarnings("unchecked")
  void testRecordAppendedDuringLookupIsNotSkipped() {
    // Arrange - partition 0 holds only records older than the floor, so the lookup finds
    // nothing, and a producer appends offset 10 right after the lookup answers.
    TopicPartition tp = new TopicPartition("topic", 0);
    List<TopicPartition> partitions = Collections.singletonList(tp);
    AtomicLong end = new AtomicLong(10);
    Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
    Mockito.when(consumer.endOffsets(partitions))
        .thenAnswer(invocation -> Collections.singletonMap(tp, end.get()));
    Mockito.when(consumer.offsetsForTimes(Mockito.anyMap())).thenAnswer(invocation -> {
      end.incrementAndGet();
      return Collections.singletonMap(tp, null);
    });
    Mockito.when(consumer.beginningOffsets(partitions)).thenReturn(Collections.singletonMap(tp, 0L));

    // Act
    Map<TopicPartition, Long> offsets = HudiEngine.offsetsSince(consumer, partitions, SINCE);

    // Assert - reading starts at the appended record rather than past it
    Assertions.assertEquals(10L, offsets.get(tp));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testOffsetsSince() {
    // Arrange - partition 0 has a record past the floor, partition 1 lost the record the
    // lookup landed on to retention before the beginning offsets were read.
    TopicPartition tp0 = new TopicPartition("topic", 0);
    TopicPartition tp1 = new TopicPartition("topic", 1);
    List<TopicPartition> partitions = Arrays.asList(tp0, tp1);
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(tp0, 100L);
    endOffsets.put(tp1, 100L);
    Map<TopicPartition, OffsetAndTimestamp> found = new HashMap<>();
    found.put(tp0, new OffsetAndTimestamp(37, SINCE));
    found.put(tp1, new OffsetAndTimestamp(12, SINCE));
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(tp0, 0L);
    beginningOffsets.put(tp1, 40L);
    Consumer<byte[], byte[]> consumer = Mockito.mock(Consumer.class);
    Mockito.when(consumer.endOffsets(partitions)).thenReturn(endOffsets);
    Mockito.when(consumer.offsetsForTimes(Mockito.anyMap())).thenReturn(found);
    Mockito.when(consumer.beginningOffsets(partitions)).thenReturn(beginningOffsets);

    // Act
    Map<TopicPartition, Long> offsets = HudiEngine.offsetsSince(consumer, partitions, SINCE);

    // Assert
    Assertions.assertEquals(37L, offsets.get(tp0));
    Assertions.assertEquals(40L, offsets.get(tp1));
  }
}
