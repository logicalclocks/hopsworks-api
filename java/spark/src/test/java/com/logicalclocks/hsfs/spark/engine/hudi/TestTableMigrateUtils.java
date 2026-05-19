/*
 *  Copyright (c) 2026-2026. Hopsworks AB
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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class TestTableMigrateUtils {

  private HoodieTableMetaClient metaClientWith(String partitionFields) {
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = Mockito.mock(HoodieTableConfig.class);
    Mockito.when(metaClient.getTableConfig()).thenReturn(tableConfig);
    Mockito.when(tableConfig.getString(HoodieTableConfig.PARTITION_FIELDS)).thenReturn(partitionFields);
    return metaClient;
  }

  @Test
  void testMigratePartitionFieldsNullIsNoOp() {
    HoodieTableMetaClient metaClient = metaClientWith(null);

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), any()), never());
    }
  }

  @Test
  void testMigratePartitionFieldsEmptyIsNoOp() {
    HoodieTableMetaClient metaClient = metaClientWith("");

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), any()), never());
    }
  }

  @Test
  void testMigratePartitionFieldsAlreadySuffixedIsNoOp() {
    HoodieTableMetaClient metaClient = metaClientWith("country:SIMPLE");

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), any()), never());
    }
  }

  @Test
  void testMigratePartitionFieldsAppendsSimpleToBareKey() {
    HoodieTableMetaClient metaClient = metaClientWith("country");
    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), propsCaptor.capture()), times(1));
    }

    Assertions.assertEquals(
        "country:SIMPLE",
        propsCaptor.getValue().getProperty(HoodieTableConfig.PARTITION_FIELDS.key()));
  }

  @Test
  void testMigratePartitionFieldsMultipleBareKeys() {
    HoodieTableMetaClient metaClient = metaClientWith("country,region");
    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), propsCaptor.capture()), times(1));
    }

    Assertions.assertEquals(
        "country:SIMPLE,region:SIMPLE",
        propsCaptor.getValue().getProperty(HoodieTableConfig.PARTITION_FIELDS.key()));
  }

  @Test
  void testMigratePartitionFieldsMixedKeysPreservesSuffixedAndAppendsBare() {
    HoodieTableMetaClient metaClient = metaClientWith("event_ts:TIMESTAMP,region");
    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), propsCaptor.capture()), times(1));
    }

    Assertions.assertEquals(
        "event_ts:TIMESTAMP,region:SIMPLE",
        propsCaptor.getValue().getProperty(HoodieTableConfig.PARTITION_FIELDS.key()));
  }

  @Test
  void testMigratePartitionFieldsTrimsWhitespaceAndSkipsEmptyEntries() {
    HoodieTableMetaClient metaClient = metaClientWith("  country , , region  ");
    ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

    try (MockedStatic<HoodieTableConfig> mocked = mockStatic(HoodieTableConfig.class)) {
      new TableMigrateUtils().migratePartitionFields(metaClient);
      mocked.verify(() -> HoodieTableConfig.update(any(), any(), propsCaptor.capture()), times(1));
    }

    Assertions.assertEquals(
        "country:SIMPLE,region:SIMPLE",
        propsCaptor.getValue().getProperty(HoodieTableConfig.PARTITION_FIELDS.key()));
  }
}
