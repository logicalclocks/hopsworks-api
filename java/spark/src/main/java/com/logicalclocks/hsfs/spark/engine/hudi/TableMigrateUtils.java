/*
 *  Copyright (c) 2025-2025. Hopsworks AB
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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class TableMigrateUtils {
  private static final Logger LOG = Logger.getLogger(TableMigrateUtils.class.getName());
  
  public TableMigrateUtils() {
  
  }
  
  public void migrateTable(Map<String, String> writeOptions, JavaSparkContext javaSparkContext) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(HadoopFSUtils.getStorageConfWithCopy(javaSparkContext.hadoopConfiguration()))
            .setBasePath(writeOptions.get(HudiEngine.HUDI_BASE_PATH))
            .setLoadActiveTimelineOnLoad(false)
            .build();
    
    // During Hudi upgrades we might need to bump this version. This version matches Hudi 1.0.2
    if (metaClient.getTableConfig().contains(HoodieTableConfig.VERSION)
        && metaClient.getTableConfig().getTableVersion() != HoodieTableVersion.EIGHT) {
      doHiveSync(writeOptions, javaSparkContext, metaClient);
      // Migrate to version 6 first if the table is older than version 6.
      // If the table is version 6 or 7, it will be migrated to version 8 directly
      if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.SIX)) {
        migrateToVersionSix(writeOptions, metaClient, javaSparkContext);
      } else if (metaClient.getTableConfig().getTableVersion().greaterThan(HoodieTableVersion.EIGHT)) {
        return;
      }
      metaClient.getTableConfig().setValue(HudiEngine.HUDI_TABLE_VERSION,
          String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
      new UpgradeDowngrade(metaClient, getUpdatedWriteConfig(writeOptions, metaClient),
          new HoodieSparkEngineContext(javaSparkContext),
          SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.EIGHT, null);
      LOG.info("Migration to version 8 completed");
    }
  }
  
  private void migrateToVersionSix(Map<String, String> writeOptions, HoodieTableMetaClient metaClient,
      JavaSparkContext jsc) {
    LOG.info("Migrating Hudi table at " + writeOptions.get(HudiEngine.HUDI_BASE_PATH) + " to version 6");
    metaClient.getTableConfig().setValue(HudiEngine.HUDI_TABLE_OPERATION, WriteOperationType.UPSERT.value());
    metaClient.getTableConfig().setValue("hoodie.table.version",
        String.valueOf(HoodieTableVersion.SIX.versionCode()));
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
    
    new UpgradeDowngrade(metaClient, getUpdatedWriteConfig(writeOptions, metaClient), new HoodieSparkEngineContext(jsc),
      SparkUpgradeDowngradeHelper.getInstance()).run(HoodieTableVersion.SIX, null);
    LOG.info("Migration to version 6 completed");
  }
  
  private HoodieWriteConfig getUpdatedWriteConfig(Map<String, String> writeOptions, HoodieTableMetaClient metaClient) {
    return HoodieWriteConfig.newBuilder()
        .forTable(metaClient.getTableConfig().getTableName())
        .withPath(writeOptions.get(HudiEngine.HUDI_BASE_PATH))
        .withRollbackUsingMarkers(true)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
  }
  
  private void doHiveSync(Map<String, String> writeOptions, JavaSparkContext javaSparkContext,
      HoodieTableMetaClient metaClient) {
    Properties properties = new Properties();
    for (Map.Entry<String, String> entry : writeOptions.entrySet()) {
      properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
    }
    properties.setProperty("hoodie.allow.empty.commit", "true");
    properties.setProperty("hoodie.datasource.meta.sync.base.path", writeOptions.get(HudiEngine.HUDI_BASE_PATH));
    HiveSyncTool hiveSyncTool = new HiveSyncTool(properties, javaSparkContext.hadoopConfiguration(),
        Option.of(metaClient));
    hiveSyncTool.syncHoodieTable();
  }
}
