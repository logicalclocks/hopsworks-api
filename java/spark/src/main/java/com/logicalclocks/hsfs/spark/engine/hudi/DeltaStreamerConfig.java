/*
 *  Copyright (c) 2021-2023. Hopsworks AB
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

import com.google.common.base.Strings;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

public class DeltaStreamerConfig implements Serializable {
  
  private static Logger LOG = Logger.getLogger(DeltaStreamerConfig.class.getName());

  private HoodieStreamer.Config deltaStreamerConfig(Map<String, String> writeOptions, SparkSession spark) {
    
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();

    // hsfs path for the target hoodie table.
    cfg.targetBasePath = writeOptions.get(HudiEngine.HUDI_BASE_PATH);
    // name of the target table
    cfg.targetTableName = writeOptions.get(HudiEngine.HUDI_TABLE_NAME);
    cfg.tableType = writeOptions.get(HudiEngine.HUDI_TABLE_STORAGE_TYPE);
    if (Strings.isNullOrEmpty(cfg.tableType)) {
      cfg.tableType = HudiEngine.HUDI_COPY_ON_WRITE;
    }

    // Takes one of these values : UPSERT (default), INSERT
    if (writeOptions.containsKey("operation")
        && EnumUtils.isValidEnum(WriteOperationType.class, writeOptions.get("operation"))) {
      cfg.operation = WriteOperationType.valueOf(writeOptions.get("operation"));
    } else {
      cfg.operation = WriteOperationType.UPSERT;
    }

    if (writeOptions.containsKey(HudiEngine.INITIAL_CHECKPOINT_STRING)) {
      // Resume Delta Streamer from this checkpoint
      cfg.checkpoint = writeOptions.get(HudiEngine.INITIAL_CHECKPOINT_STRING);
    }

    // Enable syncing to hive metastore
    cfg.enableHiveSync = true;

    // Subclass of org.apache.hudi.utilities.sources to read data
    cfg.sourceClassName = HudiEngine.KAFKA_SOURCE;

    // subclass of org.apache.hudi.utilities.schema.SchemaProvider to attach schemas to input & target table data,
    cfg.schemaProviderClassName = HudiEngine.SCHEMA_PROVIDER;

    if (writeOptions.get(HudiEngine.MIN_SYNC_INTERVAL_SECONDS) != null) {
      // the min sync interval of each sync in continuous mode
      cfg.minSyncIntervalSeconds = Integer.parseInt(writeOptions.get(HudiEngine.MIN_SYNC_INTERVAL_SECONDS));
      // Delta Streamer runs in continuous mode running source-fetch -> Transform -> Hudi Write in loop
      cfg.continuousMode = true;
    }

    cfg.sparkMaster = spark.sparkContext().master();
    cfg.sparkAppName = spark.sparkContext().appName();
    

    // A subclass or a list of subclasses of org.apache.hudi.utilities.transform.Transformer. Allows transforming raw
    // source Dataset to a target Dataset (conforming to target schema) before writing. Default : Not set.
    // E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which allows a SQL query templated to be
    // passed as a transformation function). Pass a comma-separated list of subclass names to chain the transformations
    cfg.transformerClassNames = new ArrayList<String>() {{
        add(HudiEngine.DELTA_STREAMER_TRANSFORMER);
      }};
    
    cfg.retryOnSourceFailures = true;

    // Field within source record to decide how to break ties between records with same key in input data.
    cfg.sourceOrderingField =  writeOptions.get(HudiEngine.DELTA_SOURCE_ORDERING_FIELD_OPT_KEY);

    cfg.configs = new ArrayList<String>() {{
        // User provided options
        writeOptions.entrySet().stream().filter(e -> !e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey() + "=" + e.getValue()));
        // Kafka props
        writeOptions.entrySet().stream().filter(e -> e.getKey().startsWith("kafka."))
            .forEach(e -> add(e.getKey().replace("kafka.", "") + "=" + e.getValue()));
      }};
    
    return cfg;
  }

  public void streamToHoodieTable(Map<String, String> writeOptions, SparkSession spark) throws Exception {
    JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

    migrateTable(writeOptions, javaSparkContext);
    
    HoodieStreamer deltaSync = new HoodieStreamer(deltaStreamerConfig(writeOptions, spark), javaSparkContext);
    
    try {
      LOG.info("Starting DeltaStreamer sync with config: " + deltaSync.toString());
      deltaSync.sync();
      LOG.info("DeltaStreamer sync completed successfully");
    } catch (Exception e) {
      LOG.info("Error during DeltaStreamer sync" + e.getMessage() + " " + e.getCause());
      throw e;
    }
  }

  private void migrateTable(Map<String, String> writeOptions, JavaSparkContext javaSparkContext) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(HadoopFSUtils.getStorageConfWithCopy(javaSparkContext.hadoopConfiguration()))
            .setBasePath(writeOptions.get(HudiEngine.HUDI_BASE_PATH))
            .setLoadActiveTimelineOnLoad(false)
            .build();

    // During Hudi upgrades we might need to bump this version. This version matches Hudi 0.12.x
    if (metaClient.getTableConfig().contains(HoodieTableConfig.VERSION)
        && metaClient.getTableConfig().getTableVersion() != HoodieTableVersion.EIGHT) {
      migrateToVersionSix(writeOptions, metaClient, javaSparkContext);
      // We need to update the hoodie.datasource.write.operation option in the metadata table as newer
      // HoodieStreamer versions fail if the value doesn't match with the operation (upsert).
      metaClient.getTableConfig().setValue(HudiEngine.HUDI_TABLE_OPERATION, WriteOperationType.UPSERT.value());
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(),
          metaClient.getTableConfig().getProps());
      
      new UpgradeDowngrade(metaClient, getUpdatedWriteConfig(writeOptions, metaClient),
          new HoodieSparkEngineContext(javaSparkContext),
          SparkUpgradeDowngradeHelper.getInstance())
          .run(HoodieTableVersion.EIGHT, null);
    }
  }
  
  private void migrateToVersionSix(Map<String, String> writeOptions, HoodieTableMetaClient metaClient,
      JavaSparkContext jsc) {
    LOG.info("Migrating Hudi table at " + writeOptions.get(HudiEngine.HUDI_BASE_PATH) + " to version 6");
    metaClient.getTableConfig().setValue(HudiEngine.HUDI_TABLE_OPERATION, WriteOperationType.UPSERT.value());
    metaClient.getTableConfig().setValue("hoodie.table.version", String.valueOf(HoodieTableVersion.SIX));
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
    
    new UpgradeDowngrade(metaClient, getUpdatedWriteConfig(writeOptions, metaClient), new HoodieSparkEngineContext(jsc),
      SparkUpgradeDowngradeHelper.getInstance())
      .run(HoodieTableVersion.SIX, null);
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
}
