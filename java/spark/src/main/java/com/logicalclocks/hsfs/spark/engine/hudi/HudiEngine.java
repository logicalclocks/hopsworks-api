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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.parquet.Strings;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureGroupCommit;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.HudiOperationType;
import com.logicalclocks.hsfs.constructor.FeatureGroupAlias;
import com.logicalclocks.hsfs.engine.FeatureGroupUtils;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureStore;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;

import scala.collection.Seq;

public class HudiEngine {
  private static Logger LOGGER = Logger.getLogger(HudiEngine.class.getName());
  public static final String HUDI_SPARK_FORMAT = "org.apache.hudi";

  protected static final String HUDI_TABLE_VERSION = "hoodie.table.version";
  protected static final String HUDI_BASE_PATH = "hoodie.base.path";
  protected static final String HUDI_TABLE_NAME = "hoodie.table.name";
  protected static final String HUDI_TABLE_TYPE = "hoodie.datasource.write.table.type";
  protected static final String HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type";
  protected static final String HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation";
  protected static final String HUDI_METADATA_ENABLE = "hoodie.metadata.enable";
  
  protected static final String HUDI_TABLE_RECORD_KEY_FIELD = "hoodie.table.recordkey.fields";
  protected static final String HUDI_TABLE_PARTITION_KEY_FIELDS = "hoodie.table.partition.fields";
  protected static final String HUDI_TABLE_KEY_GENERATOR_CLASS = "hoodie.table.keygenerator.class";
  protected static final String HUDI_TABLE_PRECOMBINE_FIELD = "hoodie.table.precombine.field";
  protected static final String HUDI_TABLE_BASE_FILE_FORMAT = "hoodie.table.base.file.format";
  protected static final String HUDI_TABLE_METADATA_PARTITIONS = "hoodie.table.metadata.partitions";

  protected static final String HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class";
  protected static final String HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator";
  protected static final String HUDI_WRITE_RECORD_KEY = "hoodie.datasource.write.recordkey.field";
  protected static final String HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field";
  protected static final String HUDI_WRITE_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field";

  protected static final String HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable";
  protected static final String HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table";
  protected static final String HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database";
  protected static final String HUDI_HIVE_SYNC_MODE =
      "hoodie.datasource.hive_sync.mode";
  protected static final String HUDI_HIVE_SYNC_MODE_VAL = "hms";
  protected static final String HUDI_HIVE_SYNC_PARTITION_FIELDS =
      "hoodie.datasource.hive_sync.partition_fields";
  protected static final String HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY =
      "hoodie.datasource.hive_sync.partition_extractor_class";
  private static final String HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP =
      "hoodie.datasource.hive_sync.support_timestamp";
  protected static final String DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
      "org.apache.hudi.hive.MultiPartKeysValueExtractor";
  protected static final String HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL =
      "org.apache.hudi.hive.NonPartitionedExtractor";
  protected static final String HIVE_AUTO_CREATE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.auto_create_database";
  protected static final String HIVE_AUTO_CREATE_DATABASE_OPT_VAL = "false";

  protected static final String HUDI_COPY_ON_WRITE = "COPY_ON_WRITE";
  protected static final String HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type";
  protected static final String HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental";
  protected static final String HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot";
  protected static final String HUDI_QUERY_TIME_TRAVEL_AS_OF_INSTANT = "as.of.instant";
  protected static final String HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime";
  protected static final String HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime";

  protected static final String HUDI_WRITE_INSERT_DROP_DUPLICATES = "hoodie.datasource.write.insert.drop.duplicates";

  protected static final String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  protected static final String PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload";

  protected static final String HUDI_KAFKA_TOPIC = "hoodie.streamer.source.kafka.topic";
  protected static final String COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix";
  protected static final String STREAMER_CHECKPOINT_KEY_V2 = "streamer.checkpoint.key.v2";
  protected static final String DELTASTREAMER_CHECKPOINT_KEY = "deltastreamer.checkpoint.key";
  protected static final String INITIAL_CHECKPOINT_STRING = "initialCheckPointString";
  protected static final String FEATURE_GROUP_SCHEMA = "com.logicalclocks.hsfs.spark.StreamFeatureGroup.avroSchema";
  protected static final String FEATURE_GROUP_ENCODED_SCHEMA =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.encodedAvroSchema";
  protected static final String FEATURE_GROUP_COMPLEX_FEATURES =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.complexFeatures";
  protected static final String KAFKA_SOURCE = "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerKafkaSource";
  protected static final String SCHEMA_PROVIDER =
      "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerSchemaProvider";
  protected static final String DELTA_STREAMER_TRANSFORMER =
      "com.logicalclocks.hsfs.spark.engine.hudi.DeltaStreamerTransformer";
  protected static final String DELTA_SOURCE_ORDERING_FIELD_OPT_KEY = "sourceOrderingField";

  protected static final String MIN_SYNC_INTERVAL_SECONDS = "minSyncIntervalSeconds";
  protected static final String SPARK_MASTER = "yarn";
  protected static final String PROJECT_ID = "projectId";
  protected static final String FEATURE_STORE_NAME = "featureStoreName";
  protected static final String SUBJECT_ID = "subjectId";
  protected static final String FEATURE_GROUP_ID = "featureGroupId";
  protected static final String FEATURE_GROUP_NAME = "featureGroupName";
  protected static final String FEATURE_GROUP_VERSION = "featureGroupVersion";
  protected static final String FUNCTION_TYPE = "functionType";
  protected static final String STREAMING_QUERY = "streamingQuery";

  private static final Map<String, String> HUDI_DEFAULT_PARALLELISM = new HashMap<String, String>() {
    {
      put("hoodie.bulkinsert.shuffle.parallelism", "5");
      put("hoodie.insert.shuffle.parallelism", "5");
      put("hoodie.upsert.shuffle.parallelism", "5");
    }
  };


  private FeatureGroupUtils utils = new FeatureGroupUtils();
  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();
  private FeatureGroupCommit fgCommitMetadata = new FeatureGroupCommit();
  private DeltaStreamerConfig deltaStreamerConfig = new DeltaStreamerConfig();

  protected static HudiEngine INSTANCE = null;

  public static synchronized HudiEngine getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new HudiEngine();
    }
    return INSTANCE;
  }

  // To make sure everyone uses getInstance
  private HudiEngine() {
  }

  public void saveHudiFeatureGroup(SparkSession sparkSession, FeatureGroupBase featureGroup,
                                   Dataset<Row> dataset, HudiOperationType operation,
                                   Map<String, String> writeOptions, Integer validationId)
      throws IOException, FeatureStoreException, ParseException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, operation, writeOptions);

    dataset.write()
        .format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(SaveMode.Append)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());
    if (fgCommit != null) {
      fgCommit.setValidationId(validationId);
      featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
    }
  }

  public FeatureGroupCommit deleteRecord(SparkSession sparkSession, FeatureGroupBase featureGroup,
                                             Dataset<Row> deleteDF, Map<String, String> writeOptions)
      throws IOException, FeatureStoreException,
      ParseException {

    Map<String, String> hudiArgs = setupHudiWriteOpts(featureGroup, HudiOperationType.UPSERT, writeOptions);
    hudiArgs.put(PAYLOAD_CLASS_OPT_KEY, PAYLOAD_CLASS_OPT_VAL);

    deleteDF.write().format(HUDI_SPARK_FORMAT)
        .options(hudiArgs)
        .mode(SaveMode.Append)
        .save(featureGroup.getLocation());

    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, featureGroup.getLocation());
    if (fgCommit != null) {
      FeatureGroupCommit apiFgCommit = featureGroupApi.featureGroupCommit(featureGroup, fgCommit);
      apiFgCommit.setCommitID(apiFgCommit.getCommitID());
      return apiFgCommit;
    } else {
      throw new FeatureStoreException("No commit information was found for this feature group");
    }
  }

  private HoodieTimeline getHoodieTimeline(SparkSession sparkSession, String basePath)
      throws IOException {
    FileSystem hopsfsConf = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
    return HoodieDataSourceHelpers.allCompletedCommitsCompactions(hopsfsConf, basePath);
  }

  private HoodieCommitMetadata getCommitMetadata(HoodieTimeline commitTimeline, HoodieInstant instant)
      throws IOException {
    byte[] commitsToReturn = commitTimeline.getInstantDetails(instant).get();
    return new CommitMetadataSerDeV2().deserialize(instant,
        new ByteArrayInputStream(commitsToReturn), () -> false, HoodieCommitMetadata.class);
  }
  
  private FeatureGroupCommit getLastCommitMetadata(SparkSession sparkSession, String basePath)
      throws IOException, FeatureStoreException, ParseException {
    HoodieTimeline commitTimeline = getHoodieTimeline(sparkSession, basePath);
    Option<HoodieInstant> lastInstant = commitTimeline.lastInstant();
    if (lastInstant.isPresent()) {
      fgCommitMetadata.setCommitDateString(lastInstant.get().getCompletionTime());
      fgCommitMetadata.setCommitTime(
          FeatureGroupUtils.getTimeStampFromDateString(lastInstant.get().getCompletionTime()));
      fgCommitMetadata.setLastActiveCommitTime(
          FeatureGroupUtils.getTimeStampFromDateString(commitTimeline.firstInstant().get().getCompletionTime())
      );

      HoodieCommitMetadata commitMetadata = getCommitMetadata(commitTimeline, lastInstant.get());
      fgCommitMetadata.setRowsUpdated(commitMetadata.fetchTotalUpdateRecordsWritten());
      fgCommitMetadata.setRowsInserted(commitMetadata.fetchTotalInsertRecordsWritten());
      fgCommitMetadata.setRowsDeleted(commitMetadata.getTotalRecordsDeleted());
      fgCommitMetadata.setTableSize(getHudiTableSize(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
          basePath));
      return fgCommitMetadata;
    } else {
      return null;
    }
  }
  
  /**
   * Extract the topic name from the last commit's checkpoint.
   * The checkpoint format is "topicName,partition:offset,partition:offset,...".
   * Returns null if no checkpoint exists or the format is unexpected.
   */
  private String getCheckpointTopic(HoodieTimeline commitTimeline) {
    try {
      Option<HoodieInstant> lastInstant = commitTimeline.lastInstant();
      if (!lastInstant.isPresent()) {
        return null;
      }
      HoodieCommitMetadata commitMetadata = getCommitMetadata(commitTimeline, lastInstant.get());
      Map<String, String> extraMetadata = commitMetadata.getExtraMetadata();
      String checkpoint = extraMetadata.get(STREAMER_CHECKPOINT_KEY_V2);
      if (checkpoint == null) {
        checkpoint = extraMetadata.get(DELTASTREAMER_CHECKPOINT_KEY);
      }
      if (checkpoint == null || checkpoint.isEmpty()) {
        return null;
      }
      // Topic name is the part before the first comma
      int commaIdx = checkpoint.indexOf(',');
      return commaIdx > 0 ? checkpoint.substring(0, commaIdx) : null;
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Failed to read checkpoint topic from commit metadata", e);
      return null;
    }
  }

  /**
   * Build a checkpoint string with all partitions set to their earliest available offset.
   * Format: "topicName,0:offset,1:offset,2:offset,..."
   */
  private String buildResetCheckpoint(String topic, Map<String, String> writeOptions) {
    Properties kafkaProps = new Properties();
    // writeOptions keys are Spark-formatted with "kafka." prefix (e.g. "kafka.bootstrap.servers").
    // Strip the prefix to get standard Kafka consumer property names.
    writeOptions.entrySet().stream()
        .filter(e -> e.getKey().startsWith("kafka."))
        .forEach(e -> kafkaProps.put(e.getKey().substring("kafka.".length()), e.getValue()));
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProps)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      List<org.apache.kafka.common.TopicPartition> topicPartitions = partitions.stream()
          .map(p -> new org.apache.kafka.common.TopicPartition(topic, p.partition()))
          .collect(Collectors.toList());
      Map<org.apache.kafka.common.TopicPartition, Long> beginningOffsets =
          consumer.beginningOffsets(topicPartitions);
      StringBuilder sb = new StringBuilder(topic);
      for (org.apache.kafka.common.TopicPartition tp : topicPartitions) {
        sb.append(",").append(tp.partition()).append(":").append(beginningOffsets.get(tp));
      }
      return sb.toString();
    }
  }

  private String getPrimaryColumns(FeatureGroupBase featureGroup) {
    String primaryColumns = utils.getPrimaryColumns(featureGroup).mkString(",");
    if (!Strings.isNullOrEmpty(featureGroup.getEventTime())) {
      primaryColumns = primaryColumns + "," + featureGroup.getEventTime();
    }
    return primaryColumns;
  }

  private Map<String, String> setupHudiWriteOpts(FeatureGroupBase featureGroup, HudiOperationType operation,
                                                 Map<String, String> writeOptions)
      throws FeatureStoreException {
    Map<String, String> hudiArgs = new HashMap<String, String>();

    hudiArgs.put(HUDI_TABLE_TYPE, HUDI_COPY_ON_WRITE);
    hudiArgs.put(HUDI_TABLE_STORAGE_TYPE, HUDI_COPY_ON_WRITE);

    hudiArgs.put(HUDI_KEY_GENERATOR_OPT_KEY, HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);
    hudiArgs.put(HUDI_TABLE_KEY_GENERATOR_CLASS, HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL);
    
    String primaryColumns = getPrimaryColumns(featureGroup);
    hudiArgs.put(HUDI_WRITE_RECORD_KEY, primaryColumns);
    hudiArgs.put(HUDI_TABLE_RECORD_KEY_FIELD, primaryColumns);

    // table name
    String tableName = utils.getFgName(featureGroup);
    hudiArgs.put(HUDI_TABLE_NAME, tableName);

    // partition keys
    Seq<String> partitionColumns = utils.getPartitionColumns(featureGroup);
    if (!partitionColumns.isEmpty()) {
      String partitionPath = partitionColumns.mkString(":SIMPLE,") + ":SIMPLE";
      hudiArgs.put(HUDI_TABLE_PARTITION_KEY_FIELDS, partitionPath);
      hudiArgs.put(HUDI_PARTITION_FIELD, partitionPath);
      hudiArgs.put(HUDI_HIVE_SYNC_PARTITION_FIELDS, partitionColumns.mkString(","));
      hudiArgs.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL);
    } else {
      hudiArgs.put(HUDI_PARTITION_FIELD, "");
      hudiArgs.put(HUDI_TABLE_PARTITION_KEY_FIELDS, "");
      hudiArgs.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL);
    }

    List<Feature> features = featureGroup.getFeatures();
    String precombineKey = features.stream().filter(Feature::getHudiPrecombineKey).findFirst()
        .orElseThrow(() -> new FeatureStoreException("Can't find hudi precombine key")).getName();

    hudiArgs.put(HUDI_WRITE_PRECOMBINE_FIELD, precombineKey);
    hudiArgs.put(HUDI_TABLE_PRECOMBINE_FIELD, precombineKey);
    hudiArgs.put(HUDI_TABLE_BASE_FILE_FORMAT, "PARQUET");
    //hudiArgs.put(HUDI_TABLE_METADATA_PARTITIONS, "column_stats,files");

    // Hive args
    hudiArgs.put(HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(HUDI_HIVE_SYNC_MODE, HUDI_HIVE_SYNC_MODE_VAL);
    hudiArgs.put(HUDI_HIVE_SYNC_TABLE, tableName);
    hudiArgs.put(HUDI_HIVE_SYNC_DB, featureGroup.getFeatureStore().getName());
    hudiArgs.put(HIVE_AUTO_CREATE_DATABASE_OPT_KEY, HIVE_AUTO_CREATE_DATABASE_OPT_VAL);
    hudiArgs.put(HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP, "true");
    if (operation != null) {
      hudiArgs.put(HUDI_TABLE_OPERATION, operation.getValue());
    }
    hudiArgs.putAll(HUDI_DEFAULT_PARALLELISM);
    
    hudiArgs.put(HUDI_METADATA_ENABLE, "true");

    // Overwrite with user provided options if any
    if (writeOptions != null && !writeOptions.isEmpty()) {
      hudiArgs.putAll(writeOptions);
    }

    return hudiArgs;
  }

  public Map<String, String> setupHudiReadOpts(Long startTimestamp, Long endTimestamp,
                                                Map<String, String> readOptions) {
    Map<String, String> hudiArgs = new HashMap<>();
    if (endTimestamp == null && (startTimestamp == null || startTimestamp == 0)) {
      // snapshot query latest state
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
    } else if (endTimestamp != null && (startTimestamp == null || startTimestamp == 0)) {
      // snapshot query with end time
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL);
      hudiArgs.put(HUDI_QUERY_TIME_TRAVEL_AS_OF_INSTANT, utils.timeStampToHudiFormat(endTimestamp));
    } else if (endTimestamp == null && startTimestamp != null) {
      // incremental query with start time until now
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(startTimestamp));
    } else {
      // incremental query with start and end time
      hudiArgs.put(HUDI_QUERY_TYPE_OPT_KEY, HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL);
      hudiArgs.put(HUDI_BEGIN_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(startTimestamp));
      hudiArgs.put(HUDI_END_INSTANTTIME_OPT_KEY, utils.timeStampToHudiFormat(endTimestamp));
    }

    // Overwrite with user provided options if any
    if (readOptions != null && !readOptions.isEmpty()) {
      hudiArgs.putAll(readOptions);
    }
    return hudiArgs;
  }

  private void createEmptyTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup)
      throws IOException, FeatureStoreException {
    Properties properties = new Properties();
    properties.putAll(setupHudiWriteOpts(streamFeatureGroup, null, null));
    properties.setProperty(HoodieTableConfig.NAME.key(), utils.getFgName(streamFeatureGroup));
    LOGGER.log(Level.INFO, "Creating empty table with properties: " + properties);
    Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
    HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .setTableType(HUDI_COPY_ON_WRITE)
        .setTableName(utils.getFgName(streamFeatureGroup))
        .initTable(HadoopFSUtils.getStorageConfWithCopy(configuration), streamFeatureGroup.getLocation());
    
    // Bootstrap the metadata table
    HoodieWriteConfig hudiWriteConfig = HoodieWriteConfig.newBuilder()
        .withProperties(properties)
        .withPath(streamFeatureGroup.getLocation())
        .forTable(utils.getFgName(streamFeatureGroup))
        .build();
    
    HoodieWriteConfig hoodieWriteConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(hudiWriteConfig,
        HoodieFailedWritesCleaningPolicy.EAGER);
    
    HoodieTableMetaClient.newTableBuilder()
        .fromProperties(hoodieWriteConfig.getProps())
        .setTableName(hoodieWriteConfig.getTableName())
        .setTableType(hoodieWriteConfig.getTableType())
        .initTable(HadoopFSUtils.getStorageConfWithCopy(configuration), hoodieWriteConfig.getBasePath());
  }

  public void reconcileHudiSchema(SparkSession sparkSession,
                                  FeatureGroupAlias featureGroupAlias, Map<String, String> hudiArgs)
          throws FeatureStoreException {
    String fgTableName = utils.getTableName(featureGroupAlias.getFeatureGroup());
    String[] hudiSchema = sparkSession.table(featureGroupAlias.getAlias()).columns();
    String[] hiveSchema = sparkSession.table(fgTableName).columns();
    if (!sparkSchemasMatch(hudiSchema, hiveSchema)) {
      Dataset dataframe = sparkSession.table(fgTableName).limit(0);

      FeatureStore featureStore = (FeatureStore) featureGroupAlias.getFeatureGroup().getFeatureStore();

      try {
        FeatureGroup fullFG = featureStore.getFeatureGroup(
                featureGroupAlias.getFeatureGroup().getName(),
                featureGroupAlias.getFeatureGroup().getVersion());
        saveHudiFeatureGroup(sparkSession, fullFG, dataframe,
                  HudiOperationType.UPSERT, new HashMap<>(), null);
      } catch (IOException | ParseException e) {
        throw new FeatureStoreException("Error while reconciling HUDI schema.", e);
      }

      sparkSession.read()
              .format(HudiEngine.HUDI_SPARK_FORMAT)
              .options(hudiArgs)
              .load(featureGroupAlias.getFeatureGroup().getLocation())
              .createOrReplaceTempView(featureGroupAlias.getAlias());
    }
  }

  public boolean sparkSchemasMatch(String[] schema1, String[] schema2) {
    if (schema1 == null || schema2 == null) {
      return false;
    }
    if (schema1.length != schema2.length) {
      return false;
    }

    Arrays.sort(schema1);
    Arrays.sort(schema2);

    for (int i = 0; i < schema1.length; i++) {
      if (!schema1[i].equals(schema2[i])) {
        return false;
      }
    }
    return true;
  }

  public void streamToHoodieTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup,
                                  Map<String, String> writeOptions) throws Exception {
    Map<String, String> hudiWriteOpts = setupHudiWriteOpts(streamFeatureGroup, HudiOperationType.UPSERT,
        writeOptions);
    hudiWriteOpts.put(PROJECT_ID, String.valueOf(streamFeatureGroup.getFeatureStore().getProjectId()));
    hudiWriteOpts.put(FEATURE_STORE_NAME, streamFeatureGroup.getFeatureStore().getName());
    hudiWriteOpts.put(SUBJECT_ID, String.valueOf(streamFeatureGroup.getSubject().getId()));
    hudiWriteOpts.put(FEATURE_GROUP_ID, String.valueOf(streamFeatureGroup.getId()));
    hudiWriteOpts.put(FEATURE_GROUP_NAME, streamFeatureGroup.getName());
    hudiWriteOpts.put(FEATURE_GROUP_VERSION, String.valueOf(streamFeatureGroup.getVersion()));
    hudiWriteOpts.put(HUDI_TABLE_NAME, utils.getFgName(streamFeatureGroup));
    hudiWriteOpts.put(HUDI_BASE_PATH, streamFeatureGroup.getLocation());
    hudiWriteOpts.put(HUDI_KAFKA_TOPIC, streamFeatureGroup.getOnlineTopicName());
    hudiWriteOpts.put(FEATURE_GROUP_SCHEMA, streamFeatureGroup.getAvroSchema());
    hudiWriteOpts.put(FEATURE_GROUP_ENCODED_SCHEMA, streamFeatureGroup.getEncodedAvroSchema());
    hudiWriteOpts.put(FEATURE_GROUP_COMPLEX_FEATURES,
        new JSONArray(streamFeatureGroup.getComplexFeatures()).toString());
    hudiWriteOpts.put(DELTA_SOURCE_ORDERING_FIELD_OPT_KEY,
        hudiWriteOpts.get(HUDI_WRITE_PRECOMBINE_FIELD));

    // set consumer group id
    hudiWriteOpts.put(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(streamFeatureGroup.getId()));

    // check if table was initiated and if not initiate
    Path basePath = new Path(streamFeatureGroup.getLocation());
    FileSystem fs = basePath.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
    if (!fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
      createEmptyTable(sparkSession, streamFeatureGroup);
      // set "kafka.auto.offset.reset": "earliest"
      hudiWriteOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    // it is possible that table was generated from empty topic
    HoodieTimeline commitTimeline = getHoodieTimeline(sparkSession, streamFeatureGroup.getLocation());
    if (commitTimeline.empty()) {
      // set "kafka.auto.offset.reset": "earliest"
      hudiWriteOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    } else {
      if (!writeOptions.containsKey(HudiEngine.INITIAL_CHECKPOINT_STRING)) {
        // Detect topic change: if the checkpoint references a different topic, reset offsets
        String currentTopic = streamFeatureGroup.getOnlineTopicName();
        String checkpointTopic = getCheckpointTopic(commitTimeline);
        if (checkpointTopic != null && !checkpointTopic.equals(currentTopic)) {
          LOGGER.warning("Kafka topic changed from '" + checkpointTopic + "' to '" + currentTopic
              + "'. Resetting checkpoint to read from earliest offset of new topic.");
          hudiWriteOpts.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
          hudiWriteOpts.put(INITIAL_CHECKPOINT_STRING, buildResetCheckpoint(currentTopic, hudiWriteOpts));
        }
      }
    }
    
    // Testing...
    hudiWriteOpts.put("hoodie.streamer.source.kafka.append.offsets", "true");

    deltaStreamerConfig.streamToHoodieTable(hudiWriteOpts, sparkSession);
    FeatureGroupCommit fgCommit = getLastCommitMetadata(sparkSession, streamFeatureGroup.getLocation());
    if (fgCommit != null) {
      featureGroupApi.featureGroupCommit(streamFeatureGroup, fgCommit);
      streamFeatureGroup.computeStatistics();
    }
  }
  
  // Extracted from org.apache.hudi.utilities.TableSizeStats
  public Long getHudiTableSize(JavaSparkContext jsc, String basePath) throws IOException {
    LOGGER.info("Calculating Hudi table size for base path: " + basePath);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration());
    HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
        engineContext, new HoodieHadoopStorage(basePath, storageConf), metadataConfig, basePath);
    List<String> allPartitions = tableMetadata.getAllPartitionPaths();
    final Histogram tableHistogram = new Histogram(new UniformReservoir(1_000_000));
    HoodieTableMetaClient metaClientLocal = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(storageConf.newInstance()).build();
    HoodieMetadataConfig metadataConfig1 = HoodieMetadataConfig.newBuilder()
        .enable(false)
        .build();
    allPartitions.forEach(partition -> {
      HoodieTableFileSystemView fileSystemView = FileSystemViewManager
          .createInMemoryFileSystemView(new HoodieLocalEngineContext(storageConf), metaClientLocal, metadataConfig1);
      List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList());
      baseFiles.forEach(baseFile -> {
        tableHistogram.update(baseFile.getFileSize());
      });
    });
    Long totalSize = Arrays.stream(tableHistogram.getSnapshot().getValues()).sum();
    LOGGER.info("Total size of Hudi table at base path " + basePath + " is: " + totalSize + " bytes");
    return totalSize;
  }
}
