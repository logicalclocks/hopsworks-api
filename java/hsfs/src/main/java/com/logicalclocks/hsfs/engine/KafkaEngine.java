/*
 *  Copyright (c) 2024. Hopsworks AB
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

package com.logicalclocks.hsfs.engine;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.metadata.HopsworksInternalClient;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;

public class KafkaEngine {

  Logger logger = Logger.getLogger(KafkaEngine.class.getName());

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();
  private final EngineBase engineBase;

  public enum ConfigType {
    SPARK,
    KAFKA
  }

  public KafkaEngine(EngineBase engineBase) {
    this.engineBase = engineBase;
  }

  public Map<String, String> getKafkaConfig(FeatureGroupBase featureGroup, Map<String, String> writeOptions,
        ConfigType configType)
      throws FeatureStoreException, IOException {
    boolean external = !(System.getProperties().containsKey(HopsworksInternalClient.REST_ENDPOINT_SYS)
        || (writeOptions != null
        && Boolean.parseBoolean(writeOptions.getOrDefault("internal_kafka", "false"))));

    StorageConnector.KafkaConnector storageConnector =
        storageConnectorApi.getKafkaStorageConnector(featureGroup.getFeatureStore(), external);
    storageConnector.setSslTruststoreLocation(engineBase.addFile(storageConnector.getSslTruststoreLocation()));
    storageConnector.setSslKeystoreLocation(engineBase.addFile(storageConnector.getSslKeystoreLocation()));

    Map<String, String> config;
    if (ConfigType.SPARK.equals(configType)) {
      config = storageConnector.sparkOptions();
    } else {
      config = storageConnector.kafkaOptions();
    }

    if (writeOptions != null) {
      config.putAll(writeOptions);
    }
    config.put("enable.idempotence", "false");
    return config;
  }

  public String kafkaGetOffsets(FeatureGroupBase featureGroup, Map<String, String> writeOptions, boolean high)
      throws FeatureStoreException, IOException {
    Properties properties = new Properties();
    properties.putAll(getKafkaConfig(featureGroup, writeOptions, ConfigType.KAFKA));

    if (!properties.contains("group.id")) {
      properties.put("group.id", "hsfs_consumer_group");
    }

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singletonList(featureGroup.getOnlineTopicName()));

    Set<TopicPartition> assignedPartitions = consumer.assignment();

    Map<TopicPartition, Long> offsets;
    if (high) {
      offsets = consumer.endOffsets(assignedPartitions);
    } else {
      offsets = consumer.beginningOffsets(assignedPartitions);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(featureGroup.getOnlineTopicName());
    for (Entry<TopicPartition, Long> entry: offsets.entrySet()) {
      sb.append("," + entry.getKey().partition() + ":" + entry.getValue());
    }
    consumer.close();

    return sb.toString();
  }
}
