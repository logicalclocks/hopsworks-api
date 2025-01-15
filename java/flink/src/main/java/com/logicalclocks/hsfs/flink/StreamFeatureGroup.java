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

package com.logicalclocks.hsfs.flink;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.OnlineConfig;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.flink.engine.FeatureGroupEngine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;

@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamFeatureGroup extends FeatureGroupBase<DataStream<?>> {

  protected FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
                            List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
                            boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
                            StatisticsConfig statisticsConfig, String onlineTopicName, String topicName,
                            String notificationTopicName, String eventTime, OnlineConfig onlineConfig,
                            StorageConnector storageConnector, String path) {
    this();
    this.featureStore = featureStore;
    this.name = name;
    this.version = version;
    this.description = description;
    this.primaryKeys = primaryKeys != null
      ? primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.partitionKeys = partitionKeys != null
      ? partitionKeys.stream().map(String::toLowerCase).collect(Collectors.toList()) : null;
    this.hudiPrecombineKey = hudiPrecombineKey != null ? hudiPrecombineKey.toLowerCase() : null;
    this.onlineEnabled = onlineEnabled;
    this.timeTravelFormat = timeTravelFormat != null ? timeTravelFormat : TimeTravelFormat.HUDI;
    this.features = features;
    this.statisticsConfig = statisticsConfig != null ? statisticsConfig : new StatisticsConfig();
    this.onlineTopicName = onlineTopicName;
    this.topicName = topicName;
    this.notificationTopicName = notificationTopicName;
    this.eventTime = eventTime;
    this.onlineConfig = onlineConfig;
    this.storageConnector = storageConnector;
    this.path = path;
  }

  public StreamFeatureGroup() {
    this.type = "streamFeatureGroupDTO";
  }

  // used for updates
  public StreamFeatureGroup(Integer id, String description, List<Feature> features) {
    this();
    this.id = id;
    this.description = description;
    this.features = features;
  }

  public StreamFeatureGroup(FeatureStore featureStore, int id) {
    this();
    this.featureStore = featureStore;
    this.id = id;
  }

  /**
   * Ingest a feature data to the online feature store using Flink DataStream API. Currently, only POJO
   * types as feature data type are supported.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *
   *        // get feature group handle
   *        StreamFeatureGroup fg = fs.getStreamFeatureGroup("card_transactions", 1);
   *
   *        // read stream from the source and aggregate stream
   *        DataStream<TransactionAgg> aggregationStream =
   *          env.fromSource(transactionSource, customWatermark, "Transaction Kafka Source")
   *          .keyBy(r -> r.getCcNum())
   *          .window(SlidingEventTimeWindows.of(Time.minutes(windowLength), Time.minutes(1)))
   *          .aggregate(new TransactionCountAggregate());
   *
   *        // insert streaming feature data
   *        fg.insertStream(featureData);
   * }
   * </pre>
   *
   * @param featureData Features in Streaming Dataframe to be saved.
   * @return DataStreamSink object.
   */
  @Override
  public DataStreamSink<?> insertStream(DataStream<?> featureData) throws Exception {
    return featureGroupEngine.insertStream(this, featureData, null);
  }

  @Override
  public DataStreamSink<?>  insertStream(DataStream<?> featureData, Map<String, String> writeOptions) throws Exception {
    return featureGroupEngine.insertStream(this, featureData, writeOptions);
  }
}
