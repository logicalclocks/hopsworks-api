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

package com.logicalclocks.hsfs.beam;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.JobConfiguration;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.beam.constructor.Query;
import org.apache.beam.sdk.values.PCollection;

import com.logicalclocks.hsfs.DataSource;
import com.logicalclocks.hsfs.Feature;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.OnlineConfig;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.beam.engine.BeamProducer;
import com.logicalclocks.hsfs.beam.engine.FeatureGroupEngine;

import lombok.Builder;
import lombok.NonNull;

public class StreamFeatureGroup extends FeatureGroupBase<PCollection<Object>> {


  protected FeatureGroupEngine featureGroupEngine = new FeatureGroupEngine();

  @Builder
  public StreamFeatureGroup(FeatureStore featureStore, @NonNull String name, Integer version, String description,
      List<String> primaryKeys, List<String> partitionKeys, String hudiPrecombineKey,
      boolean onlineEnabled, TimeTravelFormat timeTravelFormat, List<Feature> features,
      StatisticsConfig statisticsConfig, String onlineTopicName, String eventTime,
      OnlineConfig onlineConfig, StorageConnector storageConnector, String path) {
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
    this.eventTime = eventTime;
    this.onlineConfig = onlineConfig;
    this.storageConnector = storageConnector;
    this.dataSource.setPath(path);
  }

  public StreamFeatureGroup() {
    this.type = "streamFeatureGroupDTO";
    this.dataSource = new DataSource();
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
   * Save the feature group metadata on Hopsworks.
   * This method is idempotent, if the feature group already exists the method does nothing.
   *
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save() throws FeatureStoreException, IOException {
    save(null, null);
  }

  /**
   * Save the feature group metadata on Hopsworks.
   * This method is idempotent, if the feature group already exists, the method does nothing
   *
   * @param writeOptions Options to provide to the materialization job
   * @param materializationJobConfiguration Resource configuration for the materialization job
   * @throws FeatureStoreException
   * @throws IOException
   */
  public void save(Map<String, String> writeOptions, JobConfiguration materializationJobConfiguration)
      throws FeatureStoreException, IOException {
    featureGroupEngine.save(this, partitionKeys, hudiPrecombineKey,
        writeOptions, materializationJobConfiguration);
  }

  /**
   * Ingest a feature data to the online feature store using Beam Pipeline object. Currently,
   * only org.apache.beam.sdk.values.Row types as feature data type are supported.
   *
   * <pre>
   * {@code
   *        // get feature store handle
   *        FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
   *
   *        // get feature group handle
   *        StreamFeatureGroup fg = fs.getStreamFeatureGroup("taxi_ride", 1);
   *
   *        // create Beam pipeline
   *        Pipeline pipeline = Pipeline.create();
   *        pipeline
   *         .apply("read stream from the source", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
   *         .apply("Parse JSON to Beam Rows", JsonToRow.withSchema(schema))
   *         .apply("insert streaming feature data", fg.insertStream());
   * }
   * </pre>
   *
   * @return BeamProducer object, that can be wrapped inside Beam Pipeline `apply` method.
   */
  public BeamProducer insertStream() throws Exception {
    return featureGroupEngine.insertStream(this, null);
  }

  public BeamProducer insertStream(Map<String, String> writeOptions) throws Exception {
    return featureGroupEngine.insertStream(this, writeOptions);
  }

  @Override
  public Object insertStream(PCollection<Object> featureData) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  @Override
  public Object insertStream(PCollection<Object> featureData, Map<String, String> writeOptions) throws Exception {
    throw new UnsupportedOperationException("Not supported for Beam");
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature meta data objects.
   * @return Query object.
   */
  public Query selectFeatures(List<Feature> features) {
    return new Query(this, features);
  }

  /**
   * Select a subset of features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @param features List of Feature names.
   * @return Query object.
   */
  public Query select(List<String> features) {
    // Create a feature object for each string feature given by the user.
    // For the query building each feature need only the name set.
    List<Feature> featureObjList = features.stream().map(Feature::new).collect(Collectors.toList());
    return selectFeatures(featureObjList);
  }

  /**
   * Select all features of the feature group and return a query object. The query can be used to construct
   * joins of feature groups or create a feature view with a subset of features of the feature group.
   * @return Query object.
   */
  public Query selectAll() {
    return new Query(this, getFeatures());
  }

  /**
   * Select all features including primary key and event time feature of the feature group except provided `features`
   * and return a query object.
   * The query can be used to construct joins of feature groups or create a feature view with a subset of features of
   * the feature group.
   * @param features List of Feature meta data objects.
   * @return Query object.
   */
  public Query selectExceptFeatures(List<Feature> features) {
    List<String> exceptFeatures = features.stream().map(Feature::getName).collect(Collectors.toList());
    return selectExcept(exceptFeatures);
  }

  /**
   * Select all features including primary key and event time feature of the feature group except provided `features`
   * and return a query object.
   * The query can be used to construct joins of feature groups or create a feature view with a subset of features of
   * the feature group.
   * @param features List of Feature names.
   * @return Query object.
   */
  public Query selectExcept(List<String> features) {
    return new Query(this,
        getFeatures().stream().filter(f -> !features.contains(f.getName())).collect(Collectors.toList()));
  }
}
