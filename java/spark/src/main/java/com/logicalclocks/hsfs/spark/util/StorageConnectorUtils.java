/*
 *  Copyright (c) 2023-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.util;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.DataSource;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import com.logicalclocks.hsfs.util.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

public class StorageConnectorUtils {
  Logger logger = Logger.getLogger(StorageConnectorUtils.class.getName());

  /**
   * Reads path into a spark dataframe using the HopsFsConnector.
   *
   * @param connector HopsFsConnector object.
   * @param dataSource Data source object.
   * @param dataFormat specify the file format to be read, e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.HopsFsConnector connector, DataSource dataSource,
                           String dataFormat, Map<String, String> options)
      throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(connector, dataFormat, options, dataSource.getPath());
  }

  /**
   * Reads path into a spark dataframe using the S3Connector.
   *
   * @param connector S3Connector object.
   * @param dataSource Data source object.
   * @param dataFormat specify the file format to be read, e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.S3Connector connector, DataSource dataSource, String dataFormat,
                           Map<String, String> options) throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions(dataSource);
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }
    String path = dataSource.getPath();
    if (path != null && !path.startsWith("s3://")) {
      path = connector.getPath(path);
      logger.info(String.format("Prepending default bucket specified on connector, final path: %s", path));
    }
    return SparkEngine.getInstance().read(connector, dataFormat, readOptions, path);
  }

  /**
   * Reads query into a spark dataframe using the RedshiftConnector.
   *
   * @param connector Storage connector object.
   * @param dataSource Data source object.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.RedshiftConnector connector, DataSource dataSource,
      Map<String, String> options) throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions(dataSource);
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }
    String query = dataSource.getQuery();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.JDBC_FORMAT, readOptions, null);
  }

  /**
   * Reads path into a spark dataframe using the AdlsConnector.
   *
   * @param connector AdlsConnector object.
   * @param dataSource Data source object.
   * @param dataFormat specify the file format to be read, e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.AdlsConnector connector, DataSource dataSource,
      String dataFormat, Map<String, String> options) throws FeatureStoreException, IOException {
    String path = dataSource.getPath();
    if (path != null && (!path.startsWith("abfss://") || !path.startsWith("adl://"))) {
      path = connector.getPath(path);
      logger.info(String.format("Using default container specified on connector, final path: %s", path));
    }
    return SparkEngine.getInstance().read(connector, dataFormat, options, path);
  }

  /**
   * Reads query into a spark dataframe using the SnowflakeConnector.
   *
   * @param connector SnowflakeConnector object.
   * @param dataSource Data source object.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.SnowflakeConnector connector, DataSource dataSource,
      Map<String, String> options) throws FeatureStoreException, IOException {
    Map<String, String> readOptions = connector.sparkOptions(dataSource);
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }
    String query = dataSource.getQuery();
    if (!Strings.isNullOrEmpty(query)) {
      // if table also specified we override to use query
      readOptions.remove(Constants.SNOWFLAKE_TABLE);
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.SNOWFLAKE_FORMAT, readOptions, null);
  }

  /**
   * Reads query into a spark dataframe using the JdbcConnector.
   *
   * @param connector JdbcConnector object.
   * @param dataSource Data source object.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.JdbcConnector connector, DataSource dataSource,
      Map<String, String> options) throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions(dataSource);
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }
    String query = dataSource.getQuery();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.JDBC_FORMAT, readOptions, null);
  }

  /**
   * Reads a path into a spark dataframe using the GcsConnector.
   *
   * @param connector GcsConnector object.
   * @param dataSource Data source object.
   * @param dataFormat Specify the file format to be read, e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.GcsConnector connector, DataSource dataSource, String dataFormat,
      Map<String, String> options) throws FeatureStoreException, IOException {
    String path = dataSource.getPath();
    if (path != null && !path.startsWith("gs://")) {
      path = connector.getPath(path);
      logger.info(String.format("Prepending default bucket specified on connector, final path: %s", path));
    }
    return SparkEngine.getInstance().read(connector, dataFormat, options, path);
  }

  /**
   * Reads a query or a path into a spark dataframe using the sBigqueryConnector.
   *
   * @param connector BigqueryConnector object.
   * @param dataSource Data source object.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.BigqueryConnector connector, DataSource dataSource,
      Map<String, String> options) throws FeatureStoreException, IOException {

    Map<String, String> readOptions = connector.sparkOptions(dataSource);

    // Base64 encode the credentials file
    String localKeyPath = SparkEngine.getInstance().addFile(connector.getKeyPath());
    byte[] fileContent = Files.readAllBytes(Paths.get(localKeyPath));
    options.put(Constants.BIGQ_CREDENTIALS, Base64.getEncoder().encodeToString(fileContent));

    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }

    String query = dataSource.getQuery();
    String path = dataSource.getPath();
    if (!Strings.isNullOrEmpty(query)) {
      path = query;
    } else if (!Strings.isNullOrEmpty(dataSource.getTable())) {
      path = dataSource.getTable();
    } else if (!Strings.isNullOrEmpty(connector.getQueryTable())) {
      path = connector.getQueryTable();
    } else if (!Strings.isNullOrEmpty(path)) {
      path = path;
    } else {
      throw new IllegalArgumentException("Either query should be provided"
          + " or Query Project,Dataset and Table should be set");
    }

    return SparkEngine.getInstance().read(connector, Constants.BIGQUERY_FORMAT, readOptions, path);
  }

  /**
   * Reads a query into a spark dataframe using the SqlConnector.
   *
   * @param connector SqlConnector object.
   * @param dataSource Data source object.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector.SqlConnector connector, DataSource dataSource,
      Map<String, String> options) throws FeatureStoreException, IOException {
    connector.update();
    Map<String, String> readOptions = connector.sparkOptions(dataSource);
    // merge user spark options on top of default spark options
    if (options != null && !options.isEmpty()) {
      readOptions.putAll(options);
    }
    String query = dataSource.getQuery();
    if (!Strings.isNullOrEmpty(query)) {
      readOptions.put("query", query);
    }
    return SparkEngine.getInstance().read(connector, Constants.JDBC_FORMAT, readOptions, null);
  }

  /**
   * Reads a query or a path into a spark dataframe using the storage connector.
   *
   * @param connector Storage connector object.
   * @param dataSource Data source object.
   * @param dataFormat When reading from object stores such as S3, HopsFS and ADLS, specify the file format to be read,
   *                  e.g. `csv`, `parquet`.
   * @param options Any additional key/value options to be passed to the connector.
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> read(StorageConnector connector, DataSource dataSource, String dataFormat,
      Map<String, String> options) throws FeatureStoreException, IOException {
    if (connector instanceof StorageConnector.HopsFsConnector) {
      return read((StorageConnector.HopsFsConnector) connector, dataSource, dataFormat, options);
    } else if (connector instanceof  StorageConnector.S3Connector) {
      return read((StorageConnector.S3Connector) connector, dataSource, dataFormat, options);
    } else if (connector instanceof StorageConnector.RedshiftConnector) {
      return read((StorageConnector.RedshiftConnector) connector, dataSource, options);
    } else if (connector instanceof StorageConnector.AdlsConnector) {
      return read((StorageConnector.AdlsConnector) connector, dataSource, dataFormat, options);
    } else if (connector instanceof StorageConnector.SnowflakeConnector) {
      return read((StorageConnector.SnowflakeConnector) connector, dataSource, options);
    } else if (connector instanceof StorageConnector.JdbcConnector) {
      return read((StorageConnector.JdbcConnector) connector, dataSource, options);
    } else if (connector instanceof StorageConnector.GcsConnector) {
      return read((StorageConnector.GcsConnector) connector, dataSource, dataFormat, options);
    } else if (connector instanceof StorageConnector.BigqueryConnector) {
      return read((StorageConnector.BigqueryConnector) connector, dataSource, options);
    } else if (connector instanceof StorageConnector.SqlConnector) {
      return read((StorageConnector.SqlConnector) connector, dataSource, options);
    } else if (connector instanceof StorageConnector.KafkaConnector) {
      throw new NotSupportedException("Reading a Kafka Stream into a static Spark Dataframe is not supported.");
    } else {
      throw new FeatureStoreException("Unknown type of StorageConnector.");
    }
  }

  /**
   * Reads stream into a spark dataframe using the kafka storage connector.
   *
   * @param connector Storage connector object.
   * @param topic name of the topic.
   * @param topicPattern if provided will subscribe topics that match provided pattern.
   * @param messageFormat format of the message. "avro" or "json".
   * @param schema schema of the message
   * @param options Any additional key/value options to be passed to the connector.
   * @param includeMetadata whether to include metadata of the topic in the dataframe, such as "key", "topic",
   *                        "partition", offset", "timestamp", "timestampType", "value.*".
   * @return Spark dataframe.
   * @throws FeatureStoreException If unable to retrieve StorageConnector from the feature store.
   * @throws IOException Generic IO exception.
   */
  public Dataset<Row> readStream(StorageConnector.KafkaConnector connector, String topic, boolean topicPattern,
                                 String messageFormat, String schema, Map<String, String> options,
                                 boolean includeMetadata) throws FeatureStoreException, IOException {
    if (!Arrays.asList("avro", "json", null).contains(messageFormat.toLowerCase())) {
      throw new IllegalArgumentException("Can only read JSON and AVRO encoded records from Kafka.");
    }

    if (topicPattern) {
      options.put("subscribePattern", topic);
    } else {
      options.put("subscribe", topic);
    }

    return SparkEngine.getInstance().readStream(connector, connector.sparkFormat,
        messageFormat.toLowerCase(), schema, options, includeMetadata);
  }


}
