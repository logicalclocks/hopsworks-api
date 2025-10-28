/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark;

import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.DataSource;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.StorageConnectorType;
import com.logicalclocks.hsfs.StorageConnector.S3Connector;
import com.logicalclocks.hsfs.metadata.HopsworksClient;
import com.logicalclocks.hsfs.metadata.HopsworksHttpClient;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.util.Constants;

import com.logicalclocks.hsfs.StorageConnector;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;

import com.logicalclocks.hsfs.spark.util.StorageConnectorUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.parquet.Strings;
import org.apache.spark.SparkContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStorageConnector {

  @Nested
  class BigQuery {
    @Test
    public void testBigQueryCredentialsBase64Encoded(@TempDir Path tempDir) throws IOException, FeatureStoreException {
      // Arrange
      String credentials = "{\"type\": \"service_account\", \"project_id\": \"test\"}";
      Path credentialsFile = tempDir.resolve("bigquery.json");
      Files.write(credentialsFile, credentials.getBytes());

      StorageConnector.BigqueryConnector bigqueryConnector = new StorageConnector.BigqueryConnector();
      if (SystemUtils.IS_OS_WINDOWS) {
        bigqueryConnector.setKeyPath("file:///" + credentialsFile.toString().replace( "\\", "/" ));
      } else {
        bigqueryConnector.setKeyPath("file://" + credentialsFile);
      }

      HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
      hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

      // Act
      // Base64 encode the credentials file
      String localKeyPath = SparkEngine.getInstance().addFile(bigqueryConnector.getKeyPath());
      String fileContent = Base64.getEncoder().encodeToString(Files.readAllBytes(Paths.get(localKeyPath)));

      // Assert
      Assertions.assertEquals(credentials,
        new String(Base64.getDecoder().decode(fileContent), StandardCharsets.UTF_8));
    }

    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.BigqueryConnector bigqueryConnector = new StorageConnector.BigqueryConnector();
      bigqueryConnector.setKeyPath("file://tmp/test.json");
      bigqueryConnector.setParentProject("test");

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      Mockito.when(sparkEngine.addFile(Mockito.any())).thenReturn("test.json");
      SparkEngine.setInstance(sparkEngine);
      
      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      String query = "select * from dbtable";
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery(query);
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = new HashMap<>();
      expectedOptions.put(Constants.BIGQ_PARENT_PROJECT, dataSource.getDatabase());
      expectedOptions.put("credentials", "SGVsbG8=");
      expectedOptions.put(Constants.BIGQ_DATASET, dataSource.getGroup());

      // Act
      try (MockedStatic<Files> filesMock = mockStatic(Files.class)) {
        filesMock.when(() -> Files.readAllBytes(Mockito.any()))
            .thenReturn(new byte[] { 72, 101, 108, 108, 111 });
        storageConnectorUtils.read(bigqueryConnector, dataSource, new HashMap<String, String>());
      }

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(Constants.BIGQUERY_FORMAT, formatArg.getValue());
      Assertions.assertEquals(expectedOptions, mapArg.getValue());
      Assertions.assertEquals(query, pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class Snowflake {
    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.SnowflakeConnector snowflakeConnector = new StorageConnector.SnowflakeConnector();

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);

      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      String query = "select * from dbtable";
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery(query);
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = new HashMap<>();
      expectedOptions.put("query", query);
      expectedOptions.put(Constants.SNOWFLAKE_URL, null);
      expectedOptions.put(Constants.SNOWFLAKE_TOKEN, null);
      expectedOptions.put(Constants.SNOWFLAKE_DB, dataSource.getDatabase());
      expectedOptions.put(Constants.SNOWFLAKE_SCHEMA, dataSource.getGroup());
      expectedOptions.put(Constants.SNOWFLAKE_AUTH, "oauth");
      expectedOptions.put(Constants.SNOWFLAKE_USER, null);

      // Act
      storageConnectorUtils.read(snowflakeConnector, dataSource, null);

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(formatArg.getValue(), Constants.SNOWFLAKE_FORMAT);
      Assertions.assertEquals(mapArg.getValue(), expectedOptions);
      Assertions.assertEquals(null, pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class Rds {
    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.RdsConnector connector = new StorageConnector.RdsConnector();

      StorageConnector.RdsConnector spyConnector = spy(connector);
      doNothing().when(spyConnector).update();

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);

      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      String query = "select * from dbtable";
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery(query);
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = new HashMap<>();
      expectedOptions.put("query", query);
      expectedOptions.put("url", "jdbc:postgresql://null:null/test_database");
      expectedOptions.put("driver", "org.postgresql.Driver");
      expectedOptions.put("user", null);
      expectedOptions.put("password", null);

      // Act
      storageConnectorUtils.read(spyConnector, dataSource, null);

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(formatArg.getValue(), Constants.JDBC_FORMAT);
      Assertions.assertEquals(mapArg.getValue(), expectedOptions);
      Assertions.assertEquals(null, pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class Jdbc {
    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.JdbcConnector connector = new StorageConnector.JdbcConnector();

      StorageConnector.JdbcConnector spyConnector = spy(connector);
      doNothing().when(spyConnector).update();

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);

      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      String query = "select * from dbtable";
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery(query);
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = new HashMap<>();
      expectedOptions.put("query", query);
      expectedOptions.put("url", null);

      // Act
      storageConnectorUtils.read(spyConnector, dataSource, null);

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(formatArg.getValue(), Constants.JDBC_FORMAT);
      Assertions.assertEquals(mapArg.getValue(), expectedOptions);
      Assertions.assertEquals(null, pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class Redshift {
    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.RedshiftConnector connector = new StorageConnector.RedshiftConnector();

      StorageConnector.RedshiftConnector spyConnector = spy(connector);
      doNothing().when(spyConnector).update();

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);

      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      String query = "select * from dbtable";
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery(query);
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = new HashMap<>();
      expectedOptions.put("query", query);
      expectedOptions.put("url", "jdbc:redshift://null.null:null/test_database");
      expectedOptions.put("user", null);
      expectedOptions.put("password", null);
      expectedOptions.put("driver", null);

      // Act
      storageConnectorUtils.read(spyConnector, dataSource, null);

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(formatArg.getValue(), Constants.JDBC_FORMAT);
      Assertions.assertEquals(mapArg.getValue(), expectedOptions);
      Assertions.assertEquals(null, pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class HopsFs {
    @Test
    public void test_read() throws Exception {
      // Arrange
      StorageConnector.HopsFsConnector connector = new StorageConnector.HopsFsConnector();

      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);

      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      ArgumentCaptor<String> formatArg = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<Map> mapArg = ArgumentCaptor.forClass(Map.class);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery("test_query");
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      Map<String, String> expectedOptions = null;

      // Act
      storageConnectorUtils.read(connector, dataSource, DataFormat.CSV.name(), null);

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), formatArg.capture(), mapArg.capture(), pathArg.capture());
      Assertions.assertEquals(formatArg.getValue(), DataFormat.CSV.name());
      Assertions.assertEquals(mapArg.getValue(), expectedOptions);
      Assertions.assertEquals(dataSource.getPath(), pathArg.getValue());
      // clear static instance
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class Gcs {
    StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
    StorageConnector.GcsConnector gcsConnector = new StorageConnector.GcsConnector();
    @BeforeEach
    public void setup(
      @TempDir
      Path tempDir) throws IOException {
      String credentials =
        "{\"type\": \"service_account\", \"project_id\": \"test\", \"private_key_id\": \"123456\", " +
          "\"private_key\": \"-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----\", " +
          "\"client_email\": \"test@project.iam.gserviceaccount.com\"}";
      Path credentialsFile = tempDir.resolve("gcs.json");
      Files.write(credentialsFile, credentials.getBytes());

      if (SystemUtils.IS_OS_WINDOWS) {
        gcsConnector.setKeyPath("file:///" + credentialsFile.toString().replace("\\", "/"));
      } else {
        gcsConnector.setKeyPath("file://" + credentialsFile);
      }
      gcsConnector.setStorageConnectorType(StorageConnectorType.GCS);
      gcsConnector.setBucket("bucket");
    }

    @Test
    public void testGcsConnectorCredentials() throws IOException, FeatureStoreException {
      // Arrange
      HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
      hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

      // Act
      SparkEngine.getInstance().setupConnectorHadoopConf(gcsConnector);
      SparkContext sc = SparkEngine.getInstance().getSparkSession().sparkContext();

      // Assert
      Assertions.assertEquals(
        "test@project.iam.gserviceaccount.com",
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_EMAIL));
      Assertions.assertEquals(
        "123456", sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY_ID));
      Assertions.assertEquals("-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY));
      Assertions.assertEquals(Constants.PROPERTY_GCS_FS_VALUE,
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_FS_KEY));
      Assertions.assertEquals("true", sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_ENABLE));
      Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_KEY)));
      Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_HASH)));
      Assertions.assertTrue(Strings.isNullOrEmpty(sc.hadoopConfiguration().get(Constants.PROPERTY_ALGORITHM)));
    }

    @Test
    public void testGcsConnectorCredentials_encrypted() throws IOException, FeatureStoreException {
      // Arrange
      gcsConnector.setAlgorithm("AES256");
      gcsConnector.setEncryptionKey("encryptionkey");
      gcsConnector.setEncryptionKeyHash("encryptionkeyhash");

      HopsworksClient hopsworksClient = Mockito.mock(HopsworksClient.class);
      hopsworksClient.setInstance(new HopsworksClient(Mockito.mock(HopsworksHttpClient.class), "host"));

      // Act
      SparkEngine.getInstance().setupConnectorHadoopConf(gcsConnector);
      SparkContext sc = SparkEngine.getInstance().getSparkSession().sparkContext();
      // Assert
      Assertions.assertEquals("test@project.iam.gserviceaccount.com",
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_EMAIL));
      Assertions.assertEquals("123456", sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY_ID));
      Assertions.assertEquals("-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_KEY));
      Assertions.assertEquals(Constants.PROPERTY_GCS_FS_VALUE,
        sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_FS_KEY));
      Assertions.assertEquals("true", sc.hadoopConfiguration().get(Constants.PROPERTY_GCS_ACCOUNT_ENABLE));
      Assertions.assertEquals("encryptionkey", sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_KEY));
      Assertions.assertEquals("encryptionkeyhash", sc.hadoopConfiguration().get(Constants.PROPERTY_ENCRYPTION_HASH));
      Assertions.assertEquals("AES256", sc.hadoopConfiguration().get(Constants.PROPERTY_ALGORITHM));
    }

    @Test
    void testDefaultPathGcs() throws FeatureStoreException, IOException {
      // Arrange
      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);

      DataSource dataSource = new DataSource();
      dataSource.setQuery("test_query");
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      // Act
      storageConnectorUtils.read(gcsConnector, dataSource, "csv", new HashMap<String, String>());
      Mockito.verify(sparkEngine).read(Mockito.any(), Mockito.any(), Mockito.any(), pathArg.capture());

      // Assert
      Assertions.assertEquals("gs://bucket/test_path", pathArg.getValue());
      SparkEngine.setInstance(null);
    }
  }

  @Nested
  class S3 {
    StorageConnector.S3Connector s3Connector = new StorageConnector.S3Connector();

    @BeforeEach
    public void setup() {
      s3Connector.setBucket("test-bucket");
      s3Connector.setName("testName");
      s3Connector.setStorageConnectorType(StorageConnectorType.S3);
      List<Option> arguments = new java.util.ArrayList<>();
      arguments.add(new Option("fs.s3a.endpoint", "testEndpoint"));
      arguments.add(new Option("fs.s3a.path.style.access", "false"));
      arguments.add(new Option("fs.s3a.connection.ssl.enabled", "true"));
      s3Connector.setArguments(arguments);
      s3Connector.setAccessKey("testAccessKey");
      s3Connector.setSecretKey("testSecretKey");
      s3Connector.setServerEncryptionAlgorithm("AES256");
      s3Connector.setServerEncryptionKey("testEncryptionKey");
      s3Connector.setSessionToken("testSessionToken");
    }

    @Test
    void testS3HadoopConf() throws IOException, FeatureStoreException {
      // Act
      SparkEngine.getInstance().setupConnectorHadoopConf(s3Connector);
      SparkContext sc = SparkEngine.getInstance().getSparkSession().sparkContext();
      // Assert
      Assertions.assertEquals("testEndpoint", sc.hadoopConfiguration().get(Constants.S3_ENDPOINT));
      Assertions.assertEquals("testAccessKey", sc.hadoopConfiguration().get(Constants.S3_ACCESS_KEY_ENV));
      Assertions.assertEquals("testSecretKey", sc.hadoopConfiguration().get(Constants.S3_SECRET_KEY_ENV));
      Assertions.assertEquals("AES256", sc.hadoopConfiguration().get(Constants.S3_ENCRYPTION_ALGO));
      Assertions.assertEquals("testEncryptionKey", sc.hadoopConfiguration().get(Constants.S3_ENCRYPTION_KEY));
      Assertions.assertEquals("testSessionToken", sc.hadoopConfiguration().get(Constants.S3_SESSION_KEY_ENV));
      Assertions.assertEquals(Constants.S3_TEMPORARY_CREDENTIAL_PROVIDER,
        sc.hadoopConfiguration().get(Constants.S3_CREDENTIAL_PROVIDER_ENV));
    }

    @Test
    void testDefaultPathS3() throws FeatureStoreException, IOException {
      // Arrange
      StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
      SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);

      StorageConnector.S3Connector spyConnector = spy(s3Connector);
      doNothing().when(spyConnector).update();
      
      DataSource dataSource = new DataSource();
      dataSource.setQuery("test_query");
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      // act
      storageConnectorUtils.read(spyConnector, dataSource, "csv", new HashMap<String, String>());

      // Assert
      Mockito.verify(sparkEngine).read(Mockito.any(), Mockito.any(), Mockito.any(), pathArg.capture());
      Assertions.assertEquals("s3://" + s3Connector.getBucket() + "/test_path", pathArg.getValue());
      // reset
      SparkEngine.setInstance(null);
    }

    @Test
    void testGetPath() throws FeatureStoreException, IOException {
      // Arrange
      S3Connector connector = new S3Connector();
      connector.setBucket("testBucket");

      // Act
      String path = connector.getPath("some/location");

      // Assert
      Assertions.assertEquals("s3://testBucket/some/location", path);
    }

    @Test
    void testGetPathStorageConnectorWithPath() throws FeatureStoreException, IOException {
      // Arrange
      S3Connector connector = new S3Connector();
      connector.setBucket("testBucket");
      connector.setPath("abc/def");

      // Act
      String path = connector.getPath("some/location");

      // Assert
      Assertions.assertEquals("s3://testBucket/abc/def/some/location", path);
    }
  }

  @Nested
  class Adls {
    StorageConnector.AdlsConnector adlsConnector = new StorageConnector.AdlsConnector();
    StorageConnectorUtils storageConnectorUtils = new StorageConnectorUtils();
    SparkEngine sparkEngine;

    @BeforeEach
    public void setup() {
      adlsConnector.setName("test");
      adlsConnector.setAccountName("test_acccount");
      adlsConnector.setContainerName("test_container");
      adlsConnector.setGeneration(2);
    }

    @Test
    void testDefaultPathAdls() throws FeatureStoreException, IOException {
      // Arrange
      sparkEngine = Mockito.mock(SparkEngine.class);
      SparkEngine.setInstance(sparkEngine);
      ArgumentCaptor<String> pathArg = ArgumentCaptor.forClass(String.class);

      DataSource dataSource = new DataSource();
      dataSource.setQuery("test_query");
      dataSource.setDatabase("test_database");
      dataSource.setGroup("test_group");
      dataSource.setGroup("test_table");
      dataSource.setPath("test_path");

      // Act
      storageConnectorUtils.read(adlsConnector, dataSource, "csv", new HashMap<String, String>());
      Mockito.verify(sparkEngine).read(Mockito.any(), Mockito.any(), Mockito.any(), pathArg.capture());
      // Assert
      Assertions.assertEquals("abfss://test_container@test_acccount.dfs.core.windows.net/test_path", pathArg.getValue());
      SparkEngine.setInstance(null);
    }
  }
}
