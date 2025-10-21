/*
 *  Copyright (c) 2025. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.StorageConnector.BigqueryConnector;
import com.logicalclocks.hsfs.StorageConnector.RdsConnector;
import com.logicalclocks.hsfs.StorageConnector.RedshiftConnector;
import com.logicalclocks.hsfs.StorageConnector.S3Connector;
import com.logicalclocks.hsfs.StorageConnector.SnowflakeConnector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;


class TestDataSource {
  @Test
  void testUpdateStorageConnectorRedshift() {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    RedshiftConnector sc = new RedshiftConnector();
    sc.setStorageConnectorType(StorageConnectorType.REDSHIFT);

    // Act
    ds.updateStorageConnector(sc);

    // Assert
    assertEquals(ds.getDatabase(), sc.getDatabaseName());
    assertEquals(ds.getGroup(), sc.getDatabaseGroup());
    assertEquals(ds.getTable(), sc.getTableName());
  }

  @Test
  void testUpdateStorageConnectorSnowflake() {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    SnowflakeConnector sc = new SnowflakeConnector();
    sc.setStorageConnectorType(StorageConnectorType.SNOWFLAKE);

    // Act
    ds.updateStorageConnector(sc);

    // Assert
    assertEquals(ds.getDatabase(), sc.getDatabase());
    assertEquals(ds.getGroup(), sc.getSchema());
    assertEquals(ds.getTable(), sc.getTable());
  }

  @Test
  void testUpdateStorageConnectorBigQuery() {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    BigqueryConnector sc = new BigqueryConnector();
    sc.setStorageConnectorType(StorageConnectorType.BIGQUERY);

    // Act
    ds.updateStorageConnector(sc);

    // Assert
    assertEquals(ds.getDatabase(), sc.getQueryProject());
    assertEquals(ds.getGroup(), sc.getDataset());
    assertEquals(ds.getTable(), sc.getQueryTable());
  }

  @Test
  void testUpdateStorageConnectorRds() {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    RdsConnector sc = new RdsConnector();
    sc.setStorageConnectorType(StorageConnectorType.RDS);

    // Act
    ds.updateStorageConnector(sc);

    // Assert
    assertEquals(ds.getDatabase(), sc.getDatabase());
  }

  @Test
  void testUpdateStorageConnectorOther() throws JsonProcessingException {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    S3Connector sc = new S3Connector();
    sc.setStorageConnectorType(StorageConnectorType.S3);

    ObjectMapper mapper = new ObjectMapper();
    String scBefore = mapper.writeValueAsString(sc);

    // Act
    ds.updateStorageConnector(sc);

    // Assert
    assertEquals(scBefore, mapper.writeValueAsString(sc));
  }

  @Test
  void testUpdateStorageConnectorNone() {
    // Arrange
    DataSource ds = new DataSource();
    ds.setDatabase("test_database");
    ds.setGroup("test_group");
    ds.setTable("test_table");

    // Act & Assert
    ds.updateStorageConnector(null);
  }
}
