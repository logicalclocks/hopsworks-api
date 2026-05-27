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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.logicalclocks.hsfs.StorageConnector.BigqueryConnector;
import com.logicalclocks.hsfs.StorageConnector.SqlConnector;
import com.logicalclocks.hsfs.StorageConnector.RedshiftConnector;
import com.logicalclocks.hsfs.StorageConnector.SnowflakeConnector;
import com.logicalclocks.hsfs.metadata.RestDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class DataSource extends RestDto<DataSource> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(DataSource.class);

  @Getter
  @Setter
  private String query = "";

  @Getter
  @Setter
  private String database = "";

  @Getter
  @Setter
  private String group = "";

  @Getter
  @Setter
  private String table = "";

  @Getter
  @Setter
  private String path = "";

  @Getter
  @Setter
  private StorageConnector storageConnector = null;

  public void updateStorageConnector(StorageConnector storageConnector) {
    if (storageConnector == null) {
      return;
    }
    switch (storageConnector.getStorageConnectorType()) {
      case REDSHIFT:
        RedshiftConnector redshiftConnector = (RedshiftConnector) storageConnector;
        if (!Strings.isNullOrEmpty(database)) {
          redshiftConnector.setDatabaseName(database);
        }
        if (!Strings.isNullOrEmpty(group)) {
          redshiftConnector.setDatabaseGroup(group);
        }
        if (!Strings.isNullOrEmpty(table)) {
          redshiftConnector.setTableName(table);
        }
        break;
      case SNOWFLAKE:
        SnowflakeConnector snowflakeConnector = (SnowflakeConnector) storageConnector;
        if (!Strings.isNullOrEmpty(database)) {
          snowflakeConnector.setDatabase(database);
        }
        if (!Strings.isNullOrEmpty(group)) {
          snowflakeConnector.setSchema(group);
        }
        if (!Strings.isNullOrEmpty(table)) {
          snowflakeConnector.setTable(table);
        }
        break;
      case BIGQUERY:
        BigqueryConnector bigqueryConnector = (BigqueryConnector) storageConnector;
        if (!Strings.isNullOrEmpty(database)) {
          bigqueryConnector.setQueryProject(database);
        }
        if (!Strings.isNullOrEmpty(group)) {
          bigqueryConnector.setDataset(group);
        }
        if (!Strings.isNullOrEmpty(table)) {
          bigqueryConnector.setQueryTable(table);
        }
        break;
      case SQL:
        SqlConnector sqlConnector = (SqlConnector) storageConnector;
        if (!Strings.isNullOrEmpty(database)) {
          sqlConnector.setDatabase(database);
        }
        break;
      default:
        break;
    }
  }
}
