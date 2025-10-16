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

import com.logicalclocks.hsfs.StorageConnector.BigqueryConnector;
import com.logicalclocks.hsfs.StorageConnector.RdsConnector;
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

  public void updateStorageConnector(StorageConnector storage_connector) {
    if (storage_connector == null) {
      return;
    }
    switch (storage_connector.storageConnectorType) {
      case REDSHIFT:
        RedshiftConnector redshiftConnector = (RedshiftConnector) storage_connector;
        redshiftConnector.setDatabaseName(database);
        redshiftConnector.setDatabaseGroup(group);
        redshiftConnector.setTableName(table);
        break;
      case SNOWFLAKE:
        SnowflakeConnector snowflakeConnector = (SnowflakeConnector) storage_connector;
        snowflakeConnector.setDatabase(database);
        snowflakeConnector.setSchema(group);
        snowflakeConnector.setTable(table);
        break;
      case BIGQUERY:
        BigqueryConnector bigqueryConnector = (BigqueryConnector) storage_connector;
        bigqueryConnector.setQueryProject(query);
        bigqueryConnector.setDataset(group);
        bigqueryConnector.setQueryTable(table);
        break;
      case RDS:
        RdsConnector rdsConnector = (RdsConnector) storage_connector;
        rdsConnector.setDatabase(database);
        break;
      default:
        break;
    }
  }
}
