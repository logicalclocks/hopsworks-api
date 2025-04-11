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
  private String query;

  @Getter
  @Setter
  private String database;

  @Getter
  @Setter
  private String group;

  @Getter
  @Setter
  private String table;

  @Getter
  @Setter
  private String path;

}