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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OnlineConfig {

  @Getter
  @Setter
  private List<String> onlineComments;

  @Getter
  @Setter
  private String tableSpace;

  @Getter
  @Setter
  private PrimaryKeyIndexType primaryKeyIndexType;

  @Getter
  private List<List<String>> secondaryIndexes;

  public OnlineConfig(List<String> onlineComments, String tableSpace) {
    this.onlineComments = onlineComments;
    this.tableSpace = tableSpace;
  }

  public OnlineConfig(List<String> onlineComments, String tableSpace, PrimaryKeyIndexType primaryKeyIndexType) {
    this.onlineComments = onlineComments;
    this.tableSpace = tableSpace;
    this.primaryKeyIndexType = primaryKeyIndexType;
  }

  // Lower-case the index column names to match the online table column names, which are
  // derived from feature names that the SDK lower-cases (see Feature and StreamFeatureGroup).
  public void setSecondaryIndexes(List<List<String>> secondaryIndexes) {
    this.secondaryIndexes = secondaryIndexes == null ? null
        : secondaryIndexes.stream()
            .map(index -> index == null ? null
                : index.stream().map(String::toLowerCase).collect(Collectors.toList()))
            .collect(Collectors.toList());
  }
}