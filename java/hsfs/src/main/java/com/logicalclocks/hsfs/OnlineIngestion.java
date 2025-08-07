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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logicalclocks.hsfs.metadata.RestDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class OnlineIngestion extends RestDto<OnlineIngestion> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(OnlineIngestion.class);

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private Long numEntries;

  @Getter
  @Setter
  private List<OnlineIngestionResult> results;

  @Getter
  @Setter
  private FeatureGroupBase featureGroup;

  public OnlineIngestion(Long numEntries) {
    this.numEntries = numEntries;
  }

  public void refresh() throws FeatureStoreException, IOException {
    OnlineIngestion onlineIngestion = featureGroup.getOnlineIngestion(id);

    // Method to copy data from another object
    this.id = onlineIngestion.id;
    this.numEntries = onlineIngestion.numEntries;
    this.results = onlineIngestion.results;
    this.featureGroup = onlineIngestion.featureGroup;
  }

  public void waitForCompletion(int timeout, int period)
      throws InterruptedException, FeatureStoreException, IOException {
    long startTime = System.currentTimeMillis();

    // Convert to milliseconds
    timeout = timeout * 1000;
    period = period * 1000;

    while (true) {
      // Get total number of rows processed
      long rowsProcessed = results.stream().collect(Collectors.summarizingLong(o -> o.getRows())).getSum();

      // Check if the online ingestion is complete
      if (numEntries != null && rowsProcessed >= numEntries) {
        break;
      }

      // Check if the timeout has been reached (if timeout is 0 we will wait indefinitely)
      if (timeout != 0 && System.currentTimeMillis() - startTime > timeout) {
        LOGGER.warn("Timeout of " + timeout
            + " was exceeded while waiting for online ingestion completion.");
        break;
      }
      
      // Sleep for the specified period in seconds
      Thread.sleep(period);

      refresh();
    }
  }

}