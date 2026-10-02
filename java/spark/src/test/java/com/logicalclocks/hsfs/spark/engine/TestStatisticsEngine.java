/*
 *  Copyright (c) 2026. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class TestStatisticsEngine {

  @AfterEach
  void resetSparkEngine() {
    SparkEngine.setInstance(null);
  }

  @Test
  @SuppressWarnings("unchecked")
  void histogramsAndCorrelationsReachTheProfilerUnswapped() {
    SparkEngine sparkEngine = Mockito.mock(SparkEngine.class);
    SparkEngine.setInstance(sparkEngine);
    Dataset<Row> split = Mockito.mock(Dataset.class);
    Mockito.when(split.isEmpty()).thenReturn(false);
    Mockito.when(sparkEngine.profile(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn("{\"columns\": []}");
    TrainingDataset trainingDataset = new TrainingDataset();
    StatisticsConfig config = new StatisticsConfig();
    config.setHistograms(true);
    config.setCorrelations(false);
    trainingDataset.setStatisticsConfig(config);

    new StatisticsEngine(EntityEndpointType.TRAINING_DATASET)
        .computeSplitStatistics(trainingDataset, Collections.singletonMap("train", split));

    // SparkEngine.profile takes (correlation, histogram)
    Mockito.verify(sparkEngine).profile(Mockito.eq(split), Mockito.any(), Mockito.eq(false), Mockito.eq(true),
        Mockito.eq(false));
  }
}
