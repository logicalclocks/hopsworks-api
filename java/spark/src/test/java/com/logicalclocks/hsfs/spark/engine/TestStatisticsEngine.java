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
import com.logicalclocks.hsfs.metadata.Variable;
import com.logicalclocks.hsfs.metadata.VariablesApi;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

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

  private static VariablesApi variables(String value) throws Exception {
    VariablesApi variablesApi = Mockito.mock(VariablesApi.class);
    Variable variable = new Variable();
    variable.setValue(value);
    Mockito.when(variablesApi.get(StatisticsEngine.INCREMENTAL_STATISTICS_VARIABLE))
        .thenReturn(value == null ? Optional.empty() : Optional.of(variable));
    return variablesApi;
  }

  @Test
  void incrementalStatisticsFollowTheClusterSetting() throws Exception {
    Assertions.assertTrue(new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, variables("true"))
        .incrementalStatisticsEnabled());
    Assertions.assertFalse(new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, variables("false"))
        .incrementalStatisticsEnabled());
    // a backend without the setting, or one that refuses it
    Assertions.assertFalse(new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, variables(null))
        .incrementalStatisticsEnabled());
    VariablesApi failing = Mockito.mock(VariablesApi.class);
    Mockito.when(failing.get(Mockito.any())).thenThrow(new IOException("forbidden"));
    Assertions.assertFalse(new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, failing)
        .incrementalStatisticsEnabled());
  }
}
