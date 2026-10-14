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

  private static VariablesApi variables(String enabled, String allowed) throws Exception {
    VariablesApi variablesApi = Mockito.mock(VariablesApi.class);
    Mockito.when(variablesApi.get(Mockito.any())).thenReturn(Optional.empty());
    stub(variablesApi, StatisticsEngine.INCREMENTAL_STATISTICS_VARIABLE, enabled);
    stub(variablesApi, StatisticsEngine.INCREMENTAL_STATISTICS_ALLOWED_VARIABLE, allowed);
    return variablesApi;
  }

  private static void stub(VariablesApi variablesApi, String name, String value) throws Exception {
    if (value == null) {
      return;
    }
    Variable variable = new Variable();
    variable.setValue(value);
    Mockito.when(variablesApi.get(name)).thenReturn(Optional.of(variable));
  }

  private static StatisticsConfig config(Boolean incremental) {
    StatisticsConfig config = new StatisticsConfig();
    config.setIncremental(incremental);
    return config;
  }

  private static boolean effective(VariablesApi variablesApi, Boolean incremental) {
    return new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, variablesApi)
        .incrementalStatisticsEnabled(config(incremental));
  }

  @Test
  void incrementalStatisticsFollowTheFeatureGroupAndTheClusterVariables() throws Exception {
    // unset: the cluster default decides
    Assertions.assertTrue(effective(variables("true", "true"), null));
    Assertions.assertFalse(effective(variables("false", "true"), null));
    // set: the feature group decides
    Assertions.assertTrue(effective(variables("false", "true"), true));
    Assertions.assertFalse(effective(variables("true", "true"), false));
    // the kill switch beats everything
    Assertions.assertFalse(effective(variables("true", "false"), null));
    Assertions.assertFalse(effective(variables("true", "false"), true));
    // a missing allowed variable allows; a missing default is off
    Assertions.assertTrue(effective(variables("true", null), null));
    Assertions.assertTrue(effective(variables(null, null), true));
    Assertions.assertFalse(effective(variables(null, "true"), null));
    // no config at all follows the cluster default
    Assertions.assertTrue(new StatisticsEngine(EntityEndpointType.FEATURE_GROUP, variables("true", "true"))
        .incrementalStatisticsEnabled(null));
  }

  @Test
  void unreadableVariablesAreTolerated() throws Exception {
    VariablesApi failing = Mockito.mock(VariablesApi.class);
    Mockito.when(failing.get(Mockito.any())).thenThrow(new IOException("forbidden"));
    Assertions.assertFalse(effective(failing, null));
    Assertions.assertTrue(effective(failing, true));
  }
}
