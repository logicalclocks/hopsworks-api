/*
 *  Copyright (c) 2020-2023. Hopsworks AB
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
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.metadata.FeatureDescriptiveStatistics;
import com.logicalclocks.hsfs.metadata.SplitStatistics;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.StatisticsApi;
import com.logicalclocks.hsfs.metadata.Variable;
import com.logicalclocks.hsfs.metadata.VariablesApi;

import com.logicalclocks.hsfs.spark.FeatureGroup;
import com.logicalclocks.hsfs.spark.FeatureView;
import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class StatisticsEngine {

  // the cluster default under which statistics jobs merge a commit's rows into the previous snapshot,
  // for feature groups that leave their own setting unset
  static final String INCREMENTAL_STATISTICS_VARIABLE = "statistics_incremental_enabled";
  // the cluster kill switch: when false no feature group merges, whatever it sets
  static final String INCREMENTAL_STATISTICS_ALLOWED_VARIABLE = "statistics_incremental_allowed";

  private StatisticsApi statisticsApi;
  private VariablesApi variablesApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsEngine.class);

  public StatisticsEngine(EntityEndpointType entityType) {
    this(entityType, new VariablesApi());
  }

  StatisticsEngine(EntityEndpointType entityType, VariablesApi variablesApi) {
    this.statisticsApi = new StatisticsApi(entityType);
    this.variablesApi = variablesApi;
  }

  public Statistics computeStatistics(TrainingDataset trainingDataset, Dataset<Row> dataFrame)
      throws IOException, FeatureStoreException {
    Statistics statistics = computeStatistics(dataFrame, trainingDataset.getStatisticsConfig(), null, false);

    return statisticsApi.post(trainingDataset, statistics);
  }

  public Statistics computeStatistics(FeatureView featureView, TrainingDataset trainingDataset,
      Dataset<Row> dataFrame) throws FeatureStoreException, IOException {
    Statistics statistics = computeStatistics(dataFrame, trainingDataset.getStatisticsConfig(), null, false);

    return statisticsApi.post(featureView, trainingDataset.getVersion(), statistics);
  }

  public Statistics computeStatistics(FeatureGroupBase featureGroup, Dataset<Row> dataFrame, Long commitId)
      throws FeatureStoreException, IOException {
    // A time-travel feature group's statistics carry the state the statistics job merges the next commit's rows
    // into when incremental statistics are on; whether a merge applies is decided there.
    boolean mergeableState = mergeable(featureGroup)
        && incrementalStatisticsEnabled(featureGroup.getStatisticsConfig());
    Statistics statistics = computeStatistics(dataFrame, featureGroup.getStatisticsConfig(), commitId,
        mergeableState);

    return statisticsApi.post(featureGroup, statistics);
  }

  private Statistics computeStatistics(Dataset<Row> dataFrame, StatisticsConfig config, Long commitId,
      boolean mergeableState) {
    List<String> statisticColumns = config.getColumns();
    String content;

    if (dataFrame.isEmpty()) {
      if (statisticColumns == null || statisticColumns.isEmpty()) {
        // if no specific columns are specified, use all column names in the dataframe
        statisticColumns = Arrays.asList((String[]) dataFrame.columns());
      }
      LOGGER.warn("There is no data in the entity that you are trying to compute statistics for. A "
          + "possible cause might be that you inserted only data to the online storage of a feature group.");
      content = buildEmptyStatistics(statisticColumns);
    } else {
      // if no empty, compute statistics
      content = SparkEngine.getInstance().profile(dataFrame, statisticColumns, config.getCorrelations(),
          config.getHistograms(), config.getExactUniqueness(), config.getKll(), config.getHistogramBins(),
          mergeableState);
    }

    Collection<FeatureDescriptiveStatistics> featureDescriptiveStatistics = parseDeequStatistics(content);
    
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    return new Statistics(commitTime, 1.0f, featureDescriptiveStatistics, commitId, null);
  }

  // only the feature groups whose commits the statistics job can read between versions ever merge
  private static boolean mergeable(FeatureGroupBase featureGroup) {
    return (featureGroup instanceof FeatureGroup || featureGroup instanceof StreamFeatureGroup)
        && (featureGroup.getTimeTravelFormat() == TimeTravelFormat.HUDI
            || featureGroup.getTimeTravelFormat() == TimeTravelFormat.DELTA);
  }

  /**
   * Whether incremental statistics are on for a feature group: the cluster allows them, and the feature group's
   * own setting decides, falling back to the cluster default when it has none.
   * When the allow switch cannot be read, they are not allowed; when the cluster default cannot be read, it is off.
   */
  public boolean incrementalStatisticsEnabled(StatisticsConfig statisticsConfig) {
    String allowed = readVariable(INCREMENTAL_STATISTICS_ALLOWED_VARIABLE);
    if (!"true".equalsIgnoreCase(allowed)) {
      return false;
    }
    if (statisticsConfig != null && statisticsConfig.getIncremental() != null) {
      return statisticsConfig.getIncremental();
    }
    return "true".equalsIgnoreCase(readVariable(INCREMENTAL_STATISTICS_VARIABLE));
  }

  /** The value of a cluster variable, or null when it is missing or not readable. */
  private String readVariable(String name) {
    try {
      return variablesApi.get(name).map(Variable::getValue).orElse(null);
    } catch (IOException | FeatureStoreException | RuntimeException e) {
      LOGGER.debug("Incremental statistics setting {} not readable", name, e);
      return null;
    }
  }

  public Statistics computeAndSaveSplitStatistics(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    Statistics statistics = computeSplitStatistics(trainingDataset);
    return statisticsApi.post(trainingDataset, statistics);
  }

  public Statistics computeAndSaveSplitStatistics(FeatureView featureView, TrainingDataset trainingDataset,
      Map<String, Dataset<Row>> splitDatasets)
      throws FeatureStoreException, IOException {
    Statistics statistics = computeSplitStatistics(trainingDataset, splitDatasets);
    return statisticsApi.post(featureView, trainingDataset.getVersion(), statistics);
  }

  public Statistics computeSplitStatistics(TrainingDataset trainingDataset)
      throws FeatureStoreException, IOException {
    List<SplitStatistics> splitStatistics = new ArrayList<>();
    for (Split split : trainingDataset.getSplits()) {
      Statistics statistics = computeStatistics(trainingDataset.read(split.getName()),
          trainingDataset.getStatisticsConfig(), null, false);
      splitStatistics.add(new SplitStatistics(split.getName(), statistics.getFeatureDescriptiveStatistics()));
    }
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    return new Statistics(commitTime, 1.0f, splitStatistics);
  }

  public Statistics computeSplitStatistics(TrainingDataset trainingDataset, Map<String, Dataset<Row>> splitDatasets) {
    List<SplitStatistics> splitStatistics = new ArrayList<>();
    for (Map.Entry<String, Dataset<Row>> entry : splitDatasets.entrySet()) {
      Statistics statistics = computeStatistics(entry.getValue(), trainingDataset.getStatisticsConfig(),
          null, false);
      splitStatistics.add(new SplitStatistics(entry.getKey(), statistics.getFeatureDescriptiveStatistics()));
    }
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    return new Statistics(commitTime, 1.0f, splitStatistics);
  }

  public Statistics get(FeatureGroupBase featureGroup, String commitTime) throws FeatureStoreException, IOException {
    return statisticsApi.get(featureGroup, commitTime);
  }

  public Statistics get(TrainingDataset trainingDataset, String commitTime) throws FeatureStoreException, IOException {
    return statisticsApi.get(trainingDataset, commitTime);
  }

  public Statistics getLast(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return statisticsApi.getLast(featureGroup);
  }

  public Statistics getLast(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return statisticsApi.getLast(trainingDataset);
  }

  private Collection<FeatureDescriptiveStatistics> parseDeequStatistics(String content) {
    JSONArray columns = (new JSONObject(content)).getJSONArray("columns");
    List<FeatureDescriptiveStatistics> descStats = new ArrayList<>();
    for (int i = 0; i < columns.length(); i++) {
      JSONObject colStats = (JSONObject) columns.get(i);
      descStats.add(FeatureDescriptiveStatistics.fromDeequStatisticsJson(colStats));
    }
    return descStats;
  }

  private String buildEmptyStatistics(List<String> featureNames) {
    JSONArray columns = new JSONArray();
    for (String name : featureNames) {
      JSONObject colStats = new JSONObject();
      colStats.put("column", name);
      colStats.put("count", 0L);
      columns.put(colStats);
    }
    JSONObject emptyStats = new JSONObject();
    emptyStats.put("columns", columns);
    return emptyStats.toString();
  }
}
