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

package com.logicalclocks.hsfs.spark.engine.profile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * The profile of the second half of the fixture merged into the statistics of the first half
 * equals the profile of the whole fixture, up to the sketches' error.
 */
public class ProfileMergerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void recreateSparkSession() {
    if (SparkSession.getDefaultSession().isDefined()) {
      SparkSession.getDefaultSession().get().stop();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    SparkEngine.setInstance(null);
  }

  private static SparkSession spark() {
    return SparkSession.builder().master("local[2]").appName("profile-merger-test")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
  }

  /** The DTO shape the SDK posts and reads back: what the merger gets as the previous snapshot. */
  static String toStatisticsJson(String profileJson) throws Exception {
    ArrayNode rows = MAPPER.createArrayNode();
    for (JsonNode column : MAPPER.readTree(profileJson).path("columns")) {
      ObjectNode row = rows.addObject();
      row.put("featureName", column.path("column").asText());
      row.put("numNonNullValues", column.path("numRecordsNonNull").asLong());
      row.put("numNullValues", column.path("numRecordsNull").asLong());
      row.put("count", column.path("numRecordsNonNull").asLong() + column.path("numRecordsNull").asLong());
      ObjectNode extended = MAPPER.createObjectNode();
      if (column.has("mergeable")) {
        extended.set("mergeable", column.get("mergeable"));
      }
      if (column.has("kll")) {
        extended.set("kll", column.get("kll"));
      }
      row.put("extendedStatistics", MAPPER.writeValueAsString(extended));
    }
    return MAPPER.writeValueAsString(rows);
  }

  /** NaN and the infinities must match exactly; finite values within rounding. */
  private static void assertClose(double want, double got, String what) {
    if (Double.isNaN(want) || Double.isInfinite(want)) {
      Assertions.assertEquals(0, Double.compare(want, got), what);
    } else {
      Assertions.assertEquals(want, got, Math.abs(want) * 1e-9 + 1e-9, what);
    }
  }

  private static Map<String, JsonNode> byColumn(String profileJson) throws Exception {
    Map<String, JsonNode> columns = new HashMap<>();
    for (JsonNode column : MAPPER.readTree(profileJson).path("columns")) {
      columns.put(column.path("column").asText(), column);
    }
    return columns;
  }

  @Test
  void mergedProfileMatchesTheProfileOfTheWholeTable() throws Exception {
    SparkSession spark = spark();
    Dataset<Row> all = ColumnProfilerGoldenTest.fixture(spark);
    Dataset<Row>[] halves = all.randomSplit(new double[] {0.5, 0.5}, 7L);
    ColumnProfiler profiler = new ColumnProfiler();

    String first = profiler.profile(halves[0], null, false, false, 20, false, false, true);
    String second = profiler.profile(halves[1], null, false, false, 20, false, false, true);
    String merged = ProfileMerger.merge(toStatisticsJson(first), second);
    // the reference reads its percentiles from a KLL sketch of the whole table, like the merge
    String reference = profiler.profile(all, null, false, false, 20, false, true, true);

    Map<String, JsonNode> expected = byColumn(reference);
    Map<String, JsonNode> actual = byColumn(merged);
    Assertions.assertEquals(expected.keySet(), actual.keySet());
    for (String name : expected.keySet()) {
      JsonNode want = expected.get(name);
      JsonNode got = actual.get(name);
      Assertions.assertEquals(want.path("dataType").asText(), got.path("dataType").asText(), name);
      Assertions.assertEquals(want.path("numRecordsNonNull").asLong(), got.path("numRecordsNonNull").asLong(), name);
      Assertions.assertEquals(want.path("numRecordsNull").asLong(), got.path("numRecordsNull").asLong(), name);
      Assertions.assertEquals(want.path("completeness").asDouble(), got.path("completeness").asDouble(), 1e-12, name);
      long exactDistinct = all.select(name).distinct().count();
      Assertions.assertEquals(exactDistinct, got.path("approximateNumDistinctValues").asLong(),
          Math.max(1, exactDistinct / 20), name + " distinct");
      Assertions.assertTrue(got.has("mergeable"), name + " keeps its mergeable state");
      if (want.has("mean")) {
        assertClose(want.path("sum").asDouble(), got.path("sum").asDouble(), name + " sum");
        assertClose(want.path("mean").asDouble(), got.path("mean").asDouble(), name + " mean");
        assertClose(want.path("stdDev").asDouble(), got.path("stdDev").asDouble(), name + " stdDev");
        Assertions.assertEquals(0, Double.compare(want.path("minimum").asDouble(), got.path("minimum").asDouble()),
            name + " minimum");
        Assertions.assertEquals(0, Double.compare(want.path("maximum").asDouble(), got.path("maximum").asDouble()),
            name + " maximum");
      } else {
        Assertions.assertFalse(got.has("mean"), name + " has no numeric statistics");
      }
      if (want.has("approxPercentiles")) {
        Assertions.assertTrue(got.has("approxPercentiles"), name + " percentiles");
        double range = want.path("approxPercentiles").get(98).asDouble()
            - want.path("approxPercentiles").get(0).asDouble();
        for (int ii = 0; ii < 99; ii++) {
          Assertions.assertEquals(want.path("approxPercentiles").get(ii).asDouble(),
              got.path("approxPercentiles").get(ii).asDouble(), 0.05 * range + 1e-9, name + " percentile " + ii);
        }
      }
    }
  }

  @Test
  void mergingAgainKeepsTheState() throws Exception {
    SparkSession spark = spark();
    Dataset<Row> all = ColumnProfilerGoldenTest.fixture(spark);
    Dataset<Row>[] thirds = all.randomSplit(new double[] {1, 1, 1}, 7L);
    ColumnProfiler profiler = new ColumnProfiler();

    String snapshot = profiler.profile(thirds[0], null, false, false, 20, false, false, true);
    snapshot = ProfileMerger.merge(toStatisticsJson(snapshot),
        profiler.profile(thirds[1], null, false, false, 20, false, false, true));
    snapshot = ProfileMerger.merge(toStatisticsJson(snapshot),
        profiler.profile(thirds[2], null, false, false, 20, false, false, true));

    JsonNode cInt = byColumn(snapshot).get("c_int");
    Assertions.assertEquals(all.filter("c_int IS NOT NULL").count(), cInt.path("numRecordsNonNull").asLong());
    Assertions.assertEquals(all.filter("c_int IS NULL").count(), cInt.path("numRecordsNull").asLong());
    Assertions.assertEquals(all.agg(org.apache.spark.sql.functions.sum("c_int")).first().getLong(0),
        cInt.path("sum").asDouble(), 1e-9);
  }

  @Test
  void aPreviousSnapshotWithoutMergeableStateIsRejected() throws Exception {
    SparkSession spark = spark();
    Dataset<Row> all = ColumnProfilerGoldenTest.fixture(spark);
    ColumnProfiler profiler = new ColumnProfiler();
    String plain = profiler.profile(all, null, false, false, 20, false, false);
    String delta = profiler.profile(all, null, false, false, 20, false, false, true);

    Assertions.assertFalse(plain.contains("mergeable"), "a plain profile carries no mergeable state");
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> ProfileMerger.merge(toStatisticsJson(plain), delta));
  }

  /**
   * The Python client registers the statistics of small commits with the same state: its
   * sketches must heapify here, and its HLL must hash a value as Spark's renders it, or the
   * values both sides saw would count twice.
   */
  @Test
  void aStateWrittenByThePythonClientMerges() throws Exception {
    // the Python client's profile of c = 1..10 and s = a..j
    String previous;
    try (InputStream in = getClass().getClassLoader()
        .getResourceAsStream("profile-python-mergeable-state.json")) {
      previous = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    SparkSession spark = spark();
    StructType schema = new StructType()
        .add("c", DataTypes.LongType)
        .add("s", DataTypes.StringType);
    List<Row> rows = new ArrayList<>();
    for (long value = 5; value <= 20; value++) {
      rows.add(RowFactory.create(value, String.valueOf((char) ('a' + value - 1))));
    }
    Dataset<Row> delta = spark.createDataFrame(rows, schema);

    String merged = ProfileMerger.merge(previous,
        new ColumnProfiler().profile(delta, null, false, false, 20, false, false, true));

    JsonNode c = byColumn(merged).get("c");
    Assertions.assertEquals(26, c.path("numRecordsNonNull").asLong());
    Assertions.assertEquals(55 + 200, c.path("sum").asDouble(), 1e-9);
    Assertions.assertEquals(1.0, c.path("minimum").asDouble());
    Assertions.assertEquals(20.0, c.path("maximum").asDouble());
    // 5..10 are in both profiles: 20 distinct values, not 26
    Assertions.assertEquals(20, c.path("approximateNumDistinctValues").asLong());
    Assertions.assertEquals(99, c.path("approxPercentiles").size());
    Assertions.assertEquals(20, byColumn(merged).get("s").path("approximateNumDistinctValues").asLong());
    Assertions.assertEquals(new HashSet<>(Arrays.asList("c", "s")), byColumn(merged).keySet());
  }

  @Test
  void aBatchWithoutFiniteValuesKeepsTheKllSidecar() throws Exception {
    SparkSession spark = spark();
    StructType schema = new StructType().add("x", DataTypes.DoubleType);
    List<Row> values = new ArrayList<>();
    List<Row> nulls = new ArrayList<>();
    for (int ii = 0; ii < 10; ii++) {
      values.add(RowFactory.create((double) ii));
      nulls.add(RowFactory.create((Object) null));
    }
    ColumnProfiler profiler = new ColumnProfiler();
    String snapshot = profiler.profile(spark.createDataFrame(values, schema), null, false, false, 20, false, true,
        true);
    String delta = profiler.profile(spark.createDataFrame(nulls, schema), null, false, false, 20, false, true, true);
    Assertions.assertFalse(byColumn(delta).get("x").has("kll"), "an all-null batch has no sidecar of its own");

    JsonNode merged = byColumn(ProfileMerger.merge(toStatisticsJson(snapshot), delta)).get("x");

    Assertions.assertTrue(merged.has("kll"), "the snapshot's sidecar is kept");
    Assertions.assertEquals(10, merged.path("numRecordsNull").asLong());
  }

  @Test
  void aPreviousColumnTheBatchWasNotProfiledForIsRejected() throws Exception {
    SparkSession spark = spark();
    Dataset<Row> all = ColumnProfilerGoldenTest.fixture(spark);
    ColumnProfiler profiler = new ColumnProfiler();
    String previous = profiler.profile(all, null, false, false, 20, false, false, true);
    // the batch's profile covers one column fewer than the snapshot
    String delta = profiler.profile(all.drop("c_int"), null, false, false, 20, false, false, true);

    Assertions.assertThrows(IllegalArgumentException.class,
        () -> ProfileMerger.merge(toStatisticsJson(previous), delta));
  }

  @Test
  void mergingEmptyProfilesGivesTheProfilersEmptyCompleteness() throws Exception {
    SparkSession spark = spark();
    Dataset<Row> empty = spark.createDataFrame(new ArrayList<Row>(), new StructType().add("x", DataTypes.LongType));
    ColumnProfiler profiler = new ColumnProfiler();
    String profile = profiler.profile(empty, null, false, false, 20, false, false, true);

    JsonNode merged = byColumn(ProfileMerger.merge(toStatisticsJson(profile), profile)).get("x");

    Assertions.assertEquals(byColumn(profile).get("x").path("completeness").asDouble(),
        merged.path("completeness").asDouble());
    Assertions.assertEquals(0.0, merged.path("completeness").asDouble());
  }
}
