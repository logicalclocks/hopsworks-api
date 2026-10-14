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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Pins the profiler's output on an edge-case fixture to golden files, so a change to how the
 * statistics are computed cannot silently change what they are.
 *
 * <p>Regenerate the golden files only for an intended output change, by running this test with
 * {@code -Dprofiler.golden.record=true} and reviewing the diff of {@code src/test/resources}.
 */
public class ColumnProfilerGoldenTest {

  private static final Path GOLDEN_DIR = Paths.get("src/test/resources/profiler-golden");
  // Entropy is summed in a different order once it moves from the driver to the executors.
  private static final double TOL_REL = 1e-12;

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

  @Test
  void defaultConfigMatchesGolden() throws Exception {
    profileMatchesGolden("defaults", false, false, false, false);
  }

  @Test
  void everythingButKllMatchesGolden() throws Exception {
    profileMatchesGolden("all_no_kll", true, true, true, false);
  }

  @Test
  void everythingWithKllMatchesGolden() throws Exception {
    profileMatchesGolden("all_with_kll", true, true, true, true);
  }

  private void profileMatchesGolden(String name, boolean correlation, boolean histogram,
      boolean exactUniqueness, boolean kll) throws Exception {
    SparkSession spark = SparkEngine.getInstance().getSparkSession();
    // The hopsworks-helm spark chart pins ANSI off cluster-wide; with it on, a correlation
    // over a constant or all-null column fails the job on a division by zero.
    spark.conf().set("spark.sql.ansi.enabled", "false");
    Dataset<Row> df = fixture(spark);
    String json = new ColumnProfiler().profile(df, null, correlation, histogram, 10,
        exactUniqueness, kll);
    JsonNode actual = MAPPER.readTree(json);
    // KLL compaction is randomised, so the sketch and the quantiles read from it vary by run.
    if (kll) {
      stripKllDerived(actual);
    }
    sortCategoricalBins(actual);

    Path golden = GOLDEN_DIR.resolve(name + ".json");
    if (Boolean.getBoolean("profiler.golden.record")) {
      Files.createDirectories(GOLDEN_DIR);
      Files.write(golden, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(actual));
      return;
    }
    JsonNode expected;
    try (InputStream in = Files.newInputStream(golden)) {
      expected = MAPPER.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
    sortCategoricalBins(expected);
    List<String> diffs = new ArrayList<>();
    compare("$", expected, actual, diffs);
    Assertions.assertTrue(diffs.isEmpty(), String.join("\n", diffs));
  }

  @Test
  void statisticsSplitAcrossAggregationsMatchPerColumnStatistics() throws Exception {
    SparkSession spark = SparkEngine.getInstance().getSparkSession();
    spark.conf().set("spark.sql.ansi.enabled", "false");
    // 12 numeric columns against a limit of 25 expressions: each column contributes 12
    // scalar expressions with KLL off, so the scalar pass spans 6 aggregations of two
    // columns, and the 66 correlation pairs span 3.
    int width = 12;
    StructType schema = new StructType();
    for (int c = 0; c < width; c++) {
      schema = schema.add("f" + c, DataTypes.DoubleType, true);
    }
    Random random = new Random(11L);
    List<Row> rows = new ArrayList<>();
    for (int r = 0; r < 300; r++) {
      Object[] values = new Object[width];
      for (int c = 0; c < width; c++) {
        values[c] = (r + c) % 17 == 0 ? null : random.nextGaussian() + c;
      }
      rows.add(RowFactory.create(values));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema).repartition(3);

    JsonNode columns = MAPPER.readTree(
        new ColumnProfiler(25).profile(df, null, true, false, 10, false, false)).get("columns");

    Assertions.assertEquals(width, columns.size());
    for (int c : new int[] {0, 5, width - 1}) {
      JsonNode col = columns.get(c);
      Row expected = df.agg(functions.mean("f" + c), functions.percentile_approx(
          functions.col("f" + c), functions.lit(0.5), functions.lit(10000))).first();
      assertClose(expected.getDouble(0), col.get("mean").asDouble(), "mean of f" + c);
      assertClose(expected.getDouble(1), col.get("approxPercentiles").get(49).asDouble(),
          "median of f" + c);
      for (int other : new int[] {1, 6, width - 2}) {
        assertClose(df.stat().corr("f" + c, "f" + other), correlationWith(col, "f" + other),
            "f" + c + " vs f" + other);
      }
    }
  }

  @Test
  void numericHistogramsOfSeveralColumnsCountEveryBin() throws Exception {
    SparkSession spark = SparkEngine.getInstance().getSparkSession();
    spark.conf().set("spark.sql.ansi.enabled", "false");
    // Integers 0..199 over [0, 199] in 40 bins: bin i holds the values v with
    // floor(v / (199 / 40)) == i.
    // The second column is the first shifted, binned on its own grid.
    StructType schema = new StructType().add("a", DataTypes.IntegerType, true)
        .add("b", DataTypes.DoubleType, true);
    List<Row> rows = new ArrayList<>();
    for (int r = 0; r < 200; r++) {
      rows.add(RowFactory.create(r, r % 50 == 0 ? Double.NaN : r + 1000.0));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema).repartition(3);

    JsonNode columns = MAPPER.readTree(
        new ColumnProfiler().profile(df, null, false, true, 40, false, false)).get("columns");

    long[] expectedA = new long[40];
    for (int v = 0; v < 200; v++) {
      expectedA[Math.min((int) Math.floor(v / (199.0 / 40)), 39)]++;
    }
    JsonNode histA = columns.get(0).get("histogram");
    Assertions.assertEquals(40, histA.size());
    for (int bin = 0; bin < 40; bin++) {
      Assertions.assertEquals(expectedA[bin], histA.get(bin).get("count").asLong(), "a bin " + bin);
    }
    long totalB = 0;
    for (JsonNode bin : columns.get(1).get("histogram")) {
      totalB += bin.get("count").asLong();
    }
    Assertions.assertEquals(196, totalB, "NaN rows belong to no bin");
  }

  @Test
  void equalCountCategoriesKeepTheSmallestValuesInValueOrder() throws Exception {
    // 30 categories, each twice, against 10 bins: which ten survive the top-N cut and their
    // order used to follow the shuffle.
    StructType schema = new StructType().add("s", DataTypes.StringType, true);
    List<Row> rows = new ArrayList<>();
    for (int r = 0; r < 60; r++) {
      rows.add(RowFactory.create(String.format("c%02d", 29 - r % 30)));
    }
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema)
        .repartition(4);

    JsonNode bins = MAPPER.readTree(
        new ColumnProfiler().profile(df, null, false, true, 10, false, false))
        .get("columns").get(0).get("histogram");

    Assertions.assertEquals(10, bins.size());
    for (int bin = 0; bin < 10; bin++) {
      Assertions.assertEquals(String.format("c%02d", bin), bins.get(bin).get("value").asText());
      Assertions.assertEquals(2, bins.get(bin).get("count").asLong());
    }
  }

  // repartition(3) deals rows round-robin, so each run sums them in a different order.
  private static void assertClose(double expected, double actual, String what) {
    Assertions.assertEquals(expected, actual, TOL_REL * Math.max(Math.abs(expected), Math.abs(actual)), what);
  }

  private static double correlationWith(JsonNode column, String other) {
    for (JsonNode entry : column.get("correlations")) {
      if (entry.get("column").asText().equals(other)) {
        return entry.get("correlation").asDouble();
      }
    }
    throw new AssertionError("no correlation with " + other);
  }

  private static void stripKllDerived(JsonNode root) {
    for (JsonNode col : root.get("columns")) {
      ((ObjectNode) col).remove("kll");
      ((ObjectNode) col).remove("approxPercentiles");
    }
  }

  /** Orders equal-count categorical bins by value, as the profiler breaks their ties. */
  private static void sortCategoricalBins(JsonNode root) {
    for (JsonNode col : root.get("columns")) {
      String type = col.get("dataType").asText();
      JsonNode bins = col.get("histogram");
      if (bins == null || "Integral".equals(type) || "Fractional".equals(type)) {
        continue;
      }
      List<JsonNode> sorted = new ArrayList<>();
      bins.forEach(sorted::add);
      sorted.sort(Comparator.<JsonNode>comparingLong(bin -> -bin.get("count").asLong())
          .thenComparing(bin -> bin.get("value").asText()));
      ArrayNode replacement = MAPPER.createArrayNode();
      sorted.forEach(replacement::add);
      ((ObjectNode) col).set("histogram", replacement);
    }
  }

  private static void compare(String path, JsonNode expected, JsonNode actual, List<String> diffs) {
    if (expected.isNumber() && actual.isNumber()) {
      double ev = expected.asDouble();
      double av = actual.asDouble();
      if (Math.abs(ev - av) > TOL_REL * Math.max(Math.abs(ev), Math.abs(av))) {
        diffs.add(path + ": expected " + expected + ", got " + actual);
      }
      return;
    }
    if (expected.getNodeType() != actual.getNodeType()) {
      diffs.add(path + ": expected " + expected + ", got " + actual);
      return;
    }
    if (expected.isArray()) {
      if (expected.size() != actual.size()) {
        diffs.add(path + ": expected " + expected.size() + " elements, got " + actual.size());
        return;
      }
      for (int i = 0; i < expected.size(); i++) {
        compare(path + "[" + i + "]", expected.get(i), actual.get(i), diffs);
      }
      return;
    }
    if (expected.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = expected.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        JsonNode other = actual.get(field.getKey());
        if (other == null) {
          diffs.add(path + "." + field.getKey() + ": missing");
        } else {
          compare(path + "." + field.getKey(), field.getValue(), other, diffs);
        }
      }
      Iterator<String> names = actual.fieldNames();
      while (names.hasNext()) {
        String key = names.next();
        if (!expected.has(key)) {
          diffs.add(path + "." + key + ": unexpected");
        }
      }
      return;
    }
    if (!expected.equals(actual)) {
      diffs.add(path + ": expected " + expected + ", got " + actual);
    }
  }

  static Dataset<Row> fixture(SparkSession spark) {
    StructType schema = new StructType()
        .add("c_int", DataTypes.IntegerType, true)
        .add("c_long", DataTypes.LongType, true)
        .add("c_double", DataTypes.DoubleType, true)
        .add("c_nonfinite", DataTypes.DoubleType, true)
        .add("c_decimal", DataTypes.createDecimalType(12, 3), true)
        .add("c_const", DataTypes.DoubleType, true)
        .add("c_all_null", DataTypes.DoubleType, true)
        .add("c_bool", DataTypes.BooleanType, true)
        .add("c_string", DataTypes.StringType, true)
        .add("c_numeric_string", DataTypes.StringType, true)
        .add("count", DataTypes.IntegerType, true);
    String[] vocab = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"};
    double[] nonFinite = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0.0};
    Random random = new Random(7L);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 400; i++) {
      boolean nullRow = i % 11 == 0;
      double value = random.nextGaussian();
      rows.add(RowFactory.create(
          nullRow ? null : random.nextInt(50),
          i % 13 == 0 ? null : (long) (value * 1_000_000_000L),
          nullRow ? null : value,
          i % 7 == 0 ? nonFinite[i % nonFinite.length] : value * 3,
          i % 17 == 0 ? null : new BigDecimal(value).setScale(3, java.math.RoundingMode.HALF_UP),
          4.5,
          null,
          i % 5 == 0 ? null : random.nextBoolean(),
          i % 9 == 0 ? null : vocab[random.nextInt(vocab.length)],
          i % 3 == 0 ? "007" : Integer.toString(random.nextInt(20)),
          i < 50 ? i : random.nextInt(5)));
    }
    return spark.createDataFrame(rows, schema)
        .repartition(4, functions.col("c_string"));
  }
}
