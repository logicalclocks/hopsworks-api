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
import com.logicalclocks.hsfs.spark.engine.SparkEngine;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

/**
 * Structural smoke test for {@link ColumnProfiler}.
 *
 * <p>Verifies the JSON shape and KLL sketch correctness of the new profiler.
 * Does NOT attempt byte-identity with the Deequ baseline — that is task #6.
 */
public class ColumnProfilerSmokeTest {

  private static final String[] VOCAB = {
    "alpha", "bravo", "charlie", "delta", "echo",
    "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima", "mike", "november", "oscar",
    "papa", "quebec", "romeo", "sierra", "tango"
  };

  private static final int ROW_COUNT = 10_000;
  private static final long SEED = 42L;

  /**
   * Recreate a fresh SparkSession before every test.
   *
   * <p>The Spark test suite shares a single JVM-wide SparkContext. Other test classes
   * (e.g. {@code TestStorageConnector}) register credential files via
   * {@code SparkContext.addFile()} from JUnit {@code @TempDir} paths that JUnit deletes
   * once those tests finish. Spark has no {@code removeFile}, so the stale registrations
   * linger on the shared context; the first profiler test to run a real Spark job then
   * fails in {@code Executor.updateDependencies} when it re-fetches the now-missing files.
   * Starting each test from a clean SparkContext keeps these tests independent of that state.
   */
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
  void smokeTestKll() throws Exception {
    Dataset<Row> df = buildDataset();

    String json = new ColumnProfiler().profile(df, null, true, true, 20, true, true);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(json);

    // (a) top-level has a "columns" array of 6 elements
    JsonNode columns = root.get("columns");
    Assertions.assertNotNull(columns, "top-level 'columns' key must be present");
    Assertions.assertTrue(columns.isArray(), "'columns' must be an array");
    Assertions.assertEquals(6, columns.size(), "expected 6 column profiles");

    // Find c_double profile
    JsonNode cDouble = findColumn(columns, "c_double");
    Assertions.assertNotNull(cDouble, "c_double column profile must be present");

    // (b) c_double has kll dict with kllFormat and bytes fields
    JsonNode kll = cDouble.get("kll");
    Assertions.assertNotNull(kll, "c_double must have a kll field");
    Assertions.assertEquals("datasketches-native-v1", kll.get("kllFormat").asText(),
        "kll.kllFormat must equal datasketches-native-v1");
    JsonNode bytesNode = kll.get("bytes");
    Assertions.assertNotNull(bytesNode, "kll.bytes must be present");
    String base64Bytes = bytesNode.asText();
    Assertions.assertFalse(base64Bytes.isEmpty(), "kll.bytes must not be empty");

    // (c) base64-decoded KLL sketch heapifies and its median agrees with Spark approx median.
    //     KLL with default K=200 has ~1.65% normalised rank error. On a standard normal, pdf at
    //     the median is ~0.399, so value-space error near the median is bounded by
    //     rank_err / pdf ≈ 0.041. 5e-2 is a conservative tolerance that accommodates KLL
    //     approximation + Spark percentile_approx error.
    byte[] sketchBytes = Base64.getDecoder().decode(base64Bytes);
    KllDoublesSketch sketch = KllAggregator.heapify(sketchBytes);
    double sketchMedian = sketch.getQuantile(0.5);

    // Compute reference median via Spark percentile_approx
    double[] sparkMedian = SparkEngine.getInstance().getSparkSession()
        .sql("SELECT 0").javaRDD().isEmpty()
        ? new double[]{0.0}
        : getApproxMedian(df, "c_double");

    Assertions.assertEquals(sparkMedian[0], sketchMedian, 5e-2,
        "KLL sketch median must be within 5e-2 of Spark approx median");

    // (d) correlations for c_double contains 4 entries with column and correlation keys
    JsonNode correlations = cDouble.get("correlations");
    Assertions.assertNotNull(correlations, "c_double must have correlations");
    Assertions.assertTrue(correlations.isArray(), "correlations must be an array");
    Assertions.assertEquals(4, correlations.size(),
        "c_double correlations must have 4 entries (self + 3 other numerics)");
    for (JsonNode corrEntry : correlations) {
      Assertions.assertTrue(corrEntry.has("column"), "each correlation entry must have 'column'");
      Assertions.assertTrue(corrEntry.has("correlation"),
          "each correlation entry must have 'correlation'");
    }
    // Verify self-correlation is 1.0
    JsonNode selfCorr = findCorrelation(correlations, "c_double");
    Assertions.assertNotNull(selfCorr, "self-correlation entry must be present");
    Assertions.assertEquals(1.0, selfCorr.get("correlation").asDouble(), 1e-10,
        "self-correlation must be 1.0");

    // (e) histogram for c_double has 20 entries with value, count, ratio keys
    JsonNode histogram = cDouble.get("histogram");
    Assertions.assertNotNull(histogram, "c_double must have histogram");
    Assertions.assertTrue(histogram.isArray(), "histogram must be an array");
    Assertions.assertEquals(20, histogram.size(), "histogram must have 20 entries");
    for (JsonNode entry : histogram) {
      Assertions.assertTrue(entry.has("value"), "histogram entry must have 'value'");
      Assertions.assertTrue(entry.has("count"), "histogram entry must have 'count'");
      Assertions.assertTrue(entry.has("ratio"), "histogram entry must have 'ratio'");
    }

    // Verify categorical columns do NOT have kll
    JsonNode cString = findColumn(columns, "c_string");
    Assertions.assertNotNull(cString, "c_string must be present");
    Assertions.assertNull(cString.get("kll"), "c_string must NOT have kll field");

    JsonNode cBool = findColumn(columns, "c_bool");
    Assertions.assertNotNull(cBool, "c_bool must be present");
    Assertions.assertNull(cBool.get("kll"), "c_bool must NOT have kll field");
  }

  @Test
  void omitsUniquenessFamilyWhenExactUniquenessDisabled() throws Exception {
    Dataset<Row> df = buildDataset();

    String json = new ColumnProfiler().profile(df, null, false, false, 20, false, false);

    JsonNode columns = new ObjectMapper().readTree(json).get("columns");
    for (JsonNode col : columns) {
      String name = col.get("column").asText();
      Assertions.assertFalse(col.has("uniqueness"),
          name + " must not emit 'uniqueness' when exactUniqueness is disabled");
      Assertions.assertFalse(col.has("distinctness"),
          name + " must not emit 'distinctness' when exactUniqueness is disabled");
      Assertions.assertFalse(col.has("entropy"),
          name + " must not emit 'entropy' when exactUniqueness is disabled");
      Assertions.assertFalse(col.has("exactNumDistinctValues"),
          name + " must not emit 'exactNumDistinctValues' when exactUniqueness is disabled");
      Assertions.assertTrue(col.has("approximateNumDistinctValues"),
          name + " must still emit 'approximateNumDistinctValues'");
    }
  }

  @Test
  void emitsUniquenessFamilyWhenExactUniquenessEnabled() throws Exception {
    Dataset<Row> df = buildDataset();

    String json = new ColumnProfiler().profile(df, null, false, false, 20, true, false);

    JsonNode columns = new ObjectMapper().readTree(json).get("columns");
    for (JsonNode col : columns) {
      String name = col.get("column").asText();
      Assertions.assertTrue(col.has("uniqueness"),
          name + " must emit 'uniqueness' when exactUniqueness is enabled");
      Assertions.assertTrue(col.has("distinctness"),
          name + " must emit 'distinctness' when exactUniqueness is enabled");
      Assertions.assertTrue(col.has("entropy"),
          name + " must emit 'entropy' when exactUniqueness is enabled");
      Assertions.assertTrue(col.has("exactNumDistinctValues"),
          name + " must emit 'exactNumDistinctValues' when exactUniqueness is enabled");
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private double[] getApproxMedian(Dataset<Row> df, String colName) {
    Row result = df.agg(
        org.apache.spark.sql.functions.percentile_approx(
            org.apache.spark.sql.functions.col(colName).cast("double"),
            org.apache.spark.sql.functions.array(
                org.apache.spark.sql.functions.lit(0.5)
            ),
            org.apache.spark.sql.functions.lit(10000)
        )
    ).first();
    List<Double> list = result.getList(0);
    return new double[]{list.get(0)};
  }

  private JsonNode findColumn(JsonNode columns, String name) {
    for (JsonNode col : columns) {
      if (name.equals(col.get("column").asText())) {
        return col;
      }
    }
    return null;
  }

  private JsonNode findCorrelation(JsonNode correlations, String colName) {
    for (JsonNode corr : correlations) {
      if (colName.equals(corr.get("column").asText())) {
        return corr;
      }
    }
    return null;
  }

  private Dataset<Row> buildDataset() {
    StructType schema = new StructType(new StructField[]{
      DataTypes.createStructField("c_int", DataTypes.IntegerType, true),
      DataTypes.createStructField("c_long", DataTypes.LongType, true),
      DataTypes.createStructField("c_double", DataTypes.DoubleType, true),
      DataTypes.createStructField("c_string", DataTypes.StringType, true),
      DataTypes.createStructField("c_bool", DataTypes.BooleanType, true),
      DataTypes.createStructField("c_nullable_double", DataTypes.DoubleType, true),
    });

    Random rng = new Random(SEED);
    List<Row> rows = new ArrayList<>(ROW_COUNT);
    for (int ii = 0; ii < ROW_COUNT; ii++) {
      int cInt = (int) (rng.nextDouble() * 1_000_000);
      long cLong = (long) (rng.nextDouble() * 10_000_000_000L);
      double cDouble = rng.nextGaussian();
      String cString = VOCAB[ii % VOCAB.length];
      boolean cBool = (ii % 2 == 0);
      Double cNullable = (ii % 10 == 0) ? null : rng.nextGaussian();
      rows.add(RowFactory.create(cInt, cLong, cDouble, cString, cBool, cNullable));
    }

    return SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);
  }
}
