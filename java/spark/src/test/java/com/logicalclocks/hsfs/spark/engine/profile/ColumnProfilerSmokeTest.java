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

  @Test
  void uniquenessCountsSingletonsWithHighMultiplicity() throws Exception {
    // 15 values; 5 occurs three times, 4/3/2/1 twice, 9/8/7/6 once.
    // Deequ uniqueness = singletons / nonNull = 4/15. The former shortcut
    // (2*distinct - n)/n gave 3/15 whenever a value occurred more than twice.
    StructType schema = new StructType(new StructField[]{
      DataTypes.createStructField("col_1", DataTypes.IntegerType, true),
    });
    int[] values = {5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 9, 8, 7, 6, 5};
    List<Row> rows = new ArrayList<>(values.length);
    for (int value : values) {
      rows.add(RowFactory.create(value));
    }
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, false, 20, true, false);

    JsonNode col = findColumn(new ObjectMapper().readTree(json).get("columns"), "col_1");
    Assertions.assertNotNull(col);
    Assertions.assertEquals(4.0 / 15.0, col.get("uniqueness").asDouble(), 1e-9,
        "uniqueness must be the exact singleton fraction");
    Assertions.assertEquals(9.0 / 15.0, col.get("distinctness").asDouble(), 1e-9);
    Assertions.assertEquals(9, col.get("exactNumDistinctValues").asLong());
  }

  @Test
  void profilesAllNaNColumnWithoutThrowing() throws Exception {
    // Spark counts NaN as non-null, but KllAggregator skips NaN when building the
    // sketch, so an all-NaN column used to reach getQuantiles with an empty sketch
    // and fail the whole job with SketchesArgumentException. some_nan is the
    // counterpart the emptiness check must not swallow: NaN is present but not
    // exclusive, so the sketch holds the finite values and KLL is still emitted.
    StructType schema = new StructType(new StructField[]{
      DataTypes.createStructField("all_nan", DataTypes.DoubleType, true),
      DataTypes.createStructField("some_nan", DataTypes.DoubleType, true),
    });
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(RowFactory.create(Double.NaN, i % 2 == 0 ? Double.NaN : (double) i));
    }
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, false, 20, false, true);
    JsonNode columns = new ObjectMapper().readTree(json).get("columns");

    JsonNode allNan = findColumn(columns, "all_nan");
    Assertions.assertNotNull(allNan, "all_nan column profile must be present");
    Assertions.assertFalse(allNan.has("approxPercentiles"),
        "an empty sketch must not yield approxPercentiles");
    Assertions.assertFalse(allNan.has("kll"),
        "an empty sketch must not be emitted as kll (the serializer calls getCDF on it)");

    JsonNode someNan = findColumn(columns, "some_nan");
    Assertions.assertNotNull(someNan, "some_nan column profile must be present");
    Assertions.assertTrue(someNan.has("kll"),
        "a sketch holding the finite values must still be emitted as kll");
    Assertions.assertEquals(99, someNan.get("approxPercentiles").size(),
        "a non-empty sketch must still yield the full percentile vector");
    Assertions.assertEquals(1.0, someNan.get("approxPercentiles").get(0).asDouble(), 1e-9,
        "percentiles must be estimated from the finite values only");
  }

  @Test
  void binsPartlyNaNColumnOverItsFiniteRange() throws Exception {
    // Spark ranks NaN above every other double and counts it as non-null, so a column
    // holding one NaN reported maximum=NaN. Both bin grids were derived from that: the
    // histogram labelled all 20 bins "NaN to NaN", and the KLL buckets fell back to a
    // width-1.0 grid and scaled their counts by numRecordsNonNull, which counts the NaN
    // rows the sketch never saw.
    StructType schema = new StructType(new StructField[]{
      DataTypes.createStructField("all_nan", DataTypes.DoubleType, true),
      DataTypes.createStructField("some_nan", DataTypes.DoubleType, true),
    });
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(RowFactory.create(Double.NaN, i % 2 == 0 ? Double.NaN : (double) i));
    }
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, false, true);
    JsonNode columns = new ObjectMapper().readTree(json).get("columns");

    // some_nan holds 1, 3, 5, 7, 9 plus five NaN: bins span [1, 9] and hold five rows.
    JsonNode someNan = findColumn(columns, "some_nan");
    Assertions.assertNotNull(someNan, "some_nan column profile must be present");
    JsonNode histogram = someNan.get("histogram");
    Assertions.assertNotNull(histogram, "some_nan must have a histogram");
    Assertions.assertEquals(20, histogram.size(), "histogram must have 20 bins");

    long histTotal = 0;
    double ratioTotal = 0.0;
    for (JsonNode bin : histogram) {
      Assertions.assertFalse(bin.get("value").asText().contains("NaN"),
          "no bin label may be derived from a NaN range: " + bin.get("value").asText());
      histTotal += bin.get("count").asLong();
      ratioTotal += bin.get("ratio").asDouble();
    }
    Assertions.assertEquals(5, histTotal, "histogram must count the five finite values only");
    Assertions.assertEquals(1.0, ratioTotal, 1e-9, "bin ratios must sum to 1 over non-NaN rows");
    Assertions.assertTrue(histogram.get(0).get("value").asText().startsWith("1.00 to"),
        "first bin must start at the finite minimum, not NaN: "
            + histogram.get(0).get("value").asText());

    // The KLL buckets bin the sketch over its own range and weight.
    JsonNode buckets = someNan.get("kll").get("buckets");
    Assertions.assertEquals(20, buckets.size(), "kll must have 20 buckets");
    Assertions.assertEquals(1.0, buckets.get(0).get("low_value").asDouble(), 1e-9,
        "first bucket must start at the sketch minimum");
    Assertions.assertEquals(9.0, buckets.get(19).get("high_value").asDouble(), 1e-9,
        "last bucket must end at the sketch maximum, not a width-1.0 fallback grid");
    long bucketTotal = 0;
    for (JsonNode bucket : buckets) {
      bucketTotal += bucket.get("count").asLong();
    }
    Assertions.assertEquals(5, bucketTotal,
        "bucket counts must be scaled by the sketch weight, not numRecordsNonNull");

    // Nothing finite to bin: the histogram is omitted rather than emitted as NaN bins.
    JsonNode allNan = findColumn(columns, "all_nan");
    Assertions.assertNotNull(allNan, "all_nan column profile must be present");
    Assertions.assertFalse(allNan.has("histogram"),
        "a column with no finite values must not emit a histogram");
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
