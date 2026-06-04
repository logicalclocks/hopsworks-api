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
import org.apache.spark.sql.functions;
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
 * Smoke test for KLL sketch merge correctness — validates that:
 * <ol>
 *   <li>Two independently-profiled halves of a distribution can be merged via
 *       {@link KllDoublesSketch#merge(KllDoublesSketch)}.</li>
 *   <li>The merged sketch's median matches a reference median computed over the
 *       full combined dataset (via Spark {@code approxQuantile}) within 5e-2.</li>
 * </ol>
 *
 * <p>This is the Phase-2 unlock: the JVM can merge per-partition sketches without
 * re-reading the underlying data.
 */
public class KllMergeSmokeTest {

  private static final String COL_NAME = "c_double";

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
  void testMergedSketchMedianMatchesReference() throws Exception {
    // Build two half-datasets with different seeds to avoid trivial overlap.
    Dataset<Row> halfA = buildHalf(42L, 5_000);
    Dataset<Row> halfB = buildHalf(43L, 5_000);

    // Profile each half independently with kll=true.
    // exactUniqueness=true required: ColumnProfiler always reads __exact_distinct from the agg row;
    // passing false causes an IllegalArgumentException (see bug flagged in report).
    String jsonA = new ColumnProfiler().profile(halfA, null, false, false, 20, true, true);
    String jsonB = new ColumnProfiler().profile(halfB, null, false, false, 20, true, true);

    byte[] bytesA = extractKllBytes(jsonA, COL_NAME);
    byte[] bytesB = extractKllBytes(jsonB, COL_NAME);

    // Heapify and merge
    KllDoublesSketch sketchA = KllAggregator.heapify(bytesA);
    KllDoublesSketch sketchB = KllAggregator.heapify(bytesB);
    sketchA.merge(sketchB);

    double mergedMedian = sketchA.getQuantile(0.5);

    // Reference median from Spark approxQuantile over the full combined dataset.
    Dataset<Row> fullDf = halfA.union(halfB);
    double referenceMedian = computeApproxMedian(fullDf, COL_NAME);

    double diff = Math.abs(mergedMedian - referenceMedian);
    System.out.printf("[KllMergeSmokeTest] merged sketch median=%.8f  reference median=%.8f  diff=%.6f%n",
        mergedMedian, referenceMedian, diff);

    Assertions.assertEquals(referenceMedian, mergedMedian, 5e-2,
        String.format("Merged KLL median %.8f differs from reference median %.8f by %.6f (>5e-2)",
            mergedMedian, referenceMedian, diff));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Dataset<Row> buildHalf(long seed, int rowCount) {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField(COL_NAME, DataTypes.DoubleType, true),
    });

    Random rng = new Random(seed);
    List<Row> rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      rows.add(RowFactory.create(rng.nextGaussian()));
    }

    return SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);
  }

  private byte[] extractKllBytes(String profileJson, String columnName) throws Exception {
    JsonNode root = MAPPER.readTree(profileJson);
    JsonNode columns = root.get("columns");
    for (JsonNode col : columns) {
      if (columnName.equals(col.get("column").asText())) {
        JsonNode kll = col.get("kll");
        Assertions.assertNotNull(kll, "kll field missing for column " + columnName);
        String base64 = kll.get("bytes").asText();
        Assertions.assertFalse(base64.isEmpty(), "kll.bytes is empty for column " + columnName);
        return Base64.getDecoder().decode(base64);
      }
    }
    throw new AssertionError("Column '" + columnName + "' not found in profile JSON");
  }

  private double computeApproxMedian(Dataset<Row> df, String colName) {
    // approxQuantile with relative error 0.001 gives accurate-enough reference
    double[] quantiles = df.stat().approxQuantile(colName, new double[]{0.5}, 0.001);
    Assertions.assertEquals(1, quantiles.length,
        "approxQuantile must return exactly one value");
    return quantiles[0];
  }
}
