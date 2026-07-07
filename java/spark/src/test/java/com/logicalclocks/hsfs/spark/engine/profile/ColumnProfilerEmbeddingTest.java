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
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Structural test for embedding (numeric-array) profiling in {@link ColumnProfiler}.
 *
 * <p>Verifies the {@code embedding} JSON block (dimension, count, centroid, norm histogram/KLL),
 * every documented row-validity edge case, and that non-embedding columns are unchanged.
 */
public class ColumnProfilerEmbeddingTest {

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

  // -------------------------------------------------------------------------
  // Embedding block shape + edge cases
  // -------------------------------------------------------------------------

  @Test
  void embeddingBlockShapeAndEdgeCases() throws Exception {
    Dataset<Row> df = buildEmbeddingDataset();

    // correlation=true, histogram=true, bins=20, exactUniqueness=true, kll=true.
    String json = new ColumnProfiler().profile(df, null, true, true, 20, true, true);
    JsonNode columns = MAPPER.readTree(json).get("columns");

    JsonNode emb = findColumn(columns, "emb");
    Assertions.assertNotNull(emb, "emb column profile must be present");

    // (a) top-level dataType is the stable "Embedding" string.
    Assertions.assertEquals("Embedding", emb.get("dataType").asText());

    // (b) standard count/completeness/null fields reflect the array column (1 null row of 7).
    Assertions.assertEquals(7, emb.get("numRecordsNonNull").asLong() + emb.get("numRecordsNull").asLong());
    Assertions.assertEquals(1, emb.get("numRecordsNull").asLong(),
        "exactly one null array row");
    Assertions.assertEquals(6, emb.get("numRecordsNonNull").asLong(),
        "six non-null array rows (including empty/ragged/null-element)");

    JsonNode embedding = emb.get("embedding");
    Assertions.assertNotNull(embedding, "emb must carry an 'embedding' block");

    // (c) dimension is the modal valid length (2): the [1,2,3] ragged row is dropped.
    Assertions.assertEquals(2, embedding.get("dimension").asInt(),
        "dimension must be the modal valid length");

    // (d) count = rows with a fully valid vector of the modal length. The valid rows are
    //     [3,4], [0,5], [6,8]. Null/empty/null-element/ragged rows are all excluded.
    Assertions.assertEquals(3, embedding.get("count").asLong(),
        "only three vectors are valid (correct length, no null/NaN elements)");

    // (e) centroid is the element-wise mean over the three valid rows.
    //     mean([3,0,6]) = 3.0 ; mean([4,5,8]) = 17/3.
    JsonNode centroid = embedding.get("centroid");
    Assertions.assertTrue(centroid.isArray(), "centroid must be an array");
    Assertions.assertEquals(2, centroid.size(), "centroid length must equal dimension");
    Assertions.assertEquals(3.0, centroid.get(0).asDouble(), 1e-9);
    Assertions.assertEquals(17.0 / 3.0, centroid.get(1).asDouble(), 1e-9);

    // (f) norm block: histogram has the same shape as the numeric histogram.
    JsonNode norm = embedding.get("norm");
    Assertions.assertNotNull(norm, "embedding must carry a 'norm' block");
    JsonNode histogram = norm.get("histogram");
    Assertions.assertNotNull(histogram, "norm.histogram must be present when histogram=true");
    Assertions.assertEquals(20, histogram.size(), "norm histogram must have 20 bins");
    long histCountSum = 0;
    for (JsonNode entry : histogram) {
      Assertions.assertTrue(entry.has("value"), "histogram entry must have 'value'");
      Assertions.assertTrue(entry.has("count"), "histogram entry must have 'count'");
      Assertions.assertTrue(entry.has("ratio"), "histogram entry must have 'ratio'");
      histCountSum += entry.get("count").asLong();
    }
    Assertions.assertEquals(3, histCountSum, "norm histogram counts must sum to the valid count");

    // (g) norm KLL has the same shape as a scalar column's kll, and its median is a valid norm.
    //     Norms are {5, 5, 10}; the median is 5.0.
    JsonNode kll = norm.get("kll");
    Assertions.assertNotNull(kll, "norm.kll must be present when kll=true");
    Assertions.assertEquals("datasketches-native-v1", kll.get("kllFormat").asText());
    String base64 = kll.get("bytes").asText();
    Assertions.assertFalse(base64.isEmpty(), "norm.kll.bytes must not be empty");
    Assertions.assertNotNull(kll.get("buckets"), "norm.kll.buckets must be present");
    KllDoublesSketch sketch = KllAggregator.heapify(Base64.getDecoder().decode(base64));
    Assertions.assertEquals(5.0, sketch.getQuantile(0.5), 1e-9,
        "KLL median of norms {5,5,10} must be 5.0");
    Assertions.assertEquals(5.0, sketch.getMinItem(), 1e-9);
    Assertions.assertEquals(10.0, sketch.getMaxItem(), 1e-9);
  }

  // -------------------------------------------------------------------------
  // kll=false: norm block has histogram but no kll
  // -------------------------------------------------------------------------

  @Test
  void embeddingNormOmitsKllWhenDisabled() throws Exception {
    Dataset<Row> df = buildEmbeddingDataset();

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, false);
    JsonNode columns = MAPPER.readTree(json).get("columns");
    JsonNode norm = findColumn(columns, "emb").get("embedding").get("norm");

    Assertions.assertNotNull(norm.get("histogram"), "norm.histogram present when histogram=true");
    Assertions.assertNull(norm.get("kll"), "norm.kll absent when kll=false");
  }

  // -------------------------------------------------------------------------
  // all-rows-invalid: count=0, empty centroid, no norm kll, no throw
  // -------------------------------------------------------------------------

  @Test
  void allInvalidEmbeddingEmitsEmptyBlock() throws Exception {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("emb",
            DataTypes.createArrayType(DataTypes.DoubleType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create((Object) null));                       // null
    rows.add(RowFactory.create((Object) new ArrayList<Double>()));    // empty
    rows.add(RowFactory.create(Arrays.asList(1.0, null)));            // null element
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, true);
    JsonNode embedding = findColumn(MAPPER.readTree(json).get("columns"), "emb").get("embedding");

    Assertions.assertEquals(0, embedding.get("dimension").asInt());
    Assertions.assertEquals(0, embedding.get("count").asLong());
    Assertions.assertEquals(0, embedding.get("centroid").size(), "centroid must be empty");
    Assertions.assertNull(embedding.get("norm").get("kll"), "no norm.kll when count=0");
    Assertions.assertNull(embedding.get("norm").get("histogram"), "no norm.histogram when count=0");
  }

  // -------------------------------------------------------------------------
  // NaN element drops the row (never fed to KLL)
  // -------------------------------------------------------------------------

  @Test
  void nanElementDropsRow() throws Exception {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("emb",
            DataTypes.createArrayType(DataTypes.DoubleType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(Arrays.asList(3.0, 4.0)));             // norm 5, valid
    rows.add(RowFactory.create(Arrays.asList(Double.NaN, 1.0)));     // NaN element, dropped
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, true);
    JsonNode embedding = findColumn(MAPPER.readTree(json).get("columns"), "emb").get("embedding");

    Assertions.assertEquals(1, embedding.get("count").asLong(), "NaN-element row must be dropped");
    Assertions.assertEquals(3.0, embedding.get("centroid").get(0).asDouble(), 1e-9);
    Assertions.assertEquals(4.0, embedding.get("centroid").get(1).asDouble(), 1e-9);
  }

  // -------------------------------------------------------------------------
  // Infinite element drops the row (never poisons the norm histogram/KLL)
  // -------------------------------------------------------------------------

  @Test
  void infiniteElementDropsRow() throws Exception {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("emb",
            DataTypes.createArrayType(DataTypes.DoubleType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(Arrays.asList(3.0, 4.0)));                          // norm 5, valid
    rows.add(RowFactory.create(Arrays.asList(Double.POSITIVE_INFINITY, 1.0)));    // +Inf, dropped
    rows.add(RowFactory.create(Arrays.asList(1.0, Double.NEGATIVE_INFINITY)));    // -Inf, dropped
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, true);
    JsonNode embedding = findColumn(MAPPER.readTree(json).get("columns"), "emb").get("embedding");

    // (a) only the finite [3,4] vector is counted; both infinite rows are excluded.
    Assertions.assertEquals(1, embedding.get("count").asLong(),
        "rows with an infinite element must be dropped");
    Assertions.assertEquals(3.0, embedding.get("centroid").get(0).asDouble(), 1e-9);
    Assertions.assertEquals(4.0, embedding.get("centroid").get(1).asDouble(), 1e-9);

    // (b) the norm histogram and KLL stay finite (an infinite norm would poison both).
    //     The histogram bin "value" is a string label ("5.00 to 5.25"), not a number,
    //     so finiteness must be checked against the numeric KLL bucket boundaries
    //     (low_value/high_value are real doubles) and the sketch min/max.
    JsonNode norm = embedding.get("norm");
    KllDoublesSketch sketch =
        KllAggregator.heapify(Base64.getDecoder().decode(norm.get("kll").get("bytes").asText()));
    JsonNode buckets = norm.get("kll").get("buckets");
    Assertions.assertNotNull(buckets, "norm.kll.buckets must be present");
    for (JsonNode bucket : buckets) {
      Assertions.assertTrue(Double.isFinite(bucket.get("low_value").asDouble()),
          "norm KLL bucket low_value must be finite");
      Assertions.assertTrue(Double.isFinite(bucket.get("high_value").asDouble()),
          "norm KLL bucket high_value must be finite");
    }
    // The only finite vector is [3,4] (norm 5); an infinite norm would push min/max to
    // +/-Infinity, so asserting exact finite bounds fails the test if one leaked in.
    Assertions.assertTrue(Double.isFinite(sketch.getMinItem()), "norm KLL min must be finite");
    Assertions.assertTrue(Double.isFinite(sketch.getMaxItem()), "norm KLL max must be finite");
    Assertions.assertEquals(5.0, sketch.getMinItem(), 1e-9);
    Assertions.assertEquals(5.0, sketch.getMaxItem(), 1e-9);
    Assertions.assertTrue(Double.isFinite(sketch.getQuantile(0.5)), "norm KLL median must be finite");
  }

  // -------------------------------------------------------------------------
  // array<string> stays skipped; non-embedding columns are unchanged
  // -------------------------------------------------------------------------

  @Test
  void nonNumericArraySkippedAndScalarsUnchanged() throws Exception {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("c_double", DataTypes.DoubleType, true),
        DataTypes.createStructField("tags",
            DataTypes.createArrayType(DataTypes.StringType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1.0, Arrays.asList("a", "b")));
    rows.add(RowFactory.create(2.0, Arrays.asList("c")));
    rows.add(RowFactory.create(3.0, null));
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, false);
    JsonNode columns = MAPPER.readTree(json).get("columns");

    // array<string> is unsupported: skipped entirely (one column profiled).
    Assertions.assertEquals(1, columns.size(), "array<string> column must be skipped");
    JsonNode cDouble = findColumn(columns, "c_double");
    Assertions.assertNotNull(cDouble, "scalar column must still be profiled");

    // Scalar profile is unchanged: numeric dataType, no embedding key, standard numeric fields.
    Assertions.assertEquals("Fractional", cDouble.get("dataType").asText());
    Assertions.assertNull(cDouble.get("embedding"), "scalar column must not carry an embedding block");
    Assertions.assertEquals(2.0, cDouble.get("mean").asDouble(), 1e-9);
    Assertions.assertEquals(1.0, cDouble.get("minimum").asDouble(), 1e-9);
    Assertions.assertEquals(3.0, cDouble.get("maximum").asDouble(), 1e-9);
  }

  // -------------------------------------------------------------------------
  // array<float> element type is supported too
  // -------------------------------------------------------------------------

  @Test
  void arrayFloatIsSupported() throws Exception {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("emb",
            DataTypes.createArrayType(DataTypes.FloatType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(Arrays.asList(3.0f, 4.0f)));
    rows.add(RowFactory.create(Arrays.asList(6.0f, 8.0f)));
    Dataset<Row> df = SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);

    String json = new ColumnProfiler().profile(df, null, false, true, 20, true, true);
    JsonNode embedding = findColumn(MAPPER.readTree(json).get("columns"), "emb").get("embedding");

    Assertions.assertEquals(2, embedding.get("dimension").asInt());
    Assertions.assertEquals(2, embedding.get("count").asLong());
    // centroid = mean([3,6])=4.5 ; mean([4,8])=6.0
    Assertions.assertEquals(4.5, embedding.get("centroid").get(0).asDouble(), 1e-6);
    Assertions.assertEquals(6.0, embedding.get("centroid").get(1).asDouble(), 1e-6);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Builds a single-column {@code array<double>} fixture exercising every validity case:
   * valid vectors, a null row, an empty row, a null-element row, and a ragged-length row.
   */
  private Dataset<Row> buildEmbeddingDataset() {
    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("emb",
            DataTypes.createArrayType(DataTypes.DoubleType, true), true),
    });
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(Arrays.asList(3.0, 4.0)));            // norm 5,  valid
    rows.add(RowFactory.create(Arrays.asList(0.0, 5.0)));            // norm 5,  valid
    rows.add(RowFactory.create(Arrays.asList(6.0, 8.0)));            // norm 10, valid
    rows.add(RowFactory.create((Object) null));                     // null row
    rows.add(RowFactory.create((Object) new ArrayList<Double>()));  // empty row
    rows.add(RowFactory.create(Arrays.asList(1.0, null)));          // null element
    rows.add(RowFactory.create(Arrays.asList(1.0, 2.0, 3.0)));      // ragged (len 3)
    return SparkEngine.getInstance().getSparkSession().createDataFrame(rows, schema);
  }

  private JsonNode findColumn(JsonNode columns, String name) {
    for (JsonNode col : columns) {
      if (name.equals(col.get("column").asText())) {
        return col;
      }
    }
    return null;
  }
}
