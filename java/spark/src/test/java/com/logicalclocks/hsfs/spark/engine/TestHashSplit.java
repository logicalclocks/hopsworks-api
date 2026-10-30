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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import com.logicalclocks.hsfs.spark.TrainingDatasetBundle;
import com.logicalclocks.hsfs.spark.FeatureView;
import com.logicalclocks.hsfs.StatisticsConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestHashSplit {

  // Shared with the Python client tests: both must give these hashes and splits.
  private static final File GOLDEN = new File("../../python/tests/fixtures/hash_split_golden.json");

  private static JsonNode golden() throws IOException {
    return new ObjectMapper().readTree(GOLDEN);
  }

  private static Object decode(JsonNode node, HashSplit.Type type) {
    if (node == null || node.isNull()) {
      return null;
    }
    switch (type.kind) {
      case "int":
        switch (type.name) {
          case "tinyint":
            return (byte) node.intValue();
          case "smallint":
            return (short) node.intValue();
          case "int":
          case "integer":
            return node.intValue();
          default:
            return node.longValue();
        }
      case "double":
        return node.isTextual() ? Double.parseDouble(node.asText()) : node.doubleValue();
      case "float":
        return (float) (node.isTextual() ? Double.parseDouble(node.asText()) : node.doubleValue());
      case "timestamp":
        return "timestamp_ntz".equals(type.name)
            ? LocalDateTime.parse(node.asText()) : Instant.parse(node.asText());
      case "date":
        return LocalDate.parse(node.asText());
      case "decimal":
        return new BigDecimal(node.asText());
      case "binary": {
        String hex = node.asText();
        byte[] raw = new byte[hex.length() / 2];
        for (int i = 0; i < raw.length; i++) {
          raw[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return raw;
      }
      case "struct": {
        Object[] fields = new Object[type.fieldNames.size()];
        for (int i = 0; i < fields.length; i++) {
          // The frame's field names may differ in case from the declared ones.
          JsonNode field = null;
          for (Iterator<String> names = node.fieldNames(); names.hasNext(); ) {
            String name = names.next();
            if (name.equalsIgnoreCase(type.fieldNames.get(i))) {
              field = node.get(name);
            }
          }
          fields[i] = decode(field, type.fieldTypes.get(i));
        }
        return RowFactory.create(fields);
      }
      case "array": {
        List<Object> items = new ArrayList<>();
        node.forEach(item -> items.add(decode(item, type.element)));
        return items;
      }
      case "map": {
        Map<Object, Object> entries = new HashMap<>();
        node.fields().forEachRemaining(e -> entries.put(
            decode(new TextNode(e.getKey()), type.key), decode(e.getValue(), type.element)));
        return entries;
      }
      case "boolean":
        return node.booleanValue();
      default:
        return node.asText();
    }
  }

  private static List<String> types(JsonNode golden) {
    List<String> types = new ArrayList<>();
    golden.get("columns").forEach(column -> types.add(column.get(1).asText()));
    return types;
  }

  private static List<Object[]> rows(JsonNode golden) {
    HashSplit.Type[] parsed = HashSplit.parseTypes(types(golden));
    List<Object[]> rows = new ArrayList<>();
    for (JsonNode row : golden.get("rows")) {
      Object[] values = new Object[parsed.length];
      for (int i = 0; i < values.length; i++) {
        values[i] = decode(row.get(i), parsed[i]);
      }
      rows.add(values);
    }
    return rows;
  }

  @Test
  public void testRowHashesMatchGolden() throws IOException {
    // Arrange
    JsonNode golden = golden();
    HashSplit.Type[] parsed = HashSplit.parseTypes(types(golden));
    TrainingDataset trainingDataset = new TrainingDataset();
    trainingDataset.setSeed(golden.get("seed").longValue());

    // Act
    List<String> hashes = new ArrayList<>();
    for (Object[] row : rows(golden)) {
      hashes.add(String.format("%016x", HashSplit.rowHash(row, parsed, HashSplit.salt(trainingDataset))));
    }

    // Assert
    List<String> expected = new ArrayList<>();
    golden.get("expected").get("hashes").forEach(hash -> expected.add(hash.asText()));
    Assertions.assertEquals(expected, hashes);
  }

  @Test
  public void testSplitMatchesGolden() throws IOException {
    assertSplitMatchesGolden(false);
  }

  @Test
  public void testLegacySplitMatchesGolden() throws IOException {
    assertSplitMatchesGolden(true);
  }

  private void assertSplitMatchesGolden(boolean legacy) throws IOException {
    // Arrange
    JsonNode golden = golden();
    List<String> types = types(golden);
    List<TrainingDatasetFeature> features = new ArrayList<>();
    StringBuilder ddl = new StringBuilder();
    for (int i = 0; i < types.size(); i++) {
      JsonNode column = golden.get("columns").get(i);
      String name = column.get(0).asText();
      features.add(new TrainingDatasetFeature(name, types.get(i)));
      // The frame's type, where it differs from the declared one.
      ddl.append(i == 0 ? "" : ", ").append(name).append(' ').append(column.get(column.size() - 1).asText());
    }
    List<Split> splits = new ArrayList<>();
    golden.get("splits").forEach(s -> splits.add(new Split(s.get(0).asText(), (float) s.get(1).doubleValue())));
    TrainingDataset trainingDataset = new TrainingDataset();
    trainingDataset.setSplits(splits);
    trainingDataset.setId(golden.get("legacy").get("id").intValue());
    trainingDataset.setVersion(golden.get("legacy").get("version").intValue());
    trainingDataset.setSeed(legacy ? null : golden.get("seed").longValue());
    trainingDataset.setSplitMethod(HashSplit.METHOD);
    JsonNode buckets = golden.get("expected").get(legacy ? "legacy_buckets" : "buckets");

    SparkSession session = SparkEngine.getInstance().getSparkSession();
    String zone = session.conf().get("spark.sql.session.timeZone");
    String java8Api = session.conf().get("spark.sql.datetime.java8API.enabled");
    session.conf().set("spark.sql.session.timeZone", "America/New_York");
    // Instant and LocalDate keep proleptic Gregorian dates such as 0001-01-01 exact.
    session.conf().set("spark.sql.datetime.java8API.enabled", "true");
    try {
      List<Row> rows = new ArrayList<>();
      for (Object[] row : rows(golden)) {
        rows.add(RowFactory.create(row));
      }
      Dataset<Row> dataset = session.createDataFrame(rows, StructType.fromDDL(ddl.toString())).repartition(3);

      // Act
      Dataset<Row>[] result = HashSplit.split(dataset, trainingDataset, features);

      // Assert
      Assertions.assertEquals(splits.size(), result.length);
      for (int index = 0; index < result.length; index++) {
        Set<Long> expected = new HashSet<>();
        for (int row = 0; row < buckets.size(); row++) {
          if (buckets.get(row).intValue() == index) {
            expected.add((long) row);
          }
        }
        Set<Long> actual = new HashSet<>();
        result[index].select("k").collectAsList().forEach(r -> actual.add(r.getLong(0)));
        Assertions.assertEquals(expected, actual, splits.get(index).getName());
      }
    } finally {
      session.conf().set("spark.sql.session.timeZone", zone);
      session.conf().set("spark.sql.datetime.java8API.enabled", java8Api);
    }
  }

  @Test
  public void testSplitIgnoresTheSplitListOrder() {
    // The server returns the persisted splits in another order than the creation: the rows per split
    // name stay the same.
    // Arrange
    Dataset<Row> dataset = SparkEngine.getInstance().getSparkSession().range(500).toDF();
    Map<String, Float> fractions = new HashMap<>();
    fractions.put("train", 0.6f);
    fractions.put("validation", 0.2f);
    fractions.put("test", 0.2f);
    List<Map<String, Set<Long>>> results = new ArrayList<>();
    for (String[] order : new String[][] {{"train", "validation", "test"}, {"test", "train", "validation"}}) {
      TrainingDataset trainingDataset = new TrainingDataset();
      List<Split> splits = new ArrayList<>();
      for (String name : order) {
        splits.add(new Split(name, fractions.get(name)));
      }
      trainingDataset.setSplits(splits);
      trainingDataset.setSeed(42L);
      trainingDataset.setSplitMethod(HashSplit.METHOD);

      // Act
      Dataset<Row>[] result = HashSplit.split(dataset, trainingDataset, null);

      // Assert
      Map<String, Set<Long>> byName = new HashMap<>();
      for (int i = 0; i < result.length; i++) {
        Set<Long> ids = new HashSet<>();
        result[i].select("id").collectAsList().forEach(r -> ids.add(r.getLong(0)));
        byName.put(order[i], ids);
      }
      results.add(byName);
      HashSplit.unpersist(result);
    }
    Assertions.assertEquals(results.get(0), results.get(1));
    results.get(0).values().forEach(ids -> Assertions.assertFalse(ids.isEmpty()));
  }

  @Test
  public void testTrainingDatasetWithoutSplitMethodKeepsTheLegacySplit() {
    // Arrange
    Dataset<Row> dataset = SparkEngine.getInstance().getSparkSession().range(200).toDF().coalesce(1);
    TrainingDataset trainingDataset = new TrainingDataset();
    List<Split> splits = new ArrayList<>();
    splits.add(new Split("train", 0.7f));
    splits.add(new Split("test", 0.3f));
    trainingDataset.setSplits(splits);
    trainingDataset.setSeed(7L);
    Dataset<Row>[] legacy = dataset.randomSplit(new double[] {0.7f, 0.3f}, 7L);

    // Act
    Dataset<Row>[] result = SparkEngine.randomSplit(dataset, trainingDataset, null);

    // Assert
    for (int i = 0; i < result.length; i++) {
      Assertions.assertEquals(legacy[i].collectAsList(), result[i].collectAsList());
    }
  }

  @Test
  public void testFailedSplitLeavesNoCachedDataset() {
    // Arrange
    SparkSession session = SparkEngine.getInstance().getSparkSession();
    session.catalog().clearCache();
    Dataset<Row> dataset = session.range(50).toDF().withColumn("broken",
        functions.udf((UDF1<Long, Long>) v -> {
          throw new IllegalStateException("unreadable row");
        }, DataTypes.LongType).apply(functions.col("id")));
    TrainingDataset trainingDataset = new TrainingDataset();
    List<Split> splits = new ArrayList<>();
    splits.add(new Split("train", 0.7f));
    splits.add(new Split("test", 0.3f));
    trainingDataset.setSplits(splits);
    trainingDataset.setSeed(7L);
    trainingDataset.setSplitMethod(HashSplit.METHOD);

    // Act
    Assertions.assertThrows(Exception.class, () -> SparkEngine.randomSplit(dataset, trainingDataset, null));

    // Assert
    Assertions.assertTrue(session.sharedState().cacheManager().isEmpty());
  }

  @Test
  public void testFailedInMemoryStatisticsReleasesTheSplits() {
    // Arrange: hash splits cached as the in-memory read leaves them; statistics on, and no Hopsworks client, so
    // saving the statistics fails for real.
    SparkSession session = SparkEngine.getInstance().getSparkSession();
    session.catalog().clearCache();
    TrainingDataset trainingDataset = new TrainingDataset();
    List<Split> splits = new ArrayList<>();
    splits.add(new Split("train", 0.7f));
    splits.add(new Split("test", 0.3f));
    trainingDataset.setSplits(splits);
    trainingDataset.setSeed(7L);
    trainingDataset.setSplitMethod(HashSplit.METHOD);
    trainingDataset.setStatisticsConfig(new StatisticsConfig(true, false, false, false));
    Dataset<Row>[] datasets = SparkEngine.randomSplit(session.range(50).toDF(), trainingDataset, null);
    Assertions.assertFalse(session.sharedState().cacheManager().isEmpty());

    // Act
    Assertions.assertThrows(Exception.class,
        () -> new FeatureViewEngine().computeStatisticsOrRelease(new FeatureView(), trainingDataset, datasets));

    // Assert
    Assertions.assertTrue(session.sharedState().cacheManager().isEmpty());
  }

  @Test
  public void testBundleUnpersistReleasesTheSplitsAfterALabelProjection() {
    // Arrange
    SparkSession session = SparkEngine.getInstance().getSparkSession();
    session.catalog().clearCache();
    TrainingDataset trainingDataset = new TrainingDataset();
    List<Split> splits = new ArrayList<>();
    splits.add(new Split("train", 0.7f));
    splits.add(new Split("test", 0.3f));
    trainingDataset.setSplits(splits);
    trainingDataset.setSeed(7L);
    trainingDataset.setSplitMethod(HashSplit.METHOD);
    Dataset<Row>[] datasets = SparkEngine.randomSplit(
        session.range(50).toDF().withColumn("label", functions.col("id").mod(2)), trainingDataset, null);
    Map<String, Dataset<Row>> bySplit = new HashMap<>();
    bySplit.put("train", datasets[0]);
    bySplit.put("test", datasets[1]);
    TrainingDatasetBundle bundle = new TrainingDatasetBundle(1, bySplit, Arrays.asList("label"));
    bundle.getDataset("train", true).forEach(Dataset::count);

    // Act
    bundle.unpersist();

    // Assert
    Assertions.assertTrue(session.sharedState().cacheManager().isEmpty());
  }
}
