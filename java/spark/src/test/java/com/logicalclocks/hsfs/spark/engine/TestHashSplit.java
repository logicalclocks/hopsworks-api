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
import com.logicalclocks.hsfs.Split;
import com.logicalclocks.hsfs.TrainingDatasetFeature;
import com.logicalclocks.hsfs.spark.TrainingDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
        return node.longValue();
      case "double":
        return node.isTextual() ? Double.parseDouble(node.asText()) : node.doubleValue();
      case "float":
        return (float) (node.isTextual() ? Double.parseDouble(node.asText()) : node.doubleValue());
      case "timestamp":
        return Instant.parse(node.asText());
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
          fields[i] = decode(node.get(type.fieldNames.get(i)), type.fieldTypes.get(i));
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
        node.fields().forEachRemaining(e -> entries.put(e.getKey(), decode(e.getValue(), type.element)));
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
      String name = golden.get("columns").get(i).get(0).asText();
      features.add(new TrainingDatasetFeature(name, types.get(i)));
      ddl.append(i == 0 ? "" : ", ").append(name).append(' ').append(types.get(i));
    }
    List<Split> splits = new ArrayList<>();
    golden.get("splits").forEach(s -> splits.add(new Split(s.get(0).asText(), (float) s.get(1).doubleValue())));
    TrainingDataset trainingDataset = new TrainingDataset();
    trainingDataset.setSplits(splits);
    trainingDataset.setId(golden.get("legacy").get("id").intValue());
    trainingDataset.setVersion(golden.get("legacy").get("version").intValue());
    trainingDataset.setSeed(legacy ? null : golden.get("seed").longValue());
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
}
