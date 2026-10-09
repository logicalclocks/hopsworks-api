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
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Merges the profile of a commit's new rows into the registered statistics of the snapshot before
 * it, giving the statistics of the snapshot after it without a scan of the whole table.
 *
 * <p>Both sides carry the state written by {@link ColumnProfiler} with {@code mergeableState}: the
 * moments of the non-null values (count, sum, mean, M2, min, max), an HLL sketch of the distinct
 * values and, for a numeric column, a KLL sketch of the finite values. Counts and sums add, the
 * mean and the standard deviation follow the parallel variance formula, the sketches are unioned
 * and the percentiles are read from the merged KLL sketch. The output has the profiler's JSON
 * shape, with the merged state under {@code mergeable} so the next commit can merge again.
 *
 * <p>Exact uniqueness, correlations and exact histograms have no mergeable state, so a profile
 * with any of them is not merged: the caller profiles the whole snapshot instead.
 */
public final class ProfileMerger {
  static final String FORMAT = "datasketches-native-v1";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // the default of Spark's hll_sketch_agg; a union at a lower lgK would lose precision
  private static final int HLL_LG_K = 12;

  private ProfileMerger() {
  }

  /**
   * Merges the profile of new rows into the statistics of the snapshot before them.
   *
   * @param previousStatisticsJson the registered descriptive statistics of the previous snapshot,
   *                               a JSON array in the DTO shape (featureName, numNullValues,
   *                               extendedStatistics holding the mergeable state)
   * @param deltaProfileJson the profile of the new rows, as {@link ColumnProfiler} emits it with
   *                         mergeable state
   * @return the merged profile in the profiler's JSON shape
   * @throws IllegalArgumentException when a column has no previous statistics or no profile of
   *                                  the new rows, or either side lacks the mergeable state
   */
  public static String merge(String previousStatisticsJson, String deltaProfileJson) throws IOException {
    Map<String, JsonNode> previousByName = new HashMap<String, JsonNode>();
    for (JsonNode row : MAPPER.readTree(previousStatisticsJson)) {
      previousByName.put(row.path("featureName").asText(), row);
    }
    ArrayNode columns = MAPPER.createArrayNode();
    for (JsonNode delta : MAPPER.readTree(deltaProfileJson).path("columns")) {
      String name = delta.path("column").asText();
      JsonNode previous = previousByName.get(name);
      if (previous == null) {
        throw new IllegalArgumentException("No previous statistics to merge into for feature " + name);
      }
      columns.add(mergeColumn(name, previous, delta));
      previousByName.remove(name);
    }
    if (!previousByName.isEmpty()) {
      // a column the batch was not profiled for would drop out of the merged snapshot
      throw new IllegalArgumentException("No profile of the new rows for features " + previousByName.keySet());
    }
    ObjectNode out = MAPPER.createObjectNode();
    out.set("columns", columns);
    return MAPPER.writeValueAsString(out);
  }

  private static ObjectNode mergeColumn(String name, JsonNode previous, JsonNode delta) throws IOException {
    JsonNode previousExtended = extendedOf(previous);
    JsonNode previousState = mergeableOf(previousExtended, name);
    JsonNode deltaState = delta.path("mergeable");
    if (deltaState.isMissingNode() || !deltaState.has("hll")) {
      throw new IllegalArgumentException("The profile of the new rows carries no mergeable state for " + name);
    }
    long previousNulls = previous.path("numNullValues").asLong(0);
    long deltaNulls = delta.path("numRecordsNull").asLong(0);
    Moments merged = Moments.of(previousState.path("moments")).plus(Moments.of(deltaState.path("moments")));
    long total = merged.count + previousNulls + deltaNulls;

    ObjectNode out = MAPPER.createObjectNode();
    out.put("column", name);
    out.put("dataType", delta.path("dataType").asText());
    out.put("isDataTypeInferred", "false");
    out.put("completeness", total == 0 ? 0.0 : (double) merged.count / total);
    out.put("numRecordsNonNull", merged.count);
    out.put("numRecordsNull", previousNulls + deltaNulls);

    Union union = new Union(HLL_LG_K);
    union.update(HllSketch.heapify(Memory.wrap(bytes(previousState, "hll"))));
    union.update(HllSketch.heapify(Memory.wrap(bytes(deltaState, "hll"))));
    out.put("approximateNumDistinctValues", Math.round(union.getEstimate()));

    ObjectNode mergeable = out.putObject("mergeable");
    mergeable.put("format", FORMAT);
    mergeable.put("hll", Base64.getEncoder().encodeToString(union.getResult().toCompactByteArray()));

    String dataType = delta.path("dataType").asText();
    boolean numeric = "Fractional".equals(dataType) || "Integral".equals(dataType);
    if (numeric && merged.count > 0) {
      out.put("mean", merged.mean);
      out.put("maximum", merged.max);
      out.put("minimum", merged.min);
      out.put("sum", merged.sum);
      out.put("stdDev", Math.sqrt(Math.max(0.0, merged.m2 / merged.count)));
    }
    KllDoublesSketch sketch = numeric ? mergeKll(previousState, deltaState) : null;
    if (sketch != null && !sketch.isEmpty()) {
      ArrayNode percentiles = out.putArray("approxPercentiles");
      for (double quantile : sketch.getQuantiles(KllMerger.PERCENTILE_FRACTIONS)) {
        percentiles.add(quantile);
      }
      byte[] kllBytes = sketch.toByteArray();
      mergeable.put("kll", Base64.getEncoder().encodeToString(kllBytes));
      // a batch with no finite value carries no sidecar of its own, while the snapshot may
      if (delta.has("kll") || previousExtended.has("kll")) {
        out.set("kll", MAPPER.valueToTree(ProfileJsonSerializer.buildKllMap(kllBytes)));
      }
    }
    ObjectNode moments = mergeable.putObject("moments");
    moments.put("n", merged.count);
    if (merged.count > 0 && numeric) {
      moments.put("sum", Double.toString(merged.sum));
      moments.put("mean", Double.toString(merged.mean));
      moments.put("m2", Double.toString(merged.m2));
      moments.put("min", Double.toString(merged.min));
      moments.put("max", Double.toString(merged.max));
    }
    return out;
  }

  private static JsonNode extendedOf(JsonNode previous) throws IOException {
    JsonNode extended = previous.path("extendedStatistics");
    return extended.isTextual() ? MAPPER.readTree(extended.asText()) : extended;
  }

  private static JsonNode mergeableOf(JsonNode extended, String name) {
    JsonNode state = extended.path("mergeable");
    if (state.isMissingNode() || !state.has("hll")) {
      throw new IllegalArgumentException("The previous statistics of " + name + " carry no mergeable state");
    }
    return state;
  }

  private static byte[] bytes(JsonNode state, String key) {
    return Base64.getDecoder().decode(state.path(key).asText());
  }

  private static KllDoublesSketch mergeKll(JsonNode previousState, JsonNode deltaState) {
    KllDoublesSketch merged = null;
    for (JsonNode state : new JsonNode[] {previousState, deltaState}) {
      if (!state.has("kll")) {
        continue;
      }
      KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(bytes(state, "kll")));
      if (merged == null) {
        merged = sketch;
      } else {
        merged.merge(sketch);
      }
    }
    return merged;
  }

  /** The moments of a column's non-null values. Spark's min, max and mean propagate NaN and the infinities. */
  static final class Moments {
    final long count;
    final double sum;
    final double mean;
    final double m2;
    final double min;
    final double max;

    Moments(long count, double sum, double mean, double m2, double min, double max) {
      this.count = count;
      this.sum = sum;
      this.mean = mean;
      this.m2 = m2;
      this.min = min;
      this.max = max;
    }

    static Moments of(JsonNode node) {
      long count = node.path("n").asLong(0);
      if (count == 0 || !node.has("mean")) {
        return new Moments(count, 0.0, 0.0, 0.0, Double.NaN, Double.NaN);
      }
      return new Moments(count, Double.parseDouble(node.path("sum").asText()),
          Double.parseDouble(node.path("mean").asText()), Double.parseDouble(node.path("m2").asText()),
          Double.parseDouble(node.path("min").asText()), Double.parseDouble(node.path("max").asText()));
    }

    Moments plus(Moments other) {
      if (count == 0) {
        return other;
      }
      if (other.count == 0) {
        return this;
      }
      long total = count + other.count;
      double meanDelta = other.mean - mean;
      double mergedMean = (mean * count + other.mean * other.count) / total;
      double mergedM2 = m2 + other.m2 + meanDelta * meanDelta * ((double) count * other.count / total);
      return new Moments(total, sum + other.sum, mergedMean, mergedM2, sparkMin(min, other.min),
          sparkMax(max, other.max));
    }

    // Spark orders NaN above every number: it is the maximum as soon as one value is NaN, and
    // never the minimum unless every value is.
    private static double sparkMin(double a, double b) {
      if (Double.isNaN(a)) {
        return b;
      }
      if (Double.isNaN(b)) {
        return a;
      }
      return Math.min(a, b);
    }

    private static double sparkMax(double a, double b) {
      if (Double.isNaN(a) || Double.isNaN(b)) {
        return Double.NaN;
      }
      return Math.max(a, b);
    }
  }
}
