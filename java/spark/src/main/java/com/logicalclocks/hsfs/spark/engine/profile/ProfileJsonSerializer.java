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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.datasketches.kll.KllDoublesSketch;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialises column profiles to JSON matching the Deequ 2.0.7-spark-3.5 wire format.
 *
 * <p>Key ordering is preserved via {@link LinkedHashMap}. Numeric columns emit:
 * {@code column, dataType, isDataTypeInferred, completeness, numRecordsNonNull, numRecordsNull,
 * distinctness, entropy, uniqueness, approximateNumDistinctValues, exactNumDistinctValues,
 * mean, maximum, minimum, sum, stdDev, correlations, histogram, kll, approxPercentiles}.
 *
 * <p>Categorical/Boolean columns omit {@code mean, maximum, minimum, sum, stdDev, correlations,
 * kll, approxPercentiles}.
 *
 * <p>Top-level envelope: {@code {"columns": [...]}}. No additional top-level keys (Deequ
 * does not emit a top-level {@code count} field — verified against the captured baseline).
 *
 * <p>KLL divergence from Deequ: this serialiser emits {@code kll} only when the profile
 * carries non-null {@link ColumnProfile#getKllBytes()}. Deequ always-emits {@code kll}
 * for numeric columns. When emitted, the Phase 1.5 native format is:
 * <pre>{@code
 *   "kll": {
 *     "kllFormat": "datasketches-native-v1",
 *     "bytes": "<base64(KllDoublesSketch.toByteArray())>",
 *     "buckets": [{"low_value":..,"high_value":..,"count":..,"ratio":..}, ...]
 *   }
 * }</pre>
 */
class ProfileJsonSerializer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Serialises a list of column profiles to the Deequ-compatible JSON envelope.
   *
   * @param profiles list of column profile objects produced by ColumnProfiler
   * @return JSON string with top-level {@code {"columns": [...]}}
   * @throws JsonProcessingException if Jackson serialisation fails
   */
  String toJson(List<ColumnProfile> profiles) throws JsonProcessingException {
    List<Map<String, Object>> columnList = new ArrayList<Map<String, Object>>(profiles.size());
    for (ColumnProfile profile : profiles) {
      columnList.add(toMap(profile));
    }
    Map<String, Object> envelope = new LinkedHashMap<String, Object>();
    envelope.put("columns", columnList);
    return MAPPER.writeValueAsString(envelope);
  }

  private Map<String, Object> toMap(ColumnProfile profile) {
    if (profile.isNumeric()) {
      return toNumericMap(profile);
    } else if (profile.isEmbedding()) {
      return toEmbeddingMap(profile);
    } else {
      return toCategoricalMap(profile);
    }
  }

  private Map<String, Object> toNumericMap(ColumnProfile profile) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("column", profile.getColumnName());
    map.put("dataType", profile.getDataType());
    map.put("isDataTypeInferred", "false");
    map.put("completeness", profile.getCompleteness());
    map.put("numRecordsNonNull", profile.getNumRecordsNonNull());
    map.put("numRecordsNull", profile.getNumRecordsNull());
    map.put("distinctness", profile.getDistinctness());
    map.put("entropy", profile.getEntropy());
    map.put("uniqueness", profile.getUniqueness());
    map.put("approximateNumDistinctValues", profile.getApproximateNumDistinctValues());
    map.put("exactNumDistinctValues", profile.getExactNumDistinctValues());
    map.put("mean", profile.getMean());
    map.put("maximum", profile.getMaximum());
    map.put("minimum", profile.getMinimum());
    map.put("sum", profile.getSum());
    map.put("stdDev", profile.getStdDev());
    if (profile.getCorrelations() != null) {
      map.put("correlations", buildCorrelationsList(profile.getCorrelations()));
    }
    if (profile.getHistogram() != null) {
      map.put("histogram", profile.getHistogram());
    }
    if (profile.getKllBytes() != null) {
      map.put("kll", buildKllMap(profile.getKllBytes(), profile.getMinimum(),
          profile.getMaximum(), profile.getNumRecordsNonNull()));
    }
    if (profile.getApproxPercentiles() != null) {
      map.put("approxPercentiles", toDoubleList(profile.getApproxPercentiles()));
    }
    return map;
  }

  private Map<String, Object> toCategoricalMap(ColumnProfile profile) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("column", profile.getColumnName());
    map.put("dataType", profile.getDataType());
    map.put("isDataTypeInferred", "false");
    map.put("completeness", profile.getCompleteness());
    map.put("numRecordsNonNull", profile.getNumRecordsNonNull());
    map.put("numRecordsNull", profile.getNumRecordsNull());
    map.put("distinctness", profile.getDistinctness());
    map.put("entropy", profile.getEntropy());
    map.put("uniqueness", profile.getUniqueness());
    map.put("approximateNumDistinctValues", profile.getApproximateNumDistinctValues());
    map.put("exactNumDistinctValues", profile.getExactNumDistinctValues());
    if (profile.getHistogram() != null) {
      map.put("histogram", profile.getHistogram());
    }
    return map;
  }

  /**
   * Builds the JSON map for an embedding (numeric-array) column.
   *
   * <p>Emits the standard count/completeness/null fields (computed over the array column itself)
   * plus an {@code embedding} block:
   * <pre>{@code
   *   "embedding": {
   *     "dimension": <int>,
   *     "count": <long>,
   *     "norm": {
   *       "histogram": [ {value, count, ratio}, ... ],   // when histogram=true
   *       "kll": { "kllFormat":..., "bytes":..., "buckets":[...] }   // when kll=true
   *     },
   *     "centroid": [ <double>, ... ]
   *   }
   * }</pre>
   *
   * <p>The {@code norm.kll} map reuses {@link #buildKllMap} so its shape matches a scalar
   * numeric column's {@code kll} exactly.
   */
  private Map<String, Object> toEmbeddingMap(ColumnProfile profile) {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("column", profile.getColumnName());
    map.put("dataType", profile.getDataType());
    map.put("isDataTypeInferred", "false");
    map.put("completeness", profile.getCompleteness());
    map.put("numRecordsNonNull", profile.getNumRecordsNonNull());
    map.put("numRecordsNull", profile.getNumRecordsNull());
    map.put("distinctness", profile.getDistinctness());
    map.put("entropy", profile.getEntropy());
    map.put("uniqueness", profile.getUniqueness());
    map.put("approximateNumDistinctValues", profile.getApproximateNumDistinctValues());
    map.put("exactNumDistinctValues", profile.getExactNumDistinctValues());

    ColumnProfile.EmbeddingStats stats = profile.getEmbedding();
    Map<String, Object> embedding = new LinkedHashMap<String, Object>();
    embedding.put("dimension", stats.getDimension());
    embedding.put("count", stats.getValidCount());
    embedding.put("norm", buildNormMap(stats));
    embedding.put("centroid", toDoubleList(stats.getCentroid()));
    map.put("embedding", embedding);
    return map;
  }

  private Map<String, Object> buildNormMap(ColumnProfile.EmbeddingStats stats) {
    Map<String, Object> norm = new LinkedHashMap<String, Object>();
    if (stats.getNormHistogram() != null) {
      norm.put("histogram", stats.getNormHistogram());
    }
    if (stats.getNormKllBytes() != null) {
      norm.put("kll", buildKllMap(stats.getNormKllBytes(), stats.getNormMin(),
          stats.getNormMax(), stats.getNormNonNull()));
    }
    return norm;
  }

  private List<Map<String, Object>> buildCorrelationsList(Map<String, Double> correlations) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(correlations.size());
    for (Map.Entry<String, Double> entry : correlations.entrySet()) {
      Map<String, Object> corr = new LinkedHashMap<String, Object>();
      corr.put("column", entry.getKey());
      corr.put("correlation", entry.getValue());
      list.add(corr);
    }
    return list;
  }

  /**
   * Builds the Phase 1.5 native KLL map.
   *
   * <p>The {@code buckets} field is derived from the sketch CDF using 20 equal-width bin edges
   * matching the numeric histogram, so read-time consumers have a histogram-from-sketch shape
   * without re-merging.
   */
  private Map<String, Object> buildKllMap(byte[] kllBytes, double minValue, double maxValue,
      long totalRows) {
    KllDoublesSketch sketch = KllAggregator.heapify(kllBytes);
    Map<String, Object> kll = new LinkedHashMap<String, Object>();
    kll.put("kllFormat", "datasketches-native-v1");
    kll.put("bytes", Base64.getEncoder().encodeToString(kllBytes));
    kll.put("buckets", buildKllBuckets(sketch, minValue, maxValue, totalRows));
    return kll;
  }

  private List<Map<String, Object>> buildKllBuckets(KllDoublesSketch sketch,
      double minValue, double maxValue, long totalRows) {
    int numBuckets = 20;
    double range = maxValue - minValue;
    double binWidth = range > 0 ? range / numBuckets : 1.0;

    double[] splitPoints = new double[numBuckets - 1];
    for (int ii = 0; ii < numBuckets - 1; ii++) {
      splitPoints[ii] = minValue + (ii + 1) * binWidth;
    }

    double[] cdf = sketch.getCDF(splitPoints);

    List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>(numBuckets);
    for (int ii = 0; ii < numBuckets; ii++) {
      double low = minValue + ii * binWidth;
      double high = low + binWidth;
      double fraction = (ii == 0) ? cdf[0] : cdf[ii] - cdf[ii - 1];
      long count = Math.round(fraction * totalRows);
      double ratio = totalRows > 0 ? (double) count / totalRows : 0.0;

      Map<String, Object> bucket = new LinkedHashMap<String, Object>();
      bucket.put("low_value", low);
      bucket.put("high_value", high);
      bucket.put("count", count);
      bucket.put("ratio", ratio);
      buckets.add(bucket);
    }
    return buckets;
  }

  private List<Double> toDoubleList(double[] arr) {
    List<Double> list = new ArrayList<Double>(arr.length);
    for (double value : arr) {
      list.add(value);
    }
    return list;
  }
}
