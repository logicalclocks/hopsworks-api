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
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Unit-level verification of {@link KllMerger}. No Spark session required — operates directly
 * on {@link KllDoublesSketch} byte arrays.
 */
class KllMergerSmokeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void testMergeProducesCorrectPercentilesAndHistogram() throws Exception {
    // Build two batches of N(0,1) draws, each with 5000 samples.
    Random rngA = new Random(42L);
    Random rngB = new Random(43L);
    List<Double> combined = new ArrayList<>();

    KllDoublesSketch sketchA = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 5000; i++) {
      double v = rngA.nextGaussian();
      sketchA.update(v);
      combined.add(v);
    }

    KllDoublesSketch sketchB = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 5000; i++) {
      double v = rngB.nextGaussian();
      sketchB.update(v);
      combined.add(v);
    }

    String b64A = Base64.getEncoder().encodeToString(sketchA.toByteArray());
    String b64B = Base64.getEncoder().encodeToString(sketchB.toByteArray());

    // Act: merge via KllMerger.
    String json = KllMerger.merge(Arrays.asList(b64A, b64B), 10);
    JsonNode result = MAPPER.readTree(json);

    // Assertions

    // Contract fields present.
    Assertions.assertEquals("datasketches-native-v1", result.get("kllFormat").asText());
    Assertions.assertEquals(10000L, result.get("n").asLong());
    Assertions.assertTrue(result.has("percentiles"));
    Assertions.assertTrue(result.has("histogram"));
    Assertions.assertTrue(result.has("kll"));
    Assertions.assertTrue(result.has("min"));
    Assertions.assertTrue(result.has("max"));

    // Percentiles: 99 entries, monotonic non-decreasing.
    JsonNode perc = result.get("percentiles");
    Assertions.assertEquals(99, perc.size());
    double prev = perc.get(0).asDouble();
    for (int i = 1; i < perc.size(); i++) {
      double cur = perc.get(i).asDouble();
      Assertions.assertTrue(cur >= prev,
          "percentiles must be non-decreasing, found " + cur + " < " + prev + " at index " + i);
      prev = cur;
    }

    // Median (index 49 is p0.50) within rank error of the true combined median (~0).
    double sketchMedian = perc.get(49).asDouble();
    Collections.sort(combined);
    double trueMedian = combined.get(combined.size() / 2);
    Assertions.assertEquals(trueMedian, sketchMedian, 5e-2,
        "merged-sketch median must be within 5e-2 of the combined-dataset median");

    // Histogram: 10 bins, counts sum to ~10000 (rounding may drop ±1 per bin).
    JsonNode hist = result.get("histogram");
    Assertions.assertEquals(10, hist.size());
    long totalCount = 0;
    double totalRatio = 0.0;
    for (int i = 0; i < hist.size(); i++) {
      JsonNode bucket = hist.get(i);
      // Wire shape must match HistogramBuilder.buildNumeric: {"value":"X.XX to Y.YY","count":...,"ratio":...}
      Assertions.assertTrue(bucket.has("value"),
          "histogram bucket must have 'value' key (got: " + bucket + ")");
      Assertions.assertFalse(bucket.has("low_value"),
          "histogram bucket must not have 'low_value' key");
      Assertions.assertFalse(bucket.has("high_value"),
          "histogram bucket must not have 'high_value' key");
      // value format: "X.XX to Y.YY"
      String value = bucket.get("value").asText();
      Assertions.assertTrue(value.contains(" to "),
          "bucket value must be in '%.2f to %.2f' format, got: " + value);
      String[] parts = value.split(" to ");
      Assertions.assertEquals(2, parts.length, "bucket value must have exactly two parts");
      Assertions.assertTrue(bucket.has("count"));
      Assertions.assertTrue(bucket.has("ratio"));
      totalCount += bucket.get("count").asLong();
      totalRatio += bucket.get("ratio").asDouble();
    }
    // Rounding tolerance: each bin rounds N*ratio independently, so sum may drift up to bins/2.
    Assertions.assertTrue(Math.abs(totalCount - 10000) <= 5,
        "histogram count sum must be within 5 of N=10000, got " + totalCount);
    // Ratios sum to CDF(max) - CDF(min), which is slightly less than 1.0 because the sketch's
    // CDF at the observed min is a small positive (counts the min itself, not 0). ~1e-4 drift
    // on 10k samples is normal.
    Assertions.assertEquals(1.0, totalRatio, 1e-3, "ratios sum close to 1.0");

    // Merged kll bytes round-trip: can heapify and get same n.
    String mergedB64 = result.get("kll").asText();
    byte[] mergedBytes = Base64.getDecoder().decode(mergedB64);
    KllDoublesSketch roundTrip = KllDoublesSketch.heapify(Memory.wrap(mergedBytes));
    Assertions.assertEquals(10000L, roundTrip.getN());
    Assertions.assertEquals(sketchMedian, roundTrip.getQuantile(0.5), 1e-12,
        "round-tripped sketch median must match the p0.50 in the percentiles array");
  }

  @Test
  void testMergeEmptyInputReturnsEmptyArtifacts() throws Exception {
    JsonNode result = MAPPER.readTree(KllMerger.merge(Collections.<String>emptyList(), 10));
    Assertions.assertEquals(0, result.get("n").asLong());
    Assertions.assertEquals(0, result.get("percentiles").size());
    Assertions.assertEquals(0, result.get("histogram").size());
    Assertions.assertFalse(result.has("kll"), "empty input must not emit a merged kll field");
  }

  @Test
  void testMergeSingleSketchRoundTrips() throws Exception {
    Random rng = new Random(7L);
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 1000; i++) {
      sketch.update(rng.nextGaussian());
    }
    String b64 = Base64.getEncoder().encodeToString(sketch.toByteArray());

    JsonNode result = MAPPER.readTree(KllMerger.merge(Collections.singletonList(b64), 5));
    Assertions.assertEquals(1000L, result.get("n").asLong());
    Assertions.assertEquals(99, result.get("percentiles").size());
    Assertions.assertEquals(5, result.get("histogram").size());
  }

  @Test
  void testMergeMalformedBase64RaisesException() {
    // Malformed base64 must propagate a clear exception rather than silently returning
    // a wrong/empty result. The Python caller's broad `except` translates this into a
    // fallback-to-re-profile. Assert it does raise so that behaviour stays load-bearing.
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      KllMerger.merge(Collections.singletonList("not-valid-base64!@#$"), 10);
    });
  }

  @Test
  void testHistogramValueKeyMatchesHistogramBuilderShape() throws Exception {
    // Regression test for the wire-format alignment fix (item #1):
    // KllMerger must emit {"value":"X.XX to Y.YY","count":...,"ratio":...} —
    // the same shape as HistogramBuilder.buildNumeric — so that
    // DistributionEngine._align_numeric_histogram can parse both paths identically.
    Random rng = new Random(99L);
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 5000; i++) {
      sketch.update(rng.nextGaussian());
    }
    String b64 = Base64.getEncoder().encodeToString(sketch.toByteArray());
    JsonNode result = MAPPER.readTree(KllMerger.merge(Collections.singletonList(b64), 5));
    JsonNode hist = result.get("histogram");

    Assertions.assertFalse(hist.isEmpty(), "histogram must not be empty");
    for (int i = 0; i < hist.size(); i++) {
      JsonNode bucket = hist.get(i);

      // Key "value" present and in "%.2f to %.2f" format
      Assertions.assertTrue(bucket.has("value"),
          "bucket[" + i + "] must have 'value' key (HistogramBuilder.buildNumeric shape)");
      String valueStr = bucket.get("value").asText();
      Assertions.assertTrue(valueStr.matches("-?[0-9]+\\.[0-9]{2} to -?[0-9]+\\.[0-9]{2}"),
          "bucket[" + i + "] value must match '%.2f to %.2f' pattern, got: " + valueStr);

      // Keys "low_value" / "high_value" must not exist
      Assertions.assertFalse(bucket.has("low_value"),
          "bucket[" + i + "] must not have 'low_value' key");
      Assertions.assertFalse(bucket.has("high_value"),
          "bucket[" + i + "] must not have 'high_value' key");
    }
  }

  @Test
  void testMergeConstantFeatureProducesPopulatedSingleBin() throws Exception {
    // Regression test for review item P2: when every merged value is identical (min == max),
    // the distribution is degenerate but still valid and fully populated. The merger must emit
    // a single bin holding all the weight rather than an empty histogram — an empty histogram
    // leaves the synthetic reference FDS unbinned and collapses downstream distribution metrics
    // to epsilon-only probabilities, suppressing shift detection for constant features.
    double constant = 5.0;
    KllDoublesSketch sketchA = KllDoublesSketch.newHeapInstance(2048);
    KllDoublesSketch sketchB = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 1000; i++) {
      sketchA.update(constant);
      sketchB.update(constant);
    }
    String b64A = Base64.getEncoder().encodeToString(sketchA.toByteArray());
    String b64B = Base64.getEncoder().encodeToString(sketchB.toByteArray());

    JsonNode result = MAPPER.readTree(KllMerger.merge(Arrays.asList(b64A, b64B), 10));

    Assertions.assertEquals(2000L, result.get("n").asLong());
    Assertions.assertEquals(constant, result.get("min").asDouble(), 1e-12);
    Assertions.assertEquals(constant, result.get("max").asDouble(), 1e-12);

    JsonNode hist = result.get("histogram");
    Assertions.assertEquals(1, hist.size(),
        "degenerate (min == max) distribution must produce exactly one bin");
    JsonNode bucket = hist.get(0);
    Assertions.assertEquals(2000L, bucket.get("count").asLong(),
        "the single bin must hold all the weight");
    Assertions.assertEquals(1.0, bucket.get("ratio").asDouble(), 1e-12);

    // The bin label must span a strictly-increasing, non-zero range so DistributionEngine can
    // parse it into usable edges (margin mirrors _DEGENERATE_RANGE_MARGIN on the Python side).
    String valueStr = bucket.get("value").asText();
    Assertions.assertTrue(valueStr.matches("-?[0-9]+\\.[0-9]{2} to -?[0-9]+\\.[0-9]{2}"),
        "bucket value must match '%.2f to %.2f' pattern, got: " + valueStr);
    String[] parts = valueStr.split(" to ");
    Assertions.assertTrue(Double.parseDouble(parts[0]) < Double.parseDouble(parts[1]),
        "degenerate bin must have lower < upper, got: " + valueStr);
  }

  @Test
  void testMergeHistogramRatioMatchesCountFraction() throws Exception {
    // Regression test for the ratio-semantics fix (review M1): ratio must equal
    // count / totalWeight (matching the Deequ wire schema and ProfileJsonSerializer),
    // NOT the raw CDF mass fraction.
    Random rng = new Random(11L);
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(2048);
    for (int i = 0; i < 10000; i++) {
      sketch.update(rng.nextGaussian());
    }
    String b64 = Base64.getEncoder().encodeToString(sketch.toByteArray());

    JsonNode result = MAPPER.readTree(KllMerger.merge(Collections.singletonList(b64), 10));
    long n = result.get("n").asLong();
    JsonNode hist = result.get("histogram");
    for (int i = 0; i < hist.size(); i++) {
      long count = hist.get(i).get("count").asLong();
      double ratio = hist.get(i).get("ratio").asDouble();
      double expectedRatio = n > 0 ? (double) count / n : 0.0;
      Assertions.assertEquals(expectedRatio, ratio, 1e-12,
          "bucket[" + i + "] ratio must equal count / n");
    }
  }
}
