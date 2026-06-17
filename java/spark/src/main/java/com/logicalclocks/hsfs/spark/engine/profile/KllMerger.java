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
import org.apache.datasketches.memory.Memory;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Merges per-batch native-format KLL sketches into a single aggregate sketch, then derives a
 * 100-element percentiles vector and a histogram-from-CDF. The Phase-2 unlock for rolling-window
 * reference distributions on HUDI/DELTA feature groups.
 *
 * <p>Called from the Python SDK via Py4J:
 *
 * <pre>
 * engine.get_instance()._jvm
 *   .com.logicalclocks.hsfs.spark.engine.profile.KllMerger
 *   .merge(baseB64List, histogramBins)
 * </pre>
 *
 * <p>Input: list of base64-encoded {@code KllDoublesSketch.toByteArray()} blobs, each emitted
 * by {@link ProfileJsonSerializer} when {@code kll=true} on a per-batch profile run. Empty
 * input returns a JSON object with empty arrays and no {@code kll} field.
 *
 * <p>Output JSON shape (stable contract, consumed by the Python merge driver):
 *
 * <pre>
 * {
 *   "percentiles": [p01, p02, ..., p99],
 *   "histogram":   [{"value":"X.XX to Y.YY","count":..,"ratio":..}, ...],
 *   "kll":         "&lt;base64 of merged sketch bytes&gt;",
 *   "kllFormat":   "datasketches-native-v1",
 *   "n":           &lt;total weight&gt;,
 *   "min":         &lt;double&gt;,
 *   "max":         &lt;double&gt;
 * }
 * </pre>
 *
 * <p>Histogram counts are approximated from the merged sketch's CDF; exact per-bin counts are
 * not recoverable from a KLL structure. The error is bounded by the KLL rank error
 * (~0.13% with K=2048) — acceptable for PSI/KL/JS/Hellinger on 10-20 bin histograms.
 */
public final class KllMerger {

  private static final String KLL_FORMAT = "datasketches-native-v1";
  private static final int PERCENTILE_COUNT = 99;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Fractional margin applied around a degenerate single-value range (min == max) so the
  // emitted bin label spans a non-zero, strictly-increasing range. Mirrors
  // _DEGENERATE_RANGE_MARGIN in the Python DistributionEngine, with this absolute fallback
  // when the value is zero.
  private static final double DEGENERATE_RANGE_MARGIN = 0.01;

  // Quantile fractions 0.01 .. 0.99 in 0.01 steps — matches the wire shape of
  // ProfileJsonSerializer's approxPercentiles (99 elements, see §3.4 of the plan).
  private static final double[] PERCENTILE_FRACTIONS = buildPercentileFractions();

  private KllMerger() {
    // Static-only utility; not instantiable.
  }

  /**
   * Merge per-batch KLL sketches into a single sketch and derive monitoring artifacts.
   *
   * @param base64Sketches list of base64-encoded sketch bytes (one per batch)
   * @param histogramBins number of equi-width bins for the approximated histogram
   * @return JSON string with percentiles, histogram, merged sketch bytes, and format tag
   */
  public static String merge(List<String> base64Sketches, int histogramBins) {
    Map<String, Object> out = new LinkedHashMap<String, Object>();

    KllDoublesSketch merged = null;
    if (base64Sketches != null) {
      for (String b64 : base64Sketches) {
        if (b64 == null || b64.isEmpty()) {
          continue;
        }
        byte[] bytes = Base64.getDecoder().decode(b64);
        KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(bytes));
        if (merged == null) {
          merged = sketch;
        } else {
          merged.merge(sketch);
        }
      }
    }

    if (merged == null || merged.isEmpty()) {
      out.put("percentiles", new double[0]);
      out.put("histogram", new Object[0]);
      out.put("n", 0L);
      return toJson(out);
    }

    double[] quantiles = merged.getQuantiles(PERCENTILE_FRACTIONS);
    double min = merged.getMinItem();
    double max = merged.getMaxItem();
    long totalWeight = merged.getN();

    out.put("percentiles", quantiles);
    out.put("histogram", buildHistogramFromCdf(merged, histogramBins, min, max, totalWeight));
    out.put("kll", Base64.getEncoder().encodeToString(merged.toByteArray()));
    out.put("kllFormat", KLL_FORMAT);
    out.put("n", totalWeight);
    out.put("min", min);
    out.put("max", max);
    return toJson(out);
  }

  /**
   * Build an equi-width histogram by sampling the merged sketch's CDF at bin boundaries.
   * Per-bin count ≈ N * (CDF(high) - CDF(low)).
   */
  private static Map<String, Object>[] buildHistogramFromCdf(
      KllDoublesSketch sketch, int bins, double min, double max, long totalWeight) {
    if (bins <= 0 || totalWeight <= 0) {
      @SuppressWarnings("unchecked")
      Map<String, Object>[] empty = (Map<String, Object>[]) new Map[0];
      return empty;
    }
    if (max <= min) {
      // Degenerate distribution: every observed value is identical (min == max). This is still
      // a valid, fully-populated distribution, so emit a single bin holding all the weight
      // rather than an empty histogram. An empty histogram would leave the synthetic reference
      // FDS unbinned, and downstream distribution metrics would fall back to epsilon-only
      // probabilities, distorting shift detection for constant-valued features. A small margin
      // keeps the bin label's range strictly increasing, matching the degenerate-range handling
      // on the Python side.
      double margin =
          min != 0.0 ? Math.abs(min) * DEGENERATE_RANGE_MARGIN : DEGENERATE_RANGE_MARGIN;
      @SuppressWarnings("unchecked")
      Map<String, Object>[] single = (Map<String, Object>[]) new Map[1];
      single[0] = buildBucket(min - margin, max + margin, totalWeight, totalWeight);
      return single;
    }
    double binWidth = (max - min) / bins;
    double[] splitPoints = new double[bins + 1];
    for (int i = 0; i <= bins; i++) {
      splitPoints[i] = min + i * binWidth;
    }
    // getCDF returns bin+2 values: cumulative probabilities at each split and a final 1.0.
    // We only need the cumulative values at split 0..bins.
    double[] cdf = sketch.getCDF(splitPoints);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] out = (Map<String, Object>[]) new Map[bins];
    for (int i = 0; i < bins; i++) {
      double low = splitPoints[i];
      double high = splitPoints[i + 1];
      double massFraction = Math.max(0.0, cdf[i + 1] - cdf[i]);
      long count = Math.round(massFraction * totalWeight);
      out[i] = buildBucket(low, high, count, totalWeight);
    }
    return out;
  }

  /**
   * Build a single Deequ-shaped histogram bucket. Ratio is derived from the rounded count so
   * {@code count} and {@code ratio} stay internally consistent and match what
   * {@link ProfileJsonSerializer} emits for per-batch buckets.
   */
  private static Map<String, Object> buildBucket(
      double low, double high, long count, long totalWeight) {
    double ratio = totalWeight > 0 ? (double) count / totalWeight : 0.0;
    Map<String, Object> bucket = new LinkedHashMap<String, Object>();
    bucket.put("value", String.format(Locale.ROOT, "%.2f to %.2f", low, high));
    bucket.put("count", count);
    bucket.put("ratio", ratio);
    return bucket;
  }

  private static double[] buildPercentileFractions() {
    double[] fractions = new double[PERCENTILE_COUNT];
    for (int i = 0; i < PERCENTILE_COUNT; i++) {
      fractions[i] = (i + 1) / 100.0;
    }
    return fractions;
  }

  private static String toJson(Map<String, Object> out) {
    try {
      return MAPPER.writeValueAsString(out);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialise merged-sketch JSON", e);
    }
  }
}
