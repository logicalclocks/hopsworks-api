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
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Replacement for Deequ's {@code ColumnProfilerRunner} producing JSON wire-compatible with
 * Deequ 2.0.7-spark-3.5 for all keys except {@code kll}.
 *
 * <h3>KLL gating divergence from Deequ</h3>
 *
 * <p>Deequ always emits {@code kll} for numeric columns regardless of {@code withKLLProfiling()};
 * the toggle is a no-op in 2.0.7-spark-3.5.
 * This implementation emits {@code kll} only when {@code kll=true}, aligning with the Phase-1
 * API contract.
 * The {@code kll=false} path produces smaller profiles.
 * The golden-parity test (task #6) accounts for this known divergence.
 *
 * <h3>Entropy computation</h3>
 *
 * <p>Shannon entropy is derived from the exact per-value frequency distribution via
 * {@code groupBy(col).count()} per column.
 * For numeric columns where all values are unique this equals
 * {@code ln(exactNumDistinctValues)}, but the groupBy is required for correctness when
 * duplicates exist.
 *
 * <h3>Uniqueness formula</h3>
 *
 * <p>{@code uniqueness = singletons / nonNull}: Deequ's exact definition (fraction of values
 * appearing exactly once).
 * The singleton count comes from the same per-value frequency pass as entropy, so no
 * additional Spark job is paid for it.
 * (An earlier shortcut, {@code (2 * exactDistinct - nonNull) / nonNull}, is only equivalent
 * when no value occurs more than twice and undercounts otherwise.)
 *
 * <h3>stdDev</h3>
 *
 * <p>Uses Spark's {@code stddev_pop()} (population standard deviation, dividing by n).
 * Deequ's StandardDeviation metric also uses population stddev; verified against the baseline.
 */
public class ColumnProfiler {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnProfiler.class);

  // Quantile fractions for approxPercentiles: 0.01, 0.02, ..., 0.99 (99 elements).
  private static final double[] PERCENTILE_FRACTIONS;

  static {
    PERCENTILE_FRACTIONS = new double[99];
    for (int ii = 0; ii < 99; ii++) {
      PERCENTILE_FRACTIONS[ii] = (ii + 1) / 100.0;
    }
  }

  private final HistogramBuilder histogramBuilder = new HistogramBuilder();
  private final ProfileJsonSerializer serializer = new ProfileJsonSerializer();

  /**
   * Profiles the given dataframe and returns a JSON string matching the Deequ wire format.
   *
   * @param df source dataframe
   * @param restrictToColumns columns to profile; null or empty means all columns
   * @param correlation whether to compute pairwise Pearson correlations for numeric columns
   * @param histogram whether to compute histogram bins
   * @param histogramBins number of histogram bins (used only when histogram=true)
   * @param exactUniqueness whether to compute exact distinct counts via countDistinct
   * @param kll whether to compute KLL sketches and derived percentiles for numeric columns
   * @return JSON string with top-level {@code {"columns": [...]}}
   */
  public String profile(Dataset<Row> df,
      List<String> restrictToColumns,
      boolean correlation,
      boolean histogram,
      int histogramBins,
      boolean exactUniqueness,
      boolean kll) {

    List<ColumnInfo> columns = selectColumns(df, restrictToColumns);
    try {
      if (columns.isEmpty()) {
        return serializer.toJson(new ArrayList<ColumnProfile>());
      }

      Row aggRow = computeScalars(df, columns, exactUniqueness);
      // entropy and the uniqueness singleton count are derived from exact per-value
      // frequencies; only pay for the per-column groupBy when exact uniqueness
      // stats were requested
      Map<String, ValueFrequencyStats> frequencyStatsMap = exactUniqueness
          ? computeValueFrequencyStats(df, columns)
          : new LinkedHashMap<String, ValueFrequencyStats>();

      Map<String, Map<String, Double>> correlationMap =
          new LinkedHashMap<String, Map<String, Double>>();
      if (correlation) {
        computeCorrelations(df, columns, correlationMap);
      }

      List<ColumnProfile> profiles = new ArrayList<ColumnProfile>(columns.size());
      for (ColumnInfo col : columns) {
        ColumnProfile cp = buildProfile(df, col, aggRow, frequencyStatsMap, correlationMap,
            histogram, histogramBins, kll, exactUniqueness);
        profiles.add(cp);
      }

      return serializer.toJson(profiles);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialise column profiles to JSON", e);
    }
  }

  // ---------------------------------------------------------------------------
  // Column selection and type inference
  // ---------------------------------------------------------------------------

  private List<ColumnInfo> selectColumns(Dataset<Row> df, List<String> restrictToColumns) {
    StructType schema = df.schema();
    List<ColumnInfo> result = new ArrayList<ColumnInfo>();
    for (StructField field : schema.fields()) {
      String name = field.name();
      if (restrictToColumns != null && !restrictToColumns.isEmpty()
          && !restrictToColumns.contains(name)) {
        continue;
      }
      String profileType = inferProfileType(field.dataType());
      if (profileType == null) {
        LOG.warn("Skipping column '{}': unsupported type {}", name, field.dataType());
        continue;
      }
      result.add(new ColumnInfo(name, profileType));
    }
    return result;
  }

  private String inferProfileType(DataType dt) {
    if (dt instanceof IntegerType || dt instanceof LongType
        || dt instanceof ShortType || dt instanceof ByteType) {
      return "Integral";
    }
    if (dt instanceof FloatType || dt instanceof DoubleType || dt instanceof DecimalType) {
      return "Fractional";
    }
    if (dt instanceof StringType) {
      return "String";
    }
    if (dt instanceof BooleanType) {
      return "Boolean";
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Pass 1: single agg() for all scalar statistics
  // ---------------------------------------------------------------------------

  private Row computeScalars(Dataset<Row> df, List<ColumnInfo> columns, boolean exactUniqueness) {
    List<Column> exprs = new ArrayList<Column>();
    for (ColumnInfo col : columns) {
      Column cc = functions.col(col.name);
      String nn = col.name;

      exprs.add(functions.count(cc).alias(nn + "__nonnull"));
      exprs.add(functions.sum(functions.when(cc.isNull(), 1).otherwise(0)).alias(nn + "__nullcount"));
      exprs.add(functions.approx_count_distinct(cc).alias(nn + "__approx_distinct"));
      if (exactUniqueness) {
        exprs.add(functions.countDistinct(cc).alias(nn + "__exact_distinct"));
      }
      if (isNumeric(col.profileType)) {
        Column cn = cc.cast("double");
        exprs.add(functions.min(cn).alias(nn + "__min"));
        exprs.add(functions.max(cn).alias(nn + "__max"));
        exprs.add(functions.mean(cn).alias(nn + "__mean"));
        // Deequ's StandardDeviation uses population stddev (divides by n), not Bessel-corrected
        // sample stddev. Use stddev_pop to match the Deequ baseline byte-for-byte.
        exprs.add(functions.stddev_pop(cn).alias(nn + "__stddev"));
        exprs.add(functions.sum(cn).alias(nn + "__sum"));
      }
    }
    Column first = exprs.get(0);
    Column[] rest = exprs.subList(1, exprs.size()).toArray(new Column[0]);
    return df.agg(first, rest).first();
  }

  // ---------------------------------------------------------------------------
  // Pass 2: per-value frequencies (entropy + uniqueness singletons) — one groupBy per column
  // ---------------------------------------------------------------------------

  /** Per-column stats derived from the exact per-value frequency distribution. */
  private static final class ValueFrequencyStats {
    private final double entropy;
    private final long singletons;

    private ValueFrequencyStats(double entropy, long singletons) {
      this.entropy = entropy;
      this.singletons = singletons;
    }
  }

  private Map<String, ValueFrequencyStats> computeValueFrequencyStats(Dataset<Row> df,
      List<ColumnInfo> columns) {
    Map<String, ValueFrequencyStats> result = new LinkedHashMap<String, ValueFrequencyStats>();
    for (ColumnInfo col : columns) {
      result.put(col.name, computeColumnValueFrequencyStats(df, col.name));
    }
    return result;
  }

  private ValueFrequencyStats computeColumnValueFrequencyStats(Dataset<Row> df,
      String columnName) {
    Column cc = functions.col(columnName);
    List<Row> rows = df.filter(cc.isNotNull())
        .groupBy(cc)
        .count()
        .select(functions.col("count"))
        .collectAsList();

    if (rows.isEmpty()) {
      return new ValueFrequencyStats(0.0, 0L);
    }
    long total = 0;
    long singletons = 0;
    for (Row row : rows) {
      long count = row.getLong(0);
      total += count;
      if (count == 1) {
        singletons++;
      }
    }
    if (total == 0) {
      return new ValueFrequencyStats(0.0, 0L);
    }
    double entropy = 0.0;
    for (Row row : rows) {
      long count = row.getLong(0);
      if (count > 0) {
        double pp = (double) count / total;
        entropy -= pp * Math.log(pp);
      }
    }
    return new ValueFrequencyStats(entropy, singletons);
  }

  // ---------------------------------------------------------------------------
  // Pass 3: Pearson correlations (numeric pairs)
  // ---------------------------------------------------------------------------

  private void computeCorrelations(Dataset<Row> df, List<ColumnInfo> columns,
      Map<String, Map<String, Double>> correlationMap) {
    List<String> numericCols = new ArrayList<String>();
    for (ColumnInfo col : columns) {
      if (isNumeric(col.profileType)) {
        numericCols.add(col.name);
      }
    }
    for (String colA : numericCols) {
      Map<String, Double> corrForA = new LinkedHashMap<String, Double>();
      for (String colB : numericCols) {
        double corr;
        if (colA.equals(colB)) {
          corr = 1.0;
        } else {
          corr = df.stat().corr(colA, colB);
        }
        corrForA.put(colB, corr);
      }
      correlationMap.put(colA, corrForA);
    }
  }

  // ---------------------------------------------------------------------------
  // Profile assembly
  // ---------------------------------------------------------------------------

  private ColumnProfile buildProfile(Dataset<Row> df, ColumnInfo col, Row aggRow,
      Map<String, ValueFrequencyStats> frequencyStatsMap,
      Map<String, Map<String, Double>> correlationMap,
      boolean histogram, int histogramBins, boolean kll, boolean exactUniqueness) {
    String nn = col.name;

    // count(col) returns Long; sum(when(...).otherwise(0)) returns Long when the sum
    // fits, but Spark can pass it back as Integer internally — cast via Number for safety.
    long nonNull = aggRow.getAs(nn + "__nonnull");
    long nullCount = ((Number) aggRow.getAs(nn + "__nullcount")).longValue();
    long total = nonNull + nullCount;
    long approxDistinct = aggRow.getAs(nn + "__approx_distinct");
    // exactNumDistinctValues + derived stats (distinctness/uniqueness/entropy) are only
    // meaningful when exactUniqueness=true. When false they stay null and the serializer
    // omits their keys, so consumers deserialize them as absent instead of a bogus 0.
    Long exactDistinct = exactUniqueness ? (Long) aggRow.getAs(nn + "__exact_distinct") : null;
    ValueFrequencyStats freqStats = frequencyStatsMap.get(nn);

    double completeness = total > 0 ? (double) nonNull / total : 0.0;
    Double distinctness = exactUniqueness
        ? (nonNull > 0 ? (double) exactDistinct / nonNull : 0.0) : null;
    Double uniqueness = exactUniqueness
        ? (nonNull > 0 && freqStats != null ? (double) freqStats.singletons / nonNull : 0.0)
        : null;
    Double entropy = exactUniqueness
        ? (freqStats != null ? freqStats.entropy : 0.0) : null;

    ColumnProfile.Builder builder = new ColumnProfile.Builder()
        .columnName(nn)
        .dataType(col.profileType)
        .completeness(completeness)
        .numRecordsNonNull(nonNull)
        .numRecordsNull(nullCount)
        .distinctness(distinctness)
        .entropy(entropy)
        .uniqueness(uniqueness)
        .approximateNumDistinctValues(approxDistinct)
        .exactNumDistinctValues(exactDistinct);

    if (isNumeric(col.profileType)) {
      buildNumericFields(df, col, aggRow, nonNull, correlationMap, histogram, histogramBins, kll,
          builder);
    } else {
      if (histogram) {
        List<Map<String, Object>> hist = histogramBuilder.buildCategorical(
            df, nn, histogramBins, nonNull);
        builder.histogram(hist);
      }
    }

    return builder.build();
  }

  private void buildNumericFields(Dataset<Row> df, ColumnInfo col, Row aggRow, long nonNullVal,
      Map<String, Map<String, Double>> correlationMap,
      boolean histogram, int histogramBins, boolean kll,
      ColumnProfile.Builder builder) {
    String nn = col.name;
    Double minVal = aggRow.getAs(nn + "__min");
    Double maxVal = aggRow.getAs(nn + "__max");
    Double meanVal = aggRow.getAs(nn + "__mean");
    Double stdDevVal = aggRow.getAs(nn + "__stddev");
    Double sumVal = aggRow.getAs(nn + "__sum");

    builder.minimum(minVal)
        .maximum(maxVal)
        .mean(meanVal)
        .stdDev(stdDevVal)
        .sum(sumVal);

    if (correlationMap.containsKey(nn)) {
      builder.correlations(correlationMap.get(nn));
    }

    if (histogram && minVal != null && maxVal != null) {
      List<Map<String, Object>> hist = histogramBuilder.buildNumeric(
          df, nn, minVal, maxVal, histogramBins, nonNullVal);
      builder.histogram(hist);
    }

    if (kll && nonNullVal > 0) {
      byte[] kllBytes = computeKll(df, nn);
      builder.kllBytes(kllBytes);
      KllDoublesSketch sketch = KllAggregator.heapify(kllBytes);
      double[] percentiles = sketch.getQuantiles(PERCENTILE_FRACTIONS);
      builder.approxPercentiles(percentiles);
    } else if (!kll && nonNullVal > 0) {
      double[] percentiles = computeApproxPercentiles(df, nn);
      builder.approxPercentiles(percentiles);
    }
  }

  // ---------------------------------------------------------------------------
  // KLL per-column aggregation
  // ---------------------------------------------------------------------------

  private byte[] computeKll(Dataset<Row> df, String columnName) {
    return new KllAggregator().computeSketch(df, columnName);
  }

  // ---------------------------------------------------------------------------
  // Approximate percentiles (kll=false path)
  // ---------------------------------------------------------------------------

  private double[] computeApproxPercentiles(Dataset<Row> df, String columnName) {
    Column col = functions.col(columnName).cast("double");
    Column[] fractionLiterals = new Column[PERCENTILE_FRACTIONS.length];
    for (int ii = 0; ii < PERCENTILE_FRACTIONS.length; ii++) {
      fractionLiterals[ii] = functions.lit(PERCENTILE_FRACTIONS[ii]);
    }
    Column fractionsArray = functions.array(fractionLiterals);
    Row result = df.agg(
        functions.percentile_approx(col, fractionsArray, functions.lit(10000))).first();

    List<Double> resultList = result.getList(0);
    double[] percentiles = new double[resultList.size()];
    for (int ii = 0; ii < resultList.size(); ii++) {
      percentiles[ii] = resultList.get(ii);
    }
    return percentiles;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private boolean isNumeric(String profileType) {
    return "Fractional".equals(profileType) || "Integral".equals(profileType);
  }

  static final class ColumnInfo {

    final String name;
    final String profileType;

    ColumnInfo(String name, String profileType) {
      this.name = name;
      this.profileType = profileType;
    }
  }
}
