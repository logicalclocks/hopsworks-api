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
import org.apache.spark.sql.types.ArrayType;
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
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Replacement for Deequ's {@code ColumnProfilerRunner} producing JSON wire-compatible with
 * Deequ 2.0.7-spark-3.5 for all keys except {@code kll}.
 *
 * <h2>KLL gating divergence from Deequ</h2>
 *
 * <p>Deequ always emits {@code kll} for numeric columns regardless of {@code withKLLProfiling()};
 * the toggle is a no-op in 2.0.7-spark-3.5.
 * This implementation emits {@code kll} only when {@code kll=true}, aligning with the Phase-1
 * API contract.
 * The {@code kll=false} path produces smaller profiles.
 * The golden-parity test (task #6) accounts for this known divergence.
 *
 * <h2>Entropy computation</h2>
 *
 * <p>Shannon entropy is derived from the exact per-value frequency distribution via
 * {@code groupBy(col).count()} per column, reduced on the executors so only one row per column
 * reaches the driver.
 * The same pass yields the exact distinct count and the singleton count.
 *
 * <h2>Uniqueness formula</h2>
 *
 * <p>{@code uniqueness = singletons / nonNull}: Deequ's exact definition (fraction of values
 * appearing exactly once).
 * The singleton count comes from the same per-value frequency pass as entropy, so no
 * additional Spark job is paid for it.
 * (An earlier shortcut, {@code (2 * exactDistinct - nonNull) / nonNull}, is only equivalent
 * when no value occurs more than twice and undercounts otherwise.)
 *
 * <h2>stdDev</h2>
 *
 * <p>Uses Spark's {@code stddev_pop()} (population standard deviation, dividing by n).
 * Deequ's StandardDeviation metric also uses population stddev; verified against the baseline.
 *
 * <h2>Correlations</h2>
 *
 * <p>Pearson correlation as {@code DataFrameStatFunctions.corr} computes it, which reads a NULL
 * as 0 and returns NaN where the variance is zero or there are no rows.
 * {@code corr(coalesce(x, 0), coalesce(y, 0))} reproduces it bit for bit, so all pairs can be
 * computed in a few aggregations instead of one Spark job per ordered pair.
 *
 * <h2>KLL sketches and numeric histograms</h2>
 *
 * <p>KLL sketches come from Spark's native {@code kll_sketch_agg_double} inside the scalar
 * aggregations.
 * Numeric histogram bins are exact counts over the finite min/max of the scalar pass, for all
 * numeric columns in one aggregation after it.
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

  // Aggregate expressions per agg() call.
  // Wide aggregations lose whole-stage codegen and take long to plan; past this size they are
  // split into several aggregations over the persisted input.
  // Untuned: revisit with the wide-table loadtests.
  private static final int MAX_AGG_EXPRESSIONS = 1000;

  private final HistogramBuilder histogramBuilder = new HistogramBuilder();
  private final ProfileJsonSerializer serializer = new ProfileJsonSerializer();
  private final int maxAggExpressions;

  public ColumnProfiler() {
    this(MAX_AGG_EXPRESSIONS);
  }

  ColumnProfiler(int maxAggExpressions) {
    this.maxAggExpressions = maxAggExpressions;
  }

  /**
   * Profiles the given dataframe and returns a JSON string matching the Deequ wire format.
   *
   * @param df source dataframe
   * @param restrictToColumns columns to profile; null or empty means all columns
   * @param correlation whether to compute pairwise Pearson correlations for numeric columns
   * @param histogram whether to compute histogram bins
   * @param histogramBins number of histogram bins (used only when histogram=true)
   * @param exactUniqueness whether to compute exact distinct counts, uniqueness and entropy
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
    return profile(df, restrictToColumns, correlation, histogram, histogramBins, exactUniqueness, kll, false);
  }

  /**
   * Profiles the columns of a dataframe, optionally with the state incremental statistics merge into.
   *
   * @param mergeableState also emit, per column, the state a later profile of more rows can be
   *                       merged into this one with: an HLL sketch of the distinct values and,
   *                       for a numeric column, a KLL sketch of the finite values; the moments
   *                       come from the regular scalars. See {@link ProfileMerger}.
   */
  public String profile(Dataset<Row> df,
      List<String> restrictToColumns,
      boolean correlation,
      boolean histogram,
      int histogramBins,
      boolean exactUniqueness,
      boolean kll,
      boolean mergeableState) {

    List<ColumnInfo> columns = selectColumns(df, restrictToColumns);
    if (columns.isEmpty()) {
      return toJson(new ArrayList<ColumnProfile>());
    }

    List<List<Column>> scalarChunks = scalarAggregations(columns, kll, mergeableState);
    List<String> numericCols = numericColumnNames(columns);
    List<List<Column>> correlationChunks = correlation
        ? chunk(correlationPairs(numericCols)) : new ArrayList<List<Column>>();
    int passes = scalarChunks.size() + correlationChunks.size()
        + (histogram && !numericCols.isEmpty() ? 1 : 0)
        + (exactUniqueness ? columns.size()
            : histogram ? columns.size() - numericCols.size() : 0);

    Dataset<Row> input = df.select(columnsOf(columns));
    // Every pass would otherwise re-read the source, which for a feature group means a Hudi or
    // Delta scan from HopsFS each time.
    boolean persisted = passes > 1;
    if (persisted) {
      input = input.persist(StorageLevel.MEMORY_AND_DISK());
    }
    try {
      Map<String, Object> scalars = aggregate(input, scalarChunks);
      Map<String, Map<String, Double>> correlationMap = correlation
          ? correlations(numericCols, aggregate(input, correlationChunks))
          : new LinkedHashMap<String, Map<String, Double>>();
      // The bin grids need the finite min/max of the scalar pass.
      Map<String, long[]> binCounts = histogram
          ? HistogramBuilder.binCounts(input, binRanges(columns, scalars, histogramBins), histogramBins)
          : new HashMap<String, long[]>();

      List<ColumnProfile> profiles = new ArrayList<ColumnProfile>(columns.size());
      for (ColumnInfo col : columns) {
        profiles.add(buildProfile(input, col, scalars, binCounts, correlationMap,
            histogram, histogramBins, kll, exactUniqueness, mergeableState));
      }
      return toJson(profiles);
    } finally {
      if (persisted) {
        input.unpersist();
      }
    }
  }

  private String toJson(List<ColumnProfile> profiles) {
    try {
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
  // Pass 1: scalar statistics, one agg() per chunk of columns
  // ---------------------------------------------------------------------------

  private List<List<Column>> scalarAggregations(List<ColumnInfo> columns, boolean kll, boolean mergeableState) {
    List<List<Column>> perColumn = new ArrayList<List<Column>>(columns.size());
    for (ColumnInfo col : columns) {
      perColumn.add(scalarExpressions(col, kll, mergeableState));
    }
    return chunk(perColumn);
  }

  private List<Column> scalarExpressions(ColumnInfo col, boolean kll, boolean mergeableState) {
    List<Column> exprs = new ArrayList<Column>();
    Column cc = functions.col(col.name);
    String nn = col.name;

    if (mergeableState) {
      // The native HLL aggregate takes no DOUBLE, so every column is sketched as text; two
      // profiles of the same column agree on the rendering, which is all a union needs.
      exprs.add(functions.call_function("hll_sketch_agg", cc.cast("string")).alias(nn + "__hll"));
    }

    exprs.add(functions.count(cc).alias(nn + "__nonnull"));
    // count(when(isNull)) rather than sum(when(isNull, 1).otherwise(0)): sum returns NULL
    // over zero rows, and the read side unboxes this into a long.
    exprs.add(functions.count(functions.when(cc.isNull(), 1)).alias(nn + "__nullcount"));
    exprs.add(functions.approx_count_distinct(cc).alias(nn + "__approx_distinct"));
    if (isNumeric(col.profileType)) {
      Column cn = cc.cast("double");
      exprs.add(functions.min(cn).alias(nn + "__min"));
      exprs.add(functions.max(cn).alias(nn + "__max"));
      exprs.add(functions.mean(cn).alias(nn + "__mean"));
      // Deequ's StandardDeviation uses population stddev (divides by n), not Bessel-corrected
      // sample stddev.
      // Use stddev_pop to match the Deequ baseline byte-for-byte.
      exprs.add(functions.stddev_pop(cn).alias(nn + "__stddev"));
      exprs.add(functions.sum(cn).alias(nn + "__sum"));
      // Finite-only copies for the binning: one NaN makes __max NaN and one infinity
      // makes it infinite, and neither works as a bin edge.
      // The emitted minimum/maximum keep Spark's values for Deequ parity.
      // Same pass, no extra Spark job.
      Column finite = functions.when(isFinite(cn), cn);
      exprs.add(functions.min(finite).alias(nn + "__min_finite"));
      exprs.add(functions.max(finite).alias(nn + "__max_finite"));
      exprs.add(functions.count(finite).alias(nn + "__nfinite"));
      if (kll || mergeableState) {
        exprs.add(KllAggregator.sketch(cn).alias(nn + "__kll"));
      }
      if (!kll) {
        exprs.add(functions.percentile_approx(cn, percentileFractions(), functions.lit(10000))
            .alias(nn + "__percentiles"));
      }
    }
    return exprs;
  }

  /** Packs expression groups into aggregations of at most maxAggExpressions each. */
  private List<List<Column>> chunk(List<List<Column>> groups) {
    List<List<Column>> chunks = new ArrayList<List<Column>>();
    List<Column> current = new ArrayList<Column>();
    for (List<Column> group : groups) {
      if (!current.isEmpty() && current.size() + group.size() > maxAggExpressions) {
        chunks.add(current);
        current = new ArrayList<Column>();
      }
      current.addAll(group);
    }
    if (!current.isEmpty()) {
      chunks.add(current);
    }
    return chunks;
  }

  /** Runs each aggregation and returns all their results keyed by alias. */
  private static Map<String, Object> aggregate(Dataset<Row> df, List<List<Column>> chunks) {
    Map<String, Object> values = new HashMap<String, Object>();
    for (List<Column> exprs : chunks) {
      Row row = df.agg(exprs.get(0), exprs.subList(1, exprs.size()).toArray(new Column[0])).first();
      StructField[] fields = row.schema().fields();
      for (int ii = 0; ii < fields.length; ii++) {
        Object value = null;
        if (!row.isNullAt(ii)) {
          value = fields[ii].dataType() instanceof ArrayType ? row.getList(ii) : row.get(ii);
        }
        values.put(fields[ii].name(), value);
      }
    }
    return values;
  }

  /**
   * True when the equi-width grid of {@code bins} over {@code [min, max]} has strictly
   * increasing edges - what {@code getCDF} demands of its split points, and what a bin label
   * needs in order to describe the bound it names.
   *
   * <p>Checked literally, edge by edge, because every shortcut has a hole: a finite range
   * can overflow to infinity, a finite width can underflow to zero, and a width that is
   * finite and positive can still be smaller than the spacing of doubles at {@code min}, so
   * that later edges round onto earlier ones. A zero range is fine: the histogram emits a
   * single bin for it and the bucket grid falls back to a width of 1.
   * No bins is no grid, so histogram_bins=0 yields an empty histogram.
   */
  static boolean hasUsableBinGrid(double min, double max, int bins) {
    if (bins <= 0) {
      return false;
    }
    double range = max - min;
    if (!Double.isFinite(range)) {
      return false;
    }
    if (range == 0.0) {
      return true;
    }
    double width = range / bins;
    double previous = min;
    for (int i = 1; i < bins; i++) {
      // The same formula HistogramBuilder and ProfileJsonSerializer use for their edges.
      double edge = min + i * width;
      if (!(edge > previous)) {
        return false;
      }
      previous = edge;
    }
    return true;
  }

  /**
   * Spark predicate selecting the finite values of a double column. Spark orders NaN above
   * +Infinity, so one comparison excludes NaN and both infinities. Matches what
   * {@link KllAggregator} feeds its sketch, so the histogram and the buckets bin the same rows.
   */
  static Column isFinite(Column doubleCol) {
    return functions.abs(doubleCol).lt(functions.lit(Double.POSITIVE_INFINITY));
  }

  // ---------------------------------------------------------------------------
  // Pass 2: per-value frequencies (exact distinct, singletons, entropy) - one groupBy per column
  // ---------------------------------------------------------------------------

  /** Per-column stats derived from the exact per-value frequency distribution. */
  private static final class ValueFrequencyStats {
    private final long distinct;
    private final long singletons;
    private final double entropy;

    private ValueFrequencyStats(long distinct, long singletons, double entropy) {
      this.distinct = distinct;
      this.singletons = singletons;
      this.entropy = entropy;
    }
  }

  private static ValueFrequencyStats frequencyStats(Dataset<Row> valueCounts, long nonNull) {
    if (nonNull == 0) {
      return new ValueFrequencyStats(0L, 0L, 0.0);
    }
    Column share = functions.col("count").divide(functions.lit((double) nonNull));
    Row row = valueCounts.agg(
        functions.count(functions.lit(1)),
        functions.count(functions.when(functions.col("count").equalTo(1), 1)),
        functions.sum(share.multiply(functions.log(share)))).first();
    double entropy = row.isNullAt(2) ? 0.0 : -row.getDouble(2);
    return new ValueFrequencyStats(row.getLong(0), row.getLong(1), entropy);
  }

  // ---------------------------------------------------------------------------
  // Pass 3: Pearson correlations (numeric pairs), one agg() per chunk of pairs
  // ---------------------------------------------------------------------------

  private static List<List<Column>> correlationPairs(List<String> numericCols) {
    List<List<Column>> pairs = new ArrayList<List<Column>>();
    for (int ii = 0; ii < numericCols.size(); ii++) {
      for (int jj = ii + 1; jj < numericCols.size(); jj++) {
        pairs.add(Collections.singletonList(functions.corr(
            zeroForNull(numericCols.get(ii)), zeroForNull(numericCols.get(jj)))
            .alias(correlationAlias(ii, jj))));
      }
    }
    return pairs;
  }

  /** Finite min/max of every numeric column that has a usable bin grid over its finite values. */
  private static Map<String, double[]> binRanges(List<ColumnInfo> columns,
      Map<String, Object> scalars, int histogramBins) {
    Map<String, double[]> ranges = new LinkedHashMap<String, double[]>();
    for (ColumnInfo col : columns) {
      if (isNumeric(col.profileType) && isBinnable(scalars, col.name, histogramBins)) {
        ranges.put(col.name, new double[] {(Double) scalars.get(col.name + "__min_finite"),
            (Double) scalars.get(col.name + "__max_finite")});
      }
    }
    return ranges;
  }

  // min/max are null exactly when the column has nothing finite.
  private static boolean isBinnable(Map<String, Object> scalars, String columnName,
      int histogramBins) {
    Double minFinite = (Double) scalars.get(columnName + "__min_finite");
    Double maxFinite = (Double) scalars.get(columnName + "__max_finite");
    return minFinite != null && maxFinite != null
        && hasUsableBinGrid(minFinite, maxFinite, histogramBins);
  }

  private static Column zeroForNull(String columnName) {
    return functions.coalesce(functions.col(columnName).cast("double"), functions.lit(0.0));
  }

  private static String correlationAlias(int ii, int jj) {
    return "__corr_" + ii + "_" + jj;
  }

  private static Map<String, Map<String, Double>> correlations(List<String> numericCols,
      Map<String, Object> pairValues) {
    Map<String, Map<String, Double>> correlationMap = new LinkedHashMap<String, Map<String, Double>>();
    for (int ii = 0; ii < numericCols.size(); ii++) {
      Map<String, Double> corrForA = new LinkedHashMap<String, Double>();
      for (int jj = 0; jj < numericCols.size(); jj++) {
        double corr = 1.0;
        if (ii != jj) {
          Double value = (Double) pairValues.get(correlationAlias(Math.min(ii, jj), Math.max(ii, jj)));
          corr = value == null ? Double.NaN : value;
        }
        corrForA.put(numericCols.get(jj), corr);
      }
      correlationMap.put(numericCols.get(ii), corrForA);
    }
    return correlationMap;
  }

  // ---------------------------------------------------------------------------
  // Profile assembly
  // ---------------------------------------------------------------------------

  private ColumnProfile buildProfile(Dataset<Row> df, ColumnInfo col, Map<String, Object> scalars,
      Map<String, long[]> binCounts, Map<String, Map<String, Double>> correlationMap,
      boolean histogram, int histogramBins, boolean kll, boolean exactUniqueness, boolean mergeableState) {
    String nn = col.name;

    // Both are count()s, so an empty dataframe yields 0 rather than a NULL to unbox.
    long nonNull = (Long) scalars.get(nn + "__nonnull");
    long nullCount = (Long) scalars.get(nn + "__nullcount");
    long total = nonNull + nullCount;
    long approxDistinct = (Long) scalars.get(nn + "__approx_distinct");

    boolean categoricalHistogram = histogram && !isNumeric(col.profileType);
    List<Map<String, Object>> categoricalBins = null;
    ValueFrequencyStats freqStats = null;
    if (exactUniqueness || categoricalHistogram) {
      Dataset<Row> valueCounts = HistogramBuilder.valueCounts(df, nn);
      boolean shared = exactUniqueness && categoricalHistogram;
      if (shared) {
        valueCounts = valueCounts.persist(StorageLevel.MEMORY_AND_DISK());
      }
      try {
        if (exactUniqueness) {
          freqStats = frequencyStats(valueCounts, nonNull);
        }
        if (categoricalHistogram) {
          categoricalBins = histogramBuilder.buildCategorical(valueCounts, histogramBins, nonNull);
        }
      } finally {
        if (shared) {
          valueCounts.unpersist();
        }
      }
    }

    // exactNumDistinctValues + derived stats (distinctness/uniqueness/entropy) are only
    // meaningful when exactUniqueness=true. When false they stay null and the serializer
    // omits their keys, so consumers deserialize them as absent instead of a bogus 0.
    double completeness = total > 0 ? (double) nonNull / total : 0.0;
    Long exactDistinct = exactUniqueness ? freqStats.distinct : null;
    Double distinctness = exactUniqueness
        ? (nonNull > 0 ? (double) freqStats.distinct / nonNull : 0.0) : null;
    Double uniqueness = exactUniqueness
        ? (nonNull > 0 ? (double) freqStats.singletons / nonNull : 0.0) : null;
    Double entropy = exactUniqueness ? freqStats.entropy : null;

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
      buildNumericFields(col, scalars, binCounts, correlationMap, histogram, histogramBins, kll,
          builder);
    } else if (categoricalHistogram) {
      builder.histogram(categoricalBins);
    }

    if (mergeableState) {
      builder.mergeableHll((byte[]) scalars.get(nn + "__hll"));
      if (isNumeric(col.profileType)) {
        builder.mergeableKll((byte[]) scalars.get(nn + "__kll"));
      }
    }
    return builder.build();
  }

  private void buildNumericFields(ColumnInfo col, Map<String, Object> scalars,
      Map<String, long[]> binCounts, Map<String, Map<String, Double>> correlationMap,
      boolean histogram, int histogramBins, boolean kll,
      ColumnProfile.Builder builder) {
    String nn = col.name;
    Double minVal = (Double) scalars.get(nn + "__min");
    Double maxVal = (Double) scalars.get(nn + "__max");
    Double meanVal = (Double) scalars.get(nn + "__mean");
    Double stdDevVal = (Double) scalars.get(nn + "__stddev");
    Double sumVal = (Double) scalars.get(nn + "__sum");

    builder.minimum(minVal)
        .maximum(maxVal)
        .mean(meanVal)
        .stdDev(stdDevVal)
        .sum(sumVal);

    if (correlationMap.containsKey(nn)) {
      builder.correlations(correlationMap.get(nn));
    }

    // Bin over the finite values: minVal/maxVal go non-finite as soon as the column holds
    // one NaN or infinity.
    long finiteVal = (Long) scalars.get(nn + "__nfinite");

    if (histogram) {
      // An empty list says "binned, nothing to bin"; omitting the key makes the SDK's
      // FeatureGroup._are_statistics_missing read the registered statistics as incomplete
      // and relaunch the statistics job on every compute_statistics() call.
      List<Map<String, Object>> hist = Collections.emptyList();
      if (isBinnable(scalars, nn, histogramBins)) {
        hist = histogramBuilder.buildNumeric(binCounts.get(nn),
            (Double) scalars.get(nn + "__min_finite"), (Double) scalars.get(nn + "__max_finite"),
            histogramBins, finiteVal);
      }
      builder.histogram(hist);
    }

    if (kll && finiteVal > 0) {
      byte[] kllBytes = (byte[]) scalars.get(nn + "__kll");
      KllDoublesSketch sketch = KllAggregator.heapify(kllBytes);
      // finiteVal > 0 already implies a non-empty sketch; kept because buildKllBuckets
      // depends on it directly, not on how the caller happened to gate the call.
      if (!sketch.isEmpty()) {
        builder.approxPercentiles(sketch.getQuantiles(PERCENTILE_FRACTIONS));
        // The buckets, unlike the percentiles, need a usable grid over the sketch's range.
        if (hasUsableBinGrid(sketch.getMinItem(), sketch.getMaxItem(),
            ProfileJsonSerializer.KLL_BUCKETS)) {
          builder.kllBytes(kllBytes);
        }
      }
    } else if (!kll) {
      // NULL exactly when the column has no non-null value.
      @SuppressWarnings("unchecked")
      List<Double> percentiles = (List<Double>) scalars.get(nn + "__percentiles");
      if (percentiles != null) {
        builder.approxPercentiles(toArray(percentiles));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static boolean isNumeric(String profileType) {
    return "Fractional".equals(profileType) || "Integral".equals(profileType);
  }

  private static List<String> numericColumnNames(List<ColumnInfo> columns) {
    List<String> names = new ArrayList<String>();
    for (ColumnInfo col : columns) {
      if (isNumeric(col.profileType)) {
        names.add(col.name);
      }
    }
    return names;
  }

  private static Column[] columnsOf(List<ColumnInfo> columns) {
    Column[] selected = new Column[columns.size()];
    for (int ii = 0; ii < selected.length; ii++) {
      selected[ii] = functions.col(columns.get(ii).name);
    }
    return selected;
  }

  private static Column percentileFractions() {
    Column[] fractionLiterals = new Column[PERCENTILE_FRACTIONS.length];
    for (int ii = 0; ii < PERCENTILE_FRACTIONS.length; ii++) {
      fractionLiterals[ii] = functions.lit(PERCENTILE_FRACTIONS[ii]);
    }
    return functions.array(fractionLiterals);
  }

  private static double[] toArray(List<Double> values) {
    double[] result = new double[values.size()];
    for (int ii = 0; ii < result.length; ii++) {
      result[ii] = values.get(ii);
    }
    return result;
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
