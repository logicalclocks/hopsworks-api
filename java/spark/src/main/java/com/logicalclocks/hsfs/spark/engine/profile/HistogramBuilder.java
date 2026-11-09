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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Computes histogram bins for a single column.
 *
 * <p>Numeric columns: equal-width bins of size {@code (max-min)/histogramBins}.
 * The bin label uses {@code "%.2f to %.2f"} formatting, matching Deequ 2.0.7-spark-3.5 output.
 *
 * <p>Categorical/Boolean columns: top-N distinct values ordered by descending count.
 * The bin label is the string representation of the category value.
 *
 * <p>Each entry in the returned list is a map with keys {@code value}, {@code count},
 * {@code ratio}, matching the Deequ JSON wire shape.
 */
class HistogramBuilder {

  /**
   * Exact counts of each column's finite values per equal-width bin, for all the columns in one
   * aggregation: every row is unpivoted into one (column, bin) cell per column and the cells are
   * counted, so the cost grows with the number of columns but not with the number of bins.
   *
   * @param ranges minimum and maximum over the finite values, keyed by column name
   * @return per-bin counts keyed by column name; no Spark job runs when ranges is empty
   */
  static Map<String, long[]> binCounts(Dataset<Row> df, Map<String, double[]> ranges,
      int histogramBins) {
    Map<String, long[]> counts = new HashMap<String, long[]>();
    if (ranges.isEmpty()) {
      return counts;
    }
    List<String> names = new ArrayList<String>(ranges.keySet());
    Column[] cells = new Column[names.size()];
    for (int ii = 0; ii < cells.length; ii++) {
      double[] range = ranges.get(names.get(ii));
      cells[ii] = functions.struct(functions.lit(ii).alias("c"),
          binIndex(names.get(ii), range[0], range[1], histogramBins).alias("b"));
      counts.put(names.get(ii), new long[histogramBins]);
    }
    Dataset<Row> counted = df.select(functions.explode(functions.array(cells)).alias("cell"))
        .select(functions.col("cell.c"), functions.col("cell.b"))
        .filter(functions.col("b").isNotNull())
        .groupBy("c", "b")
        .count();
    for (Row row : counted.collectAsList()) {
      counts.get(names.get(row.getInt(0)))[row.getInt(1)] = row.getLong(2);
    }
    return counts;
  }

  /** Bin of a column's value, or NULL for a NULL or non-finite value. */
  private static Column binIndex(String columnName, double minValue, double maxValue,
      int histogramBins) {
    double range = maxValue - minValue;
    double binWidth = range / histogramBins;

    Column col = functions.col(columnName).cast("double");
    Column binExpr;
    if (range == 0.0) {
      binExpr = functions.lit(0);
    } else {
      binExpr = functions.least(
          functions.floor((col.minus(minValue)).divide(binWidth)).cast("int"),
          functions.lit(histogramBins - 1)
      );
    }

    // Non-finite values belong to no bin and must not reach binExpr: NaN floors to bin 0,
    // and floor(Infinity) is Long.MAX_VALUE, which least() clamps into the last bin - or,
    // where ANSI mode is on, fails the job outright on the cast to int. Hopsworks pins
    // spark.sql.ansi.enabled=false, so the default is the silent miscount, not the error.
    return functions.when(col.isNotNull().and(ColumnProfiler.isFinite(col)), binExpr);
  }

  /**
   * Builds histogram bins for a numeric column from its {@link #binCounts}.
   *
   * @param minValue minimum over the finite values, as passed to {@link #binCounts}
   * @param maxValue maximum over the finite values, as passed to {@link #binCounts}
   * @param totalRows total finite rows (denominator for ratio)
   * @return list of histogram entry maps with keys: value, count, ratio
   */
  List<Map<String, Object>> buildNumeric(long[] counts,
      double minValue,
      double maxValue,
      int histogramBins,
      long totalRows) {
    double binWidth = (maxValue - minValue) / histogramBins;

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(histogramBins);
    for (int ii = 0; ii < histogramBins; ii++) {
      double low = minValue + (ii * binWidth);
      double high = low + binWidth;
      long count = counts[ii];
      double ratio = totalRows > 0 ? (double) count / totalRows : 0.0;
      String valueLabel = String.format(Locale.ROOT, "%.2f to %.2f", low, high);

      Map<String, Object> entry = new HashMap<String, Object>();
      entry.put("value", valueLabel);
      entry.put("count", count);
      entry.put("ratio", ratio);
      result.add(entry);
    }
    return result;
  }

  /**
   * Per-value counts of a column's non-null values, as columns {@code _v} and {@code count}.
   * Shared by the categorical histogram and the exact-uniqueness statistics.
   */
  static Dataset<Row> valueCounts(Dataset<Row> df, String columnName) {
    // Project to a fixed name before grouping. For a feature named "count", grouping on the
    // column itself leaves two "count" columns: the uniqueness reads cannot resolve, and
    // Spark resolves the histogram's orderBy against the grouping one rather than failing,
    // so the bins come out ordered by value and the top-N cut keeps the wrong values.
    return df.select(functions.col(columnName).alias("_v"))
        .filter(functions.col("_v").isNotNull())
        .groupBy(functions.col("_v"))
        .count();
  }

  /**
   * Builds histogram bins for a categorical (String or Boolean) column.
   *
   * @param valueCounts the column's {@link #valueCounts}
   * @param histogramBins maximum number of bins (top-N by count)
   * @param totalRows total non-null rows (denominator for ratio)
   * @return list of histogram entry maps with keys: value, count, ratio
   */
  List<Map<String, Object>> buildCategorical(Dataset<Row> valueCounts,
      int histogramBins,
      long totalRows) {
    // Ties broken by value: otherwise their order, and which of them survive the top-N cut,
    // follow the shuffle, and a monitoring comparison sees bins move between two profiles of
    // the same data.
    Dataset<Row> grouped = valueCounts
        .orderBy(functions.desc("count"), functions.asc("_v"))
        .limit(histogramBins);

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    for (Row row : grouped.collectAsList()) {
      String valueLabel = row.get(0) == null ? "null" : row.get(0).toString();
      long count = row.getLong(1);
      double ratio = totalRows > 0 ? (double) count / totalRows : 0.0;

      Map<String, Object> entry = new HashMap<String, Object>();
      entry.put("value", valueLabel);
      entry.put("count", count);
      entry.put("ratio", ratio);
      result.add(entry);
    }
    return result;
  }
}
