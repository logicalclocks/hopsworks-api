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
   * Builds histogram bins for a numeric column using equal-width bucketing.
   *
   * @param df source dataframe
   * @param columnName column to histogram
   * @param minValue pre-computed column minimum (non-null, from agg row)
   * @param maxValue pre-computed column maximum (non-null, from agg row)
   * @param histogramBins number of bins
   * @param totalRows total non-null rows (denominator for ratio)
   * @return list of histogram entry maps with keys: value, count, ratio
   */
  List<Map<String, Object>> buildNumeric(Dataset<Row> df,
      String columnName,
      double minValue,
      double maxValue,
      int histogramBins,
      long totalRows) {
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

    Dataset<Row> binned = df.filter(col.isNotNull())
        .withColumn("_bin", binExpr)
        .groupBy("_bin")
        .count();

    Map<Integer, Long> binCounts = new HashMap<Integer, Long>();
    for (Row row : binned.collectAsList()) {
      int binIdx = row.getInt(0);
      long count = row.getLong(1);
      binCounts.put(binIdx, count);
    }

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(histogramBins);
    for (int ii = 0; ii < histogramBins; ii++) {
      double low = minValue + (ii * binWidth);
      double high = low + binWidth;
      long count = binCounts.containsKey(ii) ? binCounts.get(ii) : 0L;
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
   * Builds histogram bins for a categorical (String or Boolean) column.
   *
   * @param df source dataframe
   * @param columnName column to histogram
   * @param histogramBins maximum number of bins (top-N by count)
   * @param totalRows total non-null rows (denominator for ratio)
   * @return list of histogram entry maps with keys: value, count, ratio
   */
  List<Map<String, Object>> buildCategorical(Dataset<Row> df,
      String columnName,
      int histogramBins,
      long totalRows) {
    Dataset<Row> grouped = df
        .filter(functions.col(columnName).isNotNull())
        .groupBy(functions.col(columnName))
        .count()
        .orderBy(functions.desc("count"))
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
