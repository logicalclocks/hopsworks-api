/*
 * Copyright (c) 2026 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.utils;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Hudi DeltaStreamer Transformer that materialises {@code partitioned_by} grain columns
 * (year / month / week / day / hour) into Hudi records before write, derived from the
 * feature group's {@code event_time} column.
 *
 * <p>Without this Transformer, Hudi would write to {@code year=YYYY/month=MM/} partition
 * paths via the {@code CustomKeyGenerator} but the records themselves would not carry the
 * grain columns. Reads would then see them only as strings parsed from the path layout.
 * With this Transformer, the grain columns are real {@code INT} columns in the records,
 * matching the Delta GENERATED ALWAYS AS path so reads on either format expose the same
 * schema.
 *
 * <p>Configuration (set by {@code FsJobManagerController.setupDeltaStreamerJob} when a Hudi
 * feature group has {@code partitioned_by} set):
 * <ul>
 *   <li>{@code hoodie.deltastreamer.transformer.partitionedby.eventtime} — the source column.</li>
 *   <li>{@code hoodie.deltastreamer.transformer.partitionedby.grains} — comma-separated grain list
 *       (subset of {@code year,month,week,day,hour}, in {@code partitioned_by} order).</li>
 * </ul>
 *
 * <p>Integer {@code event_time} columns follow the seconds-vs-milliseconds rule used everywhere
 * else in the SDK: values with absolute magnitude up to ten digits are treated as unix seconds,
 * anything longer as unix milliseconds. Timestamp and date columns pass through unchanged.
 */
public class PartitionedByTransformer implements Transformer {

  public static final String CONF_EVENT_TIME =
      "hoodie.deltastreamer.transformer.partitionedby.eventtime";
  public static final String CONF_GRAINS =
      "hoodie.deltastreamer.transformer.partitionedby.grains";

  // Spark SQL function name per grain. Matches the Delta GENERATED expressions
  // emitted by hsfs_utils.create_delta_table_fg so the two formats produce
  // identical grain values for the same event_time input.
  private static final Map<String, String> GRAIN_FNS = new HashMap<>();
  static {
    GRAIN_FNS.put("year", "YEAR");
    GRAIN_FNS.put("month", "MONTH");
    GRAIN_FNS.put("week", "WEEKOFYEAR");
    GRAIN_FNS.put("day", "DAYOFMONTH");
    GRAIN_FNS.put("hour", "HOUR");
  }

  @Override
  public Option<Dataset<Row>> apply(JavaSparkContext jsc, SparkSession spark,
                                    Dataset<Row> rowDataset, TypedProperties properties) {
    String eventTime = properties.getString(CONF_EVENT_TIME, null);
    String grainsCsv = properties.getString(CONF_GRAINS, null);
    if (eventTime == null || grainsCsv == null || grainsCsv.isEmpty()) {
      // partitioned_by not configured on this FG — pass through unchanged.
      return Option.of(rowDataset);
    }
    String[] grains = grainsCsv.split(",");

    Column timestampExpr = eventTimeTimestampExpr(rowDataset, eventTime);

    Dataset<Row> out = rowDataset;
    for (String grain : grains) {
      String trimmed = grain.trim();
      String fn = GRAIN_FNS.get(trimmed);
      if (fn == null) {
        throw new IllegalArgumentException(
            "PartitionedByTransformer: unsupported grain '" + trimmed
                + "'. Supported grains: " + GRAIN_FNS.keySet());
      }
      String sqlExpr = fn + "(__pbt_ts_expr)";
      out = out
          .withColumn("__pbt_ts_expr", timestampExpr)
          .withColumn(trimmed, functions.expr(sqlExpr).cast("int"))
          .drop("__pbt_ts_expr");
    }
    return Option.of(out);
  }

  // Build a Spark Column that yields a TIMESTAMP from event_time. Mirrors the
  // Delta CREATE TABLE wrapper used in hsfs_utils.create_delta_table_fg:
  //   - timestamp / date pass through;
  //   - integer columns are decoded per-row via a CASE WHEN that picks
  //     between seconds and milliseconds based on absolute magnitude
  //     (<= 9_999_999_999 → seconds, else ms), so columns with mixed
  //     units yield correct timestamps for every row.
  private Column eventTimeTimestampExpr(Dataset<Row> ds, String eventTime) {
    String dtype = null;
    for (org.apache.spark.sql.types.StructField f : ds.schema().fields()) {
      if (f.name().equals(eventTime)) {
        dtype = f.dataType().typeName().toLowerCase();
        break;
      }
    }
    if (dtype == null) {
      throw new IllegalArgumentException(
          "PartitionedByTransformer: event_time column '" + eventTime
              + "' is not present in the dataset. Available columns: "
              + Arrays.toString(ds.columns()));
    }
    if (dtype.equals("timestamp") || dtype.equals("date")) {
      return functions.col(eventTime);
    }
    if (dtype.equals("long") || dtype.equals("integer") || dtype.equals("short")
        || dtype.equals("byte") || dtype.equals("bigint")) {
      String expr = "CAST(CASE WHEN ABS(`" + eventTime + "`) <= 9999999999 "
          + "THEN `" + eventTime + "` "
          + "ELSE `" + eventTime + "` / 1000 END AS TIMESTAMP)";
      return functions.expr(expr);
    }
    // Best-effort cast for any other declared type.
    return functions.col(eventTime).cast("timestamp");
  }
}
