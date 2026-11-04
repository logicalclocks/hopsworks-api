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

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * KllDoublesSketch over a numeric column, built by Spark's native {@code kll_sketch_agg_double}
 * so that it runs inside the profiler's scalar aggregation instead of as a job of its own.
 *
 * <p>Spark's aggregate serialises a datasketches-java {@link KllDoublesSketch}, the same bytes
 * {@link KllMerger} and the {@code datasketches-native-v1} sidecar format read.
 *
 * <p>Only finite values are fed to the sketch: callers derive bin edges from its min/max, and
 * a column with nothing finite yields an <em>empty</em> sketch, which most operations reject.
 * The native aggregate skips NULL and NaN on its own but keeps infinities.
 *
 * <p>K=2048 matches the Deequ baseline's effective sketch resolution (Deequ also used K=2048).
 * Normalised rank error is ~0.13%, tight enough for extreme-quantile monitoring on wide-range
 * integer columns where K=200 showed ≥3% tail error. Larger K = more memory per sketch;
 * 2048 is the established trade-off.
 */
public class KllAggregator {

  private static final int K = 2048;

  /**
   * Serialised sketch of a numeric column's finite values; an empty sketch when it has none.
   *
   * @deprecated the profiler builds sketches inside its own aggregation with {@link #sketch}.
   */
  @Deprecated
  public byte[] computeSketch(Dataset<Row> df, String columnName) {
    Row row = df.agg(sketch(functions.col(columnName).cast("double"))).first();
    return row.isNullAt(0) ? KllDoublesSketch.newHeapInstance(K).toByteArray() : (byte[]) row.get(0);
  }

  /** Aggregate expression returning the serialised sketch of a double column's finite values. */
  static Column sketch(Column doubleCol) {
    return functions.call_function("kll_sketch_agg_double",
        functions.when(ColumnProfiler.isFinite(doubleCol), doubleCol), functions.lit(K));
  }

  /**
   * Deserialises a sketch produced by {@link #sketch} or stored as a {@code datasketches-native-v1} sidecar.
   *
   * @param bytes serialised KllDoublesSketch
   * @return the sketch, read onto the heap
   */
  public static KllDoublesSketch heapify(byte[] bytes) {
    return KllDoublesSketch.heapify(Memory.wrap(bytes));
  }
}
