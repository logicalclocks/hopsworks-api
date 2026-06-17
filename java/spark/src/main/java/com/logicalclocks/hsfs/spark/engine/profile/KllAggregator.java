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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Collections;
import java.util.Iterator;

/**
 * Builds a KllDoublesSketch over a numeric column using a distributed mapPartitions approach.
 *
 * <p>Avoids Spark's {@code Aggregator} pattern and its Kryo serialisation requirements.
 * Each partition independently builds a sketch, serialises it to {@code byte[]} and ships
 * it to the driver, where sketches are merged. This is the recommended pattern for
 * pure-Java aggregators that cannot implement {@code java.io.Serializable}.
 *
 * <p>K=2048 matches the Deequ baseline's effective sketch resolution (Deequ also used K=2048).
 * Normalised rank error is ~0.13%, tight enough for extreme-quantile monitoring on wide-range
 * integer columns where K=200 showed ≥3% tail error. Larger K = more memory per sketch;
 * 2048 is the established trade-off.
 */
public class KllAggregator {

  private static final int K = 2048;

  /**
   * Builds a merged KllDoublesSketch over the given numeric column and returns its byte
   * representation.
   *
   * @param df source dataframe
   * @param columnName numeric column (must be castable to double)
   * @return serialised sketch bytes from {@link KllDoublesSketch#toByteArray()}
   */
  public byte[] computeSketch(Dataset<Row> df, String columnName) {
    // Per-partition: build a local sketch and return its bytes.
    Dataset<Row> filtered = df
        .select(functions.col(columnName).cast("double").alias("_v"))
        .filter(functions.col("_v").isNotNull());

    java.util.List<byte[]> partitionSketches = filtered.javaRDD()
        .mapPartitions(new PartitionSketchBuilder())
        .collect();

    // Merge all partition sketches on the driver.
    KllDoublesSketch merged = KllDoublesSketch.newHeapInstance(K);
    for (byte[] partBytes : partitionSketches) {
      if (partBytes != null && partBytes.length > 0) {
        KllDoublesSketch partSketch = KllDoublesSketch.heapify(Memory.wrap(partBytes));
        merged.merge(partSketch);
      }
    }
    return merged.toByteArray();
  }

  /**
   * Deserialises bytes produced by {@link KllDoublesSketch#toByteArray()} back into a live sketch.
   */
  public static KllDoublesSketch heapify(byte[] bytes) {
    return KllDoublesSketch.heapify(Memory.wrap(bytes));
  }

  // ---------------------------------------------------------------------------
  // Per-partition builder — stateless, serialisable
  // ---------------------------------------------------------------------------

  /**
   * Builds one KllDoublesSketch per Spark partition and emits its serialised bytes.
   * Implements {@code java.io.Serializable} so Spark can ship it to executors.
   */
  private static final class PartitionSketchBuilder
      implements FlatMapFunction<Iterator<Row>, byte[]> {

    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<byte[]> call(Iterator<Row> rows) {
      KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(K);
      while (rows.hasNext()) {
        Row row = rows.next();
        if (!row.isNullAt(0)) {
          double value = ((Number) row.get(0)).doubleValue();
          if (!Double.isNaN(value)) {
            sketch.update(value);
          }
        }
      }
      // An empty partition emits an empty sketch (still valid for merging).
      return Collections.singletonList(sketch.toByteArray()).iterator();
    }
  }
}
