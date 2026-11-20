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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HistogramBuilderTest {

  @Test
  void aValueOutsideTheGridLandsInTheNearestBin() {
    SparkSession spark = SparkSession.builder().master("local[1]").appName("histogram-builder-test")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
    StructType schema = new StructType().add("x", DataTypes.DoubleType);
    List<Row> rows = Arrays.asList(RowFactory.create(-5.0), RowFactory.create(0.0), RowFactory.create(9.0),
        RowFactory.create(25.0));
    Dataset<Row> df = spark.createDataFrame(rows, schema);

    Map<String, long[]> counts = HistogramBuilder.binCounts(df,
        Collections.singletonMap("x", new double[] {0.0, 10.0}), 5);

    // -5 sits below the grid and 25 above it: both are kept, in the first and last bin
    Assertions.assertArrayEquals(new long[] {2, 0, 0, 0, 2}, counts.get("x"));
  }
}
