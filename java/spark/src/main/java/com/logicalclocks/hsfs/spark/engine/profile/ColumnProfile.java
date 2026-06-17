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

import java.util.List;
import java.util.Map;

/**
 * Intermediate representation of a single column's profile statistics.
 * Passed from {@link ColumnProfiler} to {@link ProfileJsonSerializer}.
 *
 * <p>Package-private data holder — not a public API.
 */
class ColumnProfile {

  private final String columnName;
  private final String dataType;

  private final double completeness;
  private final long numRecordsNonNull;
  private final long numRecordsNull;
  private final double distinctness;
  private final double entropy;
  private final double uniqueness;
  private final long approximateNumDistinctValues;
  private final long exactNumDistinctValues;

  private final Double mean;
  private final Double maximum;
  private final Double minimum;
  private final Double sum;
  private final Double stdDev;

  private final Map<String, Double> correlations;
  private final List<Map<String, Object>> histogram;
  private final byte[] kllBytes;
  private final double[] approxPercentiles;

  private ColumnProfile(Builder builder) {
    this.columnName = builder.columnName;
    this.dataType = builder.dataType;
    this.completeness = builder.completeness;
    this.numRecordsNonNull = builder.numRecordsNonNull;
    this.numRecordsNull = builder.numRecordsNull;
    this.distinctness = builder.distinctness;
    this.entropy = builder.entropy;
    this.uniqueness = builder.uniqueness;
    this.approximateNumDistinctValues = builder.approximateNumDistinctValues;
    this.exactNumDistinctValues = builder.exactNumDistinctValues;
    this.mean = builder.mean;
    this.maximum = builder.maximum;
    this.minimum = builder.minimum;
    this.sum = builder.sum;
    this.stdDev = builder.stdDev;
    this.correlations = builder.correlations;
    this.histogram = builder.histogram;
    this.kllBytes = builder.kllBytes;
    this.approxPercentiles = builder.approxPercentiles;
  }

  boolean isNumeric() {
    return "Fractional".equals(dataType) || "Integral".equals(dataType);
  }

  String getColumnName() {
    return columnName;
  }

  String getDataType() {
    return dataType;
  }

  double getCompleteness() {
    return completeness;
  }

  long getNumRecordsNonNull() {
    return numRecordsNonNull;
  }

  long getNumRecordsNull() {
    return numRecordsNull;
  }

  double getDistinctness() {
    return distinctness;
  }

  double getEntropy() {
    return entropy;
  }

  double getUniqueness() {
    return uniqueness;
  }

  long getApproximateNumDistinctValues() {
    return approximateNumDistinctValues;
  }

  long getExactNumDistinctValues() {
    return exactNumDistinctValues;
  }

  Double getMean() {
    return mean;
  }

  Double getMaximum() {
    return maximum;
  }

  Double getMinimum() {
    return minimum;
  }

  Double getSum() {
    return sum;
  }

  Double getStdDev() {
    return stdDev;
  }

  Map<String, Double> getCorrelations() {
    return correlations;
  }

  List<Map<String, Object>> getHistogram() {
    return histogram;
  }

  byte[] getKllBytes() {
    return kllBytes;
  }

  double[] getApproxPercentiles() {
    return approxPercentiles;
  }

  static final class Builder {

    private String columnName;
    private String dataType;
    private double completeness;
    private long numRecordsNonNull;
    private long numRecordsNull;
    private double distinctness;
    private double entropy;
    private double uniqueness;
    private long approximateNumDistinctValues;
    private long exactNumDistinctValues;
    private Double mean;
    private Double maximum;
    private Double minimum;
    private Double sum;
    private Double stdDev;
    private Map<String, Double> correlations;
    private List<Map<String, Object>> histogram;
    private byte[] kllBytes;
    private double[] approxPercentiles;

    Builder columnName(String value) {
      this.columnName = value;
      return this;
    }

    Builder dataType(String value) {
      this.dataType = value;
      return this;
    }

    Builder completeness(double value) {
      this.completeness = value;
      return this;
    }

    Builder numRecordsNonNull(long value) {
      this.numRecordsNonNull = value;
      return this;
    }

    Builder numRecordsNull(long value) {
      this.numRecordsNull = value;
      return this;
    }

    Builder distinctness(double value) {
      this.distinctness = value;
      return this;
    }

    Builder entropy(double value) {
      this.entropy = value;
      return this;
    }

    Builder uniqueness(double value) {
      this.uniqueness = value;
      return this;
    }

    Builder approximateNumDistinctValues(long value) {
      this.approximateNumDistinctValues = value;
      return this;
    }

    Builder exactNumDistinctValues(long value) {
      this.exactNumDistinctValues = value;
      return this;
    }

    Builder mean(Double value) {
      this.mean = value;
      return this;
    }

    Builder maximum(Double value) {
      this.maximum = value;
      return this;
    }

    Builder minimum(Double value) {
      this.minimum = value;
      return this;
    }

    Builder sum(Double value) {
      this.sum = value;
      return this;
    }

    Builder stdDev(Double value) {
      this.stdDev = value;
      return this;
    }

    Builder correlations(Map<String, Double> value) {
      this.correlations = value;
      return this;
    }

    Builder histogram(List<Map<String, Object>> value) {
      this.histogram = value;
      return this;
    }

    Builder kllBytes(byte[] value) {
      this.kllBytes = value;
      return this;
    }

    Builder approxPercentiles(double[] value) {
      this.approxPercentiles = value;
      return this;
    }

    ColumnProfile build() {
      return new ColumnProfile(this);
    }
  }
}
