/*
 *  Copyright (c) 2022-2023. Hopsworks AB
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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Builder
public class StatisticsConfig {
  @Getter
  @Setter
  private Boolean enabled = true;

  @Getter
  @Setter
  private Boolean histograms = false;

  @Getter
  @Setter
  private Boolean correlations = false;

  @Getter
  @Setter
  private Boolean exactUniqueness = false;

  @Getter
  @Setter
  private List<String> columns = new ArrayList<>();

  @Getter
  @Setter
  private Boolean kll = false;

  @Getter
  @Setter
  private Integer histogramBins = null;

  /**
   * Whether a commit's statistics merge the new rows into the previous statistics instead of profiling the whole
   * table. Null uses the cluster default.
   * On update, null resets the setting to the cluster default, unlike kll and histogramBins, where null keeps the
   * stored value.
   * Merged percentiles and the approximate distinct count come from KLL and HLL sketches over finite values, so they
   * can differ from a full profile computed without KLL, for example on columns holding infinities.
   */
  // always sent, even as null, so the backend tells "reset" from "not set"
  @Getter
  @Setter
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private Boolean incremental = null;

  // The all-arguments constructor before incremental existed, kept for compiled callers.
  public StatisticsConfig(Boolean enabled, Boolean histograms, Boolean correlations, Boolean exactUniqueness,
                          List<String> columns, Boolean kll, Integer histogramBins) {
    this(enabled, histograms, correlations, exactUniqueness, columns, kll, histogramBins, null);
  }

  public StatisticsConfig(Boolean enabled, Boolean histograms, Boolean correlations,
                          Boolean exactUniqueness) {
    this.enabled = enabled;
    this.histograms = histograms;
    this.correlations = correlations;
    this.exactUniqueness = exactUniqueness;
  }
}
