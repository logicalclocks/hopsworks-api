#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import json

import humps
from hopsworks_apigen import public
from hsfs import util


@public
class StatisticsConfig:
    def __init__(
        self,
        enabled=True,
        correlations=False,
        histograms=False,
        exact_uniqueness=False,
        columns=None,
        kll=False,
        histogram_bins=None,
        **kwargs,
    ):
        self._enabled = enabled
        # use setters for input validation
        self.correlations = correlations
        self.histograms = histograms
        self.exact_uniqueness = exact_uniqueness
        # overwrite default with new empty [] but keep the empty list default for documentation
        self._columns = columns or []
        self.kll = kll
        self.histogram_bins = histogram_bins

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        the_dict = {
            "enabled": self._enabled,
            "correlations": self._correlations,
            "histograms": self._histograms,
            "exactUniqueness": self._exact_uniqueness,
            "columns": self._columns,
        }
        if self._kll:
            the_dict["kll"] = self._kll
        if self._histogram_bins is not None:
            the_dict["histogramBins"] = self._histogram_bins
        return the_dict

    @public
    @property
    def enabled(self):
        """Enable statistics, by default this computes only descriptive statistics."""
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        self._enabled = enabled

    @public
    @property
    def correlations(self):
        """Enable correlations as an additional statistic to be computed for each feature pair."""
        return self._correlations

    @correlations.setter
    def correlations(self, correlations):
        self._correlations = correlations

    @public
    @property
    def histograms(self):
        """Enable histograms as an additional statistic to be computed for each feature."""
        return self._histograms

    @histograms.setter
    def histograms(self, histograms):
        self._histograms = histograms

    @public
    @property
    def exact_uniqueness(self):
        """Enable exact uniqueness as an additional statistic to be computed for each feature."""
        return self._exact_uniqueness

    @exact_uniqueness.setter
    def exact_uniqueness(self, exact_uniqueness):
        self._exact_uniqueness = exact_uniqueness

    @public
    @property
    def columns(self):
        """Specify a subset of columns to compute statistics for."""
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    @public
    @property
    def kll(self) -> bool:
        """Enable KLL sketch computation for approximate quantile estimation."""
        return self._kll

    @kll.setter
    def kll(self, kll: bool):
        self._kll = kll

    @public
    @property
    def histogram_bins(self) -> int | None:
        """Number of histogram bins to compute. If None, Deequ's default (20) is used."""
        return self._histogram_bins

    @histogram_bins.setter
    def histogram_bins(self, histogram_bins: int | None):
        self._histogram_bins = histogram_bins

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"StatisticsConfig({self._enabled}, {self._correlations}, {self._histograms},"
            f" {self._exact_uniqueness},"
            f" {self._columns},"
            f" kll={self._kll}, histogram_bins={self._histogram_bins})"
        )
