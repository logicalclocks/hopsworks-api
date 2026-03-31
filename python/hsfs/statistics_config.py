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
    enabled: bool
    """Enable statistics, by default this computes only descriptive statistics."""
    correlations: bool
    """Enable correlations as an additional statistic to be computed for each feature pair."""
    histograms: bool
    """Enable histograms as an additional statistic to be computed for each feature."""
    exact_uniqueness: bool
    """Enable exact uniqueness as an additional statistic to be computed for each feature."""
    columns: list
    """Specify a subset of columns to compute statistics for."""

    def __init__(
        self,
        enabled=True,
        correlations=False,
        histograms=False,
        exact_uniqueness=False,
        columns=None,
        **kwargs,
    ):
        self.enabled = enabled
        self.correlations = correlations
        self.histograms = histograms
        self.exact_uniqueness = exact_uniqueness
        # overwrite default with new empty [] but keep the empty list default for documentation
        self.columns = columns or []

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "enabled": self.enabled,
            "correlations": self.correlations,
            "histograms": self.histograms,
            "exactUniqueness": self.exact_uniqueness,
            "columns": self.columns,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"StatisticsConfig({self.enabled}, {self.correlations}, {self.histograms},"
            f" {self.exact_uniqueness},"
            f" {self.columns})"
        )
