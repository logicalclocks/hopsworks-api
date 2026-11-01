#
#   Copyright 2026 Hopsworks AB
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
"""Time per stage of the requests a model deployment with tracing enabled serves.

The model server opens [`hopsworks_common.tracing._request_scope`][] around every request, sampled or not, and the stages time themselves into it with [`hopsworks_common.tracing._stage`][]: `feature_lookup` of a feature view, which includes its `transformations`, `model_predict` of a model and `feature_log` of a feature view, the time to queue or write the log.
When a request ends, its time in each stage it ran goes into a histogram labelled by stage and by the feature view or model, `<name>:<version>`, so its quantiles over any interval and across replicas come from `histogram_quantile`, and into minimum and maximum gauges over the current and the last complete window of `_WINDOW_SECONDS`.
"""

from __future__ import annotations

import bisect
import logging
import threading
import time

from hopsworks_common import tracing


_logger = logging.getLogger(__name__)

_WINDOW_SECONDS = 60
_WINDOW_NS = _WINDOW_SECONDS * 1_000_000_000

STAGES = ("feature_lookup", "transformations", "model_predict", "feature_log")

# Ten bounds a decade from 10 us to 10 s, so a quantile interpolated within a bucket is off by at most 26%.
_BUCKETS_SECONDS = [float(f"{10 ** (k / 10):.3g}") for k in range(-50, 11)]


class _Series:
    """Running count and duration of one transformation, phase or request stage, plus its extremes over fixed windows.

    Updates are not locked: under concurrent requests a window can miss an extreme, and a rotation racing an update can drop one call from a window.
    """

    __slots__ = (
        "calls",
        "errors",
        "total_ns",
        "_window_end_ns",
        "_window_calls",
        "_window_min_ns",
        "_window_max_ns",
        "_last_calls",
        "_last_min_ns",
        "_last_max_ns",
    )

    def __init__(self):
        self.calls = 0
        self.errors = 0
        self.total_ns = 0
        self._window_end_ns = 0
        self._window_calls = 0
        self._window_min_ns = 0
        self._window_max_ns = 0
        self._last_calls = 0
        self._last_min_ns = 0
        self._last_max_ns = 0

    def _observe(self, duration_ns: int, now_ns: int) -> None:
        if now_ns >= self._window_end_ns:
            self._rotate(now_ns)
        self.calls += 1
        self.total_ns += duration_ns
        if self._window_calls == 0 or duration_ns < self._window_min_ns:
            self._window_min_ns = duration_ns
        if duration_ns > self._window_max_ns:
            self._window_max_ns = duration_ns
        self._window_calls += 1

    def _rotate(self, now_ns: int) -> None:
        # A window that ended more than a window ago saw no calls in the window after it, so it is not the last complete one.
        fresh = now_ns < self._window_end_ns + _WINDOW_NS
        self._last_calls = self._window_calls if fresh else 0
        self._last_min_ns = self._window_min_ns
        self._last_max_ns = self._window_max_ns
        self._window_calls = 0
        self._window_min_ns = 0
        self._window_max_ns = 0
        self._window_end_ns = now_ns + _WINDOW_NS

    def _last_window(self, now_ns: int) -> tuple[int, int, int]:
        """Calls, minimum and maximum of the last complete window as of `now_ns`, without rotating."""
        if now_ns >= self._window_end_ns + _WINDOW_NS:
            return 0, 0, 0
        if now_ns >= self._window_end_ns:
            return self._window_calls, self._window_min_ns, self._window_max_ns
        return self._last_calls, self._last_min_ns, self._last_max_ns

    def _recent(self, now_ns: int) -> tuple[int, int, int]:
        """Calls, minimum and maximum of the last complete window and the current one together, as of `now_ns`."""
        last_calls, last_min, last_max = self._last_window(now_ns)
        if now_ns >= self._window_end_ns:
            return last_calls, last_min, last_max
        current_calls = self._window_calls
        if not current_calls:
            return last_calls, last_min, last_max
        if not last_calls:
            return current_calls, self._window_min_ns, self._window_max_ns
        return (
            last_calls + current_calls,
            min(last_min, self._window_min_ns),
            max(last_max, self._window_max_ns),
        )


# Keyed by (stage, name); declared when a predictor or a feature view starts serving, so each series is exported from the first scrape.
_stages: dict[tuple[str, str], _Series] = {}
# Requests per bucket of _BUCKETS_SECONDS for each key, not cumulative; the last counts those above the largest bound.
_buckets: dict[tuple[str, str], list[int]] = {}
_register_lock = threading.Lock()
_registered = False


def _declare(stage: str, name: str) -> None:
    """Export the stage of feature view or model `name` at zero until a request runs it, so increase() counts the first one."""
    key = (stage, name)
    if key not in _stages:
        _buckets[key] = [0] * (len(_BUCKETS_SECONDS) + 1)
        _stages[key] = _Series()


def _observe(stages_ns: dict[tuple[str, str], int]) -> None:
    now_ns = time.perf_counter_ns()
    for key, duration_ns in stages_ns.items():
        if key not in _stages:
            _declare(*key)
        _stages[key]._observe(duration_ns, now_ns)
        _buckets[key][bisect.bisect_left(_BUCKETS_SECONDS, duration_ns / 1e9)] += 1


def _register() -> None:
    """Count every scoped request's stages and expose them to Prometheus; once per process."""
    global _registered
    with _register_lock:
        if _registered:
            return
        _registered = True
        tracing._request_observers.append(_observe)
        try:
            from prometheus_client import REGISTRY
        except ImportError:
            return
        try:
            REGISTRY.register(_Collector())
        except ValueError as error:
            # The default registry already holds these names, so this module was loaded a second time.
            _logger.warning("Request stage metrics not exposed: %s", error)


class _Collector:
    """Publishes the time per stage when Prometheus scrapes the predictor."""

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily
        from prometheus_client.utils import floatToGoString

        now_ns = time.perf_counter_ns()
        histogram = HistogramMetricFamily(
            "hopsworks_prediction_stage_seconds",
            "Time one request spent in a stage of a feature view or model: feature_lookup (including transformations), transformations, model_predict or feature_log",
            labels=["stage", "name"],
        )
        window_min = GaugeMetricFamily(
            "hopsworks_prediction_stage_window_min_seconds",
            f"Fastest request's stage in the current and the last complete {_WINDOW_SECONDS} s window",
            labels=["stage", "name"],
        )
        window_max = GaugeMetricFamily(
            "hopsworks_prediction_stage_window_max_seconds",
            f"Slowest request's stage in the current and the last complete {_WINDOW_SECONDS} s window",
            labels=["stage", "name"],
        )
        for key in list(_stages):
            cumulative = 0
            buckets = []
            for bound, count in zip(
                [*_BUCKETS_SECONDS, float("inf")], list(_buckets[key]), strict=True
            ):
                cumulative += count
                buckets.append((floatToGoString(bound), cumulative))
            series = _stages[key]
            histogram.add_metric(list(key), buckets, series.total_ns / 1e9)
            calls, minimum, maximum = series._recent(now_ns)
            if calls:
                window_min.add_metric(list(key), minimum / 1e9)
                window_max.add_metric(list(key), maximum / 1e9)
        yield from (histogram, window_min, window_max)
