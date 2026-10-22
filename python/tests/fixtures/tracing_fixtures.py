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

import pytest


@pytest.fixture(scope="session")
def _session_span_exporter():
    trace = pytest.importorskip("opentelemetry.trace")
    sdk_trace = pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    # The global tracer provider can be set once per process, so the whole session shares one.
    provider = trace.get_tracer_provider()
    if isinstance(provider, trace.ProxyTracerProvider):
        provider = sdk_trace.TracerProvider()
        trace.set_tracer_provider(provider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def span_exporter(_session_span_exporter, monkeypatch):
    """Spans finished during the test, recorded by the global tracer provider, with telemetry enabled as the model server enables it."""
    from hopsworks_common import tracing

    monkeypatch.setattr(tracing, "_enabled", True)
    _session_span_exporter.clear()
    return _session_span_exporter
