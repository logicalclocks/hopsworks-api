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


@pytest.fixture
def span_exporter(monkeypatch):
    """Spans finished during the test, recorded by the global tracer provider, with telemetry enabled as the model server enables it.

    The provider is patched in for this test only: a provider set for good would make every later test traced, including those of untraced agent apps.
    """
    trace = pytest.importorskip("opentelemetry.trace")
    sdk_trace = pytest.importorskip("opentelemetry.sdk.trace")
    from hopsworks_common import tracing
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    provider = sdk_trace.TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider)
    monkeypatch.setattr(tracing, "_available", True)
    monkeypatch.setattr(tracing, "_enabled", True)
    return exporter
