"""Shared test fixtures for mock-telescope."""

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from chime import CHIMEInstrument, INSTRUMENT_ID
from helixobs._store import TraceStore


@pytest.fixture
def chime_exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def chime(chime_exporter: InMemorySpanExporter) -> CHIMEInstrument:
    """CHIMEInstrument wired to an in-memory exporter. No network I/O."""
    tel = CHIMEInstrument.__new__(CHIMEInstrument)
    tel.instrument_id = INSTRUMENT_ID
    tel._store = TraceStore()

    resource = Resource.create({
        "service.name": "test-chime",
        "helix.instrument.id": INSTRUMENT_ID,
    })
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(SimpleSpanProcessor(chime_exporter))
    trace.set_tracer_provider(provider)
    tel._tracer = trace.get_tracer("helixobs", tracer_provider=provider)
    tel._provider = provider
    return tel


def finished_spans(exporter: InMemorySpanExporter) -> list:
    return list(exporter.get_finished_spans())
