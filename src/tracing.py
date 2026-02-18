"""OpenTelemetry tracing helpers and integration utilities.

Provides `setup_tracer` to initialize OTLP exporter and helpers to inject
and extract trace context into/from Kafka headers.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Tuple, Optional

try:
    from opentelemetry import propagate, trace  # type: ignore[import-not-found]  # noqa: E501
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter  # type: ignore[import-not-found]  # noqa: E501
    from opentelemetry.sdk.resources import Resource  # type: ignore[import-not-found]  # noqa: E501
    from opentelemetry.sdk.trace import TracerProvider  # type: ignore[import-not-found]  # noqa: E501
    from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore[import-not-found]  # noqa: E501
    from opentelemetry.instrumentation.requests import RequestsInstrumentor  # type: ignore[import-not-found]  # noqa: E501

    _OTEL_AVAILABLE = True
except Exception:
    # Provide no-op fallbacks when OpenTelemetry is not installed in the environment.
    propagate = None
    trace = None
    OTLPSpanExporter = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    RequestsInstrumentor = None
    _OTEL_AVAILABLE = False


def setup_tracer(service_name: str = "github-streaming-analytics") -> None:
    """Initialize OpenTelemetry tracer with OTLP exporter (HTTP).

    Config via environment variables:
      OTEL_EXPORTER_OTLP_ENDPOINT - e.g. http://otel-collector:4318/v1/traces
      OTEL_SERVICE_NAME - override service name
    """
    if not _OTEL_AVAILABLE:
        return

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    svc = os.environ.get("OTEL_SERVICE_NAME", service_name)

    resource = Resource.create({"service.name": svc})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))

    # instrument requests so HTTP calls (GitHub) are traced
    try:
        RequestsInstrumentor().instrument()
    except Exception:
        pass


def headers_dict_to_list(headers: Dict[str, str]) -> List[Tuple[str, bytes]]:
    """Convert mapping to Kafka headers list of (key, bytes).

    The opentelemetry propagator writes str values; Kafka headers should be bytes.
    """
    return [
        (k, (v.encode("utf-8") if isinstance(v, str) else v))
        for k, v in headers.items()
    ]


def inject_trace_into_kafka_headers() -> List[Tuple[str, bytes]]:
    """Return a list of Kafka header tuples containing current trace context."""
    if not _OTEL_AVAILABLE:
        return []
    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    return headers_dict_to_list(carrier)


def kafka_headers_list_to_carrier(headers: Iterable) -> Dict[str, str]:
    """Convert Kafka headers (list of tuples or array) to a carrier dict for extraction.

    Accepts Spark/Kafka header representations (list/array of structs) or
    kafka-python headers list of tuples.
    """
    carrier: Dict[str, str] = {}
    if headers is None:
        return carrier

    # headers might be list of tuples (k, bytes)
    try:
        for item in headers:
            if item is None:
                continue
            # tuple-like
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                k = item[0]
                v = item[1]
                if isinstance(v, (bytes, bytearray)):
                    try:
                        carrier[k] = v.decode("utf-8")
                    except Exception:
                        carrier[k] = str(v)
                else:
                    carrier[k] = str(v)
            else:
                # Spark might provide struct-like headers with key/value fields
                # attempt attribute access
                k = getattr(item, "key", None) or item[0]
                v = getattr(item, "value", None) or item[1]
                if isinstance(v, (bytes, bytearray)):
                    try:
                        carrier[k] = v.decode("utf-8")
                    except Exception:
                        carrier[k] = str(v)
                else:
                    carrier[k] = str(v)
    except Exception:
        pass
    return carrier


def extract_context_from_kafka_headers(headers) -> Optional[Any]:
    """Extract a context from Kafka headers and start a new span as child.

    Returns the tracer's current span (may be a no-op span if no propagator configured).
    """
    if not _OTEL_AVAILABLE:
        return None
    carrier = kafka_headers_list_to_carrier(headers)
    ctx = propagate.extract(carrier)
    tracer = trace.get_tracer(__name__)
    span = tracer.start_span("kafka.message.process", context=ctx)
    return span


__all__ = [
    "setup_tracer",
    "inject_trace_into_kafka_headers",
    "kafka_headers_list_to_carrier",
    "extract_context_from_kafka_headers",
]
