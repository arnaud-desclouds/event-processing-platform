from __future__ import annotations

import logging
import sys
from collections.abc import Mapping, MutableMapping, Sequence
from datetime import datetime, timezone
from typing import Any

import structlog
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram, start_http_server

from .settings import Settings

EVENTS_PROCESSED_TOTAL = Counter(
    "events_processed_total",
    "Total number of events processed by the worker",
    ["event_type", "source"],
)

EVENTS_FAILED_TOTAL = Counter(
    "events_failed_total",
    "Total number of events that failed processing",
    ["reason"],
)

EVENTS_DEDUPED_TOTAL = Counter(
    "events_deduped_total",
    "Total number of duplicate events skipped by the worker",
    ["source"],
)

EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing a single event in seconds",
)


def _iso_utc_timestamp(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> Mapping[str, Any]:
    event_dict["ts"] = datetime.now(timezone.utc).isoformat()
    return event_dict


def configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)
    root.setLevel(level.upper())

    logging.getLogger("aiokafka").setLevel(max(root.level, logging.INFO))
    logging.getLogger("asyncpg").setLevel(max(root.level, logging.INFO))

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            _iso_utc_timestamp,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(root.level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def extract_trace_context(headers: Sequence[tuple[str, bytes | None]]) -> Context:
    carrier: dict[str, str] = {}
    for key, value in headers:
        if value is None:
            continue
        carrier[key] = value.decode("utf-8", errors="ignore")
    return propagate.extract(carrier)


def setup_tracing(settings: Settings) -> None:
    resource = Resource.create({"service.name": settings.service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def start_metrics_server(settings: Settings, log: structlog.stdlib.BoundLogger) -> None:
    start_http_server(settings.metrics_port)
    log.info("metrics_server_started", port=settings.metrics_port)
