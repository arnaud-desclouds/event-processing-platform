import json
import os
import time
from typing import Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

app = FastAPI()

producer: Optional[AIOKafkaProducer] = None
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "ingestion-api")

# ---- Metrics --------------------------------------------------------------
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)

EVENTS_INGESTED_TOTAL = Counter(
    "events_ingested_total",
    "Total number of events accepted by the ingestion API",
    ["event_type", "source"],
)


class Event(BaseModel):
    event_id: str
    timestamp: str
    source: str
    type: str
    payload: dict


def _setup_tracing() -> None:
    resource = Resource.create({"service.name": SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


@app.middleware("http")
async def metrics_and_tracing_middleware(request: Request, call_next):
    tracer = trace.get_tracer(__name__)

    start = time.perf_counter()
    with tracer.start_as_current_span(f"{request.method} {request.url.path}") as span:
        span.set_attribute("http.method", request.method)
        span.set_attribute("http.target", request.url.path)

        response = await call_next(request)

        duration = time.perf_counter() - start

        path = request.url.path
        method = request.method
        status_code = str(response.status_code)

        HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(duration)
        HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status_code=status_code).inc()

        span.set_attribute("http.status_code", response.status_code)
        span.set_attribute("http.duration_ms", duration * 1000.0)

        return response


@app.on_event("startup")
async def startup():
    global producer
    _setup_tracing()
    producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
    await producer.start()


@app.on_event("shutdown")
async def shutdown():
    if producer is not None:
        await producer.stop()


@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/events")
async def ingest_event(event: Event):
    assert producer is not None, "producer not initialized"

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("publish_event") as span:
        span.set_attribute("event.id", event.event_id)
        span.set_attribute("event.type", event.type)
        span.set_attribute("event.source", event.source)

        # Inject trace context into Kafka headers
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]

        await producer.send_and_wait(TOPIC, json.dumps(event.model_dump()).encode(), headers=headers)

        EVENTS_INGESTED_TOTAL.labels(event_type=event.type, source=event.source).inc()

    return {"status": "accepted"}


@app.get("/health")
def health():
    return {"status": "ok"}
