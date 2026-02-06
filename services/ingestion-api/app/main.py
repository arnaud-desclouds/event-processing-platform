import json
import os
import time
from typing import Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel

app = FastAPI()

producer: Optional[AIOKafkaProducer] = None
BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")

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


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start

    path = request.url.path
    method = request.method
    status_code = str(response.status_code)

    HTTP_REQUEST_DURATION_SECONDS.labels(method=method, path=path).observe(duration)
    HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status_code=status_code).inc()

    return response


@app.on_event("startup")
async def startup():
    global producer
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
    await producer.send_and_wait(TOPIC, json.dumps(event.model_dump()).encode())

    EVENTS_INGESTED_TOTAL.labels(event_type=event.type, source=event.source).inc()
    return {"status": "accepted"}


@app.get("/health")
def health():
    return {"status": "ok"}
