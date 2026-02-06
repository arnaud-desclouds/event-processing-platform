import asyncio
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Histogram, start_http_server

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("EVENTS_TOPIC", "events.raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "events.dlq")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "events")
POSTGRES_USER = os.getenv("POSTGRES_USER", "events_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "events_password")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "processor-worker")

METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "8000"))

MAX_RETRIES = int(os.getenv("WORKER_MAX_RETRIES", "5"))
BASE_BACKOFF_SECONDS = float(os.getenv("WORKER_BASE_BACKOFF_SECONDS", "0.25"))
MAX_BACKOFF_SECONDS = float(os.getenv("WORKER_MAX_BACKOFF_SECONDS", "5.0"))
DB_WRITE_TIMEOUT_SECONDS = float(os.getenv("WORKER_DB_WRITE_TIMEOUT_SECONDS", "2.0"))

DB_CONNECT_MAX_RETRIES = int(os.getenv("WORKER_DB_CONNECT_MAX_RETRIES", "30"))
DB_CONNECT_BASE_BACKOFF_SECONDS = float(os.getenv("WORKER_DB_CONNECT_BASE_BACKOFF_SECONDS", "0.5"))
DB_CONNECT_MAX_BACKOFF_SECONDS = float(os.getenv("WORKER_DB_CONNECT_MAX_BACKOFF_SECONDS", "5.0"))

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

EVENT_RETRIES_TOTAL = Counter(
    "event_retries_total",
    "Total number of retries performed while processing events",
)

EVENTS_SENT_TO_DLQ_TOTAL = Counter(
    "events_sent_to_dlq_total",
    "Total number of events sent to the dead-letter queue",
)

EVENT_PROCESSING_DURATION_SECONDS = Histogram(
    "event_processing_duration_seconds",
    "Time spent processing a single event in seconds",
)

DB_RECONNECTS_TOTAL = Counter(
    "db_reconnects_total",
    "Total number of DB reconnect attempts performed by the worker",
)


def _setup_tracing() -> None:
    resource = Resource.create({"service.name": SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def _encode(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _extract_trace_carrier(headers: Optional[list[Tuple[str, bytes]]]) -> Dict[str, str]:
    carrier: Dict[str, str] = {}
    if not headers:
        return carrier
    for k, v in headers:
        try:
            carrier[k] = v.decode("utf-8")
        except Exception:
            continue
    return carrier


async def _connect_db_with_retries() -> asyncpg.Connection:
    attempt = 0
    while True:
        try:
            conn = await asyncpg.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            return conn
        except Exception as e:
            attempt += 1
            DB_RECONNECTS_TOTAL.inc()

            if attempt > DB_CONNECT_MAX_RETRIES:
                raise RuntimeError(f"failed to connect to postgres after {DB_CONNECT_MAX_RETRIES} retries") from e

            backoff = min(DB_CONNECT_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)), DB_CONNECT_MAX_BACKOFF_SECONDS)
            print(f"db_connect_failed attempt={attempt} backoff={backoff:.2f}s err={type(e).__name__}")
            await asyncio.sleep(backoff)


async def _ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
                                              id SERIAL PRIMARY KEY,
                                              event_id TEXT,
                                              type TEXT,
                                              source TEXT,
                                              payload JSONB,
                                              created_at TIMESTAMP DEFAULT NOW()
            )
        """
    )


async def _insert_event(conn: asyncpg.Connection, event: Dict[str, Any]) -> None:
    await conn.execute(
        "INSERT INTO events(event_id, type, source, payload) VALUES($1,$2,$3,$4)",
        event["event_id"],
        event["type"],
        event["source"],
        json.dumps(event["payload"]),
    )


async def _send_to_dlq(producer: AIOKafkaProducer, event: Dict[str, Any], reason: str) -> None:
    dlq_event = {"failed_event": event, "failed_at": time.time(), "reason": reason}
    await producer.send_and_wait(DLQ_TOPIC, _encode(dlq_event))
    EVENTS_SENT_TO_DLQ_TOTAL.inc()


async def main():
    _setup_tracing()
    tracer = trace.get_tracer(__name__)

    start_http_server(METRICS_PORT)
    print(f"metrics server listening on :{METRICS_PORT}")

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        group_id="processor-group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()

    dlq_producer = AIOKafkaProducer(bootstrap_servers=BROKERS)
    await dlq_producer.start()

    conn: Optional[asyncpg.Connection] = None

    try:
        conn = await _connect_db_with_retries()
        await _ensure_schema(conn)
        print("worker started, waiting for events...")

        async for msg in consumer:
            start = time.perf_counter()

            carrier = _extract_trace_carrier(msg.headers)
            parent_ctx = propagate.extract(carrier)

            with tracer.start_as_current_span("process_event", context=parent_ctx) as span:
                try:
                    event = json.loads(msg.value.decode("utf-8"))
                except Exception:
                    EVENTS_FAILED_TOTAL.labels(reason="invalid_json").inc()
                    span.set_attribute("error", True)
                    span.set_attribute("failure.reason", "invalid_json")

                    raw = {"raw": msg.value.decode("utf-8", errors="replace"), "reason": "invalid_json"}
                    await dlq_producer.send_and_wait(DLQ_TOPIC, _encode(raw))
                    EVENTS_SENT_TO_DLQ_TOTAL.inc()
                    continue

                span.set_attribute("event.id", event.get("event_id"))
                span.set_attribute("event.type", event.get("type"))
                span.set_attribute("event.source", event.get("source"))

                reason = "unknown"
                attempt = 0

                while True:
                    if conn is None or conn.is_closed():
                        DB_RECONNECTS_TOTAL.inc()
                        conn = await _connect_db_with_retries()
                        await _ensure_schema(conn)

                    try:
                        with tracer.start_as_current_span("db_insert"):
                            await asyncio.wait_for(_insert_event(conn, event), timeout=DB_WRITE_TIMEOUT_SECONDS)

                        EVENTS_PROCESSED_TOTAL.labels(
                            event_type=event.get("type", "unknown"),
                            source=event.get("source", "unknown"),
                        ).inc()
                        EVENT_PROCESSING_DURATION_SECONDS.observe(time.perf_counter() - start)

                        span.set_attribute("processing.duration_ms", (time.perf_counter() - start) * 1000.0)
                        print(
                            f"processed event_id={event.get('event_id')} "
                            f"type={event.get('type')} "
                            f"source={event.get('source')}"
                        )
                        break

                    except asyncio.TimeoutError:
                        reason = "db_timeout"
                    except (asyncpg.PostgresError, OSError) as e:
                        reason = "db_error"
                        span.set_attribute("db.error_type", type(e).__name__)
                        try:
                            await conn.close()
                        except Exception:
                            pass
                        conn = None
                    except Exception as e:
                        reason = "db_error"
                        span.set_attribute("db.error_type", type(e).__name__)

                    if attempt >= MAX_RETRIES:
                        EVENTS_FAILED_TOTAL.labels(reason=reason).inc()
                        span.set_attribute("error", True)
                        span.set_attribute("failure.reason", reason)

                        await _send_to_dlq(dlq_producer, event, reason=reason)
                        print(f"sent_to_dlq event_id={event.get('event_id')} reason={reason} attempts={attempt}")
                        break

                    attempt += 1
                    EVENT_RETRIES_TOTAL.inc()
                    backoff = min(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)
                    await asyncio.sleep(backoff)

    finally:
        await consumer.stop()
        await dlq_producer.stop()
        if conn is not None and not conn.is_closed():
            await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
