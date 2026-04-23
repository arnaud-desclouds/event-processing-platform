from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import asyncpg
import pytest
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from services.processor_worker import consumer_loop, parsing, processing


class WorkerLogSink:
    def info(self, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, *args: object, **kwargs: object) -> None:
        pass

    def error(self, *args: object, **kwargs: object) -> None:
        pass

    def exception(self, *args: object, **kwargs: object) -> None:
        pass


def _event_bytes(*, event_id: str, event_type: str, source: str) -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "timestamp": "2026-01-01T00:00:00Z",
            "type": event_type,
            "source": source,
            "payload": {"cart_id": "cart-42"},
        }
    ).encode("utf-8")

def _build_test_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    monkeypatch.setattr(consumer_loop.trace, "get_tracer", provider.get_tracer)
    monkeypatch.setattr(processing.trace, "get_tracer", provider.get_tracer)

    return provider, exporter


def _span_by_name(exporter: InMemorySpanExporter, name: str):
    for span in exporter.get_finished_spans():
        if span.name == name:
            return span
    raise AssertionError(f"span {name!r} not found")


def test_parse_event_sends_invalid_payloads_to_dlq() -> None:
    cases = [
        ("invalid_utf8", b"\xff", "raw_utf8"),
        ("invalid_json", b"{", "raw"),
        (
            "invalid_event",
            json.dumps(
                {
                    "event_id": "missing-timestamp",
                    "type": "user.signup",
                    "source": "web-ui",
                    "payload": {"cart_id": "cart-42"},
                }
            ).encode("utf-8"),
            "detail",
        ),
    ]

    for expected_reason, raw, expected_field in cases:
        event, dlq_payload = parsing.parse_event_or_log(raw, log=WorkerLogSink())

        assert event is None, expected_reason
        assert dlq_payload is not None, expected_reason
        assert dlq_payload["reason"] == expected_reason
        assert expected_field in dlq_payload


@pytest.mark.asyncio
async def test_process_message_sends_db_failures_to_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    class PostgresInsertError(asyncpg.PostgresError):
        pass

    async def raise_postgres_error(pool_arg: object, event: dict[str, object]) -> bool:
        assert pool_arg is pool
        assert event["event_id"] == "invoice-77"
        raise PostgresInsertError("db broke")

    pool = object()
    monkeypatch.setattr(processing, "_persist_event", raise_postgres_error)

    result = await processing.process_message(
        pool=pool,
        raw=_event_bytes(
            event_id="invoice-77",
            event_type="invoice.finalized",
            source="billing-service",
        ),
        log=WorkerLogSink(),
    )

    assert result.action == "sent_to_dlq"
    assert result.event_id == "invoice-77"
    assert result.reason == "db_error"
    assert result.dlq_payload is not None
    assert result.dlq_payload["reason"] == "db_error"
    assert result.dlq_payload["failed_event"]["event_id"] == "invoice-77"


@pytest.mark.asyncio
async def test_consumer_loop_continues_kafka_trace_context_and_emits_child_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OneMessageKafkaConsumer:
        def __init__(self, message: SimpleNamespace, stop_event: asyncio.Event) -> None:
            self._message = message
            self._stop_event = stop_event
            self._delivered = False

        async def getone(self) -> SimpleNamespace:
            if self._delivered:
                raise AssertionError("getone called more than once")
            self._delivered = True
            return self._message

        async def commit(self) -> None:
            self._stop_event.set()

    class UnexpectedDlqPublisher:
        async def send_and_wait(self, topic: str, payload: bytes) -> None:
            del topic, payload
            raise AssertionError("DLQ producer should not be used for a successful message")

    async def persist_event_successfully(pool_arg: object, event: dict[str, object]) -> bool:
        assert pool_arg is pool
        assert event["event_id"] == "trace-evt-1"
        return True

    provider, exporter = _build_test_tracing(monkeypatch)
    monkeypatch.setattr(processing, "_persist_event", persist_event_successfully)

    pool = object()
    shutdown_event = asyncio.Event()
    carrier: dict[str, str] = {}

    upstream_tracer = provider.get_tracer("test-upstream")
    with upstream_tracer.start_as_current_span("api_publish") as upstream_span:
        propagate.inject(carrier)
        message = SimpleNamespace(
            topic="events.raw",
            partition=2,
            offset=41,
            value=_event_bytes(
                event_id="trace-evt-1",
                event_type="user.signup",
                source="api-test",
            ),
            headers=[(key, value.encode("utf-8")) for key, value in carrier.items()],
        )

        await consumer_loop.run_consumer_loop(
            pool=pool,
            consumer=OneMessageKafkaConsumer(message, shutdown_event),
            dlq_producer=UnexpectedDlqPublisher(),
            dlq_topic="events.dlq",
            log=WorkerLogSink(),
            shutdown_event=shutdown_event,
        )

    provider.force_flush()
    worker_span = _span_by_name(exporter, "process_kafka_message")
    persist_span = _span_by_name(exporter, "persist_event")

    assert worker_span.parent is not None
    assert worker_span.parent.span_id == upstream_span.get_span_context().span_id
    assert worker_span.attributes["messaging.destination.name"] == "events.raw"
    assert worker_span.attributes["messaging.kafka.partition"] == 2
    assert worker_span.attributes["messaging.kafka.offset"] == 41
    assert worker_span.attributes["event.id"] == "trace-evt-1"
    assert worker_span.attributes["event.processing.action"] == "processed"
    assert persist_span.parent is not None
    assert persist_span.parent.span_id == worker_span.context.span_id
