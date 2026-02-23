from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from opentelemetry import propagate, trace
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from . import db as db_mod
from .health import ReadinessState
from .kafka import commit_message, extract_trace_carrier
from .observability import (
    EVENT_PROCESSING_DURATION_SECONDS,
    EVENT_RETRIES_TOTAL,
    EVENTS_FAILED_TOTAL,
    EVENTS_PROCESSED_TOTAL,
    EVENTS_SENT_TO_DLQ_TOTAL,
)
from .settings import Settings


def _encode(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate_event_shape(event: Dict[str, Any]) -> Optional[str]:
    required = ("event_id", "type", "source", "payload")
    missing = [k for k in required if k not in event]
    if missing:
        return "missing_fields"
    return None


async def _send_to_dlq_wrapped(
    *, producer: AIOKafkaProducer, dlq_topic: str, event: Dict[str, Any], reason: str
) -> None:
    dlq_event = {"failed_event": event, "failed_at": time.time(), "reason": reason}
    await producer.send_and_wait(dlq_topic, _encode(dlq_event))
    EVENTS_SENT_TO_DLQ_TOTAL.inc()


async def _send_to_dlq_invalid_json(
    *, producer: AIOKafkaProducer, dlq_topic: str, raw_text: str
) -> None:
    raw = {"raw": raw_text, "reason": "invalid_json"}
    await producer.send_and_wait(dlq_topic, _encode(raw))
    EVENTS_SENT_TO_DLQ_TOTAL.inc()


def _on_insert_retry(log: structlog.stdlib.BoundLogger, msg_fields: dict[str, Any], event_id: str):
    def _hook(state: RetryCallState) -> None:
        EVENT_RETRIES_TOTAL.inc()
        exc = state.outcome.exception() if state.outcome else None
        reason = "db_timeout" if isinstance(exc, asyncio.TimeoutError) else "db_error"
        log.warning(
            "retrying_event",
            **msg_fields,
            event_id=event_id,
            attempt=state.attempt_number - 1,
            reason=reason,
            error_type=type(exc).__name__ if exc else None,
        )

    return _hook


async def _insert_with_timeout(
    conn: asyncpg.Connection, event: Dict[str, Any], timeout: float
) -> None:
    await asyncio.wait_for(db_mod.insert_event(conn, event), timeout=timeout)


async def run(
    *,
    settings: Settings,
    consumer: AIOKafkaConsumer,
    dlq_producer: AIOKafkaProducer,
    stop_event: asyncio.Event,
    log: structlog.stdlib.BoundLogger,
    readiness: ReadinessState,
) -> None:
    tracer = trace.get_tracer(__name__)
    conn: Optional[asyncpg.Connection] = None

    try:
        conn = await db_mod.connect_with_retries(settings, log)
        readiness.db_ready = True
        log.info("worker_started", reason="ready")

        async for msg in consumer:
            if stop_event.is_set():
                break

            start = time.perf_counter()
            msg_fields = {"topic": msg.topic, "partition": msg.partition, "offset": msg.offset}

            parent_ctx = propagate.extract(extract_trace_carrier(msg.headers))
            with tracer.start_as_current_span("process_event", context=parent_ctx) as span:
                try:
                    text = msg.value.decode("utf-8")
                    event = json.loads(text)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    EVENTS_FAILED_TOTAL.labels(reason="invalid_json").inc()
                    span.set_attribute("error", True)
                    span.set_attribute("failure.reason", "invalid_json")

                    raw_text = msg.value.decode("utf-8", errors="replace")
                    try:
                        await _send_to_dlq_invalid_json(
                            producer=dlq_producer,
                            dlq_topic=settings.dlq_topic,
                            raw_text=raw_text,
                        )
                    except KafkaError:
                        log.exception("dlq_publish_failed_invalid_json", **msg_fields)
                        continue

                    await commit_message(consumer, msg)
                    log.warning("invalid_json_sent_to_dlq", **msg_fields)
                    continue

                if not isinstance(event, dict):
                    EVENTS_FAILED_TOTAL.labels(reason="invalid_event_type").inc()
                    span.set_attribute("error", True)
                    span.set_attribute("failure.reason", "invalid_event_type")
                    try:
                        await _send_to_dlq_wrapped(
                            producer=dlq_producer,
                            dlq_topic=settings.dlq_topic,
                            event={"failed_parse": True, "value": event},
                            reason="invalid_event_type",
                        )
                    except KafkaError:
                        log.exception("dlq_publish_failed_invalid_event_type", **msg_fields)
                        continue
                    await commit_message(consumer, msg)
                    continue

                event_id = str(event.get("event_id") or "")
                event_type = event.get("type")
                source = event.get("source")

                span.set_attribute("event.id", event_id)
                span.set_attribute("event.type", event_type)
                span.set_attribute("event.source", source)

                shape_reason = _validate_event_shape(event)
                if shape_reason is not None:
                    EVENTS_FAILED_TOTAL.labels(reason=shape_reason).inc()
                    span.set_attribute("error", True)
                    span.set_attribute("failure.reason", shape_reason)

                    try:
                        await _send_to_dlq_wrapped(
                            producer=dlq_producer,
                            dlq_topic=settings.dlq_topic,
                            event=event,
                            reason=shape_reason,
                        )
                    except KafkaError:
                        log.exception(
                            "dlq_publish_failed_missing_fields", **msg_fields, event_id=event_id
                        )
                        continue

                    await commit_message(consumer, msg)
                    log.error("sent_to_dlq", **msg_fields, event_id=event_id, reason=shape_reason)
                    continue

                if conn is None or conn.is_closed():
                    readiness.db_ready = False
                    conn = await db_mod.connect_with_retries(settings, log)
                    readiness.db_ready = True

                max_attempts = settings.max_retries + 1
                retrying = AsyncRetrying(
                    retry=retry_if_exception_type(
                        (asyncio.TimeoutError, asyncpg.PostgresError, OSError)
                    ),
                    stop=stop_after_attempt(max_attempts),
                    wait=wait_exponential(
                        multiplier=settings.base_backoff_seconds, max=settings.max_backoff_seconds
                    ),
                    reraise=True,
                    before_sleep=_on_insert_retry(log, msg_fields, event_id),
                )

                try:
                    async for attempt in retrying:
                        with attempt:
                            await _insert_with_timeout(
                                conn, event, settings.db_write_timeout_seconds
                            )
                except asyncio.TimeoutError:
                    reason = "db_timeout"
                except (asyncpg.PostgresError, OSError) as exc:
                    reason = "db_error"
                    span.set_attribute("db.error_type", type(exc).__name__)
                    try:
                        if conn is not None and not conn.is_closed():
                            await conn.close()
                    except (asyncpg.PostgresError, OSError):
                        pass
                    conn = None
                    readiness.db_ready = False
                else:
                    EVENTS_PROCESSED_TOTAL.labels(
                        event_type=str(event_type or "unknown"),
                        source=str(source or "unknown"),
                    ).inc()
                    EVENT_PROCESSING_DURATION_SECONDS.observe(time.perf_counter() - start)
                    span.set_attribute(
                        "processing.duration_ms", (time.perf_counter() - start) * 1000.0
                    )

                    await commit_message(consumer, msg)
                    log.info(
                        "event_processed",
                        **msg_fields,
                        event_id=event_id,
                        event_type=str(event_type or "unknown"),
                        source=str(source or "unknown"),
                    )
                    continue

                EVENTS_FAILED_TOTAL.labels(reason=reason).inc()
                span.set_attribute("error", True)
                span.set_attribute("failure.reason", reason)

                try:
                    await _send_to_dlq_wrapped(
                        producer=dlq_producer,
                        dlq_topic=settings.dlq_topic,
                        event=event,
                        reason=reason,
                    )
                except KafkaError:
                    log.exception(
                        "dlq_publish_failed", **msg_fields, event_id=event_id, reason=reason
                    )
                    continue

                await commit_message(consumer, msg)
                log.error("sent_to_dlq", **msg_fields, event_id=event_id, reason=reason)

    finally:
        try:
            if conn is not None and not conn.is_closed():
                await conn.close()
        except (asyncpg.PostgresError, OSError):
            log.exception("db_close_failed")
        readiness.db_ready = False
