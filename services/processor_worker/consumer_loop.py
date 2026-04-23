from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode, Tracer
from structlog.stdlib import BoundLogger

from .dlq import publish_to_dlq
from .observability import EVENT_PROCESSING_DURATION_SECONDS, extract_trace_context
from .processing import ProcessingResult, process_message


async def _poll_message(
    *,
    consumer: AIOKafkaConsumer,
    log: BoundLogger,
) -> tuple[bool, Any | None]:
    try:
        return True, await asyncio.wait_for(consumer.getone(), timeout=1.0)
    except asyncio.TimeoutError:
        return True, None
    except KafkaError:
        log.exception("kafka_consume_error")
    except OSError:
        log.exception("kafka_consume_os_error")
    except Exception:
        log.exception("kafka_consume_unexpected_error")

    return False, None


def _set_message_span_attributes(*, span: Span, msg: Any) -> None:
    span.set_attribute("messaging.system", "kafka")
    span.set_attribute("messaging.operation", "process")
    span.set_attribute("messaging.destination.name", msg.topic)
    span.set_attribute("messaging.kafka.partition", msg.partition)
    span.set_attribute("messaging.kafka.offset", msg.offset)


def _set_result_span_attributes(*, span: Span, result: ProcessingResult) -> None:
    span.set_attribute("event.processing.action", result.action)
    if result.event_id is not None:
        span.set_attribute("event.id", result.event_id)
    if result.reason is not None:
        span.set_attribute("event.processing.reason", result.reason)


async def _publish_dlq_for_result(
    *,
    result: ProcessingResult,
    dlq_producer: AIOKafkaProducer,
    dlq_topic: str,
    log: BoundLogger,
    span: Span,
) -> bool:
    if result.action != "sent_to_dlq":
        return True

    if result.dlq_payload is None or result.reason is None:
        span.set_status(Status(StatusCode.ERROR))
        log.exception("dlq_payload_missing")
        return False

    try:
        await publish_to_dlq(
            producer=dlq_producer,
            topic=dlq_topic,
            payload=result.dlq_payload,
            log=log,
            event_id=result.event_id,
            reason=result.reason,
        )
    except KafkaError as exc:
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))
        if result.event_id is not None and result.reason == "db_error":
            log.exception(
                "event_db_insert_failed_dlq_publish_error",
                event_id=result.event_id,
            )
        else:
            log.exception("dlq_publish_error")
        return False

    return True


async def _commit_message(*, consumer: AIOKafkaConsumer, log: BoundLogger, span: Span) -> bool:
    try:
        await consumer.commit()
    except KafkaError as exc:
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))
        log.exception("kafka_commit_error")
        return False

    return True


async def _process_consumed_message(
    *,
    msg: Any,
    pool: asyncpg.Pool,
    consumer: AIOKafkaConsumer,
    dlq_producer: AIOKafkaProducer,
    dlq_topic: str,
    log: BoundLogger,
    tracer: Tracer,
) -> bool:
    parent_context = extract_trace_context(msg.headers)
    with tracer.start_as_current_span("process_kafka_message", context=parent_context) as span:
        _set_message_span_attributes(span=span, msg=msg)

        with EVENT_PROCESSING_DURATION_SECONDS.time():
            result = await process_message(pool=pool, raw=msg.value, log=log)

        _set_result_span_attributes(span=span, result=result)
        if result.action == "fatal_error":
            span.set_status(Status(StatusCode.ERROR))
            return False

        if not await _publish_dlq_for_result(
            result=result,
            dlq_producer=dlq_producer,
            dlq_topic=dlq_topic,
            log=log,
            span=span,
        ):
            return False

        return await _commit_message(consumer=consumer, log=log, span=span)


async def run_consumer_loop(
    *,
    pool: asyncpg.Pool,
    consumer: AIOKafkaConsumer,
    dlq_producer: AIOKafkaProducer,
    dlq_topic: str,
    log: BoundLogger,
    shutdown_event: asyncio.Event,
) -> None:
    tracer = trace.get_tracer(__name__)

    while not shutdown_event.is_set():
        should_continue, msg = await _poll_message(consumer=consumer, log=log)
        if not should_continue:
            break

        if msg is None or msg.value is None:
            continue

        if not await _process_consumed_message(
            msg=msg,
            pool=pool,
            consumer=consumer,
            dlq_producer=dlq_producer,
            dlq_topic=dlq_topic,
            log=log,
            tracer=tracer,
        ):
            break
