from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import asyncpg
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from structlog.stdlib import BoundLogger

from .db import insert_event
from .dlq import build_dlq_message
from .json_codec import dumps_json_bytes
from .observability import EVENTS_DEDUPED_TOTAL, EVENTS_FAILED_TOTAL, EVENTS_PROCESSED_TOTAL
from .parsing import parse_event_or_log

JSONDict = dict[str, Any]
ProcessingAction = Literal["processed", "deduped", "sent_to_dlq", "fatal_error"]


@dataclass(slots=True)
class ProcessingResult:
    action: ProcessingAction
    event_id: str | None = None
    dlq_payload: JSONDict | None = None
    reason: str | None = None


async def _persist_event(pool: asyncpg.Pool, event: JSONDict) -> bool:
    async with pool.acquire() as conn:
        return await insert_event(conn, event)


async def process_message(*, pool: asyncpg.Pool, raw: bytes, log: BoundLogger) -> ProcessingResult:
    event, dlq_payload = parse_event_or_log(raw, log=log)
    if event is None:
        if dlq_payload is None:
            return ProcessingResult(action="fatal_error")
        return ProcessingResult(
            action="sent_to_dlq",
            dlq_payload=dlq_payload,
            reason=str(dlq_payload["reason"]),
        )

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("persist_event") as span:
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.operation", "INSERT")
        span.set_attribute("event.id", str(event["event_id"]))
        span.set_attribute("event.type", str(event["type"]))
        span.set_attribute("event.source", str(event["source"]))

        try:
            inserted = await _persist_event(pool, event)
            span.set_attribute("event.inserted", inserted)
        except asyncpg.PostgresError as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            EVENTS_FAILED_TOTAL.labels(reason="db_error").inc()
            dlq_payload = build_dlq_message(
                reason="db_error",
                raw=dumps_json_bytes(event),
                failed_event=event,
                detail=type(exc).__name__,
            )
            log.error(
                "event_db_insert_failed",
                err=type(exc).__name__,
                event_id=event["event_id"],
                event_type=event["type"],
                source=event["source"],
            )
            return ProcessingResult(
                action="sent_to_dlq",
                event_id=event["event_id"],
                dlq_payload=dlq_payload,
                reason="db_error",
            )
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            log.exception(
                "event_db_insert_unexpected_error",
                event_id=event["event_id"],
                event_type=event["type"],
                source=event["source"],
            )
            return ProcessingResult(action="fatal_error", event_id=event["event_id"])

    if not inserted:
        EVENTS_DEDUPED_TOTAL.labels(source=event["source"]).inc()
        log.info(
            "event_duplicate_skipped",
            event_id=event["event_id"],
            event_type=event["type"],
            source=event["source"],
        )
        return ProcessingResult(action="deduped", event_id=event["event_id"])

    EVENTS_PROCESSED_TOTAL.labels(event_type=event["type"], source=event["source"]).inc()
    log.info(
        "event_processed",
        event_id=event["event_id"],
        event_type=event["type"],
        source=event["source"],
    )
    return ProcessingResult(action="processed", event_id=event["event_id"])
