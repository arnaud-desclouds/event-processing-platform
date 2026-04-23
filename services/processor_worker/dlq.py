from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaProducer
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from structlog.stdlib import BoundLogger

from .json_codec import dumps_json_bytes

JSONDict = dict[str, Any]


def build_dlq_message(
    *,
    reason: str,
    raw: bytes,
    failed_event: JSONDict | None = None,
    detail: str | None = None,
) -> JSONDict:
    message: JSONDict = {
        "reason": reason,
        "failed_at": datetime.now(UTC).isoformat(),
    }
    if detail is not None:
        message["detail"] = detail
    if failed_event is not None:
        message["failed_event"] = failed_event
        return message

    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError:
        message["raw_utf8"] = raw.decode("utf-8", errors="replace")
    else:
        message["raw"] = decoded

    return message


async def publish_to_dlq(
    *,
    producer: AIOKafkaProducer,
    topic: str,
    payload: JSONDict,
    log: BoundLogger,
    event_id: str | None = None,
    reason: str,
) -> None:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("publish_to_dlq") as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.operation", "publish")
        span.set_attribute("messaging.destination.name", topic)
        span.set_attribute("dlq.reason", reason)
        if event_id is not None:
            span.set_attribute("event.id", event_id)

        try:
            await producer.send_and_wait(topic, dumps_json_bytes(payload))
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            raise

    log.warning("event_published_to_dlq", event_id=event_id, reason=reason)
