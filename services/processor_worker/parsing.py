from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError
from structlog.stdlib import BoundLogger

from services.common import validate_event

from .dlq import build_dlq_message
from .json_codec import loads_json_bytes
from .observability import EVENTS_FAILED_TOTAL

JSONDict = dict[str, Any]


def _emit_parse_failure(log: BoundLogger, *, reason: str, detail: str | None = None) -> None:
    EVENTS_FAILED_TOTAL.labels(reason=reason).inc()
    if detail is None:
        log.warning("event_parse_failed", reason=reason)
    else:
        log.warning("event_parse_failed", reason=reason, detail=detail)


def parse_event_or_log(raw: bytes, *, log: BoundLogger) -> tuple[JSONDict | None, JSONDict | None]:
    """Return either a validated event or a DLQ payload for one Kafka message."""
    obj: Any = None
    try:
        obj = loads_json_bytes(raw)
        event = validate_event(obj)
        return event, None
    except UnicodeDecodeError:
        _emit_parse_failure(log, reason="invalid_utf8")
        return None, build_dlq_message(reason="invalid_utf8", raw=raw)
    except json.JSONDecodeError:
        _emit_parse_failure(log, reason="invalid_json")
        return None, build_dlq_message(reason="invalid_json", raw=raw)
    except ValidationError as exc:
        _emit_parse_failure(log, reason="invalid_event", detail=str(exc))
        failed_event = obj if isinstance(obj, dict) else None
        return None, build_dlq_message(
            reason="invalid_event",
            raw=raw,
            failed_event=failed_event,
            detail=str(exc),
        )
