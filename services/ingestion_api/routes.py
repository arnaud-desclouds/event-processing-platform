from __future__ import annotations

import inspect
from typing import Protocol

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from services.common import EventModel

from .observability import metrics_response

router = APIRouter()


class SupportsPublishEvent(Protocol):
    async def publish_event(self, event: EventModel) -> None: ...


def _publisher_from_request(request: Request) -> SupportsPublishEvent:
    publisher = getattr(request.app.state, "publisher", None)
    if publisher is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")
    return publisher


async def _readiness_details(request: Request) -> tuple[bool, bool]:
    startup_complete = bool(getattr(request.app.state, "startup_complete", False))
    publisher = getattr(request.app.state, "publisher", None)
    publisher_ready = False
    if publisher is not None:
        is_ready = getattr(publisher, "is_ready", None)
        if callable(is_ready):
            try:
                ready_result = is_ready()
                if inspect.isawaitable(ready_result):
                    publisher_ready = bool(await ready_result)
                else:
                    publisher_ready = bool(ready_result)
            except Exception:
                publisher_ready = False
    return startup_complete, publisher_ready


@router.get("/metrics")
def metrics() -> Response:
    return metrics_response()


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    startup_complete, publisher_ready = await _readiness_details(request)
    ready = startup_complete and publisher_ready
    payload = {
        "ready": ready,
        "startup_complete": startup_complete,
        "publisher_ready": publisher_ready,
    }
    return JSONResponse(content=payload, status_code=200 if ready else 503)


@router.post(
    "/v1/events",
    responses={503: {"description": "Kafka producer unavailable or publish failed"}},
)
async def ingest_event(event: EventModel, request: Request) -> dict[str, str]:
    publisher = _publisher_from_request(request)
    await publisher.publish_event(event)
    return {"status": "accepted"}


@router.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
