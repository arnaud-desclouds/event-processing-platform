from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from services.common.event_schema import EventModel
from services.ingestion_api import lifespan as lifespan_module
from services.ingestion_api.observability import metrics_and_tracing_middleware
from services.ingestion_api.publisher import EventPublisher
from services.ingestion_api.routes import router


def _event_payload() -> dict[str, Any]:
    return {
        "event_id": "signup-request-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "user.signup",
        "source": "web-ui",
        "payload": {"plan": "starter"},
    }


def _build_test_app(*, publisher: object | None = None) -> FastAPI:
    app = FastAPI()
    app.middleware("http")(metrics_and_tracing_middleware)
    app.include_router(router)
    app.state.publisher = publisher
    app.state.startup_complete = False
    return app


class SuccessfulPublisher:
    def __init__(self, *, ready: bool = True) -> None:
        self.events: list[EventModel] = []
        self._ready = ready

    async def is_ready(self) -> bool:
        return self._ready

    async def publish_event(self, event: EventModel) -> None:
        self.events.append(event)


class FailingPublisher:
    async def is_ready(self) -> bool:
        return True

    async def publish_event(self, event: EventModel) -> None:
        raise HTTPException(status_code=503, detail="Kafka publish failed")


class KafkaErrorProducer:
    async def send_and_wait(self, *args: object, **kwargs: object) -> None:
        raise KafkaError("boom")


class ExplodingReadinessPublisher:
    async def is_ready(self) -> bool:
        raise RuntimeError("boom")

    async def publish_event(self, event: EventModel) -> None:
        del event


def test_ingest_event_accepts_request_when_publisher_is_available() -> None:
    publisher = SuccessfulPublisher()

    app = _build_test_app(publisher=publisher)
    app.state.startup_complete = True

    with TestClient(app) as client:
        response = client.post("/v1/events", json=_event_payload())

    assert response.status_code == 200
    assert response.json() == {"status": "accepted"}
    assert publisher.events[0].event_id == "signup-request-1"


def test_ingest_event_returns_503_without_a_publisher() -> None:
    with TestClient(_build_test_app()) as client:
        response = client.post("/v1/events", json=_event_payload())

    assert response.status_code == 503
    assert response.json() == {"detail": "Kafka producer not ready"}


def test_ingest_event_returns_503_when_publish_fails() -> None:
    app = _build_test_app(publisher=FailingPublisher())
    app.state.startup_complete = True

    with TestClient(app) as client:
        response = client.post("/v1/events", json=_event_payload())

    assert response.status_code == 503
    assert response.json() == {"detail": "Kafka publish failed"}


@pytest.mark.asyncio
async def test_event_publisher_returns_503_without_a_producer() -> None:
    publisher = EventPublisher(producer=None, topic="events.raw")

    assert await publisher.is_ready() is False

    with pytest.raises(HTTPException) as exc_info:
        await publisher.publish_event(EventModel.model_validate(_event_payload()))

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Kafka producer not ready"


@pytest.mark.asyncio
async def test_event_publisher_translates_kafka_errors_to_503() -> None:
    publisher = EventPublisher(producer=KafkaErrorProducer(), topic="events.raw")

    with pytest.raises(HTTPException) as exc_info:
        await publisher.publish_event(EventModel.model_validate(_event_payload()))

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Kafka publish failed"


@pytest.mark.asyncio
async def test_event_publisher_readiness_checks_topic_metadata() -> None:
    class ReadyProducer:
        async def partitions_for(self, topic: str) -> set[int]:
            assert topic == "events.raw"
            return {0}

    class SlowProducer:
        async def partitions_for(self, topic: str) -> set[int]:
            del topic
            await asyncio.sleep(0.05)
            return {0}

    class FailingProducer:
        async def partitions_for(self, topic: str) -> set[int]:
            del topic
            raise KafkaError("boom")

    cases = [
        ("ready", ReadyProducer(), 1.0, True),
        ("timeout", SlowProducer(), 0.01, False),
        ("kafka_error", FailingProducer(), 1.0, False),
    ]

    for case_name, producer, timeout, expected in cases:
        publisher = EventPublisher(
            producer=producer,
            topic="events.raw",
            readiness_timeout_seconds=timeout,
        )
        assert await publisher.is_ready() is expected, case_name


@pytest.mark.asyncio
async def test_lifespan_retries_publisher_start_before_succeeding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    start_attempts: list[int] = []
    sleep_calls: list[float] = []

    class StartedPublisher:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    started_publisher = StartedPublisher()

    def retry_settings() -> SimpleNamespace:
        return SimpleNamespace(
            brokers="broker:9092",
            topic="events.raw",
            publisher_ready_timeout_seconds=1.0,
            publisher_start_max_attempts=3,
            publisher_start_backoff_seconds=0.25,
        )

    async def start_after_two_failures(**kwargs: object) -> StartedPublisher:
        del kwargs
        start_attempts.append(len(start_attempts) + 1)
        if len(start_attempts) < 3:
            raise RuntimeError("broker unavailable")
        return started_publisher

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(lifespan_module, "Settings", retry_settings)
    monkeypatch.setattr(lifespan_module, "setup_tracing", lambda settings: None)
    monkeypatch.setattr(lifespan_module.EventPublisher, "start", start_after_two_failures)
    monkeypatch.setattr(lifespan_module.asyncio, "sleep", record_sleep)

    async with lifespan_module.lifespan(app):
        assert app.state.publisher is started_publisher
        assert app.state.startup_complete is True

    assert start_attempts == [1, 2, 3]
    assert sleep_calls == [0.25, 0.25]
    assert started_publisher.close_calls == 1
    assert app.state.publisher is None
    assert app.state.startup_complete is False


@pytest.mark.asyncio
async def test_lifespan_leaves_app_unready_if_publisher_start_exhausts_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    sleep_calls: list[float] = []
    attempts = 0

    def retry_settings() -> SimpleNamespace:
        return SimpleNamespace(
            brokers="broker:9092",
            topic="events.raw",
            publisher_ready_timeout_seconds=1.0,
            publisher_start_max_attempts=3,
            publisher_start_backoff_seconds=0.1,
        )

    async def always_failing_start(**kwargs: object) -> object:
        nonlocal attempts
        del kwargs
        attempts += 1
        raise RuntimeError("still unavailable")

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(lifespan_module, "Settings", retry_settings)
    monkeypatch.setattr(lifespan_module, "setup_tracing", lambda settings: None)
    monkeypatch.setattr(lifespan_module.EventPublisher, "start", always_failing_start)
    monkeypatch.setattr(lifespan_module.asyncio, "sleep", record_sleep)

    with pytest.raises(RuntimeError, match="still unavailable"):
        async with lifespan_module.lifespan(app):
            pytest.fail("lifespan should not yield on startup failure")

    assert attempts == 3
    assert sleep_calls == [0.1, 0.1]
    assert app.state.publisher is None
    assert app.state.startup_complete is False


def test_readyz_reports_startup_and_publisher_state() -> None:
    cases = [
        (
            "ready",
            SuccessfulPublisher(),
            True,
            200,
            {"ready": True, "startup_complete": True, "publisher_ready": True},
        ),
        (
            "missing_publisher",
            None,
            True,
            503,
            {"ready": False, "startup_complete": True, "publisher_ready": False},
        ),
        (
            "startup_incomplete",
            SuccessfulPublisher(),
            False,
            503,
            {"ready": False, "startup_complete": False, "publisher_ready": True},
        ),
        (
            "arbitrary_publisher",
            object(),
            True,
            503,
            {"ready": False, "startup_complete": True, "publisher_ready": False},
        ),
        (
            "publisher_not_ready",
            SuccessfulPublisher(ready=False),
            True,
            503,
            {"ready": False, "startup_complete": True, "publisher_ready": False},
        ),
        (
            "readiness_raises",
            ExplodingReadinessPublisher(),
            True,
            503,
            {"ready": False, "startup_complete": True, "publisher_ready": False},
        ),
    ]

    for case_name, publisher, startup_complete, expected_status, expected_payload in cases:
        app = _build_test_app(publisher=publisher)
        app.state.startup_complete = startup_complete

        with TestClient(app) as client:
            response = client.get("/readyz")

        assert response.status_code == expected_status, case_name
        assert response.json() == expected_payload, case_name


def test_healthz_reports_liveness() -> None:
    with TestClient(_build_test_app()) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ingest_event_rejects_blank_required_text_fields() -> None:
    app = _build_test_app(publisher=SuccessfulPublisher())
    app.state.startup_complete = True
    payload = _event_payload()
    payload["event_id"] = "   "

    with TestClient(app) as client:
        response = client.post("/v1/events", json=payload)

    assert response.status_code == 422
