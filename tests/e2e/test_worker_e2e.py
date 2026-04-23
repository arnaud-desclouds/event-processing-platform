from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import urllib.request
from collections.abc import Callable, Iterator
from uuid import uuid4

import asyncpg
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from testcontainers.compose import DockerCompose

pytestmark = pytest.mark.asyncio  # noqa: F841


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


async def _wait_for_postgres(dsn: str, timeout_s: float = 45.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
            return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("postgres did not become ready")


async def _wait_for_http_200(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError(f"{url} did not become ready")


def _post_event(url: str, event: dict[str, object]) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as resp:
        assert resp.status == 200
        payload = json.loads(resp.read().decode("utf-8"))
        assert payload == {"status": "accepted"}


async def _wait_for_event_row(
    pg_dsn: str,
    *,
    event_id: str,
    timeout_s: float = 30.0,
) -> asyncpg.Record:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await conn.fetchrow(
                "SELECT event_id, type, source FROM events WHERE event_id = $1",
                event_id,
            )
            if row is not None:
                return row
        finally:
            await conn.close()
        await asyncio.sleep(0.5)

    raise AssertionError(f"event {event_id!r} was not inserted into DB within timeout")


async def _event_count(pg_dsn: str, *, event_id: str) -> int:
    conn = await asyncpg.connect(pg_dsn)
    try:
        return int(
            await conn.fetchval("SELECT COUNT(*) FROM events WHERE event_id = $1", event_id)
        )
    finally:
        await conn.close()


async def _produce_raw_message(*, brokers: str, topic: str, payload: bytes) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=brokers)
    await producer.start()
    try:
        await producer.send_and_wait(topic, payload)
    finally:
        await producer.stop()


async def _wait_for_dlq_message(
    *,
    brokers: str,
    predicate: Callable[[dict[str, object]], bool],
    timeout_s: float = 30.0,
) -> dict[str, object]:
    consumer = AIOKafkaConsumer(
        "events.dlq",
        bootstrap_servers=brokers,
        group_id=f"test-dlq-{uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    deadline = time.time() + timeout_s

    try:
        while time.time() < deadline:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if msg.value is None:
                continue

            payload = json.loads(msg.value.decode("utf-8"))
            if isinstance(payload, dict) and predicate(payload):
                return payload
    finally:
        await consumer.stop()

    raise AssertionError("expected DLQ message was not observed within timeout")


@pytest.fixture(scope="module")
def compose_stack() -> Iterator[dict[str, str]]:
    root = _repo_root()
    compose = DockerCompose(root, compose_file_name="docker-compose.test.yml", build=True)

    with compose:
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.test.yml", "run", "--rm", "migrate"],
            cwd=root,
            check=True,
            stdout=subprocess.DEVNULL,
        )

        pg_dsn = os.getenv(
            "TEST_POSTGRES_DSN",
            "postgresql://events_user:events_password@localhost:15432/events",
        )
        broker = os.getenv("TEST_REDPANDA_BROKERS", "localhost:29092")
        api_url = "http://localhost:18000"

        asyncio.run(_wait_for_postgres(pg_dsn))
        asyncio.run(_wait_for_http_200(f"{api_url}/readyz"))
        asyncio.run(_wait_for_http_200("http://localhost:18081/readyz"))

        yield {
            "root": root,
            "pg_dsn": pg_dsn,
            "broker": broker,
            "api_url": api_url,
        }


async def test_ingestion_api_processes_event_end_to_end(compose_stack: dict[str, str]) -> None:
    event = {
        "event_id": "it-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "integration_test",
        "source": "pytest",
        "payload": {"hello": "world"},
    }

    _post_event(f"{compose_stack['api_url']}/v1/events", event)
    row = await _wait_for_event_row(compose_stack["pg_dsn"], event_id="it-1")

    assert row["event_id"] == "it-1"
    assert row["type"] == "integration_test"
    assert row["source"] == "pytest"


async def test_duplicate_events_remain_idempotent_end_to_end(compose_stack: dict[str, str]) -> None:
    event = {
        "event_id": "it-duplicate-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "integration_duplicate_test",
        "source": "pytest",
        "payload": {"hello": "world"},
    }

    _post_event(f"{compose_stack['api_url']}/v1/events", event)
    _post_event(f"{compose_stack['api_url']}/v1/events", event)

    await _wait_for_event_row(compose_stack["pg_dsn"], event_id="it-duplicate-1")
    await asyncio.sleep(2.0)

    assert await _event_count(compose_stack["pg_dsn"], event_id="it-duplicate-1") == 1


async def test_invalid_raw_message_is_sent_to_dlq(compose_stack: dict[str, str]) -> None:
    marker = f"it-invalid-{uuid4()}"

    await _produce_raw_message(
        brokers=compose_stack["broker"],
        topic="events.raw",
        payload=f'{{"event_id":"{marker}"'.encode("utf-8"),
    )

    dlq_payload = await _wait_for_dlq_message(
        brokers=compose_stack["broker"],
        predicate=lambda payload: payload.get("reason") == "invalid_json"
        and marker in str(payload.get("raw", "")),
    )

    assert dlq_payload["reason"] == "invalid_json"
    assert marker in str(dlq_payload["raw"])


async def test_dlq_replay_republishes_failed_event_end_to_end(
    compose_stack: dict[str, str],
) -> None:
    event_id = f"it-replay-{uuid4()}"
    failed_event = {
        "event_id": event_id,
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "integration_replay_test",
        "source": "pytest",
        "payload": {"hello": "replayed"},
    }

    await _produce_raw_message(
        brokers=compose_stack["broker"],
        topic="events.dlq",
        payload=json.dumps(
            {
                "reason": "db_error",
                "failed_at": "2026-01-01T00:00:00Z",
                "failed_event": failed_event,
            }
        ).encode("utf-8"),
    )

    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "docker-compose.test.yml",
            "run",
            "--rm",
            "processor-worker",
            "python",
            "-m",
            "services.processor_worker.dlq_replay",
            "--from-beginning",
            "--event-id",
            event_id,
            "--max-messages",
            "1",
        ],
        cwd=compose_stack["root"],
        check=True,
        stdout=subprocess.DEVNULL,
    )

    row = await _wait_for_event_row(compose_stack["pg_dsn"], event_id=event_id)

    assert row["event_id"] == event_id
    assert row["type"] == "integration_replay_test"
    assert row["source"] == "pytest"
