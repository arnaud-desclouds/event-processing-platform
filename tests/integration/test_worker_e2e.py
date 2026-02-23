from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.request

import asyncpg
import pytest
from aiokafka import AIOKafkaProducer
from testcontainers.compose import DockerCompose

pytestmark = pytest.mark.asyncio


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


async def _wait_for_kafka(bootstrap: str, timeout_s: float = 45.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            p = AIOKafkaProducer(bootstrap_servers=bootstrap)
            await p.start()
            await p.stop()
            return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("kafka did not become ready")


async def _wait_for_readyz(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("worker /readyz did not become ready")


async def test_worker_inserts_event_end_to_end() -> None:
    root = _repo_root()
    compose = DockerCompose(root, compose_file_name="docker-compose.test.yml")

    with compose:
        pg_dsn = os.getenv(
            "TEST_POSTGRES_DSN",
            "postgresql://events_user:events_password@localhost:15432/events",
        )
        kafka_bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP", "localhost:29092")
        topic = os.getenv("TEST_EVENTS_TOPIC", "events.raw")

        # Run migrations (one-shot service)
        rc = os.system("docker compose -f docker-compose.test.yml run --rm migrate > /dev/null")
        assert rc == 0

        await _wait_for_postgres(pg_dsn)
        await _wait_for_kafka(kafka_bootstrap)
        await _wait_for_readyz("http://localhost:18081/readyz")

        event = {
            "event_id": "it-1",
            "type": "integration_test",
            "source": "pytest",
            "payload": {"hello": "world"},
        }

        producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(topic, json.dumps(event).encode("utf-8"))
        finally:
            await producer.stop()

        deadline = time.time() + 30.0
        while time.time() < deadline:
            conn = await asyncpg.connect(pg_dsn)
            try:
                row = await conn.fetchrow(
                    "SELECT event_id, type, source FROM events WHERE event_id = $1", "it-1"
                )
                if row is not None:
                    assert row["event_id"] == "it-1"
                    assert row["type"] == "integration_test"
                    assert row["source"] == "pytest"
                    return
            finally:
                await conn.close()
            await asyncio.sleep(0.5)

        raise AssertionError("event was not inserted into DB within timeout")
