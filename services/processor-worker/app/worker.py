from __future__ import annotations

import asyncio
import json
import signal
from contextlib import suppress
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer

from app.health import ReadinessState, start_health_server
from app.observability import configure_logging, get_logger, setup_tracing, start_metrics_server
from app.settings import Settings

SQL_INSERT_EVENT = "INSERT INTO events (event_id, type, source, payload) VALUES ($1, $2, $3, $4)"


def _install_signal_handlers(*, shutdown_event: asyncio.Event) -> None:
    """Handle SIGTERM/SIGINT for clean shutdown (Docker/Kubernetes correctness)."""

    def _handler() -> None:
        shutdown_event.set()

    loop = asyncio.get_running_loop()

    # Not supported on Windows default event loop
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGTERM, _handler)
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _handler)


def _parse_event(raw: bytes, log) -> dict[str, Any] | None:
    try:
        decoded = raw.decode("utf-8")
        event = json.loads(decoded)
        if not isinstance(event, dict):
            log.warning("event_invalid_json", reason="not_an_object")
            return None
        return event
    except Exception as exc:
        log.warning("event_invalid_json", err_type=type(exc).__name__)
        return None


async def _insert_event(pool: asyncpg.Pool, event: dict[str, Any]) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            SQL_INSERT_EVENT,
            event["event_id"],
            event["type"],
            event["source"],
            json.dumps(event.get("payload", {})),
        )


async def run_worker(*, settings: Settings, shutdown_event: asyncio.Event) -> None:
    _install_signal_handlers(shutdown_event=shutdown_event)

    configure_logging(settings.log_level)
    log = get_logger("processor-worker")
    setup_tracing(settings)

    readiness = ReadinessState()

    # Start metrics as early as possible so boot failures are observable
    start_metrics_server(settings, log)

    # Start health server concurrently; it will stop when shutdown_event is set
    health_task = asyncio.create_task(
        start_health_server(
            host=settings.health_host,
            port=settings.health_port,
            state=readiness,
            stop_event=shutdown_event,
        )
    )

    pool: asyncpg.Pool | None = None
    consumer: AIOKafkaConsumer | None = None

    try:
        # --- Postgres ---
        pg_dsn = (
            f"postgresql://{settings.postgres_user}:"
            f"{settings.postgres_password}@"
            f"{settings.postgres_host}:"
            f"{settings.postgres_port}/"
            f"{settings.postgres_db}"
        )

        pool = await asyncpg.create_pool(pg_dsn)
        readiness.db_ready = True
        log.info(
            "postgres_pool_ready",
            host=settings.postgres_host,
            port=settings.postgres_port,
            db=settings.postgres_db,
        )

        # --- Kafka ---
        consumer = AIOKafkaConsumer(
            settings.topic,
            bootstrap_servers=settings.brokers,
            group_id=settings.consumer_group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        readiness.kafka_ready = True
        log.info(
            "kafka_consumer_started",
            brokers=settings.brokers,
            topic=settings.topic,
            group_id=settings.consumer_group_id,
        )

        log.info("worker_started")

        while not shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            event = _parse_event(msg.value, log)
            if event is None:
                continue

            try:
                await _insert_event(pool, event)
            except Exception as exc:
                log.exception(
                    "event_processing_failed",
                    event_id=event.get("event_id"),
                    err_type=type(exc).__name__,
                )

    finally:
        readiness.shutting_down = True
        readiness.kafka_ready = False
        readiness.db_ready = False
        shutdown_event.set()

        if consumer is not None:
            with suppress(Exception):
                await consumer.stop()

        if pool is not None:
            with suppress(Exception):
                await pool.close()

        with suppress(Exception):
            await health_task

        log.info("worker_stopped_cleanly")


def main() -> None:
    settings = Settings()
    shutdown_event = asyncio.Event()
    asyncio.run(run_worker(settings=settings, shutdown_event=shutdown_event))


if __name__ == "__main__":
    main()
