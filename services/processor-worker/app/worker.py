from __future__ import annotations

import asyncio
import json
import signal
from contextlib import suppress

import asyncpg
from aiokafka import AIOKafkaConsumer

from app.health import ReadinessState, start_health_server
from app.observability import configure_logging, get_logger, setup_tracing, start_metrics_server
from app.settings import Settings

settings = Settings()
_shutdown_event = asyncio.Event()


def _install_signal_handlers() -> None:
    """Handle SIGTERM/SIGINT for clean shutdown (Docker/Kubernetes correctness)."""

    def _handler() -> None:
        _shutdown_event.set()

    loop = asyncio.get_running_loop()

    # Not supported on Windows default event loop
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGTERM, _handler)
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _handler)


async def _process_event(pool: asyncpg.Pool, raw: bytes) -> None:
    event = json.loads(raw.decode("utf-8"))

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (event_id, type, source, payload)
            VALUES ($1, $2, $3, $4)
            """,
            event["event_id"],
            event["type"],
            event["source"],
            json.dumps(event.get("payload", {})),
        )


async def run_worker() -> None:
    _install_signal_handlers()

    configure_logging(settings.log_level)
    log = get_logger("processor-worker")
    setup_tracing(settings)

    readiness = ReadinessState()

    # Start metrics as early as possible so boot failures are observable
    start_metrics_server(settings, log)

    # Start health server concurrently; it will stop when _shutdown_event is set
    health_task = asyncio.create_task(
        start_health_server(
            host=settings.health_host,
            port=settings.health_port,
            state=readiness,
            stop_event=_shutdown_event,
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

        while not _shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                await _process_event(pool, msg.value)
            except Exception as exc:
                # Don't crash the worker on a single bad message.
                # (DLQ/retry can be added later without changing this boundary.)
                log.exception("event_processing_failed", err_type=type(exc).__name__)

    finally:
        # Flip readiness early so load balancers stop sending traffic immediately.
        readiness.shutting_down = True
        readiness.kafka_ready = False
        readiness.db_ready = False
        _shutdown_event.set()

        if consumer is not None:
            with suppress(Exception):
                await consumer.stop()

        if pool is not None:
            with suppress(Exception):
                await pool.close()

        with suppress(Exception):
            await health_task

        log.info("worker_stopped_cleanly")


if __name__ == "__main__":
    asyncio.run(run_worker())
