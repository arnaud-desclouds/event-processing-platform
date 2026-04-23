from __future__ import annotations

import asyncio
import signal
from contextlib import suppress
from dataclasses import dataclass
from typing import Self

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from structlog.stdlib import BoundLogger

from .consumer_loop import run_consumer_loop
from .health import ReadinessState, start_health_server
from .observability import configure_logging, get_logger, setup_tracing, start_metrics_server
from .settings import Settings


def _install_signal_handlers(shutdown_event: asyncio.Event, *, log: BoundLogger) -> None:
    def _handler() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGTERM, _handler)
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGINT, _handler)


@dataclass(slots=True)
class WorkerApp:
    settings: Settings
    log: BoundLogger
    shutdown_event: asyncio.Event
    readiness: ReadinessState

    pool: asyncpg.Pool | None = None
    consumer: AIOKafkaConsumer | None = None
    dlq_producer: AIOKafkaProducer | None = None
    health_task: asyncio.Task[None] | None = None

    @classmethod
    def create(cls, settings: Settings) -> Self:
        configure_logging(settings.log_level)
        log = get_logger("processor-worker")
        setup_tracing(settings)
        return cls(
            settings=settings,
            log=log,
            shutdown_event=asyncio.Event(),
            readiness=ReadinessState(),
        )

    def _start_health_server(self) -> None:
        self.health_task = asyncio.create_task(
            start_health_server(
                host=self.settings.health_host,
                port=self.settings.health_port,
                state=self.readiness,
                stop_event=self.shutdown_event,
            )
        )

    async def _start_postgres(self) -> None:
        self.pool = await asyncpg.create_pool(self.settings.postgres_dsn)
        self.readiness.db_ready = True
        self.log.info(
            "postgres_pool_ready",
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            db=self.settings.postgres_db,
        )

    async def _start_kafka(self) -> None:
        consumer = AIOKafkaConsumer(
            self.settings.topic,
            bootstrap_servers=self.settings.brokers,
            group_id=self.settings.consumer_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self.consumer = consumer
        await consumer.start()

        dlq_producer = AIOKafkaProducer(bootstrap_servers=self.settings.brokers)
        self.dlq_producer = dlq_producer
        await dlq_producer.start()

        self.readiness.kafka_ready = True
        self.log.info(
            "kafka_consumer_started",
            brokers=self.settings.brokers,
            topic=self.settings.topic,
            group_id=self.settings.consumer_group_id,
        )

    async def start(self) -> None:
        _install_signal_handlers(self.shutdown_event, log=self.log)
        start_metrics_server(self.settings, self.log)
        self._start_health_server()
        await self._start_postgres()
        await self._start_kafka()

        self.log.info("worker_started")

    def _begin_shutdown(self) -> None:
        self.readiness.shutting_down = True
        self.readiness.kafka_ready = False
        self.readiness.db_ready = False
        self.shutdown_event.set()

    async def _stop_consumer(self) -> None:
        consumer, self.consumer = self.consumer, None
        if consumer is not None:
            with suppress(Exception):
                await consumer.stop()

    async def _stop_dlq_producer(self) -> None:
        dlq_producer, self.dlq_producer = self.dlq_producer, None
        if dlq_producer is not None:
            with suppress(Exception):
                await dlq_producer.stop()

    async def _close_pool(self) -> None:
        pool, self.pool = self.pool, None
        if pool is not None:
            with suppress(Exception):
                await pool.close()

    async def _wait_for_health_task(self) -> None:
        health_task, self.health_task = self.health_task, None
        if health_task is not None and health_task is not asyncio.current_task():
            with suppress(Exception):
                await health_task

    async def stop(self) -> None:
        self._begin_shutdown()
        await self._stop_consumer()
        await self._stop_dlq_producer()
        await self._close_pool()
        await self._wait_for_health_task()

        self.log.info("worker_stopped_cleanly")

    def _started_resources(self) -> tuple[asyncpg.Pool, AIOKafkaConsumer, AIOKafkaProducer]:
        if self.pool is None:
            raise RuntimeError("worker not started: pool is None")
        if self.consumer is None:
            raise RuntimeError("worker not started: consumer is None")
        if self.dlq_producer is None:
            raise RuntimeError("worker not started: dlq_producer is None")
        return self.pool, self.consumer, self.dlq_producer

    async def run(self) -> None:
        try:
            await self.start()
            pool, consumer, dlq_producer = self._started_resources()
            await run_consumer_loop(
                pool=pool,
                consumer=consumer,
                dlq_producer=dlq_producer,
                dlq_topic=self.settings.dlq_topic,
                log=self.log,
                shutdown_event=self.shutdown_event,
            )
        finally:
            await self.stop()
