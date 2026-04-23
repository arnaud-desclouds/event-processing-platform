from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .observability import setup_tracing
from .publisher import EventPublisher
from .settings import Settings

logger = logging.getLogger(__name__)


async def _start_publisher_with_retry(settings: Settings) -> EventPublisher:
    for attempt in range(1, settings.publisher_start_max_attempts + 1):
        try:
            publisher = await EventPublisher.start(
                brokers=settings.brokers,
                topic=settings.topic,
                readiness_timeout_seconds=settings.publisher_ready_timeout_seconds,
            )
        except Exception:
            if attempt >= settings.publisher_start_max_attempts:
                logger.exception(
                    "Kafka producer startup failed after %s attempts",
                    settings.publisher_start_max_attempts,
                )
                raise

            logger.warning(
                "Kafka producer startup failed on attempt %s/%s; retrying in %.1fs",
                attempt,
                settings.publisher_start_max_attempts,
                settings.publisher_start_backoff_seconds,
                exc_info=True,
            )
            await asyncio.sleep(settings.publisher_start_backoff_seconds)
        else:
            if attempt > 1:
                logger.info("Kafka producer startup recovered on attempt %s", attempt)
            return publisher

    raise RuntimeError("unreachable")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    app.state.publisher = None
    app.state.startup_complete = False
    setup_tracing(settings)

    publisher = await _start_publisher_with_retry(settings)

    app.state.settings = settings
    app.state.publisher = publisher
    app.state.startup_complete = True

    try:
        yield
    finally:
        app.state.startup_complete = False
        try:
            await publisher.close()
        finally:
            app.state.publisher = None
