from __future__ import annotations

import json
from typing import Any, Dict

import asyncpg
import structlog
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .observability import DB_RECONNECTS_TOTAL
from .settings import Settings


def _on_db_connect_retry(log: structlog.stdlib.BoundLogger):
    def _hook(state: RetryCallState) -> None:
        DB_RECONNECTS_TOTAL.inc()
        exc = state.outcome.exception() if state.outcome else None
        log.warning(
            "db_connect_failed_retrying",
            attempt=state.attempt_number,
            error_type=type(exc).__name__ if exc else None,
        )

    return _hook


async def connect_with_retries(
    settings: Settings, log: structlog.stdlib.BoundLogger
) -> asyncpg.Connection:
    max_attempts = settings.db_connect_max_retries + 1

    retrying = AsyncRetrying(
        retry=retry_if_exception_type((OSError, asyncpg.PostgresError)),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(
            multiplier=settings.db_connect_base_backoff_seconds,
            max=settings.db_connect_max_backoff_seconds,
        ),
        reraise=True,
        before_sleep=_on_db_connect_retry(log),
    )

    async for attempt in retrying:
        with attempt:
            return await asyncpg.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                database=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
            )

    raise RuntimeError("unreachable")


async def insert_event(conn: asyncpg.Connection, event: Dict[str, Any]) -> None:
    await conn.execute(
        "INSERT INTO events(event_id, type, source, payload) VALUES($1,$2,$3,$4)",
        event["event_id"],
        event["type"],
        event["source"],
        json.dumps(event["payload"]),
    )
