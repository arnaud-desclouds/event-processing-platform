from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import asyncpg


async def insert_event(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy, event: Mapping[str, Any]
) -> bool:
    """Return True when the insert adds a row, False on duplicate event_id."""

    inserted = await conn.fetchval(
        """
        INSERT INTO events(event_id, event_timestamp, type, source, payload)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING 1
        """,
        event["event_id"],
        event["timestamp"],
        event["type"],
        event["source"],
        json.dumps(event["payload"]),
    )
    return inserted == 1
