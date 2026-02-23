from __future__ import annotations

import asyncio
from dataclasses import dataclass

from aiohttp import web


@dataclass
class ReadinessState:
    kafka_ready: bool = False
    db_ready: bool = False
    shutting_down: bool = False

    def ready(self) -> bool:
        return self.kafka_ready and self.db_ready and not self.shutting_down


async def start_health_server(
        *,
        host: str,
        port: int,
        state: ReadinessState,
        stop_event: asyncio.Event,
) -> None:
    app = web.Application()

    async def healthz(_: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def readyz(_: web.Request) -> web.Response:
        if state.ready():
            return web.json_response({"ready": True})
        return web.json_response(
            {
                "ready": False,
                "kafka_ready": state.kafka_ready,
                "db_ready": state.db_ready,
                "shutting_down": state.shutting_down,
            },
            status=503,
        )

    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", readyz)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()

    await stop_event.wait()
    state.shutting_down = True
    await runner.cleanup()
