from __future__ import annotations

from fastapi import FastAPI

from .lifespan import lifespan
from .observability import metrics_and_tracing_middleware
from .routes import router


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.state.publisher = None
    app.state.startup_complete = False
    app.middleware("http")(metrics_and_tracing_middleware)
    app.include_router(router)
    return app


app = create_app()
