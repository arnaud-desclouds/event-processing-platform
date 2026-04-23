from __future__ import annotations

import asyncio

from .bootstrap import WorkerApp
from .settings import Settings


def main() -> None:
    settings = Settings()
    app = WorkerApp.create(settings)
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
