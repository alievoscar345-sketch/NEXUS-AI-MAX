"""Standalone durable NEXUS task worker.

Run as a dedicated long-lived process beside the API service:
    python -m app.worker
"""
from __future__ import annotations

import asyncio
import logging

from .main import init_db, recover_interrupted_tasks, run_db, worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("nexus.worker")


async def main() -> None:
    await run_db(init_db)
    await run_db(recover_interrupted_tasks)
    logger.info("NEXUS durable worker started")
    try:
        await worker()
    finally:
        logger.info("NEXUS durable worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
