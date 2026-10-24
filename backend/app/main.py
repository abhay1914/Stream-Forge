
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.app.api.routes import router
from backend.app.engine.worker import EventWorker
from backend.app.storage.repository import EventRepository


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROCESSING_INTERVAL_SECONDS = float(
    os.getenv("PROCESSING_INTERVAL_SECONDS", "5")
)


async def background_worker():
    repository = EventRepository()
    worker = EventWorker(repository=repository)

    while True:
        try:
            result = await asyncio.to_thread(
                worker.process_pending_events,
                limit=100,
            )

            if result["found"] > 0:
                logger.info("Background worker result: %s", result)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Background worker encountered an error")

        await asyncio.sleep(PROCESSING_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(background_worker())
    logger.info("Background worker started")

    try:
        yield
    finally:
        worker_task.cancel()

        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        logger.info("Background worker stopped")


app = FastAPI(
    title="StreamForge API",
    description="Temperature event processing API",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/")
def root():
    return {
        "message": "StreamForge API is running",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "healthy"}
