import asyncio
import os
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import uvicorn
import logging

from .dedup_store import DedupStore
from .worker import Worker, WorkerPool
from .utils import init_logging

init_logging()
logger = logging.getLogger("aggregator.main")

DB_PATH = os.environ.get("DEDUP_DB_PATH", os.environ.get("DEDUP_DB", "data/dedup.db"))
DATABASE_URL = os.environ.get("DATABASE_URL")
QUEUE_MAXSIZE = int(os.environ.get("QUEUE_MAXSIZE", "20000"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))

queue = None
store = None
stats = None
worker_pool = None
start_time = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global queue, store, stats, worker_pool, start_time

    logger.info("=" * 60)
    logger.info("Starting PubSub Log Aggregator")
    logger.info(f"Database: {'PostgreSQL' if DATABASE_URL else 'SQLite'}")
    logger.info(f"Queue max size: {QUEUE_MAXSIZE}")
    logger.info(f"Number of workers: {NUM_WORKERS}")
    logger.info("=" * 60)

    # Init queue & store
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    
    # Initialize DedupStore with Postgres or SQLite
    if DATABASE_URL:
        store = DedupStore(database_url=DATABASE_URL)
    else:
        store = DedupStore(DB_PATH)
    
    store.sync_stats()
    
    stats = {
        "received": 0,
        "unique_processed": store.unique_processed,
        "duplicate_dropped": 0,
        "topics": store.list_topics()
    }
    
    start_time = datetime.now(timezone.utc)
    
    # Start worker pool with multiple workers untuk concurrent processing
    worker_pool = WorkerPool(queue, store, stats, num_workers=NUM_WORKERS)
    await worker_pool.start()

    # Inject dependencies ke API module
    import src.api as api_module
    api_module.QUEUE = queue
    api_module.STORE = store
    api_module.WORKER = None  # Tidak dipakai lagi, pakai worker_pool
    api_module.STATS = stats
    api_module.START_TIME = start_time

    logger.info("✓ Aggregator ready to accept events")

    yield

    # Graceful shutdown
    logger.info("Shutting down aggregator...")
    await worker_pool.stop()
    logger.info("✓ Aggregator stopped")

app = FastAPI(title="PubSub Log Aggregator - UTS Sistem Terdistribusi", lifespan=lifespan)

from .api import router as api_router
app.include_router(api_router)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.errors()},
    )

if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)

