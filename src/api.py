from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import JSONResponse
from typing import List, Union, Optional
import asyncio
import logging
from datetime import datetime

from .models import Event
from .dedup_store import DedupStore
from .worker import Worker
from .utils import uptime_seconds

logger = logging.getLogger("aggregator.api")
router = APIRouter()

# Globals diisi dari main.py
QUEUE: Optional[asyncio.Queue] = None
STORE: Optional[DedupStore] = None
WORKER: Optional[Worker] = None
STATS: Optional[dict] = None
START_TIME: Optional[datetime] = None


@router.post("/publish")
async def publish(payload: Union[Event, List[Event]] = Body(...)):
    """
    Terima event dalam format:
    - Single dict: {"topic": "...", "event_id": "...", "timestamp": "...", "source": "...", "payload": {...}}
    - Atau list of dict: [{"topic": "...", "event_id": "...", ...}, ...]
    Keduanya valid.
    """
    # Normalisasi agar jadi list
    events_data = payload if isinstance(payload, list) else [payload]
    accepted = 0

    for ev in events_data:
        try:
            await QUEUE.put(ev.dict())
            accepted += 1
            if STATS is not None:
                STATS["received"] = STATS.get("received", 0) + 1
        except asyncio.QueueFull:
            logger.warning(f"Queue full; rejecting event {ev.topic}/{ev.event_id}")
            raise HTTPException(status_code=503, detail="Internal queue full")

    return JSONResponse({"accepted": accepted})


@router.get("/stats")
async def get_stats():
    """Kembalikan statistik sistem secara real-time."""
    if STATS is None or STORE is None or START_TIME is None:
        raise HTTPException(status_code=503, detail="System not initialized")

    return {
        "received": STATS.get("received", 0),
        "unique_processed": STATS.get("unique_processed", 0),
        "duplicate_dropped": STATS.get("duplicate_dropped", 0),
        "topics": STORE.list_topics(),
        "uptime_seconds": uptime_seconds(START_TIME)
    }


@router.get("/events")
async def get_events(topic: Optional[str] = None):
    """Ambil semua event yang tersimpan (opsional per topic)."""
    if STORE is None:
        raise HTTPException(status_code=503, detail="Store not initialized")

    events = STORE.list_events(topic)
    return {"events": events}
