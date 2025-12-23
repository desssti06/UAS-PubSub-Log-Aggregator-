import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger("aggregator.worker")


class Worker:
    """
    Worker untuk memproses event dari queue secara asynchronous.
    
    Implementasi:
    - Idempotent processing: setiap event hanya diproses sekali
    - Thread-safe melalui DedupStore yang sudah implement locking/transaksi
    - Support multiple concurrent workers (scale horizontally)
    - Atomic operations untuk prevent race conditions
    """
    
    def __init__(self, queue: asyncio.Queue, store, stats: dict, worker_id: int = 0):
        """
        queue: antrean event dari publisher
        store: instance DedupStore
        stats: dict untuk metrik global
        worker_id: ID worker untuk logging (multi-worker support)
        """
        self.queue = queue
        self.store = store
        self.stats = stats
        self.worker_id = worker_id
        self._running = False
        self._stats_lock = asyncio.Lock()

    async def start(self):
        """Mulai worker loop"""
        if self._running:
            return
        self._running = True
        logger.info(f"Worker {self.worker_id} started")
        
        while self._running:
            try:
                event = await self.queue.get()
                if event is None:  # Poison pill untuk shutdown
                    break
                await self.handle_event(event)
                self.queue.task_done()
            except asyncio.CancelledError:
                logger.info(f"Worker {self.worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"[Worker {self.worker_id}] Error: {e}", exc_info=True)

    async def stop(self):
        """Berhentikan worker dengan aman"""
        self._running = False
        await self.queue.put(None)
        logger.info(f"Worker {self.worker_id} stopped")

    async def handle_event(self, event: dict):
        """
        Proses 1 event dengan idempotency guarantee.
        
        Flow:
        1. Extract event_id dan topic
        2. Attempt save ke DedupStore (atomik dengan unique constraint)
        3. Jika berhasil save -> event baru, update stats
        4. Jika gagal save (conflict) -> duplikat, skip processing
        """
        event_id = event.get("event_id")
        topic = event.get("topic", "unknown")

        if not event_id:
            logger.warning(f"[Worker {self.worker_id}] Event tanpa event_id di-skip")
            return

        # Prepare data
        ts = event.get("timestamp") or datetime.utcnow().isoformat()
        payload_str = json.dumps(event.get("payload", {}))
        
        # Atomic save dengan idempotency
        # DedupStore.save_event() sudah handle transaksi dan unique constraint
        try:
            is_new = self.store.save_event(topic, event_id, payload_str, ts)
            
            # Update stats dengan lock untuk thread-safety
            async with self._stats_lock:
                if is_new:
                    self.stats["unique_processed"] += 1
                    if topic not in self.stats["topics"]:
                        self.stats["topics"].append(topic)
                    logger.debug(f"[Worker {self.worker_id}] Processed: {topic}/{event_id}")
                else:
                    self.stats["duplicate_dropped"] += 1
                    logger.debug(f"[Worker {self.worker_id}] Duplicate: {topic}/{event_id}")
        
        except Exception as e:
            logger.error(f"[Worker {self.worker_id}] Failed to process {topic}/{event_id}: {e}")
            raise

        # Simulate processing time (optional)
        await asyncio.sleep(0.001)


class WorkerPool:
    """
    Pool of multiple workers untuk concurrent processing.
    Digunakan untuk stress test dan production scalability.
    """
    
    def __init__(self, queue: asyncio.Queue, store, stats: dict, num_workers: int = 3):
        self.queue = queue
        self.store = store
        self.stats = stats
        self.num_workers = num_workers
        self.workers = []
        self.tasks = []

    async def start(self):
        """Start semua workers"""
        logger.info(f"Starting worker pool with {self.num_workers} workers")
        
        for i in range(self.num_workers):
            worker = Worker(self.queue, self.store, self.stats, worker_id=i)
            self.workers.append(worker)
            task = asyncio.create_task(worker.start())
            self.tasks.append(task)
        
        logger.info(f"Worker pool started: {self.num_workers} workers active")

    async def stop(self):
        """Stop semua workers"""
        logger.info("Stopping worker pool")
        
        # Send poison pill untuk semua workers
        for _ in range(self.num_workers):
            await self.queue.put(None)
        
        # Cancel semua tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for completion
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        logger.info("Worker pool stopped")

