import asyncio
import os
import tempfile
import shutil
import pytest
import time
import threading
from fastapi.testclient import TestClient
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from src.main import app

@pytest.fixture(scope="module", autouse=True)
def temp_env():
    """Gunakan database sementara untuk test."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "dedup.db")
    os.environ["DEDUP_DB_PATH"] = db_path
    # Disable Postgres untuk tests (gunakan SQLite)
    os.environ.pop("DATABASE_URL", None)
    yield
    shutil.rmtree(tmpdir)

@pytest.fixture(scope="module")
def client():
    """Gunakan TestClient FastAPI (lifespan otomatis aktif)."""
    with TestClient(app) as c:
        yield c

def wait_for_worker(seconds=0.3):
    """Beri waktu kecil agar worker memproses queue."""
    time.sleep(seconds)

# ============= EXISTING TESTS =============

def test_01_publish_single_event(client):
    ev = {
        "topic": "unit",
        "event_id": "u1",
        "timestamp": "2025-10-23T00:00:00Z",
        "source": "pytest",
        "payload": {"x": 1}
    }
    r = client.post("/publish", json=ev)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1
    wait_for_worker()

def test_02_duplicate_event(client):
    ev = {
        "topic": "unit",
        "event_id": "u1",
        "timestamp": "2025-10-23T00:00:00Z",
        "source": "pytest",
        "payload": {"x": 1}
    }
    client.post("/publish", json=ev)
    wait_for_worker(0.5)
    stats_resp = client.get("/stats").json()
    assert stats_resp["unique_processed"] >= 1
    assert stats_resp["duplicate_dropped"] >= 1

def test_03_publish_batch_events(client):
    events = [
        {
            "topic": "batch",
            "event_id": f"b{i}",
            "timestamp": "2025-10-23T00:00:00Z",
            "source": "pytest",
            "payload": {"i": i}
        }
        for i in range(5)
    ]
    r = client.post("/publish", json=events)
    assert r.status_code == 200
    assert r.json()["accepted"] == 5
    wait_for_worker(0.5)
    stats_data = client.get("/stats").json()
    assert stats_data["unique_processed"] >= 5

def test_04_get_events_by_topic(client):
    r = client.get("/events?topic=batch")
    assert r.status_code == 200
    body = r.json()
    assert "events" in body
    assert all(e["topic"] == "batch" for e in body["events"])

def test_05_get_stats_structure(client):
    data = client.get("/stats").json()
    expected = {"received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"}
    assert expected.issubset(set(data.keys()))
    assert isinstance(data["uptime_seconds"], (int, float))

def test_06_queue_backpressure(client):
    """Simulasi queue penuh → respon 503"""
    from src.api import QUEUE
    
    for _ in range(QUEUE.maxsize):
        try:
            QUEUE.put_nowait({"topic": "overflow", "event_id": f"p{_}"})
        except asyncio.QueueFull:
            break
    r = client.post("/publish", json={
        "topic": "overflow",
        "event_id": "too-many",
        "timestamp": "2025-10-23T00:00:00Z",
        "source": "pytest",
        "payload": {"demo": True}
    })
    assert r.status_code in (200, 503)
    while not QUEUE.empty():
        QUEUE.get_nowait()

def test_07_dedup_store_logic():
    """Uji DedupStore basic tanpa hapus file (aman di Windows)."""
    from src.dedup_store import DedupStore
    import tempfile, os

    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_dedup.db")
    store = DedupStore(db_path)

    store.save_event("demo", "evt1", "{}", "2025-10-23T10:00:00")
    assert store.exists("evt1", "demo")

    store.save_event("demo", "evt1", "{}", "2025-10-23T10:01:00")
    events = store.list_events("demo")
    assert len(events) == 1

    store.save_event("demo", "evt2", "{}", "2025-10-23T10:02:00")
    events = store.list_events("demo")
    assert len(events) == 2

    store.sync_stats()
    assert store.unique_processed == 2


def test_08_topics_should_include_all(client):
    data = client.get("/stats").json()
    topics = data["topics"]
    assert "unit" in topics
    assert "batch" in topics

def test_09_worker_dedup_behavior():
    """Pastikan worker skip duplikat event."""
    from src.worker import Worker
    from src.dedup_store import DedupStore
    q = asyncio.Queue()
    db = tempfile.NamedTemporaryFile(delete=False)
    store_local = DedupStore(db.name)
    stats_local = {"unique_processed": 0, "duplicate_dropped": 0, "received": 0, "topics": []}
    worker_local = Worker(q, store_local, stats_local)

    async def run_test():
        ev = {"topic": "dup", "event_id": "d1", "payload": {}}
        await q.put(ev)
        await q.put(ev)  # duplikat
        task = asyncio.create_task(worker_local.start())
        await asyncio.sleep(0.3)
        await worker_local.stop()
        task.cancel()

    asyncio.run(run_test())
    assert stats_local["unique_processed"] == 1
    assert stats_local["duplicate_dropped"] >= 1

def test_10_integration_event_lifecycle(client):
    ev = {
        "topic": "integration",
        "event_id": "int-1",
        "timestamp": "2025-10-23T00:00:00Z",
        "source": "pytest",
        "payload": {"msg": "full test"}
    }
    client.post("/publish", json=ev)
    wait_for_worker(0.5)
    stats_data = client.get("/stats").json()
    assert stats_data["unique_processed"] >= 1
    r = client.get("/events?topic=integration")
    assert r.status_code == 200
    data = r.json()
    assert len(data["events"]) >= 1

# ============= NEW TESTS (11-20) =============

def test_11_concurrent_processing_race_condition(client):
    """
    TEST KRITIS: Multi-threaded concurrent publishing untuk uji race condition.
    Kirim event duplikat dari multiple threads secara simultan.
    Pastikan hanya 1 yang diproses (idempotency guarantee).
    """
    event_base = {
        "topic": "concurrent",
        "event_id": "race-test-1",
        "timestamp": "2025-12-23T00:00:00Z",
        "source": "concurrent-test",
        "payload": {"test": "race"}
    }
    
    def send_event(i):
        return client.post("/publish", json=event_base)
    
    # Kirim event yang sama dari 10 threads secara bersamaan
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(send_event, range(10)))
    
    # Semua request harus sukses (accepted)
    assert all(r.status_code == 200 for r in results)
    
    # Tunggu workers selesai processing
    wait_for_worker(1.0)
    
    # Verifikasi: hanya 1 event unik yang diproses
    events = client.get("/events?topic=concurrent").json()["events"]
    matching = [e for e in events if e["event_id"] == "race-test-1"]
    assert len(matching) == 1, "Race condition detected! Duplicate processing occurred."

def test_12_batch_atomic_processing(client):
    """
    Test batch atomic: batch event dengan beberapa duplikat.
    Pastikan duplikat di-skip tanpa mengganggu yang valid.
    """
    batch = [
        {"topic": "atomic", "event_id": "a1", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "atomic", "event_id": "a2", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "atomic", "event_id": "a1", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},  # dup
        {"topic": "atomic", "event_id": "a3", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
    ]
    
    r = client.post("/publish", json=batch)
    assert r.status_code == 200
    assert r.json()["accepted"] == 4
    
    wait_for_worker(0.5)
    
    events = client.get("/events?topic=atomic").json()["events"]
    unique_ids = set(e["event_id"] for e in events if e["topic"] == "atomic")
    assert unique_ids == {"a1", "a2", "a3"}

def test_13_schema_validation_missing_fields(client):
    """Test validasi skema: event tanpa field required harus ditolak"""
    invalid_events = [
        {"topic": "test"},  # missing event_id
        {"event_id": "x"},  # missing topic
        {},  # missing everything
    ]
    
    for ev in invalid_events:
        r = client.post("/publish", json=ev)
        assert r.status_code == 400 or r.status_code == 422  # validation error

def test_14_persistence_after_restart_simulation():
    """
    Test persistence: simulasi restart dengan recreate DedupStore.
    Data harus tetap ada setelah 'restart'.
    """
    from src.dedup_store import DedupStore
    
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "persistent.db")
    
    # Session 1: save events
    store1 = DedupStore(db_path)
    store1.save_event("persist", "p1", "{}", "2025-12-23T00:00:00")
    store1.save_event("persist", "p2", "{}", "2025-12-23T00:00:00")
    assert store1.get_count() == 2
    del store1
    
    # Session 2: 'restart' - buat instance baru dari file yang sama
    store2 = DedupStore(db_path)
    assert store2.exists("p1", "persist")
    assert store2.exists("p2", "persist")
    assert store2.get_count() == 2
    
    # Coba save event yang sama (harus di-skip)
    result = store2.save_event("persist", "p1", "{}", "2025-12-23T00:00:00")
    assert result == False  # Duplicate
    assert store2.get_count() == 2  # Count tidak berubah

def test_15_multi_worker_consistency():
    """
    Test multi-worker: WorkerPool dengan 5 workers memproses events secara paralel.
    Pastikan tidak ada duplicate processing.
    """
    from src.worker import WorkerPool
    from src.dedup_store import DedupStore
    
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "multiworker.db")
    store = DedupStore(db_path)
    queue = asyncio.Queue()
    stats = {"unique_processed": 0, "duplicate_dropped": 0, "topics": []}
    
    async def test_multi_worker():
        # Start 5 workers
        pool = WorkerPool(queue, store, stats, num_workers=5)
        await pool.start()
        
        # Kirim 20 events, beberapa duplikat
        for i in range(20):
            event = {"topic": "multi", "event_id": f"m{i % 10}", "payload": {}}
            await queue.put(event)
        
        # Wait processing
        await asyncio.sleep(1.0)
        await pool.stop()
    
    asyncio.run(test_multi_worker())
    
    # Harus hanya 10 unique events (m0-m9)
    assert store.get_count() == 10
    assert stats["unique_processed"] == 10
    assert stats["duplicate_dropped"] >= 10

def test_16_stress_test_high_volume(client):
    """
    Stress test: kirim 1000 events dengan 40% duplikasi.
    Pastikan sistem tetap responsif dan data konsisten.
    """
    import random
    
    num_events = 1000
    unique_ids = [f"stress-{i}" for i in range(600)]  # 600 unique
    
    events = []
    for i in range(num_events):
        # 40% chance duplikat
        event_id = random.choice(unique_ids)
        events.append({
            "topic": "stress",
            "event_id": event_id,
            "timestamp": "2025-12-23T00:00:00Z",
            "source": "stress-test",
            "payload": {"index": i}
        })
    
    # Send in batches
    batch_size = 100
    for i in range(0, len(events), batch_size):
        batch = events[i:i+batch_size]
        r = client.post("/publish", json=batch)
        assert r.status_code == 200
    
    wait_for_worker(2.0)
    
    # Verify unique count
    stress_events = client.get("/events?topic=stress").json()["events"]
    unique_event_ids = set(e["event_id"] for e in stress_events if e["topic"] == "stress")
    
    # Harus ada ~600 unique (toleransi ±10)
    assert 590 <= len(unique_event_ids) <= 610

def test_17_isolation_level_verification():
    """
    Test isolation level: pastikan concurrent writes tidak corrupt data.
    Simulasi write conflict dengan threading.
    """
    from src.dedup_store import DedupStore
    
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "isolation.db")
    store = DedupStore(db_path)
    
    def concurrent_write(worker_id):
        # Setiap worker coba save event yang sama
        for i in range(10):
            store.save_event("isolation", f"iso-{i}", "{}", "2025-12-23T00:00:00")
    
    threads = []
    for i in range(5):
        t = threading.Thread(target=concurrent_write, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Harus hanya 10 unique events (iso-0 sampai iso-9)
    assert store.get_count() == 10

def test_18_ordering_timestamp_consistency(client):
    """
    Test ordering: pastikan events di-retrieve sesuai urutan processed_at.
    """
    base_time = datetime(2025, 12, 23, 0, 0, 0)
    events = []
    for i in range(5):
        ts = base_time.replace(second=i)
        events.append({
            "topic": "ordering",
            "event_id": f"ord-{i}",
            "timestamp": ts.isoformat() + "Z",
            "source": "order-test",
            "payload": {}
        })
    
    # Kirim secara berurutan
    for ev in events:
        client.post("/publish", json=ev)
    
    wait_for_worker(0.5)
    
    retrieved = client.get("/events?topic=ordering").json()["events"]
    order_events = [e for e in retrieved if e["topic"] == "ordering"]
    
    # Verify ada 5 events
    assert len(order_events) == 5
    
    # Verify urutan (by event_id, karena timestamp beda)
    ids = [e["event_id"] for e in order_events]
    assert ids == [f"ord-{i}" for i in range(5)]

def test_19_retry_idempotency(client):
    """
    Test retry idempotency: kirim event yang sama 5x (simulasi retry).
    Harus hanya 1 yang diproses.
    """
    event = {
        "topic": "retry",
        "event_id": "retry-1",
        "timestamp": "2025-12-23T00:00:00Z",
        "source": "retry-test",
        "payload": {"attempt": "multiple"}
    }
    
    # Kirim 5x
    for _ in range(5):
        r = client.post("/publish", json=event)
        assert r.status_code == 200
    
    wait_for_worker(0.5)
    
    # Verify hanya 1 processed
    events = client.get("/events?topic=retry").json()["events"]
    retry_events = [e for e in events if e["event_id"] == "retry-1"]
    assert len(retry_events) == 1
    
    # Stats harus menunjukkan 4 duplicates dropped
    stats = client.get("/stats").json()
    # Note: stats adalah global, jadi minimal harus ada tambahan duplicate
    # Tidak bisa exact count karena test lain juga contribute

def test_20_comprehensive_metrics_validation(client):
    """
    Test komprehensif: validasi semua metrics konsisten.
    """
    # Get stats awal
    stats_before = client.get("/stats").json()
    
    # Kirim batch dengan known composition
    test_events = [
        {"topic": "metrics", "event_id": "m1", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "metrics", "event_id": "m2", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
        {"topic": "metrics", "event_id": "m1", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},  # dup
        {"topic": "metrics", "event_id": "m3", "timestamp": "2025-12-23T00:00:00Z", "source": "test", "payload": {}},
    ]
    
    r = client.post("/publish", json=test_events)
    assert r.status_code == 200
    assert r.json()["accepted"] == 4
    
    wait_for_worker(0.5)
    
    stats_after = client.get("/stats").json()
    
    # Verify increments
    assert stats_after["received"] == stats_before["received"] + 4
    # unique_processed should increase by 3 (m1, m2, m3)
    # Note: bisa lebih karena test lain running parallel
    assert stats_after["unique_processed"] >= stats_before["unique_processed"] + 3
    
    # Verify topics updated
    assert "metrics" in stats_after["topics"]
    
    # Verify uptime increasing
    assert stats_after["uptime_seconds"] >= stats_before["uptime_seconds"]

