# Pub-Sub Log Aggregator (UAS Sistem Terdistribusi)
**Nama:** Desti Nur Irawati  
**NIM:** 11221033

---

## 📖 Deskripsi

Proyek ini adalah implementasi **Pub/Sub Log Aggregator** dengan **Idempotent Consumer** dan mekanisme **Deduplication** berbasis PostgreSQL. Sistem meniru cara kerja event streaming platform modern seperti Kafka atau Google Pub/Sub, dengan fokus pada:

- ✅ **Idempotent Processing:** Setiap event hanya diproses sekali meski dikirim berulang
- ✅ **Transaksi Atomik:** PostgreSQL dengan unique constraint untuk prevent race conditions
- ✅ **Concurrent Workers:** Multi-worker processing (default 3 workers) untuk high throughput
- ✅ **Persistent Storage:** Data tahan restart container via Docker volumes
- ✅ **Crash Tolerance:** Fault-tolerant design dengan at-least-once delivery
- ✅ **Observability:** Real-time stats, logging, dan metrics

---

## 🏗️ Arsitektur Sistem

```mermaid
graph TD
    P[Publisher] -->|HTTP POST /publish| A[Aggregator API]
    A -->|enqueue| Q[AsyncIO Queue]
    Q -->|consume| W1[Worker 1]
    Q -->|consume| W2[Worker 2]
    Q -->|consume| W3[Worker 3]
    W1 -->|INSERT ON CONFLICT| DB[(PostgreSQL)]
    W2 -->|INSERT ON CONFLICT| DB
    W3 -->|INSERT ON CONFLICT| DB
    DB -->|persist| V[Docker Volume]
    A -->|metrics| STATS[GET /stats]
    DB -->|query| EVENTS[GET /events]
```

### Komponen Utama:

| Service | Fungsi | Technology |
|---------|--------|------------|
| **Aggregator** | API & event processor | FastAPI, Python 3.11 |
| **Storage** | Persistent database | PostgreSQL 16 Alpine |
| **Broker** | Message broker (optional) | Redis 7 Alpine |
| **Publisher** | Event simulator/generator | Python script |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Port 8080 available

### 1. Clone & Navigate
```bash
cd "h:\SISTER\PubSubLogAggregator - Copy"
```

### 2. Build & Run dengan Docker Compose
```bash
docker compose up --build
```

Ini akan:
- Build aggregator image
- Pull Postgres 16 & Redis 7 images
- Create named volumes untuk persistensi
- Start semua services dalam internal network
- Expose port 8080 untuk API

### 3. Verify Services Running
```bash
docker compose ps
```

Expected output:
```
NAME                IMAGE                      STATUS
uts-aggregator      uts-aggregator:latest      Up (healthy)
uts-postgres        postgres:16-alpine         Up (healthy)
uts-redis           redis:7-alpine             Up (healthy)
uts-publisher       uts-publisher:latest       Up
```

### 4. Akses API
- **Swagger UI:** http://localhost:8080/docs
- **Health Check:** http://localhost:8080/stats

---

## 📡 API Endpoints

### POST /publish
Kirim single event atau batch events.

**Single Event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "event_id": "order-123",
    "timestamp": "2025-12-23T10:00:00Z",
    "source": "payment-service",
    "payload": {"amount": 100, "status": "paid"}
  }'
```

**Batch Events:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic": "orders", "event_id": "order-1", "timestamp": "2025-12-23T10:00:00Z", "source": "api", "payload": {}},
    {"topic": "orders", "event_id": "order-2", "timestamp": "2025-12-23T10:00:01Z", "source": "api", "payload": {}}
  ]'
```

### GET /events
List events yang sudah diproses (unik).

**All events:**
```bash
curl http://localhost:8080/events
```

**Filter by topic:**
```bash
curl http://localhost:8080/events?topic=orders
```

### GET /stats
Statistik agregat real-time.

```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "received": 1500,
  "unique_processed": 1000,
  "duplicate_dropped": 500,
  "topics": ["orders", "payments", "inventory"],
  "uptime_seconds": 3600
}
```

---

## 🧪 Testing

### Unit Tests (20 tests)
```bash
pytest tests/test_full.py -v
```

Test coverage:
- ✅ Idempotency & deduplication
- ✅ Concurrent processing race conditions
- ✅ Persistence after restart
- ✅ Batch atomic processing
- ✅ Schema validation
- ✅ Stress test (1000 events, 40% duplication)
- ✅ Multi-worker consistency
- ✅ Isolation level verification

### Stress Test
```bash
python demo/stress_send.py
```

Kirim 5000+ events dengan 30% duplikasi, verifikasi throughput & akurasi.

### Deduplication Demo
```bash
python demo/send_dups.py
```

Kirim event duplikat dan tunjukkan idempotency.

---

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL connection (required di Compose) |
| `REDIS_URL` | - | Redis connection (optional) |
| `NUM_WORKERS` | 3 | Jumlah concurrent workers |
| `QUEUE_MAXSIZE` | 50000 | Max events dalam queue |
| `DEDUP_DB` | data/dedup.db | SQLite fallback jika no Postgres |

### Docker Compose Override
Untuk development, buat `docker-compose.override.yml`:

```yaml
services:
  aggregator:
    environment:
      - NUM_WORKERS=10
    volumes:
      - ./src:/app/src  # Hot reload
```

---

## 💾 Persistensi Data

### Named Volumes
Data disimpan di Docker volumes yang persist meski container dihapus:

```bash
# List volumes
docker volume ls | grep uts

# Inspect volume
docker volume inspect uts_postgres_data

# Backup volume
docker run --rm -v uts_postgres_data:/data -v $(pwd):/backup alpine tar czf /backup/postgres_backup.tar.gz /data
```

### Verify Persistence
```bash
# Kirim events
curl -X POST http://localhost:8080/publish -d '{"topic":"test","event_id":"persist-1",...}'

# Stop & remove containers
docker compose down

# Start ulang
docker compose up -d

# Verify data masih ada
curl http://localhost:8080/events?topic=test
# Harus muncul event "persist-1"
```

---

## 🔍 Observability

### Logs
```bash
# All services
docker compose logs -f

# Aggregator only
docker compose logs -f aggregator

# Last 100 lines
docker compose logs --tail=100 aggregator
```

### Metrics Monitoring
Akses http://localhost:8080/stats secara periodik atau integrate dengan monitoring tools.

### Database Inspection
```bash
# Masuk Postgres container
docker exec -it uts-postgres psql -U aggregator_user -d aggregator_db

# Query events
SELECT topic, event_id, processed_at FROM events LIMIT 10;

# Count by topic
SELECT topic, COUNT(*) FROM events GROUP BY topic;
```

---

## 🏗️ Development

### Local Development (tanpa Docker)
```bash
# Install dependencies
pip install -r requirements.txt

# Setup Postgres lokal atau gunakan SQLite
export DATABASE_URL="postgresql://user:pass@localhost:5432/aggregator_db"
# ATAU
export DEDUP_DB="data/dedup.db"

# Run server
python -m src.main

# Atau dengan uvicorn hot reload
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
```

### Add New Tests
Edit [tests/test_full.py](tests/test_full.py) dan tambahkan test function:

```python
def test_21_your_new_test(client):
    # Test logic here
    pass
```

---

## 📊 Performance Benchmarks

Measured dengan stress test (3 workers, PostgreSQL):

| Metric | Value |
|--------|-------|
| **Throughput** | ~500-600 events/sec |
| **Unique Processing** | ~300-400 unique/sec |
| **Latency (median)** | < 50ms |
| **Deduplication Accuracy** | 100% (0 false positives) |
| **Crash Recovery** | < 5 seconds |

---

## 🔐 Security & Network

- ✅ **Internal Network:** Semua services dalam `uts_network` (bridge)
- ✅ **No External Dependencies:** Tidak ada calls ke internet
- ✅ **Non-root User:** Container runs as `appuser`
- ✅ **Minimal Base Image:** `python:3.11-slim`, `postgres:16-alpine`
- ✅ **Port Exposure:** Hanya 8080 untuk demo lokal

---

## 📚 Dokumentasi Tambahan

- **Laporan Lengkap:** [report.md](report.md)
- **Video Demo:** [VIDEO_DEMO_CHECKLIST.md](VIDEO_DEMO_CHECKLIST.md)
- **Link YouTube:** [Masukkan link video Anda di sini]

---

## 🐛 Troubleshooting

### Port 8080 already in use
```bash
# Change port di docker-compose.yml
ports:
  - "8081:8080"
```

### Postgres connection refused
```bash
# Check Postgres health
docker compose ps storage

# Check logs
docker compose logs storage

# Restart Postgres
docker compose restart storage
```

### Tests failing
```bash
# Ensure no DATABASE_URL in test environment
unset DATABASE_URL

# Run tests dengan verbose
pytest tests/test_full.py -vv -s
```

---

## 📝 Citation

van Steen, M., & Tanenbaum, A. S. (2023). *Distributed systems* (4th ed.). distributed-systems.net. https://www.distributed-systems.net/index.php/books/ds4/

---

## 📄 License

Proyek UAS - Sistem Terdistribusi  
© 2025 Desti Nur Irawati

