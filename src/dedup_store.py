import os
import threading
from datetime import datetime
from typing import Optional, List, Dict
import logging

logger = logging.getLogger("aggregator.dedup")

# Support both SQLite and PostgreSQL
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    import sqlite3


class DedupStore:
    """
    Deduplication store dengan support PostgreSQL dan SQLite.
    PostgreSQL: gunakan environment variable DATABASE_URL
    SQLite: gunakan DEDUP_DB_PATH
    
    Implementasi:
    - Transaksi atomik dengan isolation level READ COMMITTED (default Postgres)
    - Unique constraint (topic, event_id) untuk idempotency
    - INSERT ... ON CONFLICT DO NOTHING untuk upsert atomik
    - Thread-safe dengan locking untuk SQLite, connection pool untuk Postgres
    """
    
    def __init__(self, db_path: str = "events.db", database_url: Optional[str] = None):
        self.database_url = database_url or os.environ.get("DATABASE_URL")
        self.db_path = db_path
        self._lock = threading.Lock()
        self.use_postgres = bool(self.database_url and POSTGRES_AVAILABLE)
        
        if self.use_postgres:
            logger.info(f"Using PostgreSQL: {self.database_url.split('@')[-1]}")
        else:
            logger.info(f"Using SQLite: {self.db_path}")
        
        self._init_db()
        self.unique_processed = 0
        self.total_events = 0

    def _init_db(self):
        """Inisialisasi tabel dengan unique constraint untuk deduplication"""
        if self.use_postgres:
            conn = psycopg2.connect(self.database_url)
            try:
                with conn.cursor() as cur:
                    # Postgres: SERIAL primary key, UNIQUE constraint (topic, event_id)
                    cur.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        event_json TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(topic, event_id)
                    )
                    """)
                    # Index untuk query by topic
                    cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_topic 
                    ON events(topic)
                    """)
                    conn.commit()
                    logger.info("PostgreSQL table 'events' initialized with UNIQUE(topic, event_id)")
            finally:
                conn.close()
        else:
            with self._connect_sqlite() as conn:
                conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_json TEXT,
                    processed_at TEXT,
                    UNIQUE(topic, event_id)
                )
                """)
                conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_topic 
                ON events(topic)
                """)
                conn.commit()
                logger.info("SQLite table 'events' initialized")

    def _connect_sqlite(self):
        """SQLite connection dengan autocommit disabled untuk transaksi"""
        return sqlite3.connect(self.db_path, check_same_thread=False, isolation_level='DEFERRED')

    def _connect_postgres(self):
        """PostgreSQL connection dengan isolation level READ COMMITTED"""
        conn = psycopg2.connect(self.database_url)
        conn.set_session(isolation_level='READ COMMITTED', autocommit=False)
        return conn

    def exists(self, event_id: str, topic: str = None) -> bool:
        """
        Cek apakah event_id sudah ada di database.
        Untuk Postgres, juga check topic untuk unique constraint (topic, event_id).
        """
        if self.use_postgres:
            conn = self._connect_postgres()
            try:
                with conn.cursor() as cur:
                    if topic:
                        cur.execute("SELECT 1 FROM events WHERE topic=%s AND event_id=%s LIMIT 1", 
                                  (topic, event_id))
                    else:
                        cur.execute("SELECT 1 FROM events WHERE event_id=%s LIMIT 1", (event_id,))
                    return cur.fetchone() is not None
            finally:
                conn.close()
        else:
            with self._lock, self._connect_sqlite() as conn:
                if topic:
                    cur = conn.execute("SELECT 1 FROM events WHERE topic=? AND event_id=? LIMIT 1", 
                                     (topic, event_id))
                else:
                    cur = conn.execute("SELECT 1 FROM events WHERE event_id=? LIMIT 1", (event_id,))
                return cur.fetchone() is not None

    def save_event(self, topic: str, event_id: str, event_json: str, processed_at: str = None) -> bool:
        """
        Simpan event ke database dengan transaksi atomik.
        Return True jika event baru disimpan, False jika duplikat.
        
        Implementasi idempotency:
        - PostgreSQL: INSERT ... ON CONFLICT (topic, event_id) DO NOTHING
        - SQLite: INSERT OR IGNORE
        
        Kedua approach memastikan atomicity dan mencegah race condition.
        """
        if processed_at is None:
            processed_at = datetime.utcnow().isoformat()

        if self.use_postgres:
            conn = self._connect_postgres()
            try:
                with conn.cursor() as cur:
                    # INSERT dengan ON CONFLICT untuk idempotency atomik
                    cur.execute("""
                        INSERT INTO events (topic, event_id, event_json, processed_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (topic, event_id) DO NOTHING
                    """, (topic, event_id, event_json, processed_at))
                    
                    inserted = cur.rowcount == 1
                    conn.commit()
                    
                    if inserted:
                        self.unique_processed += 1
                        logger.debug(f"Saved new event: {topic}/{event_id}")
                    else:
                        logger.debug(f"Duplicate detected: {topic}/{event_id}")
                    
                    self.total_events += 1
                    return inserted
            except Exception as e:
                conn.rollback()
                logger.error(f"Error saving event: {e}")
                raise
            finally:
                conn.close()
        else:
            with self._lock, self._connect_sqlite() as conn:
                try:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO events (topic, event_id, event_json, processed_at) VALUES (?, ?, ?, ?)",
                        (topic, event_id, event_json, processed_at)
                    )
                    inserted = cur.rowcount == 1
                    conn.commit()
                    
                    if inserted:
                        self.unique_processed += 1
                    
                    self.total_events += 1
                    return inserted
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error saving event: {e}")
                    raise

    def is_duplicate(self, event_id: str, topic: str = None) -> bool:
        """Alias untuk exists()"""
        return self.exists(event_id, topic)

    def list_events(self, topic: Optional[str] = None, limit: int = 1000) -> List[Dict]:
        """List events dengan optional filter by topic"""
        if self.use_postgres:
            conn = self._connect_postgres()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if topic:
                        cur.execute(
                            "SELECT topic, event_id, event_json, processed_at FROM events WHERE topic=%s ORDER BY id LIMIT %s",
                            (topic, limit)
                        )
                    else:
                        cur.execute("SELECT topic, event_id, event_json, processed_at FROM events ORDER BY id LIMIT %s", (limit,))
                    return [dict(row) for row in cur.fetchall()]
            finally:
                conn.close()
        else:
            with self._connect_sqlite() as conn:
                if topic:
                    cur = conn.execute(
                        "SELECT topic, event_id, event_json, processed_at FROM events WHERE topic=? ORDER BY id LIMIT ?",
                        (topic, limit)
                    )
                else:
                    cur = conn.execute("SELECT topic, event_id, event_json, processed_at FROM events ORDER BY id LIMIT ?", (limit,))
                return [
                    dict(zip(["topic", "event_id", "event_json", "processed_at"], row))
                    for row in cur.fetchall()
                ]

    def list_topics(self) -> List[str]:
        """List semua unique topics"""
        if self.use_postgres:
            conn = self._connect_postgres()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT topic FROM events ORDER BY topic")
                    return [row[0] for row in cur.fetchall()]
            finally:
                conn.close()
        else:
            with self._connect_sqlite() as conn:
                cur = conn.execute("SELECT DISTINCT topic FROM events ORDER BY topic")
                return [row[0] for row in cur.fetchall()]

    def get_count(self) -> int:
        """Get total unique events count"""
        if self.use_postgres:
            conn = self._connect_postgres()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM events")
                    return cur.fetchone()[0]
            finally:
                conn.close()
        else:
            with self._connect_sqlite() as conn:
                cur = conn.execute("SELECT COUNT(*) FROM events")
                return cur.fetchone()[0]

    def sync_stats(self):
        """Sinkronkan statistik unique_processed dari database"""
        self.unique_processed = self.get_count()

    def close(self):
        """Close connections (untuk cleanup di tests)"""
        if not self.use_postgres:
            try:
                sqlite3.connect(self.db_path).close()
            except Exception:
                pass


# Singleton instance untuk backward compatibility
dedup_store = None

