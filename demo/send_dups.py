import requests
import time
from datetime import datetime, timezone

URL = "http://localhost:8080/publish"

BASE_EVENT = {
    "topic": "demo",
    "event_id": "dup-demo-1",
    "source": "demo-script",
    "payload": {"data": {"msg": "hello idempotent"}}
}

def send(n=5, delay=0.5):
    """
    Kirim n event identik untuk uji deduplication.
    Hanya event pertama diproses, sisanya drop.
    """
    for i in range(n):
        event = BASE_EVENT.copy()
        event["timestamp"] = datetime.now(timezone.utc).isoformat()  # timezone-aware
        r = requests.post(URL, json=[event])  # selalu kirim sebagai list
        print(f"[{i+1}/{n}] status={r.status_code}")
        time.sleep(delay)

    # beri waktu worker memproses
    time.sleep(1)

if __name__ == "__main__":
    send()
