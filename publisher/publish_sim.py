import requests, time, random, os
from datetime import datetime
import uuid

API_URL = "http://localhost:8080/publish"

def make_event(i, topic="demo"):
    return {
        "topic": topic,
        "event_id": f"{topic}-{i}",
        "timestamp": datetime.utcnow().isoformat() + "Z", 
        "source": "publish_sim",
        "payload": {
            "data": {
                "n": i
            }
        }
    }



def send_batch(batch):
    try:
        r = requests.post(API_URL, json=batch, timeout=10)
        print("sent", len(batch), "status", r.status_code)
    except Exception as e:
        print("error", e)

def main():
    total = 1000
    unique = int(total * 0.8)
    events = [make_event(i, "demo") for i in range(unique)]
    duplicates = [random.choice(events) for _ in range(total - unique)]
    batch = events + duplicates
    random.shuffle(batch)
    for i in range(0, len(batch), 100):
        chunk = batch[i:i+100]
        send_batch(chunk)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
