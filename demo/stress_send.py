import requests
import time
import random

URL = "http://localhost:8080/publish"

def generate_events(n=5000, dup_fraction=0.2):
    base = []
    unique_n = int(n * (1 - dup_fraction))
    # buat event unik
    for i in range(unique_n):
        base.append({
            "topic": "stress",
            "event_id": f"stress-{i}",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": "stress-script",
            "payload": {"i": i}
        })
    # tambahkan duplikat acak untuk total n
    events = list(base)
    while len(events) < n:
        e = random.choice(base).copy()
        events.append(e)
    random.shuffle(events)
    return events

def run(n=5000):
    evs = generate_events(n=n, dup_fraction=0.2)
    start = time.time()
    batch_size = 200
    total_batches = (len(evs) + batch_size - 1) // batch_size

    print(f"🚀 Sending {n} events in {total_batches} batches...\n")

    for i in range(0, len(evs), batch_size):
        batch = evs[i:i+batch_size]
        r = requests.post(URL, json=batch)
        batch_num = i // batch_size + 1

        if r.status_code != 200:
            print(f"❌ Error sending batch {batch_num}: {r.status_code} {r.text}")
            break

        # print contoh event di batch ini
        sample = [e["event_id"] for e in batch[:5]]
        print(f"✅ Batch {batch_num}/{total_batches} sent ({len(batch)} events)")
        print(f"   Sample event_ids: {sample}")

    elapsed = time.time() - start
    print(f"\n🏁 Sent {n} events in {elapsed:.2f}s")

    # ambil statistik dari server
    try:
        stats = requests.get("http://localhost:8080/stats").json()
        print("\n📊 Aggregator Stats:")
        print(stats)
    except Exception as e:
        print("⚠️ Could not fetch stats:", e)

if __name__ == "__main__":
    run(5000)
