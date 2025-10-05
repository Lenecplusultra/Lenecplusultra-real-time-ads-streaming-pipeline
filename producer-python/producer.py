import argparse, random, time, secrets
from datetime import datetime, timezone
import orjson
from confluent_kafka import Producer

def rid(prefix): return f"{prefix}-{secrets.token_hex(3)}"

def make_event():
    return {
        "event_time": datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds"),
        "user_id": rid("u"),
        "campaign_id": random.choice(["cmp-001","cmp-002","cmp-003","cmp-004"]),
        "ad_id": rid("ad"),
        "clicked": True
    }

def main(rate, brokers, topic):
    p = Producer({"bootstrap.servers": brokers})
    interval = 1.0 / rate if rate > 0 else 0
    print(f"Producing to {brokers} topic={topic} at ~{rate}/sec (Ctrl+C to stop)")
    try:
        while True:
            p.produce(topic, value=orjson.dumps(make_event()))
            p.poll(0)               # <-- lets librdkafka drain the queue
            if interval: time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=1000)
    ap.add_argument("--brokers", default="localhost:9092")
    ap.add_argument("--topic", default="ad-clicks")
    args = ap.parse_args()
    main(args.rate, args.brokers, args.topic)
