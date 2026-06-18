"""
produce_events.py
-----------------
Tiny synthetic event generator for local testing of the streaming pipeline.
Publishes JSON events to a Kafka topic, occasionally re-emitting an older
event_id with a newer event_time to exercise the late-data MERGE path.

Usage:
    python src/produce_events.py --bootstrap localhost:9092 --topic events --rate 50

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer  # pip install kafka-python


def make_event(recent_ids: list[str]) -> dict:
    # 20% of the time, re-emit an existing id as a "correction" (late/out-of-order).
    if recent_ids and random.random() < 0.2:
        event_id = random.choice(recent_ids)
    else:
        event_id = str(uuid.uuid4())
        recent_ids.append(event_id)
        if len(recent_ids) > 500:
            recent_ids.pop(0)

    return {
        "event_id": event_id,
        "user_id": f"user_{random.randint(1, 1000)}",
        "amount": round(random.uniform(1, 500), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--rate", type=int, default=50, help="events per second")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    recent_ids: list[str] = []
    interval = 1.0 / max(args.rate, 1)

    print(f"producing ~{args.rate} events/s to '{args.topic}' (Ctrl-C to stop)")
    sent = 0
    try:
        while True:
            producer.send(args.topic, make_event(recent_ids))
            sent += 1
            if sent % args.rate == 0:
                print(f"  sent {sent} events")
            time.sleep(interval)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\nstopped. total sent: {sent}")


if __name__ == "__main__":
    main()
