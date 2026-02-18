"""Simple smoke test: create a Kafka topic, produce N messages, consume them and exit.

This is intended to be run after `docker compose up -d` with Kafka available.
"""
from __future__ import annotations
import time
import uuid
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

BOOTSTRAP = "localhost:9092"
TOPIC = "smoke_test_topic"

def produce(n: int = 5):
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: v.encode("utf-8"))
    for i in range(n):
        key = str(uuid.uuid4())
        val = f"smoke-{i}-{key}"
        p.send(TOPIC, value=val)
        print(f"produced: {val}")
    p.flush()
    p.close()

def consume(timeout_s: int = 10, expected: int = 5):
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest", consumer_timeout_ms=1000)
    c.subscribe([TOPIC])
    seen = 0
    start = time.time()
    while time.time() - start < timeout_s and seen < expected:
        for msg in c.poll(timeout_ms=500, max_records=10).values():
            for record in msg:
                print(f"consumed: {record.value.decode('utf-8')}")
                seen += 1
        time.sleep(0.2)
    c.close()
    return seen

if __name__ == "__main__":
    print("Starting smoke test: produce -> consume")
    produce(5)
    got = consume(10, 5)
    if got >= 5:
        print("SMOKE TEST: PASS")
        raise SystemExit(0)
    else:
        print(f"SMOKE TEST: FAIL (got {got})")
        raise SystemExit(2)
