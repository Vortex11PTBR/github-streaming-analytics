"""Produce example Avro-encoded messages using `schemas/PushEvent.avsc`.

Run (with venv active and Kafka up):

    python scripts/avro_demo.py --count 5

It will publish to topic `github_events.PushEvent`.
"""
from __future__ import annotations
import argparse
import json
import time
import uuid
from pathlib import Path
from io import BytesIO
from kafka import KafkaProducer
from fastavro import parse_schema, schemaless_writer

SCHEMA_PATH = Path("schemas/PushEvent.avsc")
BOOTSTRAP = "localhost:9092"
TOPIC = "github_events.PushEvent"


def load_schema(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return parse_schema(raw)


def make_event(i: int):
    return {
        "id": f"evt_avro_{i}_{uuid.uuid4().hex[:6]}",
        "type": "PushEvent",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repo": {"name": f"example/repo-{i}"},
        "payload": {"size": i},
    }


def to_avro_bytes(schema, record: dict) -> bytes:
    buf = BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=5)
    args = p.parse_args()
    schema = load_schema(SCHEMA_PATH)
    prod = KafkaProducer(bootstrap_servers=[BOOTSTRAP])
    for i in range(args.count):
        rec = make_event(i)
        data = to_avro_bytes(schema, rec)
        prod.send(TOPIC, key=rec["type"].encode("utf-8"), value=data)
        print("sent avro", rec["id"])
    prod.flush()
    prod.close()


if __name__ == "__main__":
    main()
