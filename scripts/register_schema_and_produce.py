"""Register Avro schema with local Schema Registry and produce Avro messages with Schema Registry wire format.

Usage:
    python scripts/register_schema_and_produce.py --subject github_events-PushEvent --count 5

Requires: Schema Registry at http://localhost:8081 and Kafka at localhost:9092
"""
from __future__ import annotations
import argparse
import json
import time
import uuid
from io import BytesIO
from pathlib import Path

import requests
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer

SCHEMA_DIR = Path("schemas")
SCHEMA_FILE = SCHEMA_DIR / "PushEvent.avsc"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP = "localhost:9092"
TOPIC = "github_events.PushEvent"
MAGIC_BYTE = b"\x00"


def load_raw_schema(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def register_schema(subject: str, raw_schema: dict) -> int:
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(raw_schema)}
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json()
    return int(data["id"])


def to_avro_with_schema_id(schema, record: dict, schema_id: int) -> bytes:
    buf = BytesIO()
    schemaless_writer(buf, schema, record)
    avro_bytes = buf.getvalue()
    # Confluent wire format: 0 (magic) + 4 byte schema id (big endian) + avro payload
    return MAGIC_BYTE + schema_id.to_bytes(4, byteorder="big") + avro_bytes


def make_event(i: int):
    return {
        "id": f"evt_avro_{i}_{uuid.uuid4().hex[:6]}",
        "type": "PushEvent",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repo": {"name": f"example/repo-{i}"},
        "payload": {"size": i},
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--subject", default="github_events-PushEvent")
    p.add_argument("--count", type=int, default=5)
    args = p.parse_args()

    raw = load_raw_schema(SCHEMA_FILE)
    schema = parse_schema(raw)

    print(f"Registering schema under subject {args.subject}...")
    schema_id = register_schema(args.subject, raw)
    print(f"Registered with id {schema_id}")

    prod = KafkaProducer(bootstrap_servers=[BOOTSTRAP])
    for i in range(args.count):
        rec = make_event(i)
        payload = to_avro_with_schema_id(schema, rec, schema_id)
        prod.send(TOPIC, key=rec["type"].encode("utf-8"), value=payload)
        print("sent avro with schema id", schema_id, rec["id"])
    prod.flush()
    prod.close()


if __name__ == "__main__":
    main()
