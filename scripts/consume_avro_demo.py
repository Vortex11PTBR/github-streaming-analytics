"""Consume messages produced in Confluent wire format (magic byte + schema id) and decode using local Schema Registry.

Usage:
    python scripts/consume_avro_demo.py --timeout 10

Requires: Schema Registry at http://localhost:8081 and Kafka at localhost:9092
"""
from __future__ import annotations
import time
import struct
import requests
from kafka import KafkaConsumer
from io import BytesIO
from fastavro import parse_schema, schemaless_reader

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP = "localhost:9092"
TOPIC = "github_events.PushEvent"
MAGIC_BYTE = 0


def fetch_schema_by_id(schema_id: int):
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return parse_schema(json.loads(data["schema"]))


def decode_confluent_wire(payload: bytes, schema_cache: dict):
    if not payload:
        return None
    magic = payload[0]
    if magic != MAGIC_BYTE:
        raise ValueError("Unknown magic byte")
    schema_id = int.from_bytes(payload[1:5], byteorder="big")
    avro_payload = payload[5:]
    if schema_id not in schema_cache:
        # fetch schema from registry
        url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
        resp = requests.get(url)
        resp.raise_for_status()
        schema_raw = resp.json()["schema"]
        import json

        schema_cache[schema_id] = parse_schema(json.loads(schema_raw))
    schema = schema_cache[schema_id]
    bio = BytesIO(avro_payload)
    record = schemaless_reader(bio, schema)
    return record


def main(timeout: int = 10):
    c = KafkaConsumer(TOPIC, bootstrap_servers=[BOOTSTRAP], consumer_timeout_ms=1000)
    schema_cache = {}
    start = time.time()
    print("Listening for Avro messages on", TOPIC)
    while time.time() - start < timeout:
        for tp, msgs in c.poll(timeout_ms=500, max_records=10).items():
            for m in msgs:
                try:
                    rec = decode_confluent_wire(m.value, schema_cache)
                    print("decoded:", rec)
                except Exception as e:
                    print("decode error:", e)
        time.sleep(0.2)
    c.close()


if __name__ == "__main__":
    main()
