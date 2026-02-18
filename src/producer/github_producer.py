"""GitHub events producer.

Polls the public GitHub Events API and publishes events to Kafka.

Features:
- Structured logging via structlog
- Retry for HTTP using tenacity
- DLQ on Kafka failures
- Graceful shutdown on SIGINT/SIGTERM

Usage:
        python -m src.producer.github_producer
"""

from __future__ import annotations

import json
import signal
import sys
import time
from typing import Any, Dict, Optional
import threading

import requests
import structlog
from kafka import KafkaProducer  # type: ignore[import-untyped]
from kafka.errors import KafkaError  # type: ignore[import-untyped]
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import AppConfig
from src.tracing import setup_tracer, inject_trace_into_kafka_headers

cfg = AppConfig()
logger = structlog.get_logger()


def _init_logging() -> None:
    """Initialize structlog and stdlib logging level from config."""
    import logging

    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_events(url: str | Any, headers: Dict[str, str]) -> Any:
    """Fetch events from GitHub with retries (tenacity)."""
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    # Respect rate-limit headers when present (best-effort)
    try:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None:
            rem = int(remaining)
            reset_ts = int(reset)
            if rem <= 1:
                sleep_for = max(0, reset_ts - int(time.time()) + 1)
                logger.warning(
                    "rate_limit_near_exhaustion", remaining=rem, sleeping=sleep_for
                )
                time.sleep(sleep_for)
    except Exception:
        # don't fail on header parsing
        pass
    return resp.json()


class AvroSchemaRegistry:
    """Loads Avro schemas from a directory keyed by event type (EventType.avsc)."""

    def __init__(self, schema_dir: Optional[str]) -> None:
        self.schema_dir = schema_dir
        from typing import Any

        self._cache: Dict[str, Any] = {}

    def _load_schema(self, event_type: str) -> Optional[Any]:
        if not self.schema_dir:
            return None
        import os
        from fastavro import parse_schema

        path = os.path.join(self.schema_dir, f"{event_type}.avsc")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as fh:
            schema = parse_schema(json.load(fh))
        return schema

    def get(self, event_type: str) -> Optional[Any]:
        if event_type in self._cache:
            return self._cache[event_type]
        schema = self._load_schema(event_type)
        if schema:
            self._cache[event_type] = schema
        return schema


class GitHubProducer:
    """Producer that polls GitHub Events API and publishes to Kafka topics.

    Topics are named `<kafka_topic_prefix>.<EventType>`.
    """

    def __init__(
        self, config: AppConfig, producer: Optional[KafkaProducer] = None
    ) -> None:
        self.cfg = config
        self._shutdown = False
        # init tracing early
        try:
            setup_tracer(service_name="github-producer")
        except Exception:
            pass
        # backpressure primitives
        self._inflight_sem = threading.BoundedSemaphore(self.cfg.max_in_flight)
        self._inflight_lock = threading.Lock()
        self._inflight_count = 0
        self._last_request_ts: float = 0.0
        # min interval between GitHub requests to respect configured rate_limit_per_hour
        self._min_request_interval = 3600.0 / float(
            max(1, self.cfg.rate_limit_per_hour)
        )
        self.schema_registry = AvroSchemaRegistry(
            self.cfg.avro_schema_dir if self.cfg.use_avro else None
        )
        self._events_processed = 0
        self._metrics_last_ts = time.time()
        self._metrics_last_count = 0
        self._metrics_thread: Optional["threading.Thread"] = None

        if self.cfg.metrics_enabled:
            self._start_metrics()

        # allow injection of a mock / preconfigured producer (useful for tests)
        # create the real KafkaProducer lazily when first used to avoid
        # attempting a network connection during tests or startup.
        self.producer = producer

    def _on_send_success(self, record_metadata) -> None:
        # release inflight slot and update metrics
        try:
            self._inflight_sem.release()
        except ValueError:
            logger.warning("semaphore_release_mismatch")
        try:
            with self._inflight_lock:
                self._inflight_count = max(0, self._inflight_count - 1)
                if getattr(self, "_inflight_gauge", None) is not None:
                    try:
                        self._inflight_gauge.set(self._inflight_count)
                    except Exception:
                        pass
        except Exception:
            pass
        # increment processed counter
        self._events_processed += 1
        if getattr(self, "_events_counter", None) is not None:
            try:
                self._events_counter.inc()
            except Exception:
                pass
        logger.info(
            "produced_async",
            topic=record_metadata.topic if hasattr(record_metadata, "topic") else None,
            partition=getattr(record_metadata, "partition", None),
        )

    def _on_send_error(self, excp) -> None:
        try:
            self._inflight_sem.release()
        except Exception:
            pass
        try:
            with self._inflight_lock:
                self._inflight_count = max(0, self._inflight_count - 1)
                if getattr(self, "_inflight_gauge", None) is not None:
                    try:
                        self._inflight_gauge.set(self._inflight_count)
                    except Exception:
                        pass
        except Exception:
            pass
        logger.error("produce_send_error", error=str(excp))

    def _start_metrics(self) -> None:
        import threading
        from prometheus_client import start_http_server, Counter, Gauge

        start_http_server(self.cfg.metrics_port)
        self._events_counter = Counter(
            "github_events_processed_total", "Total GitHub events processed"
        )
        self._events_per_second = Gauge(
            "github_events_per_second", "Recent events per second (instant)"
        )
        self._inflight_gauge = Gauge(
            "github_producer_inflight", "Number of in-flight producer messages"
        )

        def metrics_worker() -> None:
            while not self._shutdown:
                time.sleep(self.cfg.metrics_log_interval_seconds)
                now = time.time()
                delta_t = now - self._metrics_last_ts
                if delta_t <= 0:
                    continue
                delta_count = self._events_processed - self._metrics_last_count
                eps = delta_count / delta_t
                try:
                    self._events_per_second.set(eps)
                except Exception:
                    pass
                logger.info(
                    "metrics",
                    events_processed=self._events_processed,
                    events_per_second=eps,
                )
                self._metrics_last_ts = now
                self._metrics_last_count = self._events_processed

        self._metrics_thread = threading.Thread(target=metrics_worker, daemon=True)
        self._metrics_thread.start()

    def _handle_signal(self, signum, frame) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self._shutdown = True

    def _produce(self, topic: str, value: dict) -> bool:
        # partition by event type via key
        key = str(value.get("type", "unknown")).encode("utf-8")
        # choose serializer: JSON or Avro
        try:
            serialized = self._serialize_value(value)
        except Exception as e:
            logger.exception("serialization_failed", error=str(e))
            return False

        # Acquire inflight slot (backpressure)
        acquired = self._inflight_sem.acquire(
            timeout=self.cfg.backpressure_acquire_timeout
        )
        if not acquired:
            logger.warning("producer_backpressure_acquire_timeout", topic=topic)
            if self.cfg.backpressure_block:
                # strong backpressure: block until slot available
                self._inflight_sem.acquire()
                acquired = True
            else:
                # soft backpressure: sleep and return False so upstream can slow down
                time.sleep(self.cfg.backpressure_backoff_seconds)
                return False

        # account for newly acquired slot
        with self._inflight_lock:
            self._inflight_count += 1
            if getattr(self, "_inflight_gauge", None) is not None:
                try:
                    self._inflight_gauge.set(self._inflight_count)
                except Exception:
                    pass

        try:
            # lazy-create producer if not injected
            if self.producer is None:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.cfg.kafka_bootstrap_servers,
                    value_serializer=lambda v: v,
                    retries=5,
                    linger_ms=100,
                )

            # At this point self.producer is guaranteed to be set
            producer = self.producer  # type: KafkaProducer

            # inject trace context into Kafka headers
            try:
                headers = inject_trace_into_kafka_headers()
            except Exception:
                headers = []

            # some test mocks or older kafka clients may not accept `headers` kwarg
            try:
                fut = producer.send(topic, key=key, value=serialized, headers=headers)
            except TypeError:
                # fallback: try without headers
                fut = producer.send(topic, key=key, value=serialized)
            # use async callbacks to release semaphore when sent/errored
            try:
                fut.add_callback(self._on_send_success)
                fut.add_errback(self._on_send_error)
            except Exception:
                # older kafka-python versions may not support add_callback on future
                # fall back to synchronous get (will also release via finally)
                try:
                    fut.get(timeout=self.cfg.kafka_produce_timeout)
                    # release handled in _on_send_success is not called here.
                    # Mimic success behavior by invoking handler.
                    self._on_send_success(fut)
                except Exception as e:
                    self._on_send_error(e)
            return True
        except KafkaError as e:
            logger.error("produce_failed_immediate", topic=topic, error=str(e))
            # release inflight slot on immediate failure
            try:
                self._inflight_sem.release()
            except Exception:
                pass
            with self._inflight_lock:
                self._inflight_count = max(0, self._inflight_count - 1)
                if getattr(self, "_inflight_gauge", None) is not None:
                    try:
                        self._inflight_gauge.set(self._inflight_count)
                    except Exception:
                        pass
            return False

    def _send_to_dlq(self, original: dict, reason: str) -> None:
        dlq_payload = {"original": original, "reason": reason, "ts": int(time.time())}
        try:
            if self.producer is None:
                # If no producer available, write DLQ payload to stderr (best-effort)
                logger.warning("dlq_no_producer", reason=reason)
                dump = json.dumps(dlq_payload)
                print(dump, file=sys.stderr)
                return
            fut = self.producer.send(self.cfg.dlq_topic, value=dlq_payload)
            fut.get(timeout=self.cfg.kafka_produce_timeout)
            logger.warning("sent_to_dlq", topic=self.cfg.dlq_topic, reason=reason)
        except Exception as e:
            logger.error("dlq_failed", error=str(e))
            # last resort: write to stderr so orchestration can capture
            sys.stderr.write(json.dumps(dlq_payload) + "\n")

    def _serialize_value(self, value: dict) -> bytes:
        """Serialize event as Avro (if enabled & schema available) or JSON bytes."""
        event_type = value.get("type", "unknown")
        if self.cfg.use_avro:
            schema = self.schema_registry.get(event_type)
            if schema is not None:
                from io import BytesIO
                from fastavro import schemaless_writer

                buf = BytesIO()
                # Attempt to write using schemaless_writer. The value must match schema.
                schemaless_writer(buf, schema, value)
                return buf.getvalue()

        # fallback to JSON
        return json.dumps(value).encode("utf-8")

    def run(self) -> None:
        _init_logging()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        headers = {"Accept": "application/vnd.github.v3+json"}
        if self.cfg.github_token:
            headers["Authorization"] = f"token {self.cfg.github_token}"

        last_seen_id: Optional[str] = None

        while not self._shutdown:
            # enforce minimal interval between GitHub requests (best-effort)
            now = time.time()
            elapsed = now - self._last_request_ts
            if elapsed < self._min_request_interval:
                to_sleep = self._min_request_interval - elapsed
                time.sleep(to_sleep)

            try:
                events = fetch_events(self.cfg.github_events_url, headers=headers)
                self._last_request_ts = time.time()
            except Exception as e:
                logger.error("fetch_failed", error=str(e))
                time.sleep(self.cfg.request_backoff_seconds)
                continue

            # new events appear at start of list
            for ev in reversed(events):
                try:
                    ev_id = ev.get("id")
                    if not ev_id:
                        continue
                    if last_seen_id is not None and ev_id <= last_seen_id:
                        continue

                    topic = f"{self.cfg.kafka_topic_prefix}.{ev.get('type', 'unknown')}"
                    ok = self._produce(topic, ev)
                    if not ok:
                        self._send_to_dlq(ev, reason="kafka_produce_failed")
                except Exception as e:
                    logger.exception("processing_event_failed", error=str(e))
                    self._send_to_dlq(ev, reason="processing_exception")

            if events:
                last_seen = events[0].get("id")
                if last_seen:
                    last_seen_id = last_seen

            time.sleep(self.cfg.poll_interval_seconds)

        # shutdown metrics thread
        if self._metrics_thread is not None:
            try:
                self._metrics_thread.join(timeout=1.0)
            except Exception:
                pass

        try:
            if self.producer is not None:
                self.producer.flush(timeout=self.cfg.kafka_produce_timeout)
                self.producer.close()
        except Exception:
            pass


def main() -> None:
    prod = GitHubProducer(cfg)
    prod.run()


if __name__ == "__main__":
    main()
