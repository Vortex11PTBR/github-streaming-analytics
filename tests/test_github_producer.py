import json

from src.config import AppConfig
from src.producer.github_producer import GitHubProducer


class MockFuture:
    def __init__(self, succeed: bool = True):
        self.succeed = succeed

    def add_callback(self, cb):
        # call callback immediately to simulate quick ack
        class Meta:
            topic = "mock-topic"
            partition = 0

        if self.succeed:
            cb(Meta())

    def add_errback(self, eb):
        if not self.succeed:
            eb(Exception("send failed"))

    def get(self, timeout=None):
        if not self.succeed:
            raise Exception("send failed")
        return None


class MockKafkaProducer:
    def send(self, topic, key=None, value=None):
        return MockFuture(succeed=True)

    def flush(self, timeout=None):
        return None

    def close(self):
        return None


def test_serialize_json():
    cfg = AppConfig(use_avro=False, metrics_enabled=False)
    prod = GitHubProducer(cfg)
    payload = {"id": "1", "type": "PushEvent", "message": "hello"}
    out = prod._serialize_value(payload)
    assert isinstance(out, (bytes, bytearray))
    assert json.loads(out.decode("utf-8")) == payload


def test_produce_success_async():
    cfg = AppConfig(metrics_enabled=False)
    prod = GitHubProducer(cfg)
    # inject mock producer to avoid network
    prod.producer = MockKafkaProducer()

    # ensure inflight semaphore available
    ok = prod._produce("github_events.PushEvent", {"id": "1", "type": "PushEvent"})
    assert ok is True
    # callback increments _events_processed
    assert prod._events_processed >= 1


def test_backpressure_acquire_timeout():
    cfg = AppConfig(
        max_in_flight=1,
        backpressure_acquire_timeout=0.01,
        backpressure_block=False,
        metrics_enabled=False,
    )
    prod = GitHubProducer(cfg)
    # occupy the sole slot to simulate saturated producer
    acquired = prod._inflight_sem.acquire(timeout=0.1)
    assert acquired is True
    prod.producer = MockKafkaProducer()

    ok = prod._produce("github_events.PushEvent", {"id": "2", "type": "PushEvent"})
    assert ok is False

    # cleanup - release slot
    try:
        prod._inflight_sem.release()
    except Exception:
        pass
