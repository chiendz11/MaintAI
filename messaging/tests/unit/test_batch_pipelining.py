import hashlib
import threading
from dataclasses import replace

from maintai_messaging.config import KafkaSettings, KeyedSettings
from maintai_messaging.consumer import DurableReceipt
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import PublishUnavailable, PublishUncertain
from maintai_messaging.ingestion import BatchIngestItem, IngestionGateway, SourceIdentity
from maintai_messaging.keyed import KeyedConsumerRunner
from maintai_messaging.ordering import OrderedOutboxRelay
from maintai_messaging.producer import Delivery, KafkaPublisher

from ..conftest import ConsumerClient, ProducerClient
from .test_keyed import FencedFixture, reading


def test_publish_batch_releases_enqueue_lock_before_polling_acks(settings, contracts, example):
    events = [
        contracts.encode("sensor.readings", {**example("sensor-reading"), "event_id": f"e-{i}"})
        for i in range(4)
    ]
    client = ProducerClient()
    publisher = KafkaPublisher(settings, "ingestion", contracts=contracts, client=client)
    lock_held_during_poll = []
    orig_poll = client.poll

    def tracked_poll(timeout):
        lock_held_during_poll.append(publisher._lock.locked())
        return orig_poll(timeout)

    client.poll = tracked_poll
    results = publisher.publish_batch(events)
    assert all(isinstance(r, Delivery) for r in results)
    assert len(results) == 4
    assert lock_held_during_poll and not any(lock_held_during_poll)


def test_publish_batch_partial_ack_errors_reported_per_item(
    settings, contracts, example, monkeypatch
):
    import maintai_messaging.producer as module

    class Clock:
        now = 0.0

        def monotonic(self):
            self.now += 0.25
            return self.now

    monkeypatch.setattr(module, "time", Clock())
    events = [
        contracts.encode("sensor.readings", {**example("sensor-reading"), "event_id": f"e-{i}"})
        for i in range(2)
    ]
    publisher = KafkaPublisher(
        settings, "ingestion", contracts=contracts, client=ProducerClient(never_ack=True)
    )
    results = publisher.publish_batch(events)
    assert len(results) == 2
    assert all(isinstance(res, PublishUncertain) for res in results)


def test_ingestion_gateway_handle_batch_preserves_statuses(settings, contracts, example):
    valid_a = example("sensor-request")
    valid_b = {**valid_a, "event_id": "evt_b", "sequence_number": 2}
    bad_json = b'{"broken":'
    wrong_machine = {**valid_a, "event_id": "evt_other", "machine_id": "M-999"}
    ident = SourceIdentity("sensor-01", frozenset({valid_a["machine_id"]}))
    client = ProducerClient()
    publisher = KafkaPublisher(settings, "ingestion", contracts=contracts, client=client)
    gw = IngestionGateway(publisher, "https://business.example.invalid", contracts=contracts)

    responses = gw.handle_batch(
        [
            BatchIngestItem(canonical_json(valid_a), ident, "r1", "c1"),
            BatchIngestItem(bad_json, ident, "r2", "c2"),
            BatchIngestItem(canonical_json(wrong_machine), ident, "r3", "c3"),
            BatchIngestItem(canonical_json(valid_b), ident, "r4", "c4"),
        ]
    )
    assert [r.status for r in responses] == [202, 400, 403, 202]
    assert len(client.calls) == 3
    assert responses[0].body["event_id"] == valid_a["event_id"]
    assert responses[3].body["event_id"] == "evt_b"


def test_ingestion_gateway_handle_batch_surfaces_retryable_kafka_unavailable(
    settings, contracts, example
):
    valid = example("sensor-request")
    ident = SourceIdentity("sensor-01", frozenset({valid["machine_id"]}))
    client = ProducerClient()
    publisher = KafkaPublisher(settings, "ingestion", contracts=contracts, client=client)
    publisher.publish_batch = lambda events: [PublishUnavailable("busy")] * len(events)
    gw = IngestionGateway(publisher, "https://business.example.invalid", contracts=contracts)
    responses = gw.handle_batch([BatchIngestItem(canonical_json(valid), ident, "r1", "c1")])
    assert responses[0].status == 503
    assert responses[0].body["error"]["retryable"] is True


def test_keyed_runner_batches_queued_same_key_while_preserving_per_key_1(
    settings, contracts, example
):
    batches_seen = []
    active_per_key = {}
    lock = threading.Lock()

    class BatchFixture(FencedFixture):
        def handle_batch(self, items, context):
            key = items[0][0].key
            with lock:
                assert key not in active_per_key
                active_per_key[key] = True
            try:
                batches_seen.append((key, [e.payload["sequence_number"] for e, _ in items]))
                context.check()
                return [DurableReceipt.for_record(r) for _, r in items]
            finally:
                with lock:
                    del active_per_key[key]

    messages = [
        reading(example, "M-A", 1, 0),
        reading(example, "M-A", 2, 1),
        reading(example, "M-B", 1, 2),
        reading(example, "M-A", 3, 3),
        reading(example, "M-B", 2, 4),
    ]
    client = ConsumerClient(messages)
    limits = replace(KeyedSettings(), max_active_keys=4, max_batch_per_key=16)
    runner = KeyedConsumerRunner(
        settings,
        "processing",
        BatchFixture(lambda *_: None),
        limits=limits,
        contracts=contracts,
        client=client,
    )
    assert runner.run(max_records=5) == 5
    by_key = {}
    for key, seqs in batches_seen:
        by_key.setdefault(key, []).extend(seqs)
    assert by_key["M-A"] == [1, 2, 3]
    assert by_key["M-B"] == [1, 2]
    assert any(len(seqs) > 1 for _, seqs in batches_seen)
    assert client.commits[-1].offset == 5


def test_ordered_outbox_relay_claims_and_publishes_distinct_head_batch(
    settings, contracts, example
):
    base_completed = example("completed")
    events = [
        contracts.encode(
            "inference.results",
            {**base_completed, "machine_id": f"M-{i}", "event_id": f"evt-{i}"},
        )
        for i in range(2)
    ]
    rows = [
        {
            "topic": ev.topic,
            "message_id": ev.message_id,
            "message_key": ev.key,
            "payload": ev.wire_bytes,
            "payload_sha256": hashlib.sha256(ev.wire_bytes).hexdigest(),
            "dispatch_seq": 1,
            "attempts": 1,
            "fencing_token": 7,
        }
        for ev in events
    ]

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def transaction(self):
            return self

        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return {"enabled": True}

    publisher = KafkaPublisher(settings, "outbox", contracts=contracts, client=ProducerClient())
    relay = OrderedOutboxRelay(lambda: FakeConn(), publisher, batch_size=16)
    claimed_calls = []
    finished_batches = []
    relay._claim_batch = lambda limit=None: (
        claimed_calls.append(limit) or (rows if len(claimed_calls) == 1 else [])
    )
    relay._finish_batch = lambda items: finished_batches.append(items)

    published = relay.run_once()
    assert published == 2
    assert len(finished_batches) == 1
    assert [(r["message_key"], ok) for r, ok, _delay in finished_batches[0]] == [
        ("M-0", True),
        ("M-1", True),
    ]


def test_kafka_settings_pipelining_and_commit_batch_env():
    cfg = KafkaSettings.from_env(
        {
            "KAFKA_LOCAL_MODE": "true",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "KAFKA_LINGER_MS": "3",
            "KAFKA_COMPRESSION_TYPE": "zstd",
            "KAFKA_COMMIT_BATCH_SIZE": "32",
            "KAFKA_COMMIT_MAX_WAIT_MS": "75",
        }
    )
    prod = cfg.producer_config()
    assert prod["linger.ms"] == 3
    assert prod["compression.type"] == "zstd"
    assert cfg.commit_batch_size == 32
    assert cfg.commit_max_wait_ms == 75
