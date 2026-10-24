import threading
import time
from dataclasses import replace

import pytest
from maintai_messaging.config import KeyedSettings
from maintai_messaging.consumer import DurableReceipt, Record
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import ConfigurationError, ConsumerFailed
from maintai_messaging.keyed import Entry, KeyedConsumerRunner, OffsetCoordinator
from maintai_messaging.ordering import Assignment

from ..conftest import ConsumerClient, Message


def reading(example, machine, seq, offset):
    payload = example("sensor-reading")
    payload.update(machine_id=machine, event_id=f"evt_{machine}_{seq}", sequence_number=seq)
    return Message(canonical_json(payload), key=machine.encode(), offset=offset)


class FencedFixture:
    """A cancellation-aware fake; durable DB fencing is tested against PostgreSQL."""

    consumer_group = "processing-v1"

    def __init__(self, callback):
        self.callback = callback
        self.generation = 0
        self.tokens = []

    def acquire(self, partitions):
        self.generation += 1
        tokens = {
            (p.topic, p.partition): Assignment(
                self.consumer_group, p.topic, p.partition, "fixture", self.generation
            )
            for p in partitions
        }
        self.tokens.extend(tokens.values())
        return tokens

    def release(self, tokens):
        for token in tokens:
            token.cancelled.set()

    def handle(self, event, record, context):
        self.callback(event, record, context)
        context.check()
        return DurableReceipt.for_record(record)


def test_partition_frontier_waits_for_holes_and_accepts_offset_gaps():
    ledger = OffsetCoordinator()
    entries = [
        Entry(Record("sensor.readings", 0, n, b"A", b"x"), None, None, 0, 1)
        for n in [500, 501, 502, 503, 504]
    ]
    for entry in entries:
        ledger.admit(entry)
    entries[1].done = entries[3].done = True
    assert ledger.candidates() == []
    entries[0].done = True
    partition, prefix = ledger.candidates()[0]
    assert partition.offset == 502
    ledger.acknowledge(partition, prefix)
    entries[2].done = True
    partition, prefix = ledger.candidates()[0]
    assert partition.offset == 504
    ledger.acknowledge(partition, prefix)
    entries[4].done = True
    assert ledger.candidates()[0][0].offset == 505
    independent = Entry(Record("sensor.readings", 1, 900, b"B", b"x"), None, None, 0, 1, True)
    ledger.admit(independent)
    assert {p.offset for p, _ in ledger.candidates()} == {505, 901}
    ledger.drop([("sensor.readings", 0)])
    assert len(ledger.entries()) == 1


def test_same_machine_fifo_different_machines_overlap_without_unsafe_commit(
    settings, contracts, example
):
    b_done = threading.Event()
    completed = []
    client = ConsumerClient(
        [
            reading(example, "M-A", 1, 500),
            reading(example, "M-B", 1, 501),
            reading(example, "M-A", 2, 502),
            reading(example, "M-B", 2, 503),
        ]
    )

    def work(event, record, _context):
        if event.key == "M-A" and event.payload["sequence_number"] == 1:
            assert b_done.wait(2)
            assert client.commits == []
        if event.key == "M-A" and event.payload["sequence_number"] == 2:
            assert 500 in completed
        completed.append(record.offset)
        if event.key == "M-B":
            b_done.set()

    runner = KeyedConsumerRunner(
        settings, "processing", FencedFixture(work), contracts=contracts, client=client
    )
    assert runner.run(max_records=4) == 4
    assert completed.index(501) < completed.index(500) < completed.index(502)
    assert client.commits[-1].offset == 504 and client.closed


def test_keyed_failure_does_not_commit_past_durable_later_record(settings, contracts, example):
    b_done = threading.Event()
    client = ConsumerClient([reading(example, "M-A", 1, 0), reading(example, "M-B", 1, 1)])

    def fail_a(event, _record, _context):
        if event.key == "M-A":
            assert b_done.wait(2)
            raise OSError("database rollback")
        b_done.set()

    runner = KeyedConsumerRunner(
        settings, "processing", FencedFixture(fail_a), contracts=contracts, client=client
    )
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=2)
    assert not client.commits and client.closed
    assert all(t.cancelled.is_set() for t in runner.handler.tokens)


def test_backpressure_keeps_polling_and_bounds_completed_receipts(settings, contracts, example):
    release = threading.Event()
    snapshots = []
    limits = replace(KeyedSettings(), max_pending_records=2, max_active_keys=2)
    client = ConsumerClient([reading(example, f"M-{i}", 1, i) for i in range(8)])

    def paused(_client):
        snapshots.append(dict(runner.snapshot))
        release.set()

    client.during_wait = paused

    def work(*_args):
        assert release.wait(2)

    runner = KeyedConsumerRunner(
        settings,
        "processing",
        FencedFixture(work),
        limits=limits,
        contracts=contracts,
        client=client,
    )
    runner.run(max_records=8)
    assert snapshots and max(s["pending_records"] for s in snapshots) <= 2
    assert client.poll_count > 8 and client.pauses


def test_shutdown_is_bounded_and_unfinished_work_is_fenced(settings, contracts, example):
    stop, started, release = threading.Event(), threading.Event(), threading.Event()
    writes = []
    client = ConsumerClient([reading(example, "M-A", 1, 0)])

    def work(_event, _record, context):
        started.set()
        assert release.wait(3)
        context.check()
        writes.append("forbidden late effect")

    def paused(_client):
        if started.is_set():
            stop.set()

    client.during_wait = paused
    limits = replace(KeyedSettings(), max_pending_per_key=1, drain_timeout_seconds=0.02)
    runner = KeyedConsumerRunner(
        settings,
        "processing",
        FencedFixture(work),
        limits=limits,
        contracts=contracts,
        client=client,
    )
    before = time.monotonic()
    try:
        with pytest.raises(ConsumerFailed, match="Drain"):
            runner.run(stop)
        assert time.monotonic() - before < 1
        assert not client.commits
    finally:
        release.set()
    assert not writes and all(t.cancelled.is_set() for t in runner.handler.tokens)


def test_lost_assignment_does_not_ack_old_work(settings, contracts, example):
    stop, release = threading.Event(), threading.Event()
    client = ConsumerClient([reading(example, "M-A", 1, 0)])

    def lost(client):
        client.callbacks["on_lost"](client, client.assignment())
        client.assign([])
        release.set()
        stop.set()

    client.during_wait = lost

    def work(*_args):
        assert release.wait(2)

    runner = KeyedConsumerRunner(
        settings,
        "processing",
        FencedFixture(work),
        limits=replace(KeyedSettings(), max_pending_per_key=1),
        contracts=contracts,
        client=client,
    )
    runner.run(stop)
    assert not client.commits and client.closed


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_active_keys": 0},
        {"max_pending_records": 1},
        {"resume_low_watermark": 1},
        {"handler_timeout_seconds": float("nan")},
        {"drain_timeout_seconds": float("inf")},
    ],
)
def test_invalid_keyed_capacity_is_rejected(kwargs):
    with pytest.raises(ConfigurationError):
        KeyedSettings(**kwargs)


def test_keyed_mode_rejects_other_roles_and_unfenced_callbacks(settings, contracts):
    for role, handler in [
        ("business-live", FencedFixture(lambda *_: None)),
        ("processing", lambda *_: None),
    ]:
        with pytest.raises(ConfigurationError):
            KeyedConsumerRunner(
                settings, role, handler, contracts=contracts, client=ConsumerClient([])
            )
    assert KeyedSettings.from_env({"PROCESSING_MAX_ACTIVE_KEYS": "3"}).max_active_keys == 3


def test_invalid_receipt_and_offset_commit_failure_preserve_replay(settings, contracts, example):
    class BadReceipt(FencedFixture):
        def handle(self, *_args):
            return None

    client = ConsumerClient([reading(example, "M-A", 1, 0)])
    runner = KeyedConsumerRunner(
        settings, "processing", BadReceipt(lambda *_: None), contracts=contracts, client=client
    )
    with pytest.raises(ConsumerFailed, match="receipt"):
        runner.run(max_records=1)
    assert not client.commits
    client = ConsumerClient([reading(example, "M-A", 1, 0)], commit_error=OSError())
    runner = KeyedConsumerRunner(
        settings, "processing", FencedFixture(lambda *_: None), contracts=contracts, client=client
    )
    with pytest.raises(OSError):
        runner.run(max_records=1)
    assert not client.commits and client.closed


def test_receipt_budget_counts_headers_and_rejects_oversized_metadata(settings, contracts, example):
    class OversizedHeaders(Message):
        def headers(self):
            return [("oversized", b"x" * (settings.max_payload_bytes + 4096))]

    native = reading(example, "M-A", 1, 0)
    client = ConsumerClient([OversizedHeaders(native.value(), key=native.key())])
    runner = KeyedConsumerRunner(
        settings, "processing", FencedFixture(lambda *_: None), contracts=contracts, client=client
    )
    with pytest.raises(ConsumerFailed, match="headers"):
        runner.run(max_records=1)
    assert not client.commits


def test_serial_service_rejects_incompatible_handler_before_connecting(settings):
    from maintai_messaging.service import serve_consumers

    stop = threading.Event()
    with pytest.raises(ConfigurationError, match="callable"):
        serve_consumers(settings, {"processing": object()}, stop)
    assert stop.is_set()


def test_service_startup_failure_signals_already_started_sibling_loops(settings, monkeypatch):
    from maintai_messaging import service

    started = threading.Event()
    stop = threading.Event()

    class Runner:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, stop):
            started.set()
            assert stop.wait(1)

    monkeypatch.setattr(service, "ConsumerRunner", Runner)
    with pytest.raises(ConfigurationError):
        service.serve_consumers(settings, {"business-live": lambda *_: None, "bad": None}, stop)
    assert started.is_set() and stop.is_set()
