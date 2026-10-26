"""Owner callbacks on the real SDK fence/order protocol and PostgreSQL."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from confluent_kafka import TopicPartition
from consumer.handler import KeyedProcessingHandler
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict, LeaseLost, PublishUnavailable
from maintai_messaging.ordering import WorkContext, activate_processing_ordering, require_ordering
from relay.main import create_relay

from .test_worker_transaction import Providers, handler, rows, source

pytestmark = pytest.mark.integration


@pytest.fixture
def ordered_database(migrated_database):
    with migrated_database.factory() as connection:
        activate_processing_ordering(connection, legacy_writers_drained=True)
    return migrated_database


def reading(machine="M-001", seq=1, offset=1, event_id=None):
    original, _ = source()
    payload = {
        **original.payload,
        "machine_id": machine,
        "sequence_number": seq,
        "event_id": event_id or f"evt-{machine}-{seq}",
    }
    event = default_contracts().decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


def context(consumer):
    assignment = consumer.acquire([TopicPartition("sensor.readings", 0)])
    return WorkContext(assignment[("sensor.readings", 0)], time.monotonic() + 15)


def test_prepare_holds_no_database_connection_and_persist_is_atomic(ordered_database):
    providers = Providers()
    application = handler(ordered_database, providers)
    active = [0]

    @contextmanager
    def tracked_connection():
        with ordered_database.factory() as connection:
            active[0] += 1
            try:
                yield connection
            finally:
                active[0] -= 1

    application.connection_factory = tracked_connection
    original_predict = application.predict

    def predict(**kwargs):
        assert active[0] == 0, "HTTP must run without owner SQL connection/transaction"
        return original_predict(**kwargs)

    application.predict = predict
    consumer = KeyedProcessingHandler(application)
    consumer.handle(*reading(), context(consumer))
    assert len(rows(ordered_database, "predictions")) == 1
    with ordered_database.factory() as connection:
        assert (
            connection.execute(
                "SELECT last_sequence FROM maintai_processing_machine_cursor"
            ).fetchone()["last_sequence"]
            == 1
        )
        assert (
            connection.execute(
                "SELECT count(*) AS n FROM maintai_kafka_outbox WHERE dispatch_seq=1"
            ).fetchone()["n"]
            == 2
        )


def test_replay_and_new_group_reuse_immutable_outcomes_without_ml(ordered_database):
    providers = Providers()
    first = KeyedProcessingHandler(handler(ordered_database, providers))
    ctx = context(first)
    first.handle(*reading(), ctx)
    original = bytes(rows(ordered_database, "processing_results")[0]["outcome"])
    first.handle(*reading(offset=2), ctx)
    other = KeyedProcessingHandler(handler(ordered_database, providers, group="new-group"))
    other.handle(*reading(offset=3), context(other))
    assert providers.calls == 1
    assert len(rows(ordered_database, "predictions")) == 1
    assert bytes(rows(ordered_database, "processing_results")[0]["outcome"]) == original
    assert len(rows(ordered_database, "maintai_kafka_outbox")) == 2


def test_reassigned_partition_rejects_late_owner_database_write(ordered_database):
    entered, resume = threading.Event(), threading.Event()
    providers = Providers()
    application = handler(ordered_database, providers)
    original_predict = application.predict

    def predict(**kwargs):
        entered.set()
        assert resume.wait(5)
        return original_predict(**kwargs)

    application.predict = predict
    old = KeyedProcessingHandler(application)
    old_context = context(old)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(old.handle, *reading(), old_context)
        try:
            assert entered.wait(5)
            new = KeyedProcessingHandler(handler(ordered_database, Providers()))
            new_context = context(new)
            resume.set()
            with pytest.raises(LeaseLost):
                future.result(timeout=5)
            assert not rows(ordered_database, "processing_results")
            new.handle(*reading(), new_context)
        finally:
            resume.set()
    assert len(rows(ordered_database, "processing_results")) == 1


def test_other_machine_persists_while_slow_machine_is_in_inference(ordered_database):
    entered, resume = threading.Event(), threading.Event()
    application = handler(ordered_database, Providers())
    original_predict = application.predict

    def predict(**kwargs):
        if kwargs["machine_id"] == "A":
            entered.set()
            assert resume.wait(5)
        return original_predict(**kwargs)

    application.predict = predict
    consumer = KeyedProcessingHandler(application)
    ctx = context(consumer)
    with ThreadPoolExecutor(2) as pool:
        slow = pool.submit(consumer.handle, *reading("A", 1, 1), ctx)
        try:
            assert entered.wait(5)
            fast = pool.submit(consumer.handle, *reading("B", 1, 2), ctx)
            fast.result(timeout=5)
            assert [row["event_id"] for row in rows(ordered_database, "processing_results")] == [
                "evt-B-1"
            ]
            assert not slow.done()
        finally:
            resume.set()
        slow.result(timeout=5)
    assert len(rows(ordered_database, "processing_results")) == 2


def test_first_seen_sequence_regression_fails_and_is_audited(ordered_database):
    consumer = KeyedProcessingHandler(handler(ordered_database, Providers()))
    ctx = context(consumer)
    consumer.handle(*reading(seq=5), ctx)
    with pytest.raises(IdentityConflict):
        consumer.handle(*reading(seq=4, offset=2), ctx)
    assert len(rows(ordered_database, "processing_results")) == 1
    assert len(rows(ordered_database, "processing_conflicts")) == 1


def test_keyed_source_sequence_keeps_accepted_large_integer(ordered_database):
    consumer = KeyedProcessingHandler(handler(ordered_database, Providers()))
    consumer.handle(*reading(seq=2**63), context(consumer))
    with ordered_database.factory() as connection:
        assert (
            connection.execute(
                "SELECT last_sequence FROM maintai_processing_machine_cursor"
            ).fetchone()["last_sequence"]
            == 2**63
        )


@pytest.mark.parametrize("incoming", [9, 10])
def test_new_group_cannot_accept_regressed_or_reused_source_sequence(ordered_database, incoming):
    first = KeyedProcessingHandler(handler(ordered_database, Providers(), group="first"))
    first.handle(*reading(seq=10, event_id="original-high"), context(first))
    other = KeyedProcessingHandler(handler(ordered_database, Providers(), group="replacement"))
    with pytest.raises(IdentityConflict):
        other.handle(*reading(seq=incoming, event_id="new-first-seen", offset=2), context(other))
    assert len(rows(ordered_database, "processing_results")) == 1


@pytest.mark.parametrize("advance", [False, True])
def test_late_exact_duplicate_across_groups_preserves_live_cursor(ordered_database, advance):
    providers = Providers()
    first = KeyedProcessingHandler(handler(ordered_database, providers, group="first"))
    ctx = context(first)
    first.handle(*reading(seq=5), ctx)
    first.handle(*reading(seq=10, offset=2), ctx)
    other = KeyedProcessingHandler(handler(ordered_database, providers, group="replacement"))
    ctx = context(other)
    other.handle(*reading(seq=10, offset=3), ctx)
    if advance:
        other.handle(*reading(seq=11, offset=4), ctx)
    other.handle(*reading(seq=5, offset=5), ctx)
    assert providers.calls == (3 if advance else 2)
    assert len(rows(ordered_database, "processing_results")) == (3 if advance else 2)
    if advance:
        with ordered_database.factory() as connection:
            assert (
                connection.execute(
                    "SELECT last_sequence FROM maintai_processing_machine_cursor "
                    "WHERE consumer_group='replacement'"
                ).fetchone()["last_sequence"]
                == 11
            )


def test_concurrent_groups_cannot_create_two_events_for_one_source_sequence(ordered_database):
    barrier = threading.Barrier(2)
    consumers = []
    for group in ("one", "two"):
        application = handler(ordered_database, Providers(), group=group)
        predict = application.predict

        def concurrent_predict(_predict=predict, **kwargs):
            barrier.wait(timeout=5)
            return _predict(**kwargs)

        application.predict = concurrent_predict
        consumer = KeyedProcessingHandler(application)
        consumers.append((consumer, context(consumer)))
    committed = []
    with ThreadPoolExecutor(2) as pool:
        futures = [
            pool.submit(
                consumer.handle, *reading(seq=1, event_id=f"new-{index}", offset=index + 1), ctx
            )
            for index, (consumer, ctx) in enumerate(consumers)
        ]
        for future in futures:
            try:
                future.result(timeout=5)
                committed.append(True)
            except IdentityConflict:
                committed.append(False)
    assert sorted(committed) == [False, True]
    assert len(rows(ordered_database, "processing_results")) == 1


def test_cached_replay_requires_original_outbox_evidence(ordered_database):
    first = KeyedProcessingHandler(handler(ordered_database, Providers(), group="one"))
    first.handle(*reading(), context(first))
    with ordered_database.factory() as connection:
        connection.execute("DELETE FROM maintai_kafka_outbox WHERE topic='inference.results'")
    other = KeyedProcessingHandler(handler(ordered_database, Providers(), group="two"))
    with pytest.raises(IdentityConflict):
        other.handle(*reading(offset=2), context(other))
    with ordered_database.factory() as connection:
        assert (
            connection.execute(
                "SELECT count(*) AS n FROM maintai_kafka_inbox WHERE consumer_group='two'"
            ).fetchone()["n"]
            == 0
        )


def test_ordered_relay_does_not_dispatch_successor_past_failed_head(ordered_database):
    consumer = KeyedProcessingHandler(handler(ordered_database, Providers()))
    ctx = context(consumer)
    for args in (("A", 1, 1), ("A", 2, 2), ("B", 1, 3)):
        consumer.handle(*reading(*args), ctx)

    class Publisher:
        contracts = default_contracts()
        settings = SimpleNamespace(delivery_timeout_ms=1000)
        down = True
        sent = []

        def publish_event(self, event):
            if (
                self.down
                and event.topic == "inference.results"
                and event.payload["event_id"] == "evt-A-1"
            ):
                raise PublishUnavailable("test unavailable")
            self.sent.append((event.topic, event.payload["event_id"]))

    publisher = Publisher()
    relay = create_relay(ordered_database.factory, publisher, mode="ordered", batch_size=4)
    for _ in range(4):
        relay.run_once()
    assert ("inference.results", "evt-B-1") in publisher.sent
    assert ("inference.results", "evt-A-2") not in publisher.sent
    with ordered_database.factory() as connection:
        require_ordering(connection)
        connection.execute(
            "UPDATE maintai_kafka_outbox SET next_attempt_at=clock_timestamp() "
            "WHERE status='PENDING'"
        )
    publisher.down = False
    for _ in range(4):
        relay.run_once()
    results = [
        event_id
        for topic, event_id in publisher.sent
        if topic == "inference.results" and event_id.startswith("evt-A-")
    ]
    assert results == ["evt-A-1", "evt-A-2"]
