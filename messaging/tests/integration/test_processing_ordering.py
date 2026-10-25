"""Real PostgreSQL constraints/transactions/races; no AWS, model or Kafka mock claims."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from confluent_kafka import TopicPartition
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import ConfigurationError, ConsumerFailed, IdentityConflict, LeaseLost
from maintai_messaging.keyed import KeyedConsumerRunner
from maintai_messaging.ordering import (
    KeyedTransactionalHandler,
    OrderedOutboxRelay,
    WorkContext,
    activate_processing_ordering,
    apply_processing_ordering_schema,
    require_ordering,
)
from maintai_messaging.outcomes import OutcomeFactory
from maintai_messaging.postgres import Outbox, OutboxRelay, TransactionalHandler
from maintai_messaging.producer import KafkaPublisher

from ..conftest import ConsumerClient, Message, ProducerClient
from .test_durable_delivery import local_settings as local_settings

pytestmark = pytest.mark.integration


@pytest.fixture
def ordered_db(owner_db, contracts):
    with owner_db() as connection:
        apply_processing_ordering_schema(connection, contracts)
        activate_processing_ordering(connection, legacy_writers_drained=True)
    return owner_db


def source(contracts, example, machine="M-A", seq=1, offset=0):
    payload = example("sensor-reading")
    payload.update(machine_id=machine, sequence_number=seq, event_id=f"evt_{machine}_{seq}")
    event = contracts.decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


def invalid(event, record, contracts):
    return OutcomeFactory(
        event,
        record,
        dq_ruleset_version="fixture-dq",
        dq_ruleset_sha256="a" * 64,
        producer_git_sha="b" * 40,
        contracts=contracts,
    ).invalid([{"field": "features.torque_nm", "code": "RANGE"}])


def handler(factory, contracts, prepare=None, persist=None, *, group="processing-v1"):
    def default_prepare(event, record, context):
        context.check()
        return invalid(event, record, contracts)

    def default_persist(connection, event, _record, outbox, prepared):
        connection.execute("INSERT INTO effects VALUES (%s,1)", (event.message_id,))
        outbox.enqueue(connection, "inference.results", prepared)

    return KeyedTransactionalHandler(
        factory,
        group,
        prepare or default_prepare,
        persist or default_persist,
        contracts=contracts,
    )


def context_for(durable):
    token = durable.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]
    return WorkContext(token, time.monotonic() + 10)


def rows(factory, table):
    from psycopg import sql

    with factory() as connection:
        return connection.execute(
            sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
        ).fetchall()


def test_migration_and_activation_are_explicit_idempotent_and_fail_closed(owner_db, contracts):
    import psycopg

    with owner_db() as connection:
        apply_processing_ordering_schema(connection, contracts)
        apply_processing_ordering_schema(connection, contracts)
    with pytest.raises(ConfigurationError):
        handler(owner_db, contracts)
    with pytest.raises(ConfigurationError):
        with owner_db() as connection:
            activate_processing_ordering(connection)
    with owner_db() as connection:
        activate_processing_ordering(connection, legacy_writers_drained=True)
        activate_processing_ordering(connection, legacy_writers_drained=True)
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        with owner_db() as connection:
            connection.execute(
                "INSERT INTO maintai_kafka_inbox VALUES "
                "('old','sensor.readings','evt_old',%s,0,0,'1.3.0',now())",
                ("a" * 64,),
            )


def test_activation_refuses_unordered_pending_rows(owner_db, contracts, example):
    with owner_db() as connection:
        Outbox(contracts).enqueue(connection, "inference.results", example("invalid"))
        apply_processing_ordering_schema(connection, contracts)
    with pytest.raises(ConfigurationError, match="pending"):
        with owner_db() as connection:
            activate_processing_ordering(connection, legacy_writers_drained=True)


def test_initial_activation_refuses_unproven_legacy_source_cursor(owner_db, contracts, example):
    event, record = source(contracts, example)
    TransactionalHandler(owner_db, "processing-v1", lambda *_: None)(event, record)
    with owner_db() as connection:
        apply_processing_ordering_schema(connection, contracts)
    with pytest.raises(ConfigurationError, match="bootstrap"):
        with owner_db() as connection:
            activate_processing_ordering(connection, legacy_writers_drained=True)


def test_duplicate_is_skipped_before_prepare_sequence_gap_allowed_epoch_not_sorted(
    ordered_db, contracts, example
):
    prepared = []

    def prepare(event, record, _context):
        prepared.append(event.message_id)
        return invalid(event, record, contracts)

    durable = handler(ordered_db, contracts, prepare)
    context = context_for(durable)
    event, record = source(contracts, example, seq=10)
    durable.handle(event, record, context)
    payload = dict(event.payload, server_accepted_at="2026-10-23T00:00:00Z")
    retry = contracts.decode(event.topic, canonical_json(payload))
    durable.handle(retry, replace(record, offset=1, value=retry.wire_bytes), context)
    newer, later = source(contracts, example, seq=25, offset=2)
    payload = dict(newer.payload, stream_epoch="00000000-0000-0000-0000-000000000000")
    newer = contracts.decode(newer.topic, canonical_json(payload))
    durable.handle(newer, replace(later, value=newer.wire_bytes), context)
    assert len(prepared) == 2
    assert len(rows(ordered_db, "effects")) == 2
    assert rows(ordered_db, "maintai_processing_machine_cursor")[0]["last_sequence"] == 25
    assert [r["dispatch_seq"] for r in rows(ordered_db, "maintai_kafka_outbox")] == [1, 2]


def test_same_identity_changed_body_or_lower_new_sequence_rolls_back(
    ordered_db, contracts, example
):
    durable = handler(ordered_db, contracts)
    context = context_for(durable)
    event, record = source(contracts, example, seq=10)
    durable.handle(event, record, context)
    payload = dict(event.payload, features=dict(event.payload["features"], torque_nm=999))
    changed = contracts.decode(event.topic, canonical_json(payload))
    with pytest.raises(IdentityConflict):
        durable.handle(changed, replace(record, offset=1, value=changed.wire_bytes), context)
    older, old_record = source(contracts, example, seq=9, offset=2)
    with pytest.raises(IdentityConflict):
        durable.handle(older, old_record, context)
    assert len(rows(ordered_db, "effects")) == 1


def test_stale_db_writer_is_fenced_after_new_assignment(ordered_db, contracts, example):
    preparing, release = threading.Event(), threading.Event()

    def prepare(event, record, _context):
        preparing.set()
        assert release.wait(3)
        return invalid(event, record, contracts)

    old = handler(ordered_db, contracts, prepare)
    context = context_for(old)
    event, record = source(contracts, example)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(old.handle, event, record, context)
        try:
            assert preparing.wait(2)
            # Do NOT cancel old token locally: only the durable DB fence can detect this.
            new = handler(ordered_db, contracts)
            current = context_for(new)
            new.handle(event, record, current)
        finally:
            release.set()
        with pytest.raises(LeaseLost):
            future.result(timeout=3)
    assert len(rows(ordered_db, "effects")) == 1
    assert len(rows(ordered_db, "maintai_kafka_outbox")) == 1


def test_missing_result_or_persist_failure_rolls_back_every_effect(ordered_db, contracts, example):
    def persist(connection, event, _record, _outbox, _prepared):
        connection.execute("INSERT INTO effects VALUES (%s,1)", (event.message_id,))

    durable = handler(ordered_db, contracts, persist=persist)
    context = context_for(durable)
    event, record = source(contracts, example)
    with pytest.raises(ConsumerFailed, match="terminal"):
        durable.handle(event, record, context)
    for table in [
        "effects",
        "maintai_kafka_inbox",
        "maintai_kafka_outbox",
        "maintai_processing_machine_cursor",
        "maintai_outbox_keys",
    ]:
        assert rows(ordered_db, table) == []


def test_db_keyed_runner_overlaps_machines_and_commits_only_durable_prefix(
    ordered_db, settings, contracts, example
):
    b_done = threading.Event()

    def prepare(event, record, _context):
        if event.key == "M-A":
            assert b_done.wait(3)
        else:
            b_done.set()
        return invalid(event, record, contracts)

    durable = handler(ordered_db, contracts, prepare)
    input_records = [
        source(contracts, example, machine="M-A", offset=500)[1],
        source(contracts, example, machine="M-B", offset=501)[1],
    ]
    client = ConsumerClient([Message(r.value, key=r.key, offset=r.offset) for r in input_records])
    runner = KeyedConsumerRunner(
        settings, "processing", durable, contracts=contracts, client=client
    )
    assert runner.run(max_records=2) == 2
    assert client.commits[-1].offset == 502
    assert len(rows(ordered_db, "effects")) == 2


def enqueue_sources(factory, contracts, example):
    durable = handler(factory, contracts)
    context = context_for(durable)
    for machine, seq, offset in [("M-A", 1, 0), ("M-A", 2, 1), ("M-B", 1, 2)]:
        event, record = source(contracts, example, machine, seq, offset)
        durable.handle(event, record, context)


def publisher(settings, contracts, client=None):
    return KafkaPublisher(
        settings, "outbox", contracts=contracts, client=client or ProducerClient()
    )


def test_relay_retry_blocks_same_key_successor_but_not_another_machine(
    ordered_db, settings, contracts, example
):
    enqueue_sources(ordered_db, contracts, example)
    failed = publisher(settings, contracts, ProducerClient(delivery_error=RuntimeError()))
    first = OrderedOutboxRelay(ordered_db, failed, batch_size=1)
    assert first.run_once() == 0
    success = publisher(settings, contracts)
    relay = OrderedOutboxRelay(ordered_db, success, batch_size=10)
    assert relay.run_once() == 1  # B can publish; A/2 must wait for A/1 backoff.
    assert success.client.calls[0][1]["key"] == b"M-B"
    with ordered_db() as connection:
        require_ordering(connection)
        connection.execute("UPDATE maintai_kafka_outbox SET next_attempt_at=now()")
    assert relay.run_once() == 2
    assert [
        r["last_published"]
        for r in rows(ordered_db, "maintai_outbox_keys")
        if r["message_key"] == "M-A"
    ] == [2]


def test_two_relays_cannot_claim_successor_behind_a_leased_head(
    ordered_db, settings, contracts, example
):
    enqueue_sources(ordered_db, contracts, example)
    first = OrderedOutboxRelay(ordered_db, publisher(settings, contracts), batch_size=1)
    second = OrderedOutboxRelay(ordered_db, publisher(settings, contracts), batch_size=1)
    row = first._claim()
    assert row["message_key"] == "M-A" and row["dispatch_seq"] == 1
    assert second.run_once() == 1  # only B
    assert second.run_once() == 0
    first._finish(row, True, 0)
    assert second.run_once() == 1  # now A/2


def test_expired_key_owner_cannot_finish_after_takeover(ordered_db, settings, contracts, example):
    enqueue_sources(ordered_db, contracts, example)
    first = OrderedOutboxRelay(ordered_db, publisher(settings, contracts), batch_size=1)
    row = first._claim()
    with ordered_db() as connection:
        require_ordering(connection)
        connection.execute("UPDATE maintai_outbox_keys SET lease_until=now()-interval '1 second'")
        connection.execute("UPDATE maintai_kafka_outbox SET lease_until=now()-interval '1 second'")
    second = OrderedOutboxRelay(ordered_db, publisher(settings, contracts), batch_size=1)
    replacement = second._claim()
    assert replacement["message_id"] == row["message_id"]
    with pytest.raises(LeaseLost):
        first._finish(row, True, 0)
    second._finish(replacement, True, 0)


def test_ack_loss_replays_identical_bytes_before_successor(
    ordered_db, settings, contracts, example
):
    enqueue_sources(ordered_db, contracts, example)
    # Broker ACK succeeded but process disappeared before durable _finish.
    original = publisher(settings, contracts)
    first = OrderedOutboxRelay(ordered_db, original, batch_size=1)
    row = first._claim()
    event = contracts.decode(row["topic"], bytes(row["payload"]), writer=True)
    original.publish_event(event)
    with ordered_db() as connection:
        require_ordering(connection)
        connection.execute("UPDATE maintai_outbox_keys SET lease_until=now()-interval '1 second'")
        connection.execute("UPDATE maintai_kafka_outbox SET lease_until=now()-interval '1 second'")
    retried = publisher(settings, contracts)
    assert OrderedOutboxRelay(ordered_db, retried, batch_size=1).run_once() == 1
    assert original.client.calls[0][1]["value"] == retried.client.calls[0][1]["value"]
    assert (
        next(
            r
            for r in rows(ordered_db, "maintai_outbox_keys")
            if r["message_key"] == row["message_key"]
        )["last_published"]
        == 1
    )


def test_legacy_relay_cannot_bypass_active_ordering_gate(ordered_db, settings, contracts, example):
    import psycopg

    enqueue_sources(ordered_db, contracts, example)
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        OutboxRelay(ordered_db, publisher(settings, contracts)).run_once()
    assert all(row["status"] == "PENDING" for row in rows(ordered_db, "maintai_kafka_outbox"))


def test_real_broker_keyed_assignment_processing_and_ordered_publication(
    ordered_db, local_settings, contracts, example
):
    import uuid

    from confluent_kafka import Consumer
    from maintai_messaging.admin import create_local_topics

    create_local_topics(local_settings, contracts=contracts)
    assigned, stop = threading.Event(), threading.Event()
    group = local_settings.group_prefix + "processing-v1"
    durable = handler(ordered_db, contracts, group=group)

    class TailRunner(KeyedConsumerRunner):
        def _assigned(self, consumer, partitions):
            # Explicit end offsets avoid the lazy latest-position reset race.
            for p in partitions:
                p.offset = consumer.get_watermark_offsets(p, timeout=10)[1]
            super()._assigned(consumer, partitions)
            assigned.set()

    client = Consumer(local_settings.consumer_config("processing-v1"))
    runner = TailRunner(local_settings, "processing", durable, contracts=contracts, client=client)
    event, record = source(contracts, example, machine="M-real-" + uuid.uuid4().hex)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runner.run, stop, max_records=1)
        try:
            assert assigned.wait(15)
            with KafkaPublisher(local_settings, "ingestion", contracts=contracts) as producer:
                delivery = producer.publish("sensor.readings", event.payload)
            assert future.result(timeout=20) == 1
        finally:
            stop.set()
    outcome = rows(ordered_db, "maintai_kafka_outbox")[0]
    decoded = contracts.decode("inference.results", bytes(outcome["payload"]))
    assert (decoded.payload["source_partition"], decoded.payload["source_offset"]) == (
        delivery.partition,
        delivery.offset,
    )
    with KafkaPublisher(local_settings, "outbox", contracts=contracts) as producer:
        assert OrderedOutboxRelay(ordered_db, producer).run_once() == 1
    assert rows(ordered_db, "maintai_kafka_outbox")[0]["status"] == "PUBLISHED"


def test_simultaneous_relays_publish_different_heads_without_overtaking(
    ordered_db, settings, contracts, example
):
    enqueue_sources(ordered_db, contracts, example)
    gate = threading.Barrier(2)
    relays = []
    for _ in range(2):
        client = publisher(settings, contracts)
        original = client.publish_event

        def publish(event, original=original):
            gate.wait(timeout=3)
            return original(event)

        client.publish_event = publish
        relays.append(OrderedOutboxRelay(ordered_db, client, batch_size=1))
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert [f.result(timeout=5) for f in [pool.submit(r.run_once) for r in relays]] == [1, 1]
    assert {
        (r["message_key"], r["dispatch_seq"])
        for r in rows(ordered_db, "maintai_kafka_outbox")
        if r["status"] == "PUBLISHED"
    } == {("M-A", 1), ("M-B", 1)}


def test_invalid_outcome_cannot_emit_a_signal_even_from_a_bad_callback(
    ordered_db, contracts, example
):
    def persist(connection, event, record, outbox, prepared):
        outbox.enqueue(connection, "inference.results", prepared)
        signal = example("alert-signal")
        signal.update(machine_id=event.key, event_id=event.message_id)
        outbox.enqueue(connection, "alert.signals", signal)

    durable = handler(ordered_db, contracts, persist=persist)
    event, record = source(contracts, example)
    with pytest.raises(ConsumerFailed, match="cannot emit"):
        durable.handle(event, record, context_for(durable))
    assert rows(ordered_db, "maintai_kafka_outbox") == []
    assert rows(ordered_db, "maintai_outbox_keys") == []


def test_outbox_metrics_reflect_leased_and_unknown_heads(ordered_db, settings, contracts, example):
    from maintai_messaging.metrics import OutboxOrderMetrics
    from prometheus_client import CollectorRegistry

    enqueue_sources(ordered_db, contracts, example)
    registry = CollectorRegistry()
    metrics = OutboxOrderMetrics(ordered_db, registry)
    relay = OrderedOutboxRelay(
        ordered_db, publisher(settings, contracts), metrics=metrics, batch_size=1
    )
    head = relay._claim()
    assert (
        registry.get_sample_value(
            "maintai_outbox_blocked_keys", {"topic": "inference.results", "reason": "LEASED"}
        )
        == 1
    )
    relay._finish(head, False, 30)
    assert (
        registry.get_sample_value(
            "maintai_outbox_blocked_keys", {"topic": "inference.results", "reason": "UNKNOWN"}
        )
        == 1
    )


def test_prepare_cannot_mutate_source_identity_before_persistence(ordered_db, contracts, example):
    def prepare(event, record, context):
        event.payload["features"]["torque_nm"] += 1
        return invalid(event, record, contracts)

    durable = handler(ordered_db, contracts, prepare)
    event, record = source(contracts, example)
    with pytest.raises(IdentityConflict, match="mutated"):
        durable.handle(event, record, context_for(durable))
    assert rows(ordered_db, "maintai_kafka_inbox") == []
    assert rows(ordered_db, "maintai_kafka_outbox") == []
