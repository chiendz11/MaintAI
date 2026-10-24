"""Opt-in real Kafka/PostgreSQL tests, isolated by group prefix and DB schema."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from urllib.parse import urlparse

import pytest
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings
from maintai_messaging.consumer import ConsumerRunner, Record
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import IdentityConflict, LeaseLost
from maintai_messaging.postgres import (
    Outbox,
    OutboxRelay,
    TransactionalHandler,
)
from maintai_messaging.producer import KafkaPublisher

from ..conftest import ProducerClient

pytestmark = pytest.mark.integration


@pytest.fixture
def local_settings():
    if os.environ.get("MAINTAI_KAFKA_INTEGRATION") != "1":
        pytest.skip("Real broker/DB integration was not explicitly enabled")
    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    if not bootstrap.startswith(("127.0.0.1:", "localhost:")) or "," in bootstrap:
        pytest.fail("Integration tests require an isolated loopback broker")
    dsn = os.environ.get("KAFKA_TEST_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("Integration tests require an isolated loopback PostgreSQL database")
    return KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers=bootstrap,
        group_prefix="it-" + uuid.uuid4().hex + "-",
    )


def source_and_record(contracts, example, offset=0, changed=False):
    payload = example("sensor-reading")
    if changed:
        payload["features"]["torque_nm"] += 1
    event = contracts.decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


def count(factory, table):
    from psycopg import sql

    with factory() as connection:
        return connection.execute(
            sql.SQL("SELECT count(*) AS total FROM {}").format(sql.Identifier(table))
        ).fetchone()["total"]


def test_application_rows_inbox_and_outbox_rollback_together(owner_db, contracts, example):
    event, record = source_and_record(contracts, example)

    def callback(connection, event, _record, outbox):
        connection.execute("INSERT INTO effects VALUES (%s, 1)", (event.message_id,))
        outbox.enqueue(connection, "inference.results", example("completed"))
        raise RuntimeError("failure after creating durable candidates")

    handler = TransactionalHandler(owner_db, "fixture-group", callback, contracts=contracts)
    with pytest.raises(RuntimeError):
        handler(event, record)
    assert [
        count(owner_db, table)
        for table in ["effects", "maintai_kafka_inbox", "maintai_kafka_outbox"]
    ] == [0, 0, 0]


def test_concurrent_duplicate_delivery_has_one_effect_and_detects_conflict(
    owner_db, contracts, example
):
    event, record = source_and_record(contracts, example)
    gate = threading.Barrier(2)

    def callback(connection, event, _record, outbox):
        connection.execute("INSERT INTO effects VALUES (%s, 1)", (event.message_id,))
        outbox.enqueue(connection, "inference.results", example("completed"))

    handler = TransactionalHandler(owner_db, "fixture-group", callback, contracts=contracts)

    def dispatch(offset):
        gate.wait(timeout=5)
        return handler(event, replace(record, offset=offset))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(dispatch, offset) for offset in [0, 1]]
        for future in futures:
            future.result(timeout=10)
    assert [
        count(owner_db, table)
        for table in ["effects", "maintai_kafka_inbox", "maintai_kafka_outbox"]
    ] == [1, 1, 1]
    changed, changed_record = source_and_record(contracts, example, offset=2, changed=True)
    with pytest.raises(IdentityConflict):
        handler(changed, changed_record)


def test_outbox_failure_stays_pending_until_broker_ack(
    owner_db, contracts, example, local_settings
):
    with owner_db() as connection:
        Outbox(contracts).enqueue(connection, "inference.results", example("completed"))
    failed = KafkaPublisher(
        local_settings,
        "outbox",
        contracts=contracts,
        client=ProducerClient(delivery_error=RuntimeError()),
    )
    relay = OutboxRelay(owner_db, failed)
    assert relay.run_once() == 0
    with owner_db() as connection:
        row = connection.execute("SELECT * FROM maintai_kafka_outbox").fetchone()
        assert row["status"] == "PENDING" and row["published_at"] is None and row["attempts"] == 1
        connection.execute("UPDATE maintai_kafka_outbox SET next_attempt_at=clock_timestamp()")
    acknowledged = KafkaPublisher(
        local_settings, "outbox", contracts=contracts, client=ProducerClient()
    )
    assert OutboxRelay(owner_db, acknowledged).run_once() == 1
    with owner_db() as connection:
        row = connection.execute("SELECT * FROM maintai_kafka_outbox").fetchone()
        assert row["status"] == "PUBLISHED" and row["published_at"]


def test_stale_lease_cannot_record_success(owner_db, contracts, example, local_settings):
    with owner_db() as connection:
        Outbox(contracts).enqueue(connection, "inference.results", example("completed"))
    publisher = KafkaPublisher(
        local_settings, "outbox", contracts=contracts, client=ProducerClient()
    )
    original = publisher.publish_event

    def ack_then_lose_lease(event):
        ack = original(event)
        with owner_db() as connection:
            connection.execute(
                "UPDATE maintai_kafka_outbox SET fencing_token=fencing_token+1, "
                "lease_owner='new-owner'"
            )
        return ack

    publisher.publish_event = ack_then_lose_lease
    with pytest.raises(LeaseLost):
        OutboxRelay(owner_db, publisher).run_once()
    with owner_db() as connection:
        row = connection.execute("SELECT * FROM maintai_kafka_outbox").fetchone()
        assert row["status"] == "LEASED" and row["published_at"] is None


def consume_until(settings, role, target_id, owner_db, contracts):
    stop, observed = threading.Event(), []

    def callback(connection, event, record, _outbox):
        connection.execute(
            "INSERT INTO effects VALUES (%s, 1) ON CONFLICT DO NOTHING", (event.message_id,)
        )
        if event.message_id == target_id:
            observed.append(record)
            stop.set()

    group = settings.group_prefix + contracts.topology["profiles"][role]["group_id"]
    durable = TransactionalHandler(owner_db, group, callback, contracts=contracts)
    runner = ConsumerRunner(settings, role, durable, contracts=contracts)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runner.run, stop)
        try:
            future.result(timeout=30)
        finally:
            stop.set()
    assert observed
    return observed[0]


def test_real_broker_ack_inbox_and_commit_roundtrip(owner_db, contracts, example, local_settings):
    create_local_topics(local_settings, contracts=contracts)
    payload = example("sensor-reading")
    payload["event_id"] = "evt_it_" + uuid.uuid4().hex
    with KafkaPublisher(local_settings, "ingestion", contracts=contracts) as publisher:
        delivery = publisher.publish("sensor.readings", payload)
    record = consume_until(local_settings, "processing", payload["event_id"], owner_db, contracts)
    assert (record.partition, record.offset) == (delivery.partition, delivery.offset)
    assert count(owner_db, "maintai_kafka_inbox") >= 1


def test_real_outbox_relay_to_business_group(owner_db, contracts, example, local_settings):
    create_local_topics(local_settings, contracts=contracts)
    payload = example("invalid")
    payload["result_id"] = "res_it_" + uuid.uuid4().hex
    with owner_db() as connection:
        Outbox(contracts).enqueue(connection, "inference.results", payload)
    with KafkaPublisher(local_settings, "outbox", contracts=contracts) as publisher:
        assert OutboxRelay(owner_db, publisher).run_once() == 1
    record = consume_until(
        local_settings, "business-prediction", payload["result_id"], owner_db, contracts
    )
    assert contracts.decode("inference.results", record.value).kind == "DATA_INVALID"
