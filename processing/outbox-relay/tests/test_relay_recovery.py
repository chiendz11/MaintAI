"""Real DB application outbox recovery; broker fixture is labelled explicitly."""

from types import SimpleNamespace

import pytest
from consumer.observability import RuntimeObservability
from maintai_messaging.contracts import default_contracts
from maintai_messaging.errors import PublishUnavailable
from maintai_messaging.postgres import Outbox
from processing.worker.tests.test_persistence import invalid_outcome, reading
from relay.main import create_relay

pytestmark = pytest.mark.integration


class FixturePublisher:
    contracts = default_contracts()
    settings = SimpleNamespace(delivery_timeout_ms=1000)

    def __init__(self):
        self.down, self.delivered = True, []

    def publish_event(self, event):
        if self.down:
            raise PublishUnavailable("fixture broker outage")
        self.delivered.append(event.wire_bytes)


def test_owner_relay_retries_same_immutable_payload_after_restart(migrated_database):
    event, record = reading()
    payload = invalid_outcome(event, record)
    with migrated_database.factory() as connection:
        Outbox().enqueue(connection, "inference.results", payload)
    publisher = FixturePublisher()
    first = create_relay(migrated_database.factory, publisher)
    assert first.run_once() == 0
    with migrated_database.factory() as connection:
        row = connection.execute("SELECT * FROM maintai_kafka_outbox").fetchone()
        expected = bytes(row["payload"])
        assert row["status"] == "PENDING"
        connection.execute("UPDATE maintai_kafka_outbox SET next_attempt_at=clock_timestamp()")
    publisher.down = False
    second = create_relay(migrated_database.factory, publisher)
    assert second.run_once() == 1
    assert publisher.delivered == [expected]
    with migrated_database.factory() as connection:
        assert (
            connection.execute("SELECT status FROM maintai_kafka_outbox").fetchone()["status"]
            == "PUBLISHED"
        )


def test_batch_lease_budget_includes_sequential_publish_and_db_finish(migrated_database):
    with pytest.raises(ValueError):
        create_relay(migrated_database.factory, FixturePublisher(), lease_seconds=30, batch_size=2)


def test_publish_failure_metrics_do_not_hide_sdk_retry(migrated_database):
    from prometheus_client import generate_latest
    from relay.main import ObservedPublisher

    event, record = reading()
    with migrated_database.factory() as connection:
        Outbox().enqueue(connection, "inference.results", invalid_outcome(event, record))
    observer = RuntimeObservability("relay", profile="local-fixture")
    publisher = FixturePublisher()
    relay = create_relay(migrated_database.factory, ObservedPublisher(publisher, observer))
    assert relay.run_once() == 0
    with migrated_database.factory() as connection:
        row = connection.execute("SELECT status,attempts FROM maintai_kafka_outbox").fetchone()
        assert row == {"status": "PENDING", "attempts": 1}
        connection.execute("UPDATE maintai_kafka_outbox SET next_attempt_at=clock_timestamp()")
    publisher.down = False
    assert relay.run_once() == 1
    metrics = generate_latest(observer.registry).decode()
    assert (
        'maintai_processing_outbox_publish_attempts_total{result="UNACKNOWLEDGED"} 1.0' in metrics
    )
    assert 'maintai_processing_outbox_publish_attempts_total{result="ACKNOWLEDGED"} 1.0' in metrics
