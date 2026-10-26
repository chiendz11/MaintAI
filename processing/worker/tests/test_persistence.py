"""Real PostgreSQL owner idempotency, immutable result and rollback tests."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from maintai_messaging.outcomes import OutcomeFactory
from maintai_messaging.postgres import Outbox
from processing.shared.persistence.repository import ProcessingRepository

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def reading(*, offset=1, changed=False):
    payload = json.loads((ROOT / "contracts/kafka/examples/sensor-reading.json").read_text())
    if changed:
        payload["features"]["torque_nm"] += 1
    event = default_contracts().decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


def invalid_outcome(event, record):
    return OutcomeFactory(
        event,
        record,
        dq_ruleset_version="fixture",
        dq_ruleset_sha256="1" * 64,
        producer_git_sha="2" * 40,
    ).invalid([{"field": "machine_id", "code": "MACHINE_NOT_FOUND"}])


def test_canonical_history_and_outbox_are_durable_together(migrated_database):
    event, record = reading()
    outcome = invalid_outcome(event, record)
    repo = ProcessingRepository()
    with migrated_database.factory() as connection, connection.transaction():
        assert repo.claim(
            connection, event, record, ruleset_version="fixture", ruleset_sha256="1" * 64
        )
        repo.persist(connection, event, outcome)
        Outbox().enqueue(connection, "inference.results", outcome)
    with migrated_database.factory() as connection:
        assert connection.execute("SELECT count(*) AS n FROM sensor_events").fetchone()["n"] == 1
        stored = connection.execute("SELECT * FROM processing_results").fetchone()
        assert bytes(stored["outcome"]) == canonical_json(outcome)
        assert (
            connection.execute("SELECT count(*) AS n FROM maintai_kafka_outbox").fetchone()["n"]
            == 1
        )


def test_failure_after_outbox_rolls_back_every_owner_effect(migrated_database):
    event, record = reading()
    repo = ProcessingRepository()
    with (
        pytest.raises(RuntimeError),
        migrated_database.factory() as connection,
        connection.transaction(),
    ):
        repo.claim(connection, event, record, ruleset_version="fixture", ruleset_sha256="1" * 64)
        outcome = invalid_outcome(event, record)
        repo.persist(connection, event, outcome)
        Outbox().enqueue(connection, "inference.results", outcome)
        raise RuntimeError("simulated crash before commit")
    with migrated_database.factory() as connection:
        for table in ("sensor_events", "processing_results", "maintai_kafka_outbox"):
            assert connection.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0


def test_duplicate_logical_reading_uses_saved_result(migrated_database):
    event, record = reading()
    repo = ProcessingRepository()
    with migrated_database.factory() as connection:
        repo.claim(connection, event, record, ruleset_version="fixture", ruleset_sha256="1" * 64)
        repo.persist(connection, event, invalid_outcome(event, record))
    payload = deepcopy(event.payload)
    payload.update(correlation_id="different-retry", server_accepted_at="2026-10-24T00:00:00Z")
    retry = default_contracts().decode("sensor.readings", canonical_json(payload))
    with migrated_database.factory() as connection:
        assert (
            repo.claim(
                connection, retry, record, ruleset_version="fixture", ruleset_sha256="1" * 64
            )
            is False
        )


def test_same_event_id_with_different_features_conflicts(migrated_database):
    event, record = reading()
    repo = ProcessingRepository()
    with migrated_database.factory() as connection:
        repo.claim(connection, event, record, ruleset_version="fixture", ruleset_sha256="1" * 64)
        repo.persist(connection, event, invalid_outcome(event, record))
    changed, changed_record = reading(changed=True)
    with pytest.raises(IdentityConflict), migrated_database.factory() as connection:
        repo.claim(
            connection, changed, changed_record, ruleset_version="fixture", ruleset_sha256="1" * 64
        )
