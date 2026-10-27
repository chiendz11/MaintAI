"""Real SQL tests catch cursor regression, lost historical predictions and split commits."""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.registry import RegistryRepository
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from maintai_messaging.postgres import TransactionalHandler

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


def event(name, seq=1, event_id=None, **changes):
    payload = json.loads((ROOT / f"contracts/kafka/examples/{name}.json").read_text())
    payload["sequence_number"] = seq
    field = "source_event_id" if payload["schema_version"] == "2.0.0" else "event_id"
    payload[field] = event_id or f"e-{seq}"
    if "result_id" in payload:
        payload["result_id"] += f"-{seq}"
    payload.update(changes)
    topic = "sensor.readings" if name == "sensor-reading" else "inference.results"
    return default_contracts().decode(topic, canonical_json(payload))


def setup(db):
    from backend.shared.projections.projector import Projector

    upgrade(db.dsn, schema=db.schema)
    RegistryRepository(db.factory).provision([{"machine_id": "M-001", "display_name": "One"}])
    return Projector()


def deliver(db, projector, reading, offset=1):
    role = "business-live-v1" if reading.topic == "sensor.readings" else "business-prediction-v1"
    record = SimpleNamespace(
        topic=reading.topic,
        partition=0,
        offset=offset,
        key=reading.key.encode(),
        value=reading.wire_bytes,
    )
    return TransactionalHandler(db.factory, role, projector)(reading, record)


def snapshot(db):
    from backend.shared.persistence.projections import ProjectionRepository

    return ProjectionRepository(db.factory).live("M-001")


def test_invalid_preserves_prediction_and_axes_have_independent_cursors(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("sensor-reading", 10))
    deliver(db, projector, event("completed", 9))
    deliver(db, projector, event("invalid", 11))
    deliver(db, projector, event("completed", 8), 2)
    value = snapshot(db)
    assert value["telemetry"]["sequence_number"] == 10
    assert value["telemetry"]["validation_status"] == "UNVERIFIED"
    assert value["latest_valid_prediction"]["sequence_number"] == 9
    assert value["latest_processing"]["processing_status"] == "INVALID"
    assert value["latest_processing"]["sequence_number"] == 11


def test_terminal_before_telemetry_cannot_regress_to_pending(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed", 12))
    deliver(db, projector, event("sensor-reading", 12))
    value = snapshot(db)
    assert value["latest_processing"]["processing_status"] == "COMPLETED"
    assert value["telemetry"]["validation_status"] == "VALID"


def test_epoch_is_lineage_and_sequence_does_not_overflow_int64(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("sensor-reading", 12, stream_epoch="run-z"))
    deliver(db, projector, event("sensor-reading", 2**80 + 1, stream_epoch="run-a"))
    value = snapshot(db)
    assert value["telemetry"]["sequence_number"] == 2**80 + 1
    assert value["latest_processing"]["stream_epoch"] == "run-a"


def test_duplicate_ack_metadata_keeps_first_receipt_without_extra_mutation(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("sensor-reading"))
    before = snapshot(db)
    deliver(
        db,
        projector,
        event(
            "sensor-reading",
            server_accepted_at="2026-10-18T00:01:00Z",
            correlation_id="retry",
        ),
        5,
    )
    assert snapshot(db)["telemetry"] == before["telemetry"]
    with db.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 1


def test_same_sequence_different_event_rolls_back_inbox_and_projection(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("sensor-reading"))
    with pytest.raises(IdentityConflict):
        deliver(db, projector, event("invalid", event_id="other"))
    assert snapshot(db)["latest_processing"]["processing_status"] == "PENDING"
    with db.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 1


def test_unknown_machine_is_not_provisioned_or_acknowledged(business_database):
    db = business_database
    projector = setup(db)
    with pytest.raises(IdentityConflict):
        deliver(db, projector, event("sensor-reading", machine_id="missing"))
    assert RegistryRepository(db.factory).get("missing") is None
    with db.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 0


def test_raw_features_with_nul_round_trip_losslessly(business_database):
    db = business_database
    projector = setup(db)
    raw = {"unexpected\x00key": "raw\x00value", "huge": 2**80 + 1}
    deliver(db, projector, event("sensor-reading", features=raw))
    assert snapshot(db)["telemetry"]["features_raw"] == raw


def test_concurrent_groups_serialize_terminal_and_telemetry(business_database):
    db = business_database
    projector = setup(db)
    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = [
            executor.submit(deliver, db, projector, event(name, 12))
            for name in ("sensor-reading", "unavailable")
        ]
        for task in tasks:
            task.result(timeout=10)
    value = snapshot(db)
    assert value["latest_processing"]["processing_status"] == "FAILED"
    assert value["latest_processing"]["failure_code"] == "ML_UNAVAILABLE"
    assert value["telemetry"]["validation_status"] == "VALID"
    assert value["latest_valid_prediction"] is None


def test_conflicting_terminal_is_durable_evidence_and_not_a_receipt(business_database):
    from backend.shared.inbox.handler import ProjectionHandler

    db = business_database
    setup(db)
    handler = ProjectionHandler(db.factory, "business-prediction-v1")
    first = event("completed", 12)
    record = SimpleNamespace(topic=first.topic, partition=0, offset=1, value=first.wire_bytes)
    handler(first, record)
    conflicting = event("unavailable", 12)
    record = SimpleNamespace(
        topic=conflicting.topic, partition=0, offset=2, value=conflicting.wire_bytes
    )
    with pytest.raises(IdentityConflict):
        handler(conflicting, record)
    assert snapshot(db)["latest_processing"]["processing_status"] == "COMPLETED"
    with db.factory() as conn:
        rows = conn.execute("SELECT * FROM business_projection_conflicts").fetchall()
        assert len(rows) == 1
        assert rows[0]["source_offset"] == 2
        assert conn.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 1


def test_live_snapshot_matches_frozen_schema_for_native_and_legacy(business_database):
    from jsonschema import Draft202012Validator, FormatChecker
    from referencing import Registry, Resource

    db = business_database
    projector = setup(db)
    directory = ROOT / "contracts/v1.3/schemas"
    resources = Registry().with_resources(
        [
            (p.as_uri(), Resource.from_contents(json.loads(p.read_text())))
            for p in directory.glob("*.schema.json")
        ]
    )
    validator = Draft202012Validator(
        {"$ref": (directory / "runtime.schema.json").as_uri() + "#/$defs/LiveSnapshot"},
        registry=resources,
        format_checker=FormatChecker(),
    )
    validator.validate(snapshot(db))
    for seq, name in enumerate(
        ("completed", "legacy-completed", "legacy-invalid", "legacy-unavailable"), 1
    ):
        deliver(db, projector, event(name, seq))
        deliver(db, projector, event("sensor-reading", seq))
        validator.validate(snapshot(db))
    assert snapshot(db)["latest_valid_prediction"]["sequence_number"] == 2


def test_read_role_cannot_write_and_consumer_cannot_delete(business_database):
    from uuid import uuid4

    import psycopg
    from backend.shared.persistence.permissions import grant_business_permissions
    from psycopg import sql

    db = business_database
    setup(db)
    reader, consumer = ["m3_role_" + uuid4().hex for _ in range(2)]
    with db.factory() as conn:
        for role in (reader, consumer):
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    try:
        with db.factory() as conn:
            grant_business_permissions(conn, reader_role=reader, consumer_role=consumer)
        with db.factory() as conn:
            conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(reader)))
            conn.execute("SELECT * FROM latest_telemetry")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute("DELETE FROM latest_telemetry")
        with db.factory() as conn:
            conn.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(consumer)))
            conn.execute("SELECT * FROM machines FOR UPDATE")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute("DELETE FROM machines")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute("UPDATE machines SET display_name='bad'")
    finally:
        with db.factory() as conn:
            for role in (reader, consumer):
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_native_risk_is_retained_without_fabricating_missing_legacy_risk(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed", 1))
    with db.factory() as conn:
        row = conn.execute("SELECT metadata FROM latest_valid_prediction").fetchone()
        assert json.loads(bytes(row["metadata"])) == {
            "result_id": "res_fixture_1-1",
            "risk_score": 0.5,
            "machine_state": "HIGH",
        }
    deliver(db, projector, event("legacy-completed", 2))
    with db.factory() as conn:
        row = conn.execute("SELECT metadata FROM latest_valid_prediction").fetchone()
        assert json.loads(bytes(row["metadata"])) == {
            "result_id": "res_fixture_1-2",
            "risk_score": None,
            "machine_state": "NORMAL",
        }


def test_late_terminal_conflict_cannot_replace_prediction_after_status_advances(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed", 9))
    deliver(db, projector, event("invalid", 12))
    deliver(db, projector, event("sensor-reading", 13))
    with pytest.raises(IdentityConflict):
        deliver(db, projector, event("completed", 12), 4)
    assert snapshot(db)["latest_valid_prediction"]["sequence_number"] == 9
    assert snapshot(db)["latest_processing"]["sequence_number"] == 13


def test_source_sequence_conflict_remains_detectable_after_all_latest_axes_advance(
    business_database,
):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("sensor-reading", 1))
    deliver(db, projector, event("sensor-reading", 2))
    deliver(db, projector, event("completed", 2))
    with pytest.raises(IdentityConflict):
        deliver(db, projector, event("invalid", 1, event_id="different-old-event"), 4)
    with pytest.raises(IdentityConflict):
        deliver(db, projector, event("sensor-reading", 3, event_id="e-1"), 5)


def test_consumer_metrics_count_committed_duplicates_and_projection_visibility(business_database):
    from business_consumer.main import make_handlers
    from business_consumer.observability import Metrics
    from maintai_messaging.config import KafkaSettings

    db = business_database
    setup(db)
    metrics = Metrics(wall_time=lambda: 1792281600.25)
    settings = KafkaSettings(local_mode=True, security_protocol="PLAINTEXT")
    handlers = make_handlers(db.factory, settings, metrics=metrics)
    telemetry = event("sensor-reading")
    record = SimpleNamespace(
        topic=telemetry.topic, partition=0, offset=1, value=telemetry.wire_bytes
    )
    handlers["business-live"](telemetry, record)
    handlers["business-live"](telemetry, record)
    registry = metrics.registry
    assert (
        registry.get_sample_value(
            "maintai_business_inbox_duplicates_total", {"topic": "sensor.readings"}
        )
        == 1
    )
    assert (
        registry.get_sample_value("maintai_event_to_telemetry_visible_duration_seconds_count") == 1
    )
    assert snapshot(db)["telemetry"]["event_id"] == "e-1"


def test_metric_failure_does_not_lose_committed_receipt(business_database):
    from backend.shared.inbox.handler import ProjectionHandler

    class BrokenMetrics:
        def committed(self, *args, **kwargs):
            raise RuntimeError("collector failed")

    db = business_database
    setup(db)
    handler = ProjectionHandler(db.factory, "business-live-v1", metrics=BrokenMetrics())
    telemetry = event("sensor-reading")
    record = SimpleNamespace(
        topic=telemetry.topic, partition=0, offset=1, value=telemetry.wire_bytes
    )
    receipt = handler(telemetry, record)
    assert receipt.offset == 1
    assert snapshot(db)["telemetry"]["event_id"] == "e-1"


def test_late_telemetry_remembers_its_validation_after_processing_cursor_advances(
    business_database,
):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed", 12))
    deliver(db, projector, event("invalid", 13))
    deliver(db, projector, event("sensor-reading", 12))
    value = snapshot(db)
    assert value["telemetry"]["validation_status"] == "VALID"
    assert value["latest_processing"]["sequence_number"] == 13
    assert value["latest_processing"]["processing_status"] == "INVALID"
    assert value["latest_valid_prediction"]["sequence_number"] == 12
