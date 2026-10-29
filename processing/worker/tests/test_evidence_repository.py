"""Hot point-read returns the exact committed evidence, never latest-by-machine."""

import pytest
from maintai_messaging.contracts import default_contracts
from processing.shared.persistence.status import StatusRepository
from processing.worker.tests.test_worker_transaction import Providers, handler, source

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("kind", ["completed", "invalid", "unavailable"])
def test_point_read_preserves_each_outcome_and_raw_evidence(migrated_database, kind):
    features = {"bad\x00key": "raw\x00value"} if kind == "invalid" else None
    event, record = source(features=features)
    provider = Providers(failure="ML_UNAVAILABLE" if kind == "unavailable" else None)
    handler(migrated_database, provider)(event, record)
    reader = StatusRepository(migrated_database.factory)
    result_id = reader.get(event.message_id)["result_id"]
    value = reader.prediction(result_id)
    assert value["source_event_id"] == event.message_id
    assert value["features_raw"] == event.payload["features"]
    assert value["outcome"]["result_id"] == result_id
    assert value["raw_ref"] is None
    assert (value["risk_snapshot"] is not None) == (kind == "completed")
    assert reader.prediction("missing") is None
    contracts = default_contracts()
    from jsonschema import Draft202012Validator, FormatChecker

    Draft202012Validator(
        {
            "$ref": (contracts.root / "v1.3/schemas/runtime.schema.json").as_uri()
            + "#/$defs/HistoryRecord"
        },
        registry=contracts.registry,
        format_checker=FormatChecker(),
    ).validate(value)


@pytest.mark.parametrize("corruption", ["checksum", "risk", "identity", "missing_model"])
def test_point_read_rejects_corrupted_owner_evidence(migrated_database, corruption):
    event, record = source()
    handler(migrated_database, Providers())(event, record)
    reader = StatusRepository(migrated_database.factory)
    result_id = reader.get(event.message_id)["result_id"]
    with migrated_database.factory() as connection:
        if corruption == "checksum":
            connection.execute("UPDATE processing_results SET outcome_sha256=%s", ("0" * 64,))
        elif corruption == "risk":
            connection.execute("UPDATE risk_decisions SET risk_score=0.11")
        elif corruption == "identity":
            connection.execute("UPDATE sensor_events SET machine_id='different'")
        else:
            connection.execute("UPDATE predictions SET model_identity='{}'::jsonb")
    with pytest.raises(ValueError):
        reader.prediction(result_id)


def test_point_read_rejects_invalid_persisted_raw_json(migrated_database):
    event, record = source(features={"bad\x00key": "raw\x00value"})
    handler(migrated_database, Providers())(event, record)
    reader = StatusRepository(migrated_database.factory)
    result_id = reader.get(event.message_id)["result_id"]
    with migrated_database.factory() as connection:
        connection.execute("UPDATE sensor_events SET raw_features_wire=%s", (b"not-json",))
    with pytest.raises(ValueError):
        reader.prediction(result_id)


def test_point_read_uses_accepted_time_instead_of_sensor_clock(migrated_database):
    from dataclasses import replace

    event, record = source()
    payload = {**event.payload, "received_at": "2026-10-17T00:00:00Z"}
    event = default_contracts().encode("sensor.readings", payload)
    handler(migrated_database, Providers())(event, replace(record, value=event.wire_bytes))
    reader = StatusRepository(migrated_database.factory)
    result = reader.prediction(reader.get(event.message_id)["result_id"])
    assert result["source_server_accepted_at"] == event.payload["server_accepted_at"]
