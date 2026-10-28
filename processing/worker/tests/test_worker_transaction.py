"""Real owner DB with explicit registry/ML fixtures; not live provider acceptance."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields
from pathlib import Path

import pytest
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from maintai_messaging.outcomes import ModelIdentity
from processing.shared.data_quality.machine_registry import MachineLookupResult
from processing.shared.data_quality.validator import FeatureValidator
from processing.shared.inference_client.client import PredictionAttempt, PredictionResponse
from processing.shared.risk_engine.policy import RiskPolicy
from processing.worker.src.consumer.handler import ProcessingHandler

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def source(*, offset=1, event_id=None, features=None):
    payload = json.loads((ROOT / "contracts/kafka/examples/sensor-reading.json").read_text())
    if event_id:
        payload["event_id"] = event_id
    if features is not None:
        payload["features"] = features
    event = default_contracts().decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


class Providers:
    def __init__(self, *, registry_status="FOUND", probability=0.8, failure=None):
        self.registry_status, self.probability, self.failure = registry_status, probability, failure
        self.calls = 0
        example = json.loads((ROOT / "contracts/kafka/examples/completed.json").read_text())
        self.identity = ModelIdentity(
            **{field.name: example[field.name] for field in fields(ModelIdentity)}
        )

    def lookup(self, machine_id, *, correlation_id):
        return MachineLookupResult(
            self.registry_status,
            "REGISTRY_TIMEOUT" if self.registry_status == "UNAVAILABLE" else None,
        )

    def predict(self, *, event_id, machine_id, features, correlation_id):
        self.calls += 1
        if self.failure:
            return PredictionAttempt(None, self.failure, 2)
        response = PredictionResponse.model_validate_json(
            json.dumps(
                {
                    "prediction_id": "fixture-pred-" + event_id,
                    "event_id": event_id,
                    "prediction": "FAILURE" if self.probability >= 0.5 else "NORMAL",
                    "failure_probability": self.probability,
                    "model_threshold": 0.5,
                    "model_version": self.identity.model_version,
                    "preprocessor_version": self.identity.preprocessor_version,
                    "inference_ms": 1,
                    "predicted_at": "2026-10-24T00:00:00Z",
                }
            )
        )
        return PredictionAttempt(response, None, 1)


def handler(database, providers, *, group="fixture-group", alerts=None):
    return ProcessingHandler(
        database.factory,
        consumer_group=group,
        validator=FeatureValidator.from_file(
            ROOT / "contracts/v1.3/schemas/features.schema.json", version="sensor-validator-1.0.0"
        ),
        registry_lookup=providers.lookup,
        predict=providers.predict,
        identity_resolver=lambda response: providers.identity,
        policy=RiskPolicy.from_dict(
            {
                "policy_version": "fixture-policy",
                "thresholds": {
                    "watch": 0.4,
                    "high": 0.65,
                    "critical": 0.85,
                },
                "create_alert_for": ["HIGH", "CRITICAL"] if alerts is None else alerts,
            }
        ),
        producer_git_sha="1" * 40,
    )


def rows(database, table):
    assert table in {
        "sensor_events",
        "predictions",
        "risk_decisions",
        "processing_results",
        "maintai_kafka_inbox",
        "maintai_kafka_outbox",
        "processing_conflicts",
    }
    with database.factory() as connection:
        return connection.execute(f"SELECT * FROM {table}").fetchall()


def test_valid_event_commits_prediction_risk_outbox_and_inbox(migrated_database):
    providers = Providers()
    event, record = source()
    receipt = handler(migrated_database, providers)(event, record)
    assert receipt is not None
    assert providers.calls == 1
    assert len(rows(migrated_database, "predictions")) == 1
    assert len(rows(migrated_database, "risk_decisions")) == 1
    assert len(rows(migrated_database, "maintai_kafka_inbox")) == 1
    assert {row["topic"] for row in rows(migrated_database, "maintai_kafka_outbox")} == {
        "inference.results",
        "alert.signals",
    }


def test_feature_invalid_does_not_need_registry_or_inference(migrated_database):
    providers = Providers(registry_status="UNAVAILABLE")
    handler(migrated_database, providers)(*source(features={"product_type": "X"}))
    assert providers.calls == 0
    result = rows(migrated_database, "processing_results")[0]
    assert result["kind"] == "DATA_INVALID"
    assert rows(migrated_database, "predictions") == []


def test_unprovisioned_machine_is_invalid_without_inference(migrated_database):
    providers = Providers(registry_status="NOT_FOUND")
    handler(migrated_database, providers)(*source())
    assert providers.calls == 0
    assert rows(migrated_database, "processing_results")[0]["kind"] == "DATA_INVALID"


def test_registry_outage_rolls_back_without_false_invalid_or_ack(migrated_database):
    providers = Providers(registry_status="UNAVAILABLE")
    with pytest.raises(RuntimeError):
        handler(migrated_database, providers)(*source())
    assert providers.calls == 0
    assert rows(migrated_database, "sensor_events") == []
    assert rows(migrated_database, "maintai_kafka_inbox") == []


def test_ml_exhaustion_persists_failure_without_prediction(migrated_database):
    providers = Providers(failure="ML_TIMEOUT")
    handler(migrated_database, providers)(*source())
    result = rows(migrated_database, "processing_results")[0]
    payload = json.loads(bytes(result["outcome"]))
    assert payload["kind"] == "INFERENCE_UNAVAILABLE"
    assert payload["failure"] == {"code": "ML_TIMEOUT", "attempts": 2}
    assert "prediction" not in payload and "machine_state" not in payload
    assert rows(migrated_database, "predictions") == []


def test_duplicate_delivery_and_new_consumer_group_do_not_repeat_inference(migrated_database):
    providers = Providers()
    event, record = source()
    first = handler(migrated_database, providers)
    first(event, record)
    original = bytes(rows(migrated_database, "processing_results")[0]["outcome"])
    first(*source(offset=2))
    handler(migrated_database, providers, group="new-fixture-group")(*source(offset=3))
    assert providers.calls == 1
    assert len(rows(migrated_database, "predictions")) == 1
    assert bytes(rows(migrated_database, "processing_results")[0]["outcome"]) == original


def test_concurrent_groups_share_one_business_effect(migrated_database):
    providers = Providers()
    with ThreadPoolExecutor(2) as pool:
        futures = [
            pool.submit(handler(migrated_database, providers, group=f"group-{i}"), *source())
            for i in range(2)
        ]
        for future in futures:
            future.result(timeout=10)
    assert providers.calls == 1
    assert len(rows(migrated_database, "predictions")) == 1


def test_conflict_is_durable_even_when_sdk_rejects_before_callback(migrated_database):
    providers = Providers()
    consumer = handler(migrated_database, providers)
    event, record = source()
    consumer(event, record)
    changed = {**event.payload["features"], "torque_nm": 123}
    with pytest.raises(IdentityConflict):
        consumer(*source(offset=2, features=changed))
    assert providers.calls == 1
    assert len(rows(migrated_database, "processing_conflicts")) == 1
    assert len(rows(migrated_database, "predictions")) == 1


def test_watch_does_not_emit_signal_when_policy_disallows_it(migrated_database):
    handler(migrated_database, Providers(probability=0.5))(*source())
    assert [row["topic"] for row in rows(migrated_database, "maintai_kafka_outbox")] == [
        "inference.results"
    ]


def test_dq_wire_issues_have_no_internal_message_field(migrated_database):
    handler(migrated_database, Providers())(*source(features={}))
    payload = json.loads(bytes(rows(migrated_database, "processing_results")[0]["outcome"]))
    assert all(set(issue) == {"field", "code"} for issue in payload["validation_errors"])


@pytest.mark.parametrize("in_key", [False, True])
def test_invalid_features_unrepresentable_in_jsonb_still_have_durable_outcome(
    migrated_database, in_key
):
    original, _ = source()
    features = dict(original.payload["features"])
    if in_key:
        features["unexpected\x00key"] = "fixture"
    else:
        features["torque_nm"] = "\x00"
    providers = Providers()
    handler(migrated_database, providers)(*source(features=features))
    assert providers.calls == 0
    stored = rows(migrated_database, "sensor_events")[0]
    assert stored["raw_features_json"] is None
    assert json.loads(bytes(stored["raw_features_wire"])) == features
    assert rows(migrated_database, "processing_results")[0]["kind"] == "DATA_INVALID"
    assert len(rows(migrated_database, "maintai_kafka_inbox")) == 1
    assert len(rows(migrated_database, "maintai_kafka_outbox")) == 1


def test_sequence_integer_is_not_silently_limited_to_bigint(migrated_database):
    event, record = source()
    payload = {**event.payload, "sequence_number": 2**63}
    event = default_contracts().decode("sensor.readings", canonical_json(payload))
    record = Record(event.topic, 0, record.offset, event.key.encode(), event.wire_bytes)
    handler(migrated_database, Providers())(event, record)
    assert rows(migrated_database, "sensor_events")[0]["sequence_number"] == 2**63
    outcome = json.loads(bytes(rows(migrated_database, "processing_results")[0]["outcome"]))
    assert outcome["sequence_number"] == 2**63


def test_validation_observation_cannot_rollback_a_durable_outcome(migrated_database):
    consumer = handler(migrated_database, Providers())
    observations = []

    def broken_metrics(status, reasons):
        observations.append((status, reasons))
        raise RuntimeError("fixture metric failure")

    consumer.validation_observer = broken_metrics
    consumer(*source(features={}))
    assert observations and observations[0][0] == "INVALID"
    assert len(rows(migrated_database, "processing_results")) == 1
    assert len(rows(migrated_database, "maintai_kafka_inbox")) == 1
