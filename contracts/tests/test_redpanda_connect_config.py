"""Contract & structural validation for the Redpanda Connect MQTT->Kafka pipeline."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
CONNECT_CONFIG = ROOT / "ingestion-connect/connect.yaml"


def test_redpanda_connect_pipeline_matches_kafka_contracts():
    assert CONNECT_CONFIG.is_file(), f"Missing pipeline config: {CONNECT_CONFIG}"

    config = yaml.safe_load(CONNECT_CONFIG.read_text(encoding="utf-8"))
    mqtt_cfg = config["input"]["mqtt"]
    assert mqtt_cfg["qos"] == 1
    assert any(topic.startswith("$share/") for topic in mqtt_cfg["topics"])

    bloblang = config["pipeline"]["processors"][0]["bloblang"]
    for header in ("schema_version", "message_id", "correlation_id", "producer_git_sha"):
        assert f"meta {header} =" in bloblang, f"Missing Kafka header mapping: {header}"
    features_schema = json.loads(
        (ROOT / "contracts/v1.3/schemas/features.schema.json").read_text(encoding="utf-8")
    )
    for feature in features_schema["required"]:
        assert feature in bloblang, f"Missing allowlisted feature check: {feature}"

    cases = config["output"]["switch"]["cases"]
    assert len(cases) == 2
    valid_out = cases[0]["output"]["kafka_franz"]
    rejected_out = cases[1]["output"]["kafka_franz"]
    assert "sensor.readings" in valid_out["topic"]
    assert valid_out["compression"] == "lz4"
    assert valid_out["idempotent_write"] is True
    assert any("producer_git_sha" in p for p in valid_out["metadata"]["include_patterns"])
    assert "sensor.rejected" in rejected_out["topic"]
    assert rejected_out["compression"] == "lz4"
    assert rejected_out["idempotent_write"] is True
    assert any("producer_git_sha" in p for p in rejected_out["metadata"]["include_patterns"])

    rejected_schema = json.loads(
        (ROOT / "contracts/kafka/events/sensor-rejected.schema.json").read_text(encoding="utf-8")
    )
    raw = b"{not-valid-json"
    sample_rejected = {
        "schema_version": "1.4.0",
        "rejection_id": "rej_test_1",
        "source_identity": "maintai/v1/site/local/machines/M-001/telemetry",
        "request_id": "req_test_1",
        "correlation_id": "corr_test_1",
        "rejected_at": datetime(2026, 11, 27, 2, 0, 0, tzinfo=UTC).isoformat(),
        "reason": "MALFORMED_JSON",
        "payload_disposition": "HASH_ONLY",
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "payload_size_bytes": len(raw),
        "validation_errors": [{"field": "$", "code": "MALFORMED_JSON"}],
    }
    Draft202012Validator(rejected_schema).validate(sample_rejected)
