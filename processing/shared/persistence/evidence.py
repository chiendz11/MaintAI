"""Verify hot owner evidence using the release-pinned schema shipped in the SDK."""

import hashlib
from dataclasses import fields
from datetime import datetime
from functools import lru_cache

from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import default_contracts, strict_json
from maintai_messaging.errors import ContractViolation
from maintai_messaging.outcomes import ModelIdentity


@lru_cache(maxsize=1)
def history_validator():
    contracts = default_contracts()
    return Draft202012Validator(
        {
            "$ref": (contracts.root / "v1.3/schemas/runtime.schema.json").as_uri()
            + "#/$defs/HistoryRecord"
        },
        registry=contracts.registry,
        format_checker=FormatChecker(),
    )


def history_record(row):
    raw = bytes(row["outcome"])
    if hashlib.sha256(raw).hexdigest() != row["outcome_sha256"]:
        raise ValueError("Persisted outcome checksum mismatch")
    try:
        event = default_contracts().decode("inference.results", raw)
    except ContractViolation as error:
        raise ValueError("Persisted outcome schema is invalid") from error
    outcome = event.payload
    for output, column in (
        ("event_id", "event_id"),
        ("result_id", "result_id"),
        ("machine_id", "machine_id"),
        ("stream_epoch", "stream_epoch"),
        ("sequence_number", "sequence_number"),
        ("source_schema_version", "source_schema_version"),
        ("source_topic", "kafka_topic"),
        ("source_partition", "kafka_partition"),
        ("source_offset", "kafka_offset"),
        ("kind", "kind"),
        ("validation_status", "validation_status"),
        ("processing_status", "processing_status"),
    ):
        if outcome[output] != row[column]:
            raise ValueError("Owner source/outcome identity mismatch")
    if (
        datetime.fromisoformat(outcome["source_server_accepted_at"])
        != row["source_server_accepted_at"]
    ):
        raise ValueError("Owner source timestamp mismatch")
    risk = None
    if event.kind == "PREDICTION_COMPLETED":
        for key in (
            "prediction_id",
            "risk_decision_id",
            "risk_score",
            "policy_version",
            "policy_sha256",
            "machine_state",
            "failure_probability",
            "model_threshold",
        ):
            if outcome[key] != row[key]:
                raise ValueError("Owner prediction/risk evidence mismatch")
        if outcome["prediction"] != row["label"]:
            raise ValueError("Owner prediction label mismatch")
        identity = {field.name: outcome[field.name] for field in fields(ModelIdentity)}
        if row["model_identity"] != identity:
            raise ValueError("Owner model lineage mismatch")
        risk = {
            key: outcome[key]
            for key in ("risk_decision_id", "machine_state", "policy_version", "risk_score")
        }
    elif row["prediction_id"] is not None or row["risk_decision_id"] is not None:
        raise ValueError("Noncompleted outcome has prediction/risk rows")
    features = row["raw_features_json"]
    if features is None:
        try:
            features = strict_json(bytes(row["raw_features_wire"]))
        except ContractViolation as error:
            raise ValueError("Persisted raw features are not valid JSON") from error
    value = {
        "schema_version": "1.0.0",
        "result_id": row["result_id"],
        "source_event_id": row["event_id"],
        "machine_id": row["machine_id"],
        "source_server_accepted_at": outcome["source_server_accepted_at"],
        "outcome": outcome,
        "features_raw": features,
        "raw_ref": None,
        "risk_snapshot": risk,
    }
    if not history_validator().is_valid(value):
        raise ValueError("Owner evidence does not satisfy HistoryRecord")
    return value
