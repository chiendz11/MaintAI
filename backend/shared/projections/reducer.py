"""Pure latest-axis transitions; SQL, identity ledgers and side effects stay outside."""

import json
from dataclasses import dataclass

from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import IdentityConflict

TABLES = ("latest_telemetry", "latest_processing", "latest_valid_prediction")


@dataclass(frozen=True)
class Reduction:
    rows: dict
    changed: frozenset[str]
    telemetry_visible: bool


def reduce_projection(previous, event, *, source_validation):
    rows = {table: previous.get(table) for table in TABLES}
    changed = set()
    payload = event.payload
    sequence = payload["sequence_number"]
    for row in rows.values():
        if (
            row
            and row["sequence_number"] == sequence
            and (
                row["source_event_id"] != event.source_event_id
                or row["stream_epoch"] != payload["stream_epoch"]
            )
        ):
            raise IdentityConflict("Source sequence identifies different events")
    source = {
        "source_event_id": event.source_event_id,
        "stream_epoch": payload["stream_epoch"],
        "sequence_number": sequence,
    }

    def put(table, value, *, pending=False, metadata=None):
        old = rows[table]
        if old and old["sequence_number"] > sequence:
            return False
        if old and old["sequence_number"] == sequence:
            value_before = json.loads(bytes(old["snapshot"]))
            if pending:
                return False
            if table != "latest_processing" or value_before["processing_status"] != "PENDING":
                if old["identity_sha256"] != event.identity_sha256:
                    raise IdentityConflict("Projection has contradictory immutable content")
                return False
        rows[table] = {
            "machine_id": event.key,
            **source,
            "identity_sha256": event.identity_sha256,
            "snapshot": canonical_json(value),
            "metadata": canonical_json(metadata) if metadata is not None else None,
        }
        changed.add(table)
        return True

    visible = False
    if event.topic == "sensor.readings":
        validation = source_validation or "UNVERIFIED"
        terminal = rows["latest_processing"]
        if terminal and terminal["sequence_number"] == sequence:
            validation = json.loads(bytes(terminal["snapshot"]))["validation_status"] or validation
        visible = put(
            "latest_telemetry",
            {
                "event_id": event.source_event_id,
                "stream_epoch": payload["stream_epoch"],
                "sequence_number": sequence,
                "server_accepted_at": payload["server_accepted_at"],
                "received_at": payload["received_at"],
                "features_raw": payload["features"],
                "validation_status": validation,
            },
        )
        put(
            "latest_processing",
            {
                **source,
                "processing_status": "PENDING",
                "validation_status": None,
                "updated_at": payload["server_accepted_at"],
                "failure_code": None,
            },
            pending=True,
        )
    elif event.topic == "inference.results":
        metadata = {
            "result_id": event.message_id,
            "risk_score": payload["risk_score"] if event.kind == "PREDICTION_COMPLETED" else None,
            "machine_state": payload.get("machine_state"),
        }
        put(
            "latest_processing",
            {
                **source,
                "processing_status": payload["processing_status"],
                "validation_status": payload["validation_status"],
                "updated_at": payload["occurred_at"],
                "failure_code": payload.get("failure", {}).get("code"),
            },
            metadata=metadata,
        )
        telemetry = rows["latest_telemetry"]
        if telemetry and telemetry["sequence_number"] == sequence:
            value = json.loads(bytes(telemetry["snapshot"]))
            if value["validation_status"] != payload["validation_status"]:
                value["validation_status"] = payload["validation_status"]
                rows["latest_telemetry"] = {**telemetry, "snapshot": canonical_json(value)}
                changed.add("latest_telemetry")
        if event.kind == "PREDICTION_COMPLETED":
            put(
                "latest_valid_prediction",
                {
                    **source,
                    **{
                        key: payload[key]
                        for key in (
                            "source_server_accepted_at",
                            "prediction_id",
                            "prediction",
                            "failure_probability",
                            "model_version",
                            "preprocessor_version",
                            "dataset_version",
                            "policy_version",
                            "completed_at",
                        )
                    },
                    "machine_state_at_prediction": payload["machine_state"],
                },
                metadata=metadata,
            )
    else:
        raise ValueError("Unsupported Business projection topic")
    return Reduction(rows, frozenset(changed), visible)
