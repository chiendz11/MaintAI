"""Serialize each machine across groups; compare only durable source sequence."""

import json

from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import IdentityConflict
from psycopg import sql

from backend.shared.ordering.identity import guard_source

TABLES = ("latest_telemetry", "latest_processing", "latest_valid_prediction")


class Projector:
    def __init__(self, *, notify=None):
        self.notify = notify

    def _notify(self, connection, machine_id, changed):
        if changed and self.notify is not None:
            self.notify(connection, machine_id)

    def __call__(self, connection, event, record, outbox):
        payload = event.payload
        machine = connection.execute(
            "SELECT machine_id FROM machines WHERE machine_id=%s FOR UPDATE",
            (event.key,),
        ).fetchone()
        if machine is None:
            raise IdentityConflict("Machine must be provisioned before projection")
        source_validation = guard_source(connection, event)
        rows = {
            table: connection.execute(
                sql.SQL("SELECT * FROM {} WHERE machine_id=%s").format(sql.Identifier(table)),
                (event.key,),
            ).fetchone()
            for table in TABLES
        }
        for row in rows.values():
            if (
                row
                and row["sequence_number"] == payload["sequence_number"]
                and (
                    row["source_event_id"] != event.source_event_id
                    or row["stream_epoch"] != payload["stream_epoch"]
                )
            ):
                raise IdentityConflict("Source sequence identifies different events")
        source = {
            "source_event_id": event.source_event_id,
            "stream_epoch": payload["stream_epoch"],
            "sequence_number": payload["sequence_number"],
        }
        if event.topic == "sensor.readings":
            terminal = rows["latest_processing"]
            validation = source_validation or "UNVERIFIED"
            if terminal and terminal["sequence_number"] == payload["sequence_number"]:
                validation = (
                    json.loads(bytes(terminal["snapshot"]))["validation_status"] or validation
                )
            telemetry = {
                "event_id": event.source_event_id,
                "stream_epoch": payload["stream_epoch"],
                "sequence_number": payload["sequence_number"],
                "server_accepted_at": payload["server_accepted_at"],
                "received_at": payload["received_at"],
                "features_raw": payload["features"],
                "validation_status": validation,
            }
            visible = self._put(connection, event, rows, "latest_telemetry", telemetry)
            changed = self._put(
                connection,
                event,
                rows,
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
            self._notify(connection, event.key, visible or changed)
            return visible
        elif event.topic == "inference.results":
            metadata = {
                "result_id": event.message_id,
                "risk_score": payload["risk_score"]
                if event.kind == "PREDICTION_COMPLETED"
                else None,
                "machine_state": payload.get("machine_state"),
            }
            status = {
                **source,
                "processing_status": payload["processing_status"],
                "validation_status": payload["validation_status"],
                "updated_at": payload["occurred_at"],
                "failure_code": payload.get("failure", {}).get("code"),
            }
            changed = self._put(
                connection, event, rows, "latest_processing", status, metadata=metadata
            )
            telemetry = rows["latest_telemetry"]
            if telemetry and telemetry["sequence_number"] == payload["sequence_number"]:
                value = json.loads(bytes(telemetry["snapshot"]))
                changed = changed or value["validation_status"] != payload["validation_status"]
                value["validation_status"] = payload["validation_status"]
                connection.execute(
                    "UPDATE latest_telemetry SET snapshot=%s WHERE machine_id=%s",
                    (canonical_json(value), event.key),
                )
            if event.kind == "PREDICTION_COMPLETED":
                prediction = {
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
                }
                prediction_changed = self._put(
                    connection,
                    event,
                    rows,
                    "latest_valid_prediction",
                    prediction,
                    metadata=metadata,
                )
                changed = changed or prediction_changed
            self._notify(connection, event.key, changed)
        else:
            raise ValueError("Unsupported Business projection topic")
        return False

    @staticmethod
    def _put(connection, event, rows, table, value, *, pending=False, metadata=None):
        old = rows[table]
        seq = event.payload["sequence_number"]
        if old and old["sequence_number"] > seq:
            return False
        if old and old["sequence_number"] == seq:
            previous = json.loads(bytes(old["snapshot"]))
            if pending:
                return False
            if table != "latest_processing" or previous["processing_status"] != "PENDING":
                if old["identity_sha256"] != event.identity_sha256:
                    raise IdentityConflict("Projection has contradictory immutable content")
                return False
        connection.execute(
            sql.SQL("""INSERT INTO {}(machine_id,source_event_id,stream_epoch,
                sequence_number,identity_sha256,snapshot,metadata) VALUES(%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(machine_id) DO UPDATE SET
                source_event_id=EXCLUDED.source_event_id,stream_epoch=EXCLUDED.stream_epoch,
                sequence_number=EXCLUDED.sequence_number,identity_sha256=EXCLUDED.identity_sha256,
                snapshot=EXCLUDED.snapshot,metadata=EXCLUDED.metadata,
                updated_at=clock_timestamp()""").format(sql.Identifier(table)),
            (
                event.key,
                event.source_event_id,
                event.payload["stream_epoch"],
                seq,
                event.identity_sha256,
                canonical_json(value),
                canonical_json(metadata) if metadata is not None else None,
            ),
        )
        return True
