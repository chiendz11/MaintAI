"""Event uniqueness and terminal persistence; never commit inside a callback."""

import hashlib
from dataclasses import fields

from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from maintai_messaging.outcomes import ModelIdentity
from psycopg.types.json import Jsonb


class ProcessingRepository:
    def __init__(self, contracts=None):
        self.contracts = contracts or default_contracts()

    def claim(self, connection, event, record, *, ruleset_version, ruleset_sha256):
        """A unique insert locks the event until the enclosing transaction ends.

        PROCESSING is an uncommitted candidate, not an ACK or standalone durable
        job. Any dependency failure rolls it back with the SDK inbox entry.
        """
        p = event.payload
        # PostgreSQL JSONB cannot represent U+0000, including in INVALID feature
        # strings/keys. Keep exact canonical bytes rather than sanitizing raw data.
        stack = [p["features"]]
        raw_json, raw_wire = Jsonb(p["features"]), None
        while stack:
            item = stack.pop()
            if isinstance(item, str) and "\x00" in item:
                raw_json, raw_wire = None, canonical_json(p["features"])
                break
            if isinstance(item, dict):
                stack.extend(item.keys())
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
        inserted = connection.execute(
            """INSERT INTO sensor_events
            (event_id,machine_id,stream_epoch,sequence_number,event_time,source_server_accepted_at,
             source_schema_version,source,raw_features_json,raw_features_wire,payload_hash,processing_status,
             validator_version,dq_ruleset_sha256,correlation_id,kafka_topic,kafka_partition,kafka_offset)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PROCESSING',%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
            (
                p["event_id"],
                p["machine_id"],
                p["stream_epoch"],
                p["sequence_number"],
                p["received_at"],
                p["server_accepted_at"],
                event.schema_version,
                p["source"],
                raw_json,
                raw_wire,
                event.identity_sha256,
                ruleset_version,
                ruleset_sha256,
                p["correlation_id"],
                record.topic,
                record.partition,
                record.offset,
            ),
        ).fetchone()
        if inserted:
            return True
        existing = connection.execute(
            "SELECT payload_hash,processing_status FROM sensor_events WHERE event_id=%s",
            (event.message_id,),
        ).fetchone()
        if existing["payload_hash"] != event.identity_sha256:
            raise IdentityConflict("Event identity has different canonical content")
        if existing["processing_status"] == "PROCESSING":
            raise RuntimeError("Existing event has no durable terminal outcome")
        if (
            connection.execute(
                "SELECT result_id FROM processing_results WHERE event_id=%s", (event.message_id,)
            ).fetchone()
            is None
        ):
            raise RuntimeError("Existing terminal event has no canonical outcome")
        return False

    def persist(self, connection, event, outcome, *, risk=None, inference_ms=None):
        encoded = self.contracts.encode("inference.results", outcome)
        if any(
            outcome[name] != event.payload[name]
            for name in ("event_id", "machine_id", "stream_epoch", "sequence_number")
        ):
            raise IdentityConflict("Outcome does not belong to the claimed source event")
        updated = connection.execute(
            """UPDATE sensor_events SET
            processing_status=%s,validation_status=%s,validation_errors_json=%s,
            updated_at=clock_timestamp() WHERE event_id=%s AND processing_status='PROCESSING'
            RETURNING event_id""",
            (
                outcome["processing_status"],
                outcome["validation_status"],
                Jsonb(outcome.get("validation_errors", [])),
                event.message_id,
            ),
        ).fetchone()
        if updated is None:
            raise IdentityConflict("Terminal event cannot be rewritten")
        if encoded.kind == "PREDICTION_COMPLETED":
            if risk is None:
                raise ValueError("Completed outcome requires its product risk evaluation")
            identity = {field.name: outcome[field.name] for field in fields(ModelIdentity)}
            connection.execute(
                """INSERT INTO predictions
                (prediction_id,event_id,label,failure_probability,model_threshold,model_version,
                 preprocessor_version,model_identity,inference_ms,predicted_at,correlation_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    outcome["prediction_id"],
                    event.message_id,
                    outcome["prediction"],
                    outcome["failure_probability"],
                    outcome["model_threshold"],
                    outcome["model_version"],
                    outcome["preprocessor_version"],
                    Jsonb(identity),
                    inference_ms,
                    outcome["completed_at"],
                    outcome["correlation_id"],
                ),
            )
            connection.execute(
                """INSERT INTO risk_decisions
                (risk_decision_id,event_id,prediction_id,machine_state,severity_nullable,should_alert,
                 risk_score,policy_version,policy_sha256,decided_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    outcome["risk_decision_id"],
                    event.message_id,
                    outcome["prediction_id"],
                    outcome["machine_state"],
                    risk.severity,
                    risk.should_create_alert,
                    outcome["risk_score"],
                    outcome["policy_version"],
                    outcome["policy_sha256"],
                    outcome["occurred_at"],
                ),
            )
        connection.execute(
            """INSERT INTO processing_results
            (result_id,event_id,kind,outcome,outcome_sha256,completed_at)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (
                encoded.message_id,
                event.message_id,
                encoded.kind,
                encoded.wire_bytes,
                hashlib.sha256(encoded.wire_bytes).hexdigest(),
                outcome["occurred_at"],
            ),
        )

    def record_conflict(self, connection, event, record, *, consumer_group):
        existing = connection.execute(
            "SELECT payload_hash AS digest FROM sensor_events WHERE event_id=%s",
            (event.message_id,),
        ).fetchone()
        if existing is None:
            existing = connection.execute(
                """SELECT identity_sha256 AS digest FROM maintai_kafka_inbox
                WHERE consumer_group=%s AND topic=%s AND message_id=%s""",
                (consumer_group, event.topic, event.message_id),
            ).fetchone()
        connection.execute(
            """INSERT INTO processing_conflicts
            (event_id,source_topic,source_partition,source_offset,existing_sha256,incoming_sha256)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                event.message_id,
                record.topic,
                record.partition,
                record.offset,
                existing["digest"] if existing else None,
                event.identity_sha256,
            ),
        )
