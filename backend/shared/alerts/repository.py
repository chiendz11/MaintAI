"""Business owner transactions share machine-first locks with projections and signals."""

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import IdentityConflict

from backend.shared import audit, feedback

from .domain import ACTIVE, AlertError, transition, validate_evidence
from .notifications import ALERT_CHANNEL, emit


def now(connection):
    return connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]


def public(row):
    keys = (
        "alert_id",
        "machine_id",
        "severity",
        "status",
        "risk_score",
        "model_version",
        "policy_version",
        "created_at",
    )
    return {key: row[key].isoformat() if key == "created_at" else row[key] for key in keys}


class AlertRepository:
    def __init__(self, connection_factory, *, clock=now, channel=ALERT_CHANNEL, metrics=None):
        self.connection_factory, self.clock, self.channel = connection_factory, clock, channel
        self.metrics = metrics

    def check(self):
        with self.connection_factory() as connection:
            connection.execute("""SELECT a.alert_id FROM alerts a
                LEFT JOIN alert_evidence_snapshots e USING(alert_id)
                LEFT JOIN alert_events h USING(alert_id)
                LEFT JOIN feedback f USING(alert_id)
                LEFT JOIN alert_notifications n USING(alert_id) LIMIT 0""")
        return True

    @staticmethod
    def active(connection, machines):
        values = {machine: [] for machine in machines}
        rows = connection.execute(
            "SELECT alert_id,machine_id FROM alerts WHERE machine_id=ANY(%s) "
            "AND status=ANY(%s) ORDER BY created_at,alert_id",
            (list(machines), list(ACTIVE)),
        ).fetchall()
        for row in rows:
            values[row["machine_id"]].append(row["alert_id"])
        return values

    def create_or_dedupe(self, connection, event, evidence):
        signal = event.payload
        validate_evidence(signal, evidence)
        machine = connection.execute(
            "SELECT machine_id FROM machines WHERE machine_id=%s FOR UPDATE",
            (signal["machine_id"],),
        ).fetchone()
        if machine is None:
            raise IdentityConflict("Signal machine must be provisioned")
        decision = self.clock(connection)
        existing = connection.execute(
            "SELECT * FROM alert_signal_links WHERE signal_id=%s", (event.message_id,)
        ).fetchone()
        if existing is not None:
            if existing["identity_sha256"] != event.identity_sha256:
                raise IdentityConflict("Signal identity already has different evidence")
            return None
        row = connection.execute(
            """SELECT * FROM alerts WHERE machine_id=%s AND severity=%s AND status=ANY(%s)
            AND created_at<=%s AND created_at>%s ORDER BY created_at DESC,alert_id DESC
            LIMIT 1 FOR UPDATE""",
            (
                signal["machine_id"],
                signal["severity"],
                list(ACTIVE),
                decision,
                decision - timedelta(seconds=300),
            ),
        ).fetchone()
        created = row is None
        if created:
            identifier = "alt_" + uuid4().hex
            row = connection.execute(
                """INSERT INTO alerts(alert_id,machine_id,severity,status,risk_score,model_version,
                policy_version,source_event_id,prediction_id,created_at,updated_at,latest_signal_id,
                latest_metadata) VALUES(%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (
                    identifier,
                    signal["machine_id"],
                    signal["severity"],
                    signal["risk_score"],
                    signal["model_version"],
                    signal["policy_version"],
                    signal["event_id"],
                    signal["prediction_id"],
                    decision,
                    decision,
                    event.message_id,
                    canonical_json(signal),
                ),
            ).fetchone()
            raw = canonical_json(evidence)
            connection.execute(
                "INSERT INTO alert_evidence_snapshots(alert_id,evidence,evidence_sha256) "
                "VALUES(%s,%s,%s)",
                (identifier, raw, hashlib.sha256(raw).hexdigest()),
            )
            audit.record(connection, identifier, None, "OPEN", None, {}, decision)
        else:
            connection.execute(
                "UPDATE alerts SET updated_at=%s,latest_signal_id=%s,latest_metadata=%s "
                "WHERE alert_id=%s",
                (decision, event.message_id, canonical_json(signal), row["alert_id"]),
            )
        disposition = "CREATED" if created else "DEDUPLICATED"
        connection.execute(
            """INSERT INTO alert_signal_links(signal_id,alert_id,identity_sha256,result_id,
            source_event_id,disposition,recorded_at) VALUES(%s,%s,%s,%s,%s,%s,%s)""",
            (
                event.message_id,
                row["alert_id"],
                event.identity_sha256,
                evidence["result_id"],
                signal["event_id"],
                disposition,
                decision,
            ),
        )
        emit(connection, row, decision, created=created, channel=self.channel)
        return disposition, row["severity"], evidence["source_server_accepted_at"]

    def list(self, machines, *, status=None, severity=None, machine_id=None, limit=50, offset=0):
        if not 1 <= limit <= 200 or offset < 0:
            raise AlertError(422, "VALIDATION_FAILED")
        scope = list(machines if machine_id is None else ({machine_id} & set(machines)))
        with self.connection_factory() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = connection.execute(
                """WITH filtered AS MATERIALIZED (SELECT * FROM alerts WHERE machine_id=ANY(%s)
                AND (%s::text IS NULL OR status=%s) AND (%s::text IS NULL OR severity=%s)),
                page AS (SELECT * FROM filtered ORDER BY created_at DESC,alert_id DESC
                    LIMIT %s OFFSET %s)
                SELECT total.total,page.* FROM (SELECT count(*) AS total FROM filtered) total
                LEFT JOIN page ON TRUE ORDER BY page.created_at DESC,page.alert_id DESC""",
                (scope, status, status, severity, severity, limit, min(offset, 2**63 - 1)),
            ).fetchall()
        keys = ("alert_id", "machine_id", "severity", "status", "risk_score", "created_at")
        return {
            "items": [{k: public(row)[k] for k in keys} for row in rows if row["alert_id"]],
            "total": rows[0]["total"],
        }

    @staticmethod
    def _authorized(connection, alert_id, machines):
        row = connection.execute("SELECT * FROM alerts WHERE alert_id=%s", (alert_id,)).fetchone()
        if row is None:
            raise AlertError(404, "ALERT_NOT_FOUND")
        if row["machine_id"] not in machines:
            raise AlertError(403, "FORBIDDEN")
        return row

    def detail(self, alert_id, machines):
        with self.connection_factory() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = self._authorized(connection, alert_id, machines)
            stored = connection.execute(
                "SELECT * FROM alert_evidence_snapshots WHERE alert_id=%s", (alert_id,)
            ).fetchone()
            if stored is None:
                raise ValueError("Alert evidence missing")
            raw = bytes(stored["evidence"])
            if hashlib.sha256(raw).hexdigest() != stored["evidence_sha256"]:
                raise ValueError("Alert evidence checksum mismatch")
            evidence = json.loads(raw)
            events = connection.execute(
                "SELECT * FROM alert_events WHERE alert_id=%s ORDER BY timestamp,alert_event_id",
                (alert_id,),
            ).fetchall()
        outcome = evidence["outcome"]
        history = [
            {
                "alert_event_id": e["alert_event_id"],
                "event_type": e["event_type"],
                "from_status": e["from_status"],
                "to_status": e["to_status"],
                "actor_id": None if e["actor_id"] is None else bytes(e["actor_id"]).decode(),
                "note": None if e["note"] is None else bytes(e["note"]).decode(),
                "timestamp": e["timestamp"].isoformat(),
            }
            for e in events
        ]
        return {
            "alert": public(row),
            "sensor_event": {
                "event_id": row["source_event_id"],
                "validation_status": "VALID",
                "features": evidence["features_raw"],
            },
            "prediction": {
                "prediction_id": row["prediction_id"],
                "prediction": outcome["prediction"],
                "failure_probability": outcome["failure_probability"],
            },
            "copilot": {"status": "UNAVAILABLE", "brief_id": None, "grounded_status": None},
            "history": history,
        }

    def _locked(self, connection, alert_id, machines):
        row = self._authorized(connection, alert_id, machines)
        connection.execute(
            "SELECT machine_id FROM machines WHERE machine_id=%s FOR UPDATE", (row["machine_id"],)
        ).fetchone()
        return connection.execute(
            "SELECT * FROM alerts WHERE alert_id=%s FOR UPDATE", (alert_id,)
        ).fetchone()

    def action(self, alert_id, action, machines, actor, body):
        with self.connection_factory() as connection, connection.transaction():
            row = self._locked(connection, alert_id, machines)
            target = transition(row["status"], action)
            timestamp = self.clock(connection)
            connection.execute(
                "UPDATE alerts SET status=%s,updated_at=%s WHERE alert_id=%s",
                (target, timestamp, alert_id),
            )
            audit.record(connection, alert_id, row["status"], target, actor, body, timestamp)
            emit(connection, {**row, "status": target}, timestamp, channel=self.channel)
            value = {
                "alert_id": alert_id,
                "previous_status": row["status"],
                "status": target,
                "updated_at": timestamp.isoformat(),
            }
        if self.metrics is not None:
            try:
                self.metrics.transition(row["status"], target)
            except Exception:
                pass
        return value

    def feedback(self, alert_id, machines, actor, payload):
        with self.connection_factory() as connection, connection.transaction():
            row = self._locked(connection, alert_id, machines)
            timestamp = self.clock(connection)
            value = feedback.record(connection, alert_id, actor, payload, timestamp)
            emit(connection, row, timestamp, channel=self.channel)
        return value

    def notification(self, identifier, machine):
        with self.connection_factory() as connection:
            row = connection.execute(
                "SELECT event FROM alert_notifications WHERE notification_id=%s AND machine_id=%s",
                (identifier, machine),
            ).fetchone()
        return None if row is None else json.loads(bytes(row["event"]))
