"""Short owner transactions; original evidence and canonical provider results are durable."""

import hashlib
import json
from urllib.parse import quote
from uuid import uuid4

from maintai_messaging.contracts import canonical_json

from backend.shared.alerts.domain import AlertError
from backend.shared.alerts.repository import AlertRepository
from backend.shared.notifications import machine_key

CHANNEL = "maintai_business_m7_v1"


def emit(connection, kind, machine, data):
    identifier = uuid4().hex
    timestamp = connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]
    value = {
        "schema_version": "1.0.0",
        "type": kind,
        "machine_id": machine,
        "occurred_at": timestamp.isoformat(),
        "data": {**data, "updated_at": timestamp.isoformat()},
    }
    connection.execute(
        "INSERT INTO business_m7_notifications(notification_id,machine_id,event) VALUES(%s,%s,%s)",
        (identifier, machine, canonical_json(value)),
    )
    connection.execute(
        "SELECT pg_notify(%s,%s)",
        (
            CHANNEL,
            json.dumps(
                {
                    "version": 1,
                    "machine_key": machine_key(machine) if machine else "0" * 64,
                    "notification_id": identifier,
                }
            ),
        ),
    )


def accepted(row):
    return {
        "job_id": row["job_id"],
        "alert_id": row["alert_id"],
        "copilot_status": "PENDING",
        "status_url": "/api/v1/brief-jobs/" + quote(row["job_id"], safe=""),
    }


class BriefRepository:
    def __init__(self, factory):
        self.factory = factory

    def enqueue(self, alert_id, machines, actor, key, intent):
        if not isinstance(key, str) or not key.strip() or len(key.encode()) > 256:
            raise AlertError(400, "INVALID_REQUEST")
        if intent not in {"MAINTENANCE_BRIEF", "TECHNICIAN_CHECKLIST", "ALERT_EXPLANATION"}:
            raise AlertError(422, "VALIDATION_FAILED")
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        request_hash = hashlib.sha256(
            canonical_json({"alert_id": alert_id, "intent": intent})
        ).hexdigest()
        with self.factory() as connection, connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (actor + ":" + key_hash,)
            )
            row = AlertRepository(self.factory)._locked(connection, alert_id, machines)
            binding = connection.execute(
                "SELECT * FROM brief_idempotency WHERE actor_id=%s AND key_hash=%s",
                (actor, key_hash),
            ).fetchone()
            if binding:
                if binding["request_hash"] != request_hash:
                    raise AlertError(409, "IDEMPOTENCY_CONFLICT")
                return accepted({"job_id": binding["job_id"], "alert_id": alert_id})
            stored = connection.execute(
                "SELECT * FROM alert_evidence_snapshots WHERE alert_id=%s", (alert_id,)
            ).fetchone()
            if stored is None:
                raise AlertError(503, "DEPENDENCY_UNAVAILABLE")
            raw = bytes(stored["evidence"])
            if hashlib.sha256(raw).hexdigest() != stored["evidence_sha256"]:
                raise AlertError(503, "DEPENDENCY_UNAVAILABLE")
            evidence = json.loads(raw)
            outcome = evidence["outcome"]
            payload = {
                "alert": {
                    k: row[k]
                    for k in (
                        "alert_id",
                        "machine_id",
                        "severity",
                        "status",
                        "risk_score",
                        "policy_version",
                    )
                },
                "prediction": {
                    k: outcome[k]
                    for k in ("prediction_id", "prediction", "failure_probability", "model_version")
                },
                "sensor_context": evidence["features_raw"],
                "intent": intent,
            }
            from .schemas import valid

            if not valid("brief-request", payload):
                raise AlertError(503, "DEPENDENCY_UNAVAILABLE")
            job = connection.execute(
                "SELECT * FROM brief_jobs WHERE alert_id=%s AND intent=%s "
                "AND evidence_sha256=%s AND status IN ('QUEUED','RUNNING')",
                (alert_id, intent, stored["evidence_sha256"]),
            ).fetchone()
            if job is None:
                job = connection.execute(
                    "INSERT INTO brief_jobs(job_id,alert_id,machine_id,actor_id,"
                    "intent,evidence_sha256,input,input_sha256,status) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'QUEUED') RETURNING *",
                    (
                        "briefjob_" + uuid4().hex,
                        alert_id,
                        row["machine_id"],
                        actor,
                        intent,
                        stored["evidence_sha256"],
                        canonical_json(payload),
                        hashlib.sha256(canonical_json(payload)).hexdigest(),
                    ),
                ).fetchone()
                self._audit(connection, job["job_id"], "QUEUED")
            connection.execute(
                "INSERT INTO brief_idempotency VALUES(%s,%s,%s,%s)",
                (actor, key_hash, request_hash, job["job_id"]),
            )
            connection.execute(
                "INSERT INTO brief_facades VALUES(%s,%s) "
                "ON CONFLICT(alert_id) DO UPDATE SET job_id=excluded.job_id",
                (alert_id, job["job_id"]),
            )
            self._notify(connection, job)
            return accepted(job)

    @staticmethod
    def _audit(connection, job, event):
        connection.execute(
            "INSERT INTO brief_job_audit(audit_id,job_id,event_type) VALUES(%s,%s,%s)",
            (uuid4().hex, job, event),
        )

    def _notify(self, connection, row, success=False):
        emit(
            connection,
            "brief.ready" if success else "brief.updated",
            row["machine_id"],
            {"job_id": row["job_id"], "alert_id": row["alert_id"]},
        )

    def claim(self, owner):
        with self.factory() as connection, connection.transaction():
            expired = connection.execute("""UPDATE brief_jobs SET
                status=CASE WHEN deadline<=clock_timestamp() THEN 'EXPIRED' ELSE 'FAILED' END,
                error_code=CASE WHEN deadline<=clock_timestamp() THEN 'BRIEF_DEADLINE_EXCEEDED'
                    ELSE 'BRIEF_ATTEMPTS_EXHAUSTED' END,updated_at=clock_timestamp(),
                lease_owner=NULL,lease_until=NULL WHERE status IN ('QUEUED','RUNNING')
                AND (deadline<=clock_timestamp() OR
                (attempts>=max_attempts AND lease_until<=clock_timestamp()))
                RETURNING *""").fetchall()
            for row in expired:
                self._audit(connection, row["job_id"], row["status"])
                self._notify(connection, row)
            row = connection.execute(
                """WITH candidate AS (
                SELECT job_id FROM brief_jobs WHERE deadline>clock_timestamp()
                AND attempts<max_attempts AND next_attempt_at<=clock_timestamp()
                AND (status='QUEUED' OR (status='RUNNING' AND lease_until<=clock_timestamp()))
                ORDER BY created_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1)
                UPDATE brief_jobs j SET status='RUNNING',attempts=attempts+1,
                lease_owner=%s,lease_until=LEAST(deadline,clock_timestamp()+interval '90 seconds'),
                fencing_token=fencing_token+1,updated_at=clock_timestamp()
                FROM candidate WHERE j.job_id=candidate.job_id RETURNING j.*""",
                (owner,),
            ).fetchone()
            if row:
                self._audit(connection, row["job_id"], "RUNNING")
                from .schemas import valid

                raw = bytes(row["input"])
                try:
                    payload = json.loads(raw)
                    correct = hashlib.sha256(raw).hexdigest() == row["input_sha256"] and valid(
                        "brief-request", payload
                    )
                except (ValueError, TypeError):
                    correct = False
                if not correct:
                    connection.execute(
                        "UPDATE brief_jobs SET status='FAILED',error_code="
                        "'BRIEF_INPUT_INVALID',lease_owner=NULL,lease_until=NULL,"
                        "updated_at=clock_timestamp() WHERE job_id=%s",
                        (row["job_id"],),
                    )
                    self._audit(connection, row["job_id"], "FAILED")
                    self._notify(connection, row)
                    return None
                row["input"] = payload
            return row

    def heartbeat(self, claim):
        with self.factory() as connection:
            return bool(
                connection.execute(
                    """UPDATE brief_jobs SET lease_until=
                LEAST(deadline,clock_timestamp()+interval '90 seconds') WHERE job_id=%s
                AND lease_owner=%s AND fencing_token=%s AND status='RUNNING'
                AND lease_until>clock_timestamp() AND deadline>clock_timestamp()
                RETURNING job_id""",
                    (claim["job_id"], claim["lease_owner"], claim["fencing_token"]),
                ).fetchone()
            )

    def complete(self, claim, output):
        from rag_client import validate_response

        validate_response(output, claim["alert_id"])
        return self._finish(claim, output=output)

    def fail(self, claim, code, *, retryable=False, delay=1):
        return self._finish(claim, code=code, retryable=retryable, delay=delay)

    def _finish(self, claim, *, output=None, code=None, retryable=False, delay=1):
        with self.factory() as connection, connection.transaction():
            row = connection.execute(
                """SELECT * FROM brief_jobs WHERE job_id=%s AND status='RUNNING'
                AND lease_owner=%s AND fencing_token=%s AND lease_until>clock_timestamp()
                AND deadline>clock_timestamp() FOR UPDATE""",
                (claim["job_id"], claim["lease_owner"], claim["fencing_token"]),
            ).fetchone()
            if row is None:
                return False
            retry = retryable and row["attempts"] < row["max_attempts"]
            status = "SUCCEEDED" if output else "QUEUED" if retry else "FAILED"
            connection.execute(
                """UPDATE brief_jobs SET status=%s,output=%s,error_code=%s,
                updated_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,
                next_attempt_at=clock_timestamp()+(%s * interval '1 second') WHERE job_id=%s""",
                (
                    status,
                    canonical_json(output) if output else None,
                    None if output or retry else code,
                    max(0, min(delay, 60)),
                    row["job_id"],
                ),
            )
            self._audit(connection, row["job_id"], status)
            self._notify(connection, row, success=bool(output))
            return True

    def job(self, job_id, machines):
        with self.factory() as connection:
            row = connection.execute(
                "SELECT * FROM brief_jobs WHERE job_id=%s", (job_id,)
            ).fetchone()
            if row is None:
                raise AlertError(404, "NOT_FOUND")
            AlertRepository._authorized(connection, row["alert_id"], machines)
        output = json.loads(bytes(row["output"])) if row["output"] else None
        state = (
            "PENDING"
            if row["status"] in {"QUEUED", "RUNNING"}
            else "AVAILABLE"
            if output and output["grounded_status"] == "GROUNDED"
            else output["grounded_status"]
            if output
            else "UNAVAILABLE"
        )
        return {
            **{
                k: row[k]
                for k in ("job_id", "alert_id", "intent", "status", "attempts", "error_code")
            },
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
            "status_url": accepted(row)["status_url"],
            "copilot_status": state,
            "brief_id": output["brief_id"] if output else None,
        }

    def facade(self, alert_id, machines):
        with self.factory() as connection:
            AlertRepository._authorized(connection, alert_id, machines)
            return self.facade_in(connection, alert_id)

    @staticmethod
    def facade_in(connection, alert_id):
        row = connection.execute(
            "SELECT j.* FROM brief_facades f JOIN brief_jobs j USING(job_id) WHERE f.alert_id=%s",
            (alert_id,),
        ).fetchone()
        output = json.loads(bytes(row["output"])) if row and row["output"] else None
        state = (
            "NOT_REQUESTED"
            if row is None
            else "AVAILABLE"
            if output
            else "PENDING"
            if row["status"] in {"QUEUED", "RUNNING"}
            else "UNAVAILABLE"
        )
        return {"copilot_status": state, "message": state, "brief": output}
