"""Read committed Processing status using only owner-local SELECT queries."""

import hashlib
import json


class StatusRepository:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def get(self, event_id):
        with self.connection_factory() as connection:
            row = connection.execute(
                """SELECT e.event_id,e.machine_id,e.processing_status,e.validation_status,
                e.updated_at,r.result_id,r.outcome,r.outcome_sha256
                FROM sensor_events e JOIN processing_results r USING(event_id)
                WHERE e.event_id=%s AND e.processing_status IN ('COMPLETED','INVALID','FAILED')""",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        raw = bytes(row["outcome"])
        if hashlib.sha256(raw).hexdigest() != row["outcome_sha256"]:
            raise ValueError("Persisted outcome checksum mismatch")
        payload = json.loads(raw)
        return {
            "event_id": row["event_id"],
            "machine_id": row["machine_id"],
            "processing_status": row["processing_status"],
            "result_id": row["result_id"],
            "validation_status": row["validation_status"],
            "updated_at": row["updated_at"].isoformat(),
            "failure_code": payload.get("failure", {}).get("code"),
        }

    def check(self):
        with self.connection_factory() as connection:
            connection.execute("SELECT event_id FROM sensor_events LIMIT 0")
            connection.execute("SELECT event_id FROM processing_results LIMIT 0")
        return True
