"""Read committed Processing status using only owner-local SELECT queries."""

import base64
import hashlib
import json
from datetime import UTC, datetime


class InvalidWindowCursorError(ValueError):
    """Raised when a runtime window pagination cursor is malformed."""


def _encode_before_cursor(accepted_at: datetime, event_id: str) -> str:
    payload = json.dumps(
        {"t": accepted_at.astimezone(UTC).isoformat(), "e": event_id},
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_before_cursor(token: str) -> tuple[datetime, str]:
    try:
        if not token or len(token) > 1024 or not token.isascii():
            raise ValueError("Invalid cursor length")
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
        if (
            not isinstance(payload, dict)
            or set(payload) != {"t", "e"}
            or not isinstance(payload["t"], str)
            or not isinstance(payload["e"], str)
            or not payload["e"]
        ):
            raise ValueError("Invalid cursor structure")
        parsed = datetime.fromisoformat(payload["t"].replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("Cursor timestamp requires UTC offset")
        return parsed.astimezone(UTC), payload["e"]
    except Exception as exc:
        raise InvalidWindowCursorError("INVALID_CURSOR") from exc


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

    def prediction(self, result_id):
        from .evidence import history_record

        with self.connection_factory() as connection:
            row = connection.execute(
                """SELECT e.*,r.result_id,r.kind,r.outcome,r.outcome_sha256,
                p.prediction_id,p.label,p.failure_probability,p.model_threshold,p.model_identity,
                d.risk_decision_id,d.risk_score,d.policy_version,d.policy_sha256,d.machine_state
                FROM processing_results r JOIN sensor_events e USING(event_id)
                LEFT JOIN predictions p USING(event_id) LEFT JOIN risk_decisions d USING(event_id)
                WHERE r.result_id=%s AND e.processing_status IN ('COMPLETED','INVALID','FAILED')""",
                (result_id,),
            ).fetchone()
        return None if row is None else history_record(row)

    def runtime_window(
        self,
        *,
        machines,
        model_version: str,
        dataset_version: str,
        window_start: datetime,
        window_end: datetime,
        before: str | None = None,
        limit: int = 500,
    ):
        from .evidence import history_record

        cursor_at, cursor_id = _decode_before_cursor(before) if before is not None else (None, None)
        start_utc = window_start.astimezone(UTC)
        end_utc = window_end.astimezone(UTC)
        with self.connection_factory() as connection:
            rows = connection.execute(
                """SELECT e.*,r.result_id,r.kind,r.outcome,r.outcome_sha256,
                p.prediction_id,p.label,p.failure_probability,p.model_threshold,p.model_identity,
                d.risk_decision_id,d.risk_score,d.policy_version,d.policy_sha256,d.machine_state
                FROM sensor_events e
                JOIN processing_results r USING(event_id)
                JOIN predictions p USING(event_id)
                LEFT JOIN risk_decisions d USING(event_id)
                WHERE e.machine_id=ANY(%s)
                  AND e.processing_status='COMPLETED'
                  AND e.validation_status='VALID'
                  AND r.kind='PREDICTION_COMPLETED'
                  AND p.model_version=%s
                  AND p.model_identity->>'dataset_version'=%s
                  AND e.source_server_accepted_at>=%s
                  AND e.source_server_accepted_at<%s
                  AND (
                    %s::timestamptz IS NULL
                    OR (e.source_server_accepted_at, e.event_id) < (%s::timestamptz, %s::text)
                  )
                ORDER BY e.source_server_accepted_at DESC, e.event_id DESC
                LIMIT %s""",
                (
                    sorted(machines),
                    model_version,
                    dataset_version,
                    start_utc,
                    end_utc,
                    cursor_at,
                    cursor_at,
                    cursor_id,
                    limit + 1,
                ),
            ).fetchall()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = [history_record(row, validate=index == 0) for index, row in enumerate(page_rows)]
        next_before = (
            _encode_before_cursor(
                page_rows[-1]["source_server_accepted_at"],
                page_rows[-1]["event_id"],
            )
            if has_more and page_rows
            else None
        )
        digest_input = "|".join(
            [
                model_version,
                dataset_version,
                start_utc.isoformat(),
                end_utc.isoformat(),
                before or "",
                str(limit),
                page_rows[0]["event_id"] if page_rows else "",
                page_rows[-1]["event_id"] if page_rows else "",
            ]
        ).encode("utf-8")
        return {
            "items": items,
            "model_version": model_version,
            "dataset_version": dataset_version,
            "window_start": start_utc.isoformat().replace("+00:00", "Z"),
            "window_end": end_utc.isoformat().replace("+00:00", "Z"),
            "snapshot_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "next_before": next_before,
            "snapshot_id": "rw_" + hashlib.sha256(digest_input).hexdigest()[:24],
        }
