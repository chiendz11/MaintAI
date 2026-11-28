"""Append-only lifecycle history, with complete action payload kept internally."""

from uuid import uuid4

from maintai_messaging.contracts import canonical_json


def record(connection, alert_id, before, after, actor, body, timestamp):
    connection.execute(
        """INSERT INTO alert_events
        (alert_event_id,alert_id,event_type,from_status,to_status,actor_id,note,details,timestamp)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            "ae_" + uuid4().hex,
            alert_id,
            "CREATED" if before is None else after,
            before,
            after,
            None if actor is None else actor.encode(),
            body.get("note", "").encode() or None,
            canonical_json(body),
            timestamp,
        ),
    )


def record_feedback(connection, alert_id, current_status, actor, payload, timestamp):
    verdict = payload.get("technician_verdict", "UNKNOWN")
    rec = payload.get("recommendation_feedback", "NOT_AVAILABLE")
    comment = str(payload.get("comment") or "").strip()
    summary = f"[Feedback: {verdict} / {rec}]"
    if comment:
        summary = f"{summary} {comment[:200]}"
    event_type = "CREATED" if current_status == "OPEN" else current_status
    connection.execute(
        """INSERT INTO alert_events
        (alert_event_id,alert_id,event_type,from_status,to_status,actor_id,note,details,timestamp)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            "ae_" + uuid4().hex,
            alert_id,
            event_type,
            current_status,
            current_status,
            None if actor is None else actor.encode("utf-8"),
            summary.encode("utf-8"),
            canonical_json({"feedback": payload}),
            timestamp,
        ),
    )
