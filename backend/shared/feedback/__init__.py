"""Append-only technician feedback; no lifecycle or training side effects."""

from uuid import uuid4

from maintai_messaging.contracts import canonical_json


def record(connection, alert_id, actor, payload, timestamp):
    identifier = "fb_" + uuid4().hex
    connection.execute(
        "INSERT INTO feedback(feedback_id,alert_id,actor_id,payload,recorded_at) "
        "VALUES(%s,%s,%s,%s,%s)",
        (identifier, alert_id, actor.encode(), canonical_json(payload), timestamp),
    )
    return {"feedback_id": identifier, "alert_id": alert_id, "recorded_at": timestamp.isoformat()}
