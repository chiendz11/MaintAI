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
