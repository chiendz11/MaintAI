"""Metadata-only source inbox prevents late contradictions after projection advance."""

from maintai_messaging.errors import IdentityConflict
from psycopg import sql


def guard_source(connection, event):
    if event.topic not in {"sensor.readings", "inference.results"}:
        raise ValueError("Unsupported Business projection topic")
    column = "reading_sha256" if event.topic == "sensor.readings" else "outcome_sha256"
    source = (event.key, event.payload["sequence_number"])
    connection.execute(
        """INSERT INTO business_source_inbox
        (machine_id,sequence_number,source_event_id,stream_epoch)
        VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (*source, event.source_event_id, event.payload["stream_epoch"]),
    )
    row = connection.execute(
        "SELECT * FROM business_source_inbox WHERE machine_id=%s AND sequence_number=%s FOR UPDATE",
        source,
    ).fetchone()
    if row is None or (
        row["source_event_id"] != event.source_event_id
        or row["stream_epoch"] != event.payload["stream_epoch"]
        or row[column] not in (None, event.identity_sha256)
    ):
        raise IdentityConflict("Durable source identity or terminal outcome differs")
    validation = (
        event.payload["validation_status"]
        if event.topic == "inference.results"
        else row["validation_status"]
    )
    connection.execute(
        sql.SQL(
            "UPDATE business_source_inbox SET {}=%s,validation_status=%s "
            "WHERE machine_id=%s AND sequence_number=%s"
        ).format(sql.Identifier(column)),
        (event.identity_sha256, validation, *source),
    )
    return validation
