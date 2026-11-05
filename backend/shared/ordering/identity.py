"""Metadata-only source inbox prevents late contradictions after projection advance."""

from maintai_messaging.errors import IdentityConflict
from psycopg import sql


def guard_source(connection, event, *, generation=None):
    if event.topic not in {"sensor.readings", "inference.results"}:
        raise ValueError("Unsupported Business projection topic")
    column = "reading_sha256" if event.topic == "sensor.readings" else "outcome_sha256"
    source = (event.key, event.payload["sequence_number"])
    table = sql.Identifier(
        "business_source_inbox" if generation is None else "projection_source_inbox"
    )
    prefix = sql.SQL("") if generation is None else sql.SQL("generation_id,")
    condition = sql.SQL("") if generation is None else sql.SQL("generation_id=%s AND ")
    parameters = source if generation is None else (generation, *source)
    connection.execute(
        sql.SQL(
            "INSERT INTO {} ({}machine_id,sequence_number,source_event_id,stream_epoch) "
            "VALUES({}%s,%s,%s,%s) ON CONFLICT DO NOTHING"
        ).format(table, prefix, sql.SQL("") if generation is None else sql.SQL("%s,")),
        (*parameters, event.source_event_id, event.payload["stream_epoch"]),
    )
    row = connection.execute(
        sql.SQL("SELECT * FROM {} WHERE {}machine_id=%s AND sequence_number=%s FOR UPDATE").format(
            table, condition
        ),
        parameters,
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
            "UPDATE {} SET {}=%s,validation_status=%s WHERE {}machine_id=%s AND sequence_number=%s"
        ).format(table, sql.Identifier(column), condition),
        (event.identity_sha256, validation, *parameters),
    )
    return validation
