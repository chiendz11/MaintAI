"""Owner bootstrap and shared/exclusive transaction gate before machine locks."""

import hashlib
from uuid import uuid4

from maintai_messaging.contracts import canonical_json
from psycopg import sql

from backend.shared.projections.reducer import TABLES

GATE = (8068, 1)


def gate(connection, *, exclusive=False):
    function = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
    connection.execute(sql.SQL("SELECT {}(%s,%s)").format(sql.Identifier(function)), GATE)


def registry_hash(connection):
    machines = [
        row["machine_id"]
        for row in connection.execute(
            "SELECT machine_id FROM machines ORDER BY machine_id"
        ).fetchall()
    ]
    return hashlib.sha256(canonical_json(machines)).hexdigest(), machines


def bootstrap(connection, *, writers_drained):
    if writers_drained is not True:
        raise ValueError("Explicit writers-drained assertion is required")
    gate(connection, exclusive=True)
    current = connection.execute("SELECT * FROM projection_control FOR UPDATE").fetchone()
    if current["active_generation"] is not None:
        raise ValueError("Generation serving has already been bootstrapped")
    identifier = uuid4().hex
    connection.execute(
        "INSERT INTO projection_generations(generation_id,state,activated_at) "
        "VALUES(%s,'ACTIVE',clock_timestamp())",
        (identifier,),
    )
    for table in TABLES:
        connection.execute(
            sql.SQL(
                "INSERT INTO {} (generation_id,machine_id,source_event_id,stream_epoch,"
                "sequence_number,identity_sha256,snapshot,metadata,updated_at) SELECT %s,"
                "machine_id,source_event_id,stream_epoch,sequence_number,identity_sha256,"
                "snapshot,metadata,updated_at FROM {}"
            ).format(sql.Identifier("projection_" + table), sql.Identifier(table)),
            (identifier,),
        )
    connection.execute(
        "UPDATE projection_control SET active_generation=%s WHERE singleton", (identifier,)
    )
    return identifier


def current(connection):
    value = connection.execute("SELECT active_generation FROM projection_control").fetchone()
    if value is None or value["active_generation"] is None:
        raise RuntimeError("Generation serving requires explicit owner bootstrap")
    return value["active_generation"]


def capture(connection, event, record):
    """Called inside the same live transaction under the shared generation gate."""
    row = connection.execute("""SELECT j.* FROM projection_rebuild_jobs j
        JOIN projection_control c ON c.building_job=j.job_id FOR UPDATE OF j""").fetchone()
    if row is None:
        return
    if row["status"] not in {"QUEUED", "RUNNING", "VERIFIED"}:
        return
    size = len(event.wire_bytes)
    now = connection.execute("SELECT clock_timestamp() AS value").fetchone()["value"]
    if row["deadline"] <= now or row["journal_bytes"] + size > row["journal_budget"]:
        code = "DEADLINE_EXCEEDED" if row["deadline"] <= now else "JOURNAL_BUDGET_EXCEEDED"
        connection.execute(
            """UPDATE projection_rebuild_jobs SET status='FAILED',last_error=%s,
            lease_until=NULL,updated_at=clock_timestamp() WHERE job_id=%s""",
            (code, row["job_id"]),
        )
        connection.execute(
            "UPDATE projection_generations SET state='FAILED' WHERE generation_id=%s",
            (row["generation_id"],),
        )
        connection.execute("UPDATE projection_control SET building_job=NULL WHERE singleton")
        return
    inserted = connection.execute(
        """INSERT INTO projection_journal
        (job_id,topic,message_id,identity_sha256,wire_sha256,wire,source_partition,source_offset)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING journal_id""",
        (
            row["job_id"],
            event.topic,
            event.message_id,
            event.identity_sha256,
            hashlib.sha256(event.wire_bytes).hexdigest(),
            event.wire_bytes,
            record.partition,
            record.offset,
        ),
    ).fetchone()
    if inserted:
        connection.execute(
            "UPDATE projection_rebuild_jobs SET journal_bytes=journal_bytes+%s,"
            "journal_count=journal_count+1 WHERE job_id=%s",
            (size, row["job_id"]),
        )
