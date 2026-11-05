"""SQL adapter for the shared, side-effect-free latest-axis reducer."""

import hashlib

from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import IdentityConflict
from psycopg import sql

from backend.shared.ordering.identity import guard_source
from backend.shared.projections.reducer import TABLES, reduce_projection


class Projector:
    def __init__(self, *, notify=None, generations=False):
        self.notify = notify
        self.generations = generations

    def __call__(self, connection, event, record, outbox):
        generation = None
        if self.generations:
            from backend.shared.projections.generations import current, gate

            gate(connection)
            generation = current(connection)
        machine = connection.execute(
            "SELECT machine_id FROM machines WHERE machine_id=%s FOR UPDATE", (event.key,)
        ).fetchone()
        if machine is None:
            raise IdentityConflict("Machine must be provisioned before projection")
        validation = guard_source(connection, event)
        rows = load(connection, event.key, generation=generation)
        result = reduce_projection(rows, event, source_validation=validation)
        persist(connection, result, generation=generation)
        if self.generations:
            from backend.shared.projections.generations import capture

            capture(connection, event, record)
        if result.changed and self.notify is not None:
            self.notify(connection, event.key)
        return result.telemetry_visible


def load(connection, machine, *, generation=None):
    return {
        table: connection.execute(
            sql.SQL(
                "SELECT * FROM {} WHERE machine_id=%s"
                + (" AND generation_id=%s" if generation is not None else "")
            ).format(sql.Identifier("projection_" + table if generation is not None else table)),
            (machine, generation) if generation is not None else (machine,),
        ).fetchone()
        for table in TABLES
    }


def persist(connection, result, *, generation=None):
    for table in result.changed:
        row = result.rows[table]
        columns = [
            "machine_id",
            "source_event_id",
            "stream_epoch",
            "sequence_number",
            "identity_sha256",
            "snapshot",
            "metadata",
        ]
        values = [row[column] for column in columns]
        conflict = "machine_id"
        if generation is not None:
            table = "projection_" + table
            columns.insert(0, "generation_id")
            values.insert(0, generation)
            conflict = "generation_id,machine_id"
        connection.execute(
            sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET "
                "source_event_id=EXCLUDED.source_event_id,stream_epoch=EXCLUDED.stream_epoch,"
                "sequence_number=EXCLUDED.sequence_number,identity_sha256=EXCLUDED.identity_sha256,"
                "snapshot=EXCLUDED.snapshot,metadata=EXCLUDED.metadata,"
                "updated_at=clock_timestamp()"
            ).format(
                sql.Identifier(table),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(",").join(sql.Placeholder() for _ in columns),
                sql.SQL(conflict),
            ),
            values,
        )
        if generation is not None:
            connection.execute(
                """INSERT INTO projection_row_checksums
                (generation_id,axis,machine_id,sha256) VALUES(%s,%s,%s,%s)
                ON CONFLICT(generation_id,axis,machine_id) DO UPDATE SET sha256=EXCLUDED.sha256""",
                (generation, table.removeprefix("projection_"), row["machine_id"], row_hash(row)),
            )


def row_hash(row):
    return hashlib.sha256(
        canonical_json(
            {
                key: int(row[key])
                if key == "sequence_number"
                else bytes(row[key]).hex()
                if key in {"snapshot", "metadata"} and row[key] is not None
                else row[key]
                for key in (
                    "machine_id",
                    "source_event_id",
                    "stream_epoch",
                    "sequence_number",
                    "identity_sha256",
                    "snapshot",
                    "metadata",
                )
            }
        )
    ).hexdigest()
