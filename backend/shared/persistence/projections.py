"""One-statement, owner-local snapshots; no Processing SQL or full history."""

import json


class ProjectionRepository:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def check(self):
        with self.connection_factory() as connection:
            connection.execute("""SELECT m.machine_id FROM machines m
                LEFT JOIN latest_telemetry t USING(machine_id)
                LEFT JOIN latest_processing p USING(machine_id)
                LEFT JOIN latest_valid_prediction v USING(machine_id) LIMIT 0""")
        return True

    def live(self, machine_id):
        with self.connection_factory() as connection:
            row = connection.execute(
                """SELECT m.machine_id,t.snapshot AS telemetry,p.snapshot AS processing,
                v.snapshot AS prediction,statement_timestamp() AS as_of FROM machines m
                LEFT JOIN latest_telemetry t USING(machine_id)
                LEFT JOIN latest_processing p USING(machine_id)
                LEFT JOIN latest_valid_prediction v USING(machine_id)
                WHERE m.machine_id=%s""",
                (machine_id,),
            ).fetchone()
        if row is None:
            return None

        def decode(value):
            return json.loads(bytes(value)) if value is not None else None

        return {
            "machine_id": row["machine_id"],
            "telemetry": decode(row["telemetry"]),
            "latest_processing": decode(row["processing"]),
            "latest_valid_prediction": decode(row["prediction"]),
            "as_of": row["as_of"].isoformat(),
        }
