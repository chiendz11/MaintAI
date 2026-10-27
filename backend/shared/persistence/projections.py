"""One-statement, owner-local snapshots; no Processing SQL or full history."""

import json

from backend.shared.projections.views import MachineViewUnavailable, machine_detail, machine_item

# JSON (not JSONB) keeps escaped NUL in unrelated opaque IDs/raw fields lossless.
# Only bounded enum fields are extracted for filtering; complete snapshots stay bytes.
MACHINE_ROWS = """SELECT m.machine_id,m.display_name,m.last_seen_at,
    t.snapshot AS telemetry,p.snapshot AS processing,v.snapshot AS prediction,
    v.metadata AS prediction_metadata,
    CASE convert_from(p.snapshot,'UTF8')::json->>'processing_status'
      WHEN 'INVALID' THEN 'INVALID'
      WHEN 'COMPLETED' THEN convert_from(v.snapshot,'UTF8')::json->>'machine_state_at_prediction'
      ELSE 'UNKNOWN' END AS state
    FROM machines m
    LEFT JOIN latest_telemetry t USING(machine_id)
    LEFT JOIN latest_processing p USING(machine_id)
    LEFT JOIN latest_valid_prediction v USING(machine_id)"""


class ProjectionRepository:
    def __init__(self, connection_factory, *, alert_reader=None):
        self.connection_factory = connection_factory
        self.alert_reader = alert_reader

    @staticmethod
    def _decode(row):
        return {
            **row,
            **{
                key: json.loads(bytes(row[key])) if row[key] is not None else None
                for key in ("telemetry", "processing", "prediction", "prediction_metadata")
            },
        }

    def _alerts(self, connection, machines):
        if self.alert_reader is None:
            raise MachineViewUnavailable("Alert read source is not configured")
        values = self.alert_reader(connection, machines)
        if not isinstance(values, dict) or set(values) != set(machines):
            raise MachineViewUnavailable("Incomplete alert snapshot")
        for alerts in values.values():
            if (
                not isinstance(alerts, list)
                or any(not isinstance(value, str) or not value for value in alerts)
                or len(set(alerts)) != len(alerts)
            ):
                raise MachineViewUnavailable("Invalid alert snapshot")
        return values

    def detail(self, machine_id):
        with self.connection_factory() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            row = connection.execute(
                MACHINE_ROWS + " WHERE m.machine_id=%s", (machine_id,)
            ).fetchone()
            if row is None:
                return None
            alerts = self._alerts(connection, [machine_id])
            return machine_detail(self._decode(row), alerts[machine_id])

    def fleet(self, machines, *, state, limit, offset):
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("Invalid pagination")
        with self.connection_factory() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = connection.execute(
                "WITH scoped AS MATERIALIZED (" + MACHINE_ROWS + " WHERE m.machine_id=ANY(%s)), "
                "filtered AS (SELECT * FROM scoped WHERE (%s::text IS NULL OR state=%s)), "
                "page AS (SELECT * FROM filtered ORDER BY machine_id LIMIT %s OFFSET %s) "
                "SELECT total.total,page.* FROM (SELECT count(*) AS total FROM filtered) total "
                "LEFT JOIN page ON TRUE ORDER BY page.machine_id",
                # SQL OFFSET is bigint; larger contract-valid offsets are past any
                # representable row count, so clamp without changing page semantics.
                (list(machines), state, state, limit, min(offset, 2**63 - 1)),
            ).fetchall()
            total = rows[0]["total"]
            page = [row for row in rows if row["machine_id"] is not None]
            if not page:
                return {"items": [], "total": total}
            alerts = self._alerts(connection, [row["machine_id"] for row in page])
            return {
                "items": [
                    machine_item(self._decode(row), alerts[row["machine_id"]]) for row in page
                ],
                "total": total,
            }

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
