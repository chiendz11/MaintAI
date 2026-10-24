"""Explicit Business-owned registry; sensor readings never create machines."""


class RegistryRepository:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def get(self, machine_id):
        with self.connection_factory() as connection:
            return connection.execute(
                "SELECT machine_id,display_name,state,last_seen_at "
                "FROM machines WHERE machine_id=%s",
                (machine_id,),
            ).fetchone()

    def check(self):
        with self.connection_factory() as connection:
            connection.execute("SELECT machine_id FROM machines LIMIT 0")
        return True

    def provision(self, machines):
        if not isinstance(machines, list) or not 1 <= len(machines) <= 10000:
            raise ValueError("Explicit bounded machine list is required")
        ids = set()
        for machine in machines:
            if not isinstance(machine, dict) or set(machine) != {"machine_id", "display_name"}:
                raise ValueError("Provisioning only accepts machine_id and display_name")
            if any(
                not isinstance(v, str) or not v.strip() or "\x00" in v for v in machine.values()
            ):
                raise ValueError("Machine identity/name cannot be empty or contain NUL")
            if machine["machine_id"] in ids:
                raise ValueError("Duplicate machine in provisioning manifest")
            ids.add(machine["machine_id"])
        with self.connection_factory() as connection, connection.transaction():
            for machine in machines:
                connection.execute(
                    """INSERT INTO machines(machine_id,display_name)
                    VALUES (%s,%s) ON CONFLICT(machine_id) DO UPDATE
                    SET display_name=EXCLUDED.display_name, updated_at=clock_timestamp()
                    WHERE machines.display_name IS DISTINCT FROM EXCLUDED.display_name""",
                    (machine["machine_id"], machine["display_name"]),
                )
        return len(machines)
