"""Explicit metadata namespace and grants; source access uses another credential."""

from psycopg import sql

from .security import HistoryError

TABLES = (
    "snapshots",
    "snapshot_members",
    "export_jobs",
    "raw_catalog_entries",
    "raw_catalog_sync_state",
    "native_catalog_heads",
    "native_catalog_imports",
    "native_catalog_visited",
    "native_catalog_receipts",
)


def metadata_schema(source_schema="public"):
    return "history_meta" if source_schema == "public" else source_schema + "__history_meta"


def grant_metadata(connection, *, role, schema="history_meta"):
    if (
        not role
        or not connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
    ):
        raise ValueError("Provision a distinct metadata role before granting permissions")
    connection.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        )
    )
    for table in TABLES:
        connection.execute(
            sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON {}.{} TO {}").format(
                sql.Identifier(schema), sql.Identifier(table), sql.Identifier(role)
            )
        )


def check_source_permissions(connection):
    namespace = connection.execute("SELECT current_schema() AS name").fetchone()["name"]
    if (
        not namespace
        or connection.execute(
            "SELECT has_schema_privilege(current_user,%s,'CREATE') AS allowed", (namespace,)
        ).fetchone()["allowed"]
    ):
        raise HistoryError(503, "HISTORY_SOURCE_PERMISSIONS_INVALID")
    names = ("sensor_events", "processing_results", "predictions", "risk_decisions")
    rows = connection.execute(
        "SELECT c.relname,has_table_privilege(current_user,c.oid,'SELECT') AS readable,"
        "has_table_privilege(current_user,c.oid,"
        "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') "
        "OR has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE,REFERENCES') AS writable "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relname=ANY(%s)",
        (namespace, [*names, "maintai_kafka_inbox", "maintai_kafka_outbox"]),
    ).fetchall()
    if not set(names) <= {row["relname"] for row in rows if row["readable"]} or any(
        row["writable"] for row in rows
    ):
        raise HistoryError(503, "HISTORY_SOURCE_PERMISSIONS_INVALID")
    return namespace


class Metadata:
    def __init__(self, factory, schema="history_meta"):
        self.factory, self.schema = factory, schema

    def table(self, name):
        if name not in TABLES:
            raise ValueError("Unknown metadata table")
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(name))

    def check(self):
        with self.factory() as connection:
            for table in TABLES:
                connection.execute(sql.SQL("SELECT 1 FROM {} LIMIT 0").format(self.table(table)))
        return True

    def check_permissions(self, connection, *, source_schema="public"):
        """Reject source/DDL-capable metadata credentials before enabling M6."""

        def privilege(table, name):
            return connection.execute(
                "SELECT has_table_privilege(current_user,%s,%s) AS allowed",
                (table, name),
            ).fetchone()["allowed"]

        for table in TABLES:
            qualified = sql.Identifier(self.schema, table).as_string(connection)
            if not all(
                privilege(qualified, name) for name in ("SELECT", "INSERT", "UPDATE", "DELETE")
            ):
                raise HistoryError(503, "HISTORY_METADATA_PERMISSIONS_INVALID")
        sources = connection.execute(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relkind IN ('r','p')",
            (source_schema,),
        ).fetchall()
        if not sources:
            raise HistoryError(503, "HISTORY_METADATA_PERMISSIONS_INVALID")
        for source in sources:
            columns = connection.execute(
                "SELECT has_any_column_privilege(current_user,%s,"
                "'SELECT,INSERT,UPDATE,REFERENCES') AS allowed",
                (source["oid"],),
            ).fetchone()["allowed"]
            if columns or privilege(
                source["oid"], "SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"
            ):
                raise HistoryError(503, "HISTORY_METADATA_PERMISSIONS_INVALID")
        for namespace in (self.schema, source_schema):
            if connection.execute(
                "SELECT has_schema_privilege(current_user,%s,'CREATE') AS allowed", (namespace,)
            ).fetchone()["allowed"]:
                raise HistoryError(503, "HISTORY_METADATA_PERMISSIONS_INVALID")
        return True
