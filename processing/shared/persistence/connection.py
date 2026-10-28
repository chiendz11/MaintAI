"""Explicit owner migration and permission operations, never runtime startup."""

import argparse
import os
import sys
from importlib.resources import files

from alembic import command
from alembic.config import Config
from maintai_messaging.errors import ConfigurationError
from maintai_messaging.ordering import activate_processing_ordering, require_ordering
from maintai_messaging.postgres import connect
from psycopg import sql
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


def upgrade(dsn):
    url = make_url(dsn)
    if url.get_backend_name() not in {"postgres", "postgresql"}:
        raise ValueError("Processing migrations require PostgreSQL")
    engine = create_engine(url.set(drivername="postgresql+psycopg"))
    config = Config()
    config.set_main_option("script_location", str(files("processing.migrations")))
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()


def validate_execution_mode(connection, *, ordered):
    table = connection.execute(
        "SELECT to_regclass('maintai_processing_capabilities') AS name"
    ).fetchone()["name"]
    if table is None:
        if ordered:
            raise ConfigurationError("Ordering migration and activation are required")
        return
    row = connection.execute(
        "SELECT enabled FROM maintai_processing_capabilities WHERE capability='proc-order-1'"
    ).fetchone()
    if ordered:
        require_ordering(connection)
    elif row and row["enabled"]:
        raise ConfigurationError("Legacy worker/relay cannot use an activated ordered database")


def grant_processing_permissions(connection, *, reader_role, worker_role, relay_role):
    """Only grant on this owner's current schema to roles provisioned separately.

    Execute explicitly as the schema owner after reviewing role membership. This
    never creates global roles, passwords, databases or permissions in other DBs.
    """
    roles = (reader_role, worker_role, relay_role)
    if any(not isinstance(role, str) or not role.strip() for role in roles) or len(set(roles)) != 3:
        raise ValueError("Distinct existing reader, worker and relay roles are required")
    existing = connection.execute(
        "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)", (list(roles),)
    ).fetchall()
    if len(existing) != 3:
        raise ValueError(
            "Platform must provision the three roles before granting owner permissions"
        )
    schema = connection.execute("SELECT current_schema() AS schema_name").fetchone()["schema_name"]
    if not schema:
        raise ValueError("Processing owner schema must be selected explicitly")
    for role in roles:
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )

    def grant(role, privileges, tables):
        for table in tables:
            connection.execute(
                sql.SQL("GRANT {} ON {}.{} TO {}").format(
                    sql.SQL(privileges),
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(role),
                )
            )

    history = ("sensor_events", "predictions", "risk_decisions", "processing_results")
    grant(reader_role, "SELECT", history)
    grant(worker_role, "SELECT, INSERT, UPDATE", ("sensor_events",))
    grant(
        worker_role,
        "SELECT, INSERT",
        (*history[1:], "processing_conflicts", "maintai_kafka_inbox", "maintai_kafka_outbox"),
    )
    grant(relay_role, "SELECT, UPDATE", ("maintai_kafka_outbox",))
    grant(
        worker_role,
        "SELECT, INSERT, UPDATE",
        (
            "maintai_processing_assignments",
            "maintai_processing_machine_cursor",
            "maintai_outbox_keys",
            "processing_source_cursor",
        ),
    )
    grant(relay_role, "SELECT, UPDATE", ("maintai_outbox_keys",))
    for role in (worker_role, relay_role):
        # SDK uses FOR SHARE to serialize activation against active transactions.
        # Row locks require UPDATE on one column; enabled remains migration-only.
        grant(role, "SELECT, UPDATE (capability)", ("maintai_processing_capabilities",))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Explicit Processing owner migration job")
    parser.add_argument("operation", choices=["upgrade", "activate-ordering"])
    parser.add_argument("--legacy-writers-drained", action="store_true")
    args = parser.parse_args(argv)
    try:
        dsn = os.environ.get("PROCESSING_DATABASE_URL")
        if not dsn:
            raise ValueError("PROCESSING_DATABASE_URL is required")
        if args.operation == "upgrade":
            upgrade(dsn)
            print("Processing owner migration applied; ordering activation is separate")
        else:
            with connect(dsn) as connection, connection.transaction():
                activate_processing_ordering(
                    connection, legacy_writers_drained=args.legacy_writers_drained
                )
            print("Processing ordering activated")
        return 0
    except Exception as exc:
        print(f"Processing migration failed: {type(exc).__name__}", file=sys.stderr)
        return 1
