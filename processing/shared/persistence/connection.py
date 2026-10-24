"""Explicit owner migration and permission operations, never runtime startup."""

import argparse
import os
import sys
from importlib.resources import files

from alembic import command
from alembic.config import Config
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


def main(argv=None):
    parser = argparse.ArgumentParser(description="Explicit Processing owner migration job")
    parser.add_argument("operation", choices=["upgrade"])
    parser.parse_args(argv)
    try:
        dsn = os.environ.get("PROCESSING_DATABASE_URL")
        if not dsn:
            raise ValueError("PROCESSING_DATABASE_URL is required")
        upgrade(dsn)
        print("Processing owner migration applied")
        return 0
    except Exception as exc:
        print(f"Processing migration failed: {type(exc).__name__}", file=sys.stderr)
        return 1
