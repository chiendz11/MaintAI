"""M6 owner metadata migration and least-privilege boundary on PostgreSQL."""

from uuid import uuid4

import psycopg
import pytest
from processing.shared.history_meta.persistence import (
    Metadata,
    check_source_permissions,
    grant_metadata,
    metadata_schema,
)
from processing.shared.history_meta.security import HistoryError
from psycopg import sql

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("target", ["maintai_kafka_outbox", "sensor_column"])
def test_metadata_preflight_rejects_extra_owner_or_column_grants(migrated_database, target):
    db = migrated_database
    role = "m6_extra_" + uuid4().hex
    schema = metadata_schema(db.schema)
    with db.factory() as connection:
        connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        grant_metadata(connection, role=role, schema=schema)
        permission = (
            sql.SQL("UPDATE(raw_features_json)") if target == "sensor_column" else sql.SQL("UPDATE")
        )
        table = "sensor_events" if target == "sensor_column" else target
        connection.execute(
            sql.SQL("GRANT {} ON {} TO {}").format(
                permission, sql.Identifier(db.schema, table), sql.Identifier(role)
            )
        )
    try:
        with db.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            with pytest.raises(HistoryError):
                Metadata(db.factory, schema).check_permissions(connection, source_schema=db.schema)
    finally:
        with db.factory() as connection:
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_source_preflight_accepts_select_only_and_rejects_owner(migrated_database):
    db = migrated_database
    role = "m6_reader_" + uuid4().hex
    with db.factory() as connection:
        connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(db.schema), sql.Identifier(role)
            )
        )
        for table in ("sensor_events", "processing_results", "predictions", "risk_decisions"):
            connection.execute(
                sql.SQL("GRANT SELECT ON {} TO {}").format(
                    sql.Identifier(db.schema, table), sql.Identifier(role)
                )
            )
    try:
        with db.factory() as connection:
            with pytest.raises(HistoryError):
                check_source_permissions(connection)
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            assert check_source_permissions(connection) == db.schema
    finally:
        with db.factory() as connection:
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_metadata_migration_does_not_grant_source_writes(migrated_database):
    db = migrated_database
    role = "m6_meta_" + uuid4().hex
    schema = metadata_schema(db.schema)
    with db.factory() as connection:
        connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        grant_metadata(connection, role=role, schema=schema)
    try:
        with db.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
            meta = Metadata(lambda: None, schema)
            meta.check_permissions(connection, source_schema=db.schema)
            for table in (
                "snapshots",
                "snapshot_members",
                "export_jobs",
                "raw_catalog_entries",
                "raw_catalog_sync_state",
            ):
                connection.execute(
                    sql.SQL("SELECT * FROM {}.{} LIMIT 0").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute(
                    sql.SQL("UPDATE {}.sensor_events SET updated_at=clock_timestamp()").format(
                        sql.Identifier(db.schema)
                    )
                )
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute(
                    sql.SQL("CREATE TABLE {}.forbidden(id int)").format(sql.Identifier(schema))
                )
        with db.factory() as connection:
            with pytest.raises(HistoryError):
                Metadata(db.factory, schema).check_permissions(connection, source_schema=db.schema)
    finally:
        with db.factory() as connection:
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_metadata_source_records_are_not_automatically_deleted(migrated_database):
    db = migrated_database
    with db.factory() as connection:
        assert connection.execute("SELECT count(*) AS n FROM sensor_events").fetchone()["n"] == 0
        assert (
            connection.execute("SELECT version_num FROM alembic_version").fetchone()["version_num"]
            == "processing_0003_history"
        )
