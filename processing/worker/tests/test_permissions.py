"""Existing roles receive least privileges only inside the Processing owner schema."""

import uuid

import psycopg
import pytest
from processing.shared.persistence.connection import grant_processing_permissions
from psycopg import sql

pytestmark = pytest.mark.integration


def test_read_api_and_relay_roles_cannot_write_or_query_other_owner_tables(migrated_database):
    names = ["m2_role_" + uuid.uuid4().hex for _ in range(3)]
    with migrated_database.factory() as connection:
        for name in names:
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(name)))
        grant_processing_permissions(
            connection, reader_role=names[0], worker_role=names[1], relay_role=names[2]
        )
    try:
        with migrated_database.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(names[0])))
            connection.execute("SELECT * FROM sensor_events LIMIT 0")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute("DELETE FROM sensor_events")
        with migrated_database.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(names[2])))
            connection.execute("UPDATE maintai_kafka_outbox SET attempts=attempts WHERE false")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute("SELECT * FROM sensor_events")
        with migrated_database.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(names[1])))
            connection.execute("UPDATE sensor_events SET updated_at=updated_at WHERE false")
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute("DELETE FROM maintai_kafka_outbox")
    finally:
        with migrated_database.factory() as connection:
            for name in names:
                connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(name)))
                connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))
