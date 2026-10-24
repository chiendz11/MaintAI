"""Owner migrations must preserve SDK tables/data and enforce SQL invariants."""

from pathlib import Path

import psycopg
import pytest
from maintai_messaging.postgres import apply_messaging_schema

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def test_fresh_migration_creates_owner_tables_and_sdk_tables(owner_database):
    assert (ROOT / "processing/migrations/env.py").exists(), "Owner Alembic environment is missing"
    owner_database.migrate()
    owner_database.migrate()
    with owner_database.factory() as connection:
        tables = {
            row["tablename"]
            for row in connection.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname=current_schema()"
            )
        }
    assert {
        "sensor_events",
        "predictions",
        "risk_decisions",
        "processing_results",
        "processing_conflicts",
        "maintai_kafka_inbox",
        "maintai_kafka_outbox",
    } <= tables
    assert "processing_outbox" not in tables  # SDK outbox is the physical implementation.


def test_sdk_only_database_upgrades_without_losing_inbox_data(owner_database):
    with owner_database.factory() as connection:
        apply_messaging_schema(connection)
        connection.execute(
            """INSERT INTO maintai_kafka_inbox
            (consumer_group,topic,message_id,identity_sha256,source_partition,source_offset,schema_version)
            VALUES ('legacy-group','sensor.readings','fixture-event',%s,0,1,'1.3.0')""",
            ("1" * 64,),
        )
    owner_database.migrate()
    with owner_database.factory() as connection:
        assert (
            connection.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 1
        )


def test_conflict_audit_receipt_is_idempotent(migrated_database):
    with migrated_database.factory() as connection:
        values = ("evt-fixture", "sensor.readings", 0, 1, "1" * 64, "2" * 64)
        for _ in range(2):
            connection.execute(
                """INSERT INTO processing_conflicts
                (event_id,source_topic,source_partition,source_offset,existing_sha256,incoming_sha256)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                values,
            )
        assert (
            connection.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"]
            == 1
        )


def test_invalid_digest_cannot_be_saved_in_conflict_audit(migrated_database):
    with pytest.raises(psycopg.errors.CheckViolation), migrated_database.factory() as connection:
        connection.execute("""INSERT INTO processing_conflicts
            (event_id,source_topic,source_partition,source_offset,existing_sha256,incoming_sha256)
            VALUES ('evt','sensor.readings',0,1,NULL,'fake')""")
