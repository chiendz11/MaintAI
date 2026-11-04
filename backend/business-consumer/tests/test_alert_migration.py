"""An additive M5 upgrade must preserve the populated M4 owner schema."""

from importlib.resources import files

import pytest
from alembic import command
from alembic.config import Config
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.registry import RegistryRepository
from backend.shared.projections.projector import Projector
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from .test_projections import deliver, event, snapshot

pytestmark = pytest.mark.integration


def test_upgrade_populated_m4_is_additive_and_repeatable(business_database):
    db = business_database
    engine = create_engine(
        make_url(db.dsn).set(drivername="postgresql+psycopg"),
        connect_args={"options": "-csearch_path=" + db.schema},
    )
    config = Config()
    config.set_main_option("script_location", str(files("backend.migrations")))
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "business_0002_projections")
    finally:
        engine.dispose()
    RegistryRepository(db.factory).provision([{"machine_id": "M-001", "display_name": "One"}])
    projector = Projector()
    deliver(db, projector, event("sensor-reading", 7))
    deliver(db, projector, event("completed", 7))
    before = snapshot(db)
    with db.factory() as connection:
        original = {
            table: connection.execute(f"SELECT * FROM {table}").fetchall()
            for table in ("machines", "business_source_inbox", "maintai_kafka_inbox")
        }
    upgrade(db.dsn, schema=db.schema)
    upgrade(db.dsn, schema=db.schema)
    after = snapshot(db)
    # as_of describes each read's server clock, not a persisted projection field.
    assert {k: v for k, v in after.items() if k != "as_of"} == {
        k: v for k, v in before.items() if k != "as_of"
    }
    with db.factory() as connection:
        for table, rows in original.items():
            assert connection.execute(f"SELECT * FROM {table}").fetchall() == rows
        for table in (
            "alerts",
            "alert_events",
            "alert_signal_links",
            "alert_evidence_snapshots",
            "alert_notifications",
            "feedback",
        ):
            assert connection.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == {
            "version_num": "business_0004_history_markers"
        }
