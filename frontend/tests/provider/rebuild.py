"""Test-only owner operation; production exposes no rebuild mutation HTTP API."""

import time
from datetime import UTC, datetime, timedelta

from maintai_messaging.contracts import default_contracts

from backend.shared.projections.generations import current
from backend.shared.projections.rebuild import RebuildRepository


def prepare(app, factory, kafka, archive):
    @app.post("/fixture/rebuild")
    def rebuild():
        repo = RebuildRepository(factory)
        with factory() as connection:
            active = current(connection)
            before = connection.execute(
                "SELECT count(*) AS n FROM alert_events"
            ).fetchone()["n"]
        catalog, _ = archive.store.current()
        profiles = default_contracts().topology["profiles"]
        identifier = repo.start(
            expected_active=active,
            operator="browser-fixture-owner",
            source_cluster=catalog[1]["source_cluster_id"],
            groups={
                "sensor.readings": kafka.group_prefix
                + profiles["business-live"]["group_id"],
                "inference.results": kafka.group_prefix
                + profiles["business-prediction"]["group_id"],
            },
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            journal_budget=16 * 1024 * 1024,
        )
        deadline = time.monotonic() + 40
        while True:
            row = repo.status(identifier)
            if row["status"] == "VERIFIED":
                break
            if row["status"] in {"FAILED", "ABORTED"} or time.monotonic() > deadline:
                raise RuntimeError(
                    "Fixture rebuild did not verify: " + str(row["last_error"])
                )
            time.sleep(0.1)
        generation = repo.activate(identifier, expected_active=active)
        with factory() as connection:
            after = connection.execute(
                "SELECT count(*) AS n FROM alert_events"
            ).fetchone()["n"]
        return {
            "generation_id": generation,
            "business_effects_unchanged": before == after,
        }
