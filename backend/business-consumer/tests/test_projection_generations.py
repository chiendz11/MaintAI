"""Real PostgreSQL boundaries for opt-in generation serving and atomic capture."""

import json

import pytest

from .test_projections import deliver, event, setup, snapshot

pytestmark = pytest.mark.integration


def test_bootstrap_preserves_legacy_and_generation_writer_is_atomic(business_database):
    from backend.shared.persistence.projections import ProjectionRepository
    from backend.shared.projections.generations import bootstrap
    from backend.shared.projections.projector import Projector

    db = business_database
    legacy = setup(db)
    deliver(db, legacy, event("completed", 1))
    original = snapshot(db)
    with db.factory() as conn:
        generation = bootstrap(conn, writers_drained=True)
    repo = ProjectionRepository(db.factory, generations=True)
    assert repo.live("M-001")["latest_processing"] == original["latest_processing"]
    deliver(db, Projector(generations=True), event("invalid", 2), 2)
    assert repo.live("M-001")["latest_processing"]["sequence_number"] == 2
    assert snapshot(db)["latest_processing"]["sequence_number"] == 1
    with db.factory() as conn:
        assert (
            conn.execute("SELECT active_generation FROM projection_control").fetchone()[
                "active_generation"
            ]
            == generation
        )


def test_capture_records_only_committed_new_receipts(business_database):
    from datetime import UTC, datetime, timedelta

    from backend.shared.projections.generations import bootstrap
    from backend.shared.projections.projector import Projector
    from backend.shared.projections.rebuild import RebuildRepository

    db = business_database
    setup(db)
    with db.factory() as conn:
        generation = bootstrap(conn, writers_drained=True)
    repo = RebuildRepository(db.factory)
    job = repo.start(
        expected_active=generation,
        operator="test-owner",
        source_cluster="fixture-cluster",
        groups={
            "sensor.readings": "business-live-v1",
            "inference.results": "business-prediction-v1",
        },
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        journal_budget=1048576,
    )
    projector = Projector(generations=True)
    deliver(db, projector, event("sensor-reading", 1))
    deliver(db, projector, event("sensor-reading", 1))
    with db.factory() as conn:
        rows = conn.execute("SELECT * FROM projection_journal WHERE job_id=%s", (job,)).fetchall()
        assert len(rows) == 1
        assert json.loads(bytes(rows[0]["wire"]))["event_id"] == "e-1"
        assert conn.execute("SELECT count(*) AS n FROM alerts").fetchone()["n"] == 0


def test_bootstrap_requires_explicit_drain_assertion(business_database):
    from backend.shared.projections.generations import bootstrap

    db = business_database
    setup(db)
    with db.factory() as conn, pytest.raises(ValueError, match="drain"):
        bootstrap(conn, writers_drained=False)


def new_job(db, *, budget=1048576):
    from datetime import UTC, datetime, timedelta

    from backend.shared.projections.generations import bootstrap
    from backend.shared.projections.rebuild import RebuildRepository

    setup(db)
    with db.factory() as conn:
        generation = bootstrap(conn, writers_drained=True)
    repo = RebuildRepository(db.factory)
    identifier = repo.start(
        expected_active=generation,
        operator="fixture-owner",
        source_cluster="fixture-cluster",
        groups={
            "sensor.readings": "business-live-v1",
            "inference.results": "business-prediction-v1",
        },
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        journal_budget=budget,
    )
    return repo, identifier, generation


def test_membership_shadow_and_atomic_cutover_do_not_repeat_business_effects(business_database):
    from backend.shared.persistence.projections import ProjectionRepository
    from backend.shared.projections.projector import Projector

    db = business_database
    repo, identifier, original = new_job(db)
    projector = Projector(generations=True)
    # Both records arrive after the source boundary and must be replayed from journal.
    deliver(db, projector, event("completed", 1))
    deliver(db, projector, event("sensor-reading", 1))
    lease = repo.claim("fixture-runner")
    assert lease["job_id"] == identifier
    assert repo.scan_members(lease) == 0
    assert repo.catch_up(lease) == 2
    repo.verify(identifier)
    generation = repo.activate(identifier, expected_active=original)
    assert generation != original
    live = ProjectionRepository(db.factory, generations=True).live("M-001")
    assert live["latest_processing"]["processing_status"] == "COMPLETED"
    assert live["telemetry"]["validation_status"] == "VALID"
    with db.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM alerts").fetchone()["n"] == 0
        messages = conn.execute("SELECT event FROM business_m7_notifications").fetchall()
        assert json.loads(bytes(messages[0]["event"]))["type"] == "projection.resync"
        assert conn.execute("SELECT count(*) AS n FROM maintai_kafka_inbox").fetchone()["n"] == 2


def test_budget_failure_does_not_rollback_live_projection(business_database):
    from backend.shared.persistence.projections import ProjectionRepository
    from backend.shared.projections.projector import Projector

    db = business_database
    repo, identifier, original = new_job(db, budget=1)
    deliver(db, Projector(generations=True), event("sensor-reading", 1))
    assert repo.status(identifier)["last_error"] == "JOURNAL_BUDGET_EXCEEDED"
    assert ProjectionRepository(db.factory, generations=True).live("M-001")["telemetry"] is not None
    with pytest.raises(ValueError):
        repo.activate(identifier, expected_active=original)


def test_lease_takeover_fences_old_runner_and_abort_is_terminal(business_database):
    db = business_database
    repo, identifier, _ = new_job(db)
    old = repo.claim("old-runner")
    with db.factory() as conn:
        conn.execute(
            "UPDATE projection_rebuild_jobs SET lease_until=clock_timestamp()-interval '1s'"
        )
    replacement = repo.claim("new-runner")
    assert replacement["fencing_token"] > old["fencing_token"]
    with pytest.raises(ValueError, match="lease"):
        repo.scan_members(old)
    repo.abort(identifier)
    with pytest.raises(ValueError):
        repo.resume(identifier)


def test_cutover_waits_for_commit_before_capturing_journal_frontier(business_database):
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace

    from backend.shared.projections.projector import Projector

    db = business_database
    repo, identifier, original = new_job(db)
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    repo.verify(identifier)
    reading = event("sensor-reading", 3)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with db.factory() as connection, connection.transaction():
            Projector(generations=True)(
                connection, reading, SimpleNamespace(partition=0, offset=3), None
            )
            future = executor.submit(repo.activate, identifier, expected_active=original)
            import time

            time.sleep(0.05)
            assert not future.done()
        generation = future.result(timeout=5)
    with db.factory() as connection:
        row = connection.execute(
            "SELECT snapshot FROM projection_latest_telemetry WHERE generation_id=%s", (generation,)
        ).fetchone()
        assert json.loads(bytes(row["snapshot"]))["sequence_number"] == 3


def test_shadow_corruption_cannot_activate(business_database):
    db = business_database
    repo, identifier, original = new_job(db)
    from backend.shared.projections.projector import Projector

    deliver(db, Projector(generations=True), event("sensor-reading", 1))
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    repo.catch_up(lease)
    with db.factory() as connection:
        generation = repo.status(identifier)["generation_id"]
        connection.execute(
            "DELETE FROM projection_latest_telemetry WHERE generation_id=%s", (generation,)
        )
    with pytest.raises(ValueError, match="missing"):
        repo.verify(identifier)
    with pytest.raises(ValueError):
        repo.activate(identifier, expected_active=original)


def test_registry_change_prevents_activation(business_database):
    from backend.shared.persistence.registry import RegistryRepository

    db = business_database
    repo, identifier, original = new_job(db)
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    repo.verify(identifier)
    RegistryRepository(db.factory).provision([{"machine_id": "added", "display_name": "Added"}])
    with pytest.raises(ValueError, match="Registry"):
        repo.activate(identifier, expected_active=original)


def test_rebuild_role_isolated_from_business_effects_and_api_cannot_swap(business_database):
    from uuid import uuid4

    import psycopg
    from backend.shared.persistence.permissions import grant_business_permissions
    from psycopg import sql

    db = business_database
    setup(db)
    reader, consumer, api, rebuilder = ["m8_" + uuid4().hex for _ in range(4)]
    roles = (reader, consumer, api, rebuilder)
    with db.factory() as connection:
        for role in roles:
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
    try:
        with db.factory() as connection:
            grant_business_permissions(
                connection,
                reader_role=reader,
                consumer_role=consumer,
                api_role=api,
                rebuild_role=rebuilder,
            )
        for role in (reader, consumer, api):
            with db.factory() as connection:
                connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role)))
                with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                    connection.execute("UPDATE projection_control SET active_generation=NULL")
        with db.factory() as connection:
            from contextlib import contextmanager

            from backend.shared.projections.metrics import RebuildMetrics
            from prometheus_client import CollectorRegistry

            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(consumer)))

            @contextmanager
            def scoped_factory():
                yield connection

            RebuildMetrics(CollectorRegistry()).observe(scoped_factory)
        with db.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(rebuilder)))
            connection.execute("SELECT * FROM business_source_inbox")
            for table in ("alerts", "alert_events", "feedback", "maintai_kafka_inbox"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                    connection.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
    finally:
        with db.factory() as connection:
            for role in roles:
                connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_verify_checksum_scan_does_not_hold_live_capture_lock(business_database, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from backend.shared.projections import projector as module

    db = business_database
    repo, identifier, _ = new_job(db)
    projector = module.Projector(generations=True)
    deliver(db, projector, event("sensor-reading", 1))
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    repo.catch_up(lease)
    scanning, finish = threading.Event(), threading.Event()
    original = module.row_hash

    def delayed(row):
        # Only delay the verification thread; the live writer hashes its own new row normally.
        if threading.current_thread().name.startswith("verify-test"):
            scanning.set()
            assert finish.wait(5)
        return original(row)

    monkeypatch.setattr(module, "row_hash", delayed)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="verify-test") as executor:
        verify = executor.submit(repo.verify, identifier)
        assert scanning.wait(2)
        try:
            # Main thread delivery must commit while the shadow hash scan is paused.
            deliver(db, projector, event("sensor-reading", 2), 2)
        finally:
            finish.set()
        verify.result(timeout=5)


def test_transient_storage_failure_preserves_progress_for_retry(business_database):
    import threading

    from backend.shared.projections.runner import RebuildRunner
    from botocore.exceptions import EndpointConnectionError

    db = business_database
    repo, identifier, _ = new_job(db)
    stop = threading.Event()

    class OfflineStorage:
        def sync(self, lease):
            stop.set()
            raise EndpointConnectionError(endpoint_url="http://127.0.0.1:39000")

    runner = RebuildRunner(repo, OfflineStorage(), None, stop)
    runner.run()
    row = repo.status(identifier)
    assert row["status"] == "RUNNING"
    assert row["baseline_done"] is True
    assert row["lease_until"] is None


def test_immutable_job_input_cannot_be_extended_during_resume(business_database):
    import psycopg

    db = business_database
    _, identifier, _ = new_job(db)
    with db.factory() as connection, pytest.raises(psycopg.errors.CheckViolation):
        connection.execute(
            "UPDATE projection_rebuild_jobs SET deadline=deadline+interval '1 hour' "
            "WHERE job_id=%s",
            (identifier,),
        )


def test_verify_ledger_scan_does_not_block_live_capture(business_database, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import psycopg
    from backend.shared.projections.projector import Projector

    db = business_database
    repo, identifier, _ = new_job(db)
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    scanning, finish = threading.Event(), threading.Event()
    original = psycopg.Connection.execute

    def delayed(connection, query, *args, **kwargs):
        if "FROM business_source_inbox b LEFT JOIN projection_source_inbox" in str(query):
            scanning.set()
            assert finish.wait(5)
        return original(connection, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Connection, "execute", delayed)
    with ThreadPoolExecutor(max_workers=1) as executor:
        verify = executor.submit(repo.verify, identifier)
        assert scanning.wait(2)
        try:
            deliver(db, Projector(generations=True), event("sensor-reading", 1))
        finally:
            finish.set()
        verify.result(timeout=5)


def test_database_transient_keeps_job_and_can_resume(business_database, monkeypatch):
    import threading
    from types import SimpleNamespace

    import psycopg
    from backend.shared.projections.runner import RebuildRunner

    repo, identifier, _ = new_job(business_database)
    before = bytes(repo.status(identifier)["input"])
    stop = threading.Event()
    original = repo.scan_members

    def unavailable(lease):
        stop.set()
        raise psycopg.OperationalError("fixture temporary DB disconnect")

    monkeypatch.setattr(repo, "scan_members", unavailable)
    runner = RebuildRunner(repo, SimpleNamespace(sync=lambda _: False), None, stop)
    runner.run()
    assert repo.status(identifier)["status"] == "RUNNING"
    monkeypatch.setattr(repo, "scan_members", original)
    stop.clear()
    lease = repo.claim("replacement")
    runner.execute(lease, threading.Event())
    assert repo.status(identifier)["status"] == "VERIFIED"
    assert bytes(repo.status(identifier)["input"]) == before


def test_verification_database_transient_releases_lease_for_retry(business_database, monkeypatch):
    import psycopg
    from backend.shared.projections.sources import SourcePending

    repo, identifier, _ = new_job(business_database)
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    original = repo._verify_rows

    def unavailable(*args):
        raise psycopg.OperationalError("fixture temporary DB disconnect")

    monkeypatch.setattr(repo, "_verify_rows", unavailable)
    with pytest.raises(SourcePending):
        repo.verify(identifier)
    assert repo.status(identifier)["status"] == "RUNNING"
    assert repo.status(identifier)["lease_until"] is None
    monkeypatch.setattr(repo, "_verify_rows", original)
    repo.verify(identifier)
    assert repo.status(identifier)["status"] == "VERIFIED"


def test_missing_unapplied_journal_record_prevents_cutover(business_database):
    from backend.shared.projections.projector import Projector

    db = business_database
    repo, identifier, active = new_job(db)
    lease = repo.claim("fixture")
    repo.scan_members(lease)
    repo.verify(identifier)
    deliver(db, Projector(generations=True), event("sensor-reading", 1))
    with db.factory() as connection:
        connection.execute("DELETE FROM projection_journal WHERE job_id=%s", (identifier,))
    with pytest.raises(ValueError, match="journal|Journal"):
        repo.activate(identifier, expected_active=active)
