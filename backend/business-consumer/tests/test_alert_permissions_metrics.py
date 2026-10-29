"""Least privilege and post-commit metrics are part of the Alert boundary."""

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from backend.shared.alerts.handler import AlertHandler
from backend.shared.alerts.repository import AlertRepository
from backend.shared.persistence.permissions import grant_business_permissions
from business_consumer.observability import Metrics
from psycopg import sql

from .test_alert_lifecycle import EvidenceFixture, deliver_alert, signal
from .test_projections import setup

pytestmark = pytest.mark.integration


def test_api_can_transition_but_cannot_rewrite_evidence_or_audit(business_database):
    db = business_database
    setup(db)
    deliver_alert(db)
    roles = ["m5_" + uuid4().hex for _ in range(3)]
    with db.factory() as connection:
        for role in roles:
            connection.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        grant_business_permissions(
            connection, reader_role=roles[0], consumer_role=roles[1], api_role=roles[2]
        )

    @contextmanager
    def api_factory():
        with db.factory() as connection:
            connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(roles[2])))
            connection.commit()
            yield connection

    try:
        repo = AlertRepository(api_factory)
        aid = repo.list({"M-001"})["items"][0]["alert_id"]
        repo.action(aid, "ack", {"M-001"}, "tech", {})
        assert repo.detail(aid, {"M-001"})["alert"]["status"] == "ACKNOWLEDGED"
        for statement in (
            "UPDATE alert_evidence_snapshots SET evidence='changed'",
            "DELETE FROM alert_events",
            "UPDATE alert_events SET details='changed'",
            "UPDATE alerts SET model_version='changed'",
            "UPDATE machines SET display_name='changed'",
            "INSERT INTO machines(machine_id,display_name) VALUES('forged','forged')",
            "DELETE FROM feedback",
        ):
            with api_factory() as connection, pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(statement)
        with db.factory() as connection:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(roles[1])))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("UPDATE alerts SET status='RESOLVED'")
    finally:
        with db.factory() as connection:
            for role in roles:
                connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def test_duplicate_inbox_does_not_count_as_business_dedupe_or_new_alert(business_database):
    db = business_database
    setup(db)
    metrics = Metrics(wall_time=lambda: 1792281601)
    consumer = AlertHandler(db.factory, "business-alert-v1", EvidenceFixture(), metrics=metrics)

    def deliver(name):
        event = signal(name)
        consumer(
            event, SimpleNamespace(topic=event.topic, partition=0, offset=1, value=event.wire_bytes)
        )

    deliver("first")
    deliver("first")
    deliver("second")
    assert (
        metrics.registry.get_sample_value("maintai_alerts_created_total", {"severity": "HIGH"}) == 1
    )
    assert (
        metrics.registry.get_sample_value("maintai_alerts_deduplicated_total", {"severity": "HIGH"})
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "maintai_business_inbox_duplicates_total", {"topic": "alert.signals"}
        )
        == 1
    )


def test_rollback_after_alert_writes_has_no_notification_receipt_or_metric(business_database):
    db = business_database
    setup(db)

    class FailAfterWrite(AlertRepository):
        def create_or_dedupe(self, *args):
            super().create_or_dedupe(*args)
            raise RuntimeError("Crash before commit")

    metrics = Metrics()
    event = signal()
    handler = AlertHandler(
        db.factory,
        "business-alert-v1",
        EvidenceFixture(),
        repository=FailAfterWrite(db.factory),
        metrics=metrics,
    )
    with pytest.raises(RuntimeError):
        handler(
            event, SimpleNamespace(topic=event.topic, partition=0, offset=1, value=event.wire_bytes)
        )
    with db.factory() as connection:
        for table in (
            "alerts",
            "alert_events",
            "alert_notifications",
            "alert_signal_links",
            "maintai_kafka_inbox",
        ):
            assert connection.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
    assert (
        metrics.registry.get_sample_value("maintai_alerts_created_total", {"severity": "HIGH"})
        is None
    )
