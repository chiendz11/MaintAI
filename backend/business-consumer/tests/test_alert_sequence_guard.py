"""Source-sequence guard protects Alert Lifecycle from late/stale signals."""

import json
import uuid
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import psycopg
import pytest
from backend.shared.alerts.handler import AlertHandler
from backend.shared.alerts.repository import AlertRepository
from backend.shared.persistence.permissions import grant_business_permissions
from backend.shared.projections.projector import Projector
from business_consumer.observability import Metrics
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from psycopg import sql

from .test_alert_lifecycle import evidence
from .test_projections import event as proj_event
from .test_projections import setup

pytestmark = pytest.mark.integration


def build_signal_and_evidence(
    *,
    machine_id="M-001",
    seq=1,
    stream_epoch="epoch-1",
    event_id=None,
    signal_id=None,
    severity="HIGH",
    risk_score=0.88,
):
    base = evidence()
    evt_id = event_id or f"evt-{machine_id}-{seq}"
    pred_id = f"pred-{evt_id}"
    risk_id = f"risk-{evt_id}"
    res_id = f"res-{evt_id}"
    sig_id = signal_id or f"sig-{evt_id}"
    outcome = {
        **base["outcome"],
        "result_id": res_id,
        "event_id": evt_id,
        "machine_id": machine_id,
        "stream_epoch": stream_epoch,
        "sequence_number": seq,
        "prediction_id": pred_id,
        "risk_decision_id": risk_id,
        "machine_state": severity,
        "risk_score": risk_score,
        "failure_probability": risk_score,
    }
    ev_doc = {
        **base,
        "result_id": res_id,
        "source_event_id": evt_id,
        "machine_id": machine_id,
        "outcome": outcome,
        "risk_snapshot": {
            "risk_decision_id": risk_id,
            "machine_state": severity,
            "policy_version": outcome["policy_version"],
            "risk_score": risk_score,
        },
    }
    sig_payload = {
        "schema_version": "1.0.0",
        "signal_id": sig_id,
        "event_id": evt_id,
        "prediction_id": pred_id,
        "machine_id": machine_id,
        "severity": severity,
        "model_version": outcome["model_version"],
        "policy_version": outcome["policy_version"],
        "correlation_id": outcome["correlation_id"],
        "risk_score": risk_score,
        "decided_at": outcome["occurred_at"],
    }
    encoded = default_contracts().encode("alert.signals", sig_payload)
    return encoded, ev_doc


class MapEvidenceClient:
    def __init__(self):
        self.items = {}

    def register(self, sig_event, ev_doc):
        self.items[sig_event.message_id] = ev_doc

    def fetch(self, payload):
        return self.items[payload["signal_id"]]


def deliver(db, repo, client, *, metrics=None, **kwargs):
    sig_event, ev_doc = build_signal_and_evidence(**kwargs)
    client.register(sig_event, ev_doc)
    record = SimpleNamespace(topic=sig_event.topic, partition=0, offset=1, value=sig_event.wire_bytes)
    handler = AlertHandler(
        db.factory,
        "business-alert-v1",
        client,
        repository=repo,
        metrics=metrics,
    )
    handler(sig_event, record)
    return sig_event


def test_stale_signal_ignored_on_active_alert_and_after_terminal(business_database):
    db = business_database
    setup(db)
    repo = AlertRepository(db.factory)
    client = MapEvidenceClient()
    metrics = Metrics(wall_time=lambda: 1792281601)

    # 1. Newer signal seq=10 arrives and opens alert
    deliver(db, repo, client, seq=10, metrics=metrics)
    aid = repo.list({"M-001"})["items"][0]["alert_id"]

    # 2. Stale signal seq=8 arrives within active dedupe window -> STALE_IGNORED
    stale_8 = deliver(db, repo, client, seq=8, metrics=metrics)
    with db.factory() as conn:
        alert = conn.execute("SELECT * FROM alerts WHERE alert_id=%s", (aid,)).fetchone()
        assert int(alert["latest_source_sequence"]) == 10
        assert json.loads(bytes(alert["latest_metadata"]))["event_id"] == "evt-M-001-10"
        link_8 = conn.execute(
            "SELECT * FROM alert_signal_links WHERE signal_id=%s", (stale_8.message_id,)
        ).fetchone()
        assert link_8["disposition"] == "STALE_IGNORED"
        assert conn.execute("SELECT count(*) AS n FROM alert_notifications").fetchone()["n"] == 1

    # 3. Resolve alert at seq=10; stale seq=9 arrives -> STALE_AFTER_TERMINAL (must not create alert)
    repo.action(aid, "ack", {"M-001"}, "tech", {})
    repo.action(aid, "start", {"M-001"}, "tech", {})
    repo.action(aid, "resolve", {"M-001"}, "tech", {"resolution": "Replaced bearing"})
    stale_9 = deliver(db, repo, client, seq=9, metrics=metrics)
    assert repo.list({"M-001"})["total"] == 1
    assert repo.detail(aid, {"M-001"})["alert"]["status"] == "RESOLVED"
    with db.factory() as conn:
        link_9 = conn.execute(
            "SELECT * FROM alert_signal_links WHERE signal_id=%s", (stale_9.message_id,)
        ).fetchone()
        assert link_9["disposition"] == "STALE_AFTER_TERMINAL"
        # 1 (CREATED) + 3 (ack, start, resolve transitions) = 4 notifications; stale_9 emitted 0
        assert conn.execute("SELECT count(*) AS n FROM alert_notifications").fetchone()["n"] == 4

    # Metrics recorded stale counters cleanly without inflating created/deduped
    assert metrics.registry.get_sample_value("maintai_alerts_created_total", {"severity": "HIGH"}) == 1
    assert (
        metrics.registry.get_sample_value("maintai_alerts_stale_signals_total", {"reason": "STALE_IGNORED"})
        == 1
    )
    assert (
        metrics.registry.get_sample_value(
            "maintai_alerts_stale_signals_total", {"reason": "STALE_AFTER_TERMINAL"}
        )
        == 1
    )

    # 4. Newer signal seq=11 across new stream_epoch opens a new OPEN alert
    deliver(db, repo, client, seq=11, stream_epoch="epoch-2", metrics=metrics)
    assert repo.list({"M-001"})["total"] == 2
    with db.factory() as conn:
        newest = conn.execute(
            "SELECT * FROM alerts ORDER BY latest_source_sequence DESC LIMIT 1"
        ).fetchone()
        assert newest["status"] == "OPEN"
        assert int(newest["latest_source_sequence"]) == 11
        assert newest["latest_source_stream_epoch"] == "epoch-2"


def test_stale_signal_after_false_alert_and_outside_dedupe_window_are_blocked(business_database):
    db = business_database
    setup(db)
    with db.factory() as conn:
        t0 = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
    clock_state = {"t": t0}
    repo = AlertRepository(db.factory, clock=lambda _c: clock_state["t"])
    client = MapEvidenceClient()

    deliver(db, repo, client, seq=20)
    # 10 minutes later (>300s dedupe window) an older seq=15 arrives while OPEN -> STALE_IGNORED
    clock_state["t"] = t0 + timedelta(seconds=600)
    deliver(db, repo, client, seq=15)
    assert repo.list({"M-001"})["total"] == 1

    # False-alert closes it; stale seq=18 arrives -> STALE_AFTER_TERMINAL
    aid = repo.list({"M-001"})["items"][0]["alert_id"]
    repo.action(aid, "false-alert", {"M-001"}, "tech", {"reason": "Sensor noise"})
    deliver(db, repo, client, seq=18)
    assert repo.list({"M-001"})["total"] == 1
    assert repo.detail(aid, {"M-001"})["alert"]["status"] == "FALSE_ALERT"


def test_legacy_unsequenced_active_alert_bootstraps_and_same_seq_conflict_fails(business_database):
    db = business_database
    projector = setup(db)
    client = MapEvidenceClient()
    repo = AlertRepository(db.factory)

    # Insert an unsequenced pre-migration active alert directly (latest_source_sequence IS NULL)
    sig_1, ev_1 = build_signal_and_evidence(seq=5)
    with db.factory() as conn:
        now = conn.execute("SELECT clock_timestamp() AS ts").fetchone()["ts"]
        conn.execute(
            """INSERT INTO alerts(alert_id,machine_id,severity,status,risk_score,model_version,
            policy_version,source_event_id,prediction_id,created_at,updated_at,latest_signal_id,
            latest_metadata) VALUES('alt_legacy','M-001','HIGH','OPEN',0.88,'m','p',%s,%s,%s,%s,%s,%s)""",
            (
                sig_1.payload["event_id"],
                sig_1.payload["prediction_id"],
                now,
                now,
                sig_1.message_id,
                canonical_json(sig_1.payload),
            ),
        )
        conn.execute(
            "INSERT INTO alert_evidence_snapshots(alert_id,evidence,evidence_sha256) VALUES('alt_legacy',%s,%s)",
            (canonical_json(ev_1), "1" * 64),
        )

    # Next signal seq=6 within 300s dedupes legacy active alert cleanly and populates sequence
    deliver(db, repo, client, seq=6)
    with db.factory() as conn:
        row = conn.execute("SELECT * FROM alerts WHERE alert_id='alt_legacy'").fetchone()
        assert int(row["latest_source_sequence"]) == 6
        assert row["latest_source_stream_epoch"] == "epoch-1"

    # Different event claiming identical sequence 6 raises IdentityConflict
    with pytest.raises(IdentityConflict):
        deliver(db, repo, client, seq=6, event_id="evt-M-001-6-conflict", signal_id="sig-conflict")

    # Least-privilege consumer role can still update latest_source_sequence without mutating status
    roles = ["m5_guard_" + uuid.uuid4().hex for _ in range(3)]
    with db.factory() as conn:
        for role in roles:
            conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        grant_business_permissions(connection=conn, reader_role=roles[0], consumer_role=roles[1], api_role=roles[2])

    @contextmanager
    def consumer_factory():
        with db.factory() as conn:
            conn.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(roles[1])))
            conn.commit()
            yield conn

    try:
        priv_repo = AlertRepository(consumer_factory)
        with consumer_factory() as conn:
            priv_repo.create_or_dedupe(conn, *build_signal_and_evidence(seq=7))
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute("UPDATE alerts SET status='RESOLVED'")
    finally:
        with db.factory() as conn:
            for role in roles:
                conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))

    # Suppress unused import warning on proj_event/projector check
    assert projector is not None and proj_event is not None
