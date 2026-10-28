"""Owner SQL transactions, fixed-window dedupe and immutable alert evidence."""

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from maintai_messaging.contracts import default_contracts
from maintai_messaging.errors import IdentityConflict

from .test_projections import setup

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def evidence():
    outcome = json.loads((ROOT / "contracts/kafka/examples/completed.json").read_text())
    features = json.loads((ROOT / "contracts/kafka/examples/sensor-request.json").read_text())[
        "features"
    ]
    return {
        "schema_version": "1.0.0",
        "result_id": outcome["result_id"],
        "source_event_id": outcome["event_id"],
        "machine_id": outcome["machine_id"],
        "source_server_accepted_at": outcome["source_server_accepted_at"],
        "outcome": outcome,
        "features_raw": features,
        "raw_ref": None,
        "risk_snapshot": {
            k: outcome[k]
            for k in ("risk_decision_id", "machine_state", "policy_version", "risk_score")
        },
    }


def signal(name="signal-1", *, severity="HIGH"):
    outcome = evidence()["outcome"]
    payload = {
        "schema_version": "1.0.0",
        "signal_id": name,
        **{
            k: outcome[k]
            for k in (
                "event_id",
                "prediction_id",
                "machine_id",
                "risk_score",
                "model_version",
                "policy_version",
                "correlation_id",
            )
        },
        "severity": severity,
        "decided_at": outcome["occurred_at"],
    }
    return default_contracts().encode("alert.signals", payload)


class EvidenceFixture:
    calls = 0

    def fetch(self, payload):
        self.calls += 1
        value = evidence()
        value["outcome"]["machine_state"] = payload["severity"]
        value["risk_snapshot"]["machine_state"] = payload["severity"]
        return value


def deliver_alert(db, name="signal-1", *, client=None, severity="HIGH"):
    from backend.shared.alerts.handler import AlertHandler

    event = signal(name, severity=severity)
    record = SimpleNamespace(topic=event.topic, partition=0, offset=1, value=event.wire_bytes)
    return AlertHandler(db.factory, "business-alert-v1", client or EvidenceFixture())(event, record)


def repository(db):
    from backend.shared.alerts.repository import AlertRepository

    return AlertRepository(db.factory)


def test_create_replay_dedupe_and_immutable_detail(business_database):
    db = business_database
    setup(db)
    client = EvidenceFixture()
    deliver_alert(db, client=client)
    repo = repository(db)
    page = repo.list({"M-001"})
    assert page["total"] == 1
    alert_id = page["items"][0]["alert_id"]
    before = repo.detail(alert_id, {"M-001"})
    from .test_machine_reads import validate

    validate("AlertDetail", before)
    validate("AlertList", page)
    assert before["alert"]["risk_score"] == 0.5
    assert before["copilot"]["status"] == "UNAVAILABLE"
    deliver_alert(db, client=client)
    assert client.calls == 1
    deliver_alert(db, "signal-2", client=client)
    after = repo.detail(alert_id, {"M-001"})
    assert before == after
    with db.factory() as connection:
        assert (
            connection.execute("SELECT count(*) AS n FROM alert_signal_links").fetchone()["n"] == 2
        )
        assert (
            connection.execute("SELECT count(*) AS n FROM alert_notifications").fetchone()["n"] == 2
        )


@pytest.mark.parametrize("elapsed,total", [(299.999, 1), (300, 2), (301, 2)])
def test_fixed_window_boundary(business_database, elapsed, total):
    db = business_database
    setup(db)
    deliver_alert(db)
    from backend.shared.alerts.repository import AlertRepository

    with db.factory() as connection:
        created = connection.execute("SELECT created_at FROM alerts").fetchone()["created_at"]
    repo = AlertRepository(
        db.factory, clock=lambda connection: created + timedelta(seconds=elapsed)
    )
    from backend.shared.alerts.handler import AlertHandler

    event = signal("signal-2")
    record = SimpleNamespace(topic=event.topic, partition=0, offset=2, value=event.wire_bytes)
    AlertHandler(db.factory, "business-alert-v1", EvidenceFixture(), repository=repo)(event, record)
    assert repo.list({"M-001"})["total"] == total


def test_lifecycle_scope_feedback_terminal_and_snapshot(business_database):
    from backend.shared.alerts.domain import AlertError

    db = business_database
    setup(db)
    deliver_alert(db)
    repo = repository(db)
    aid = repo.list({"M-001"})["items"][0]["alert_id"]
    original = copy.deepcopy(repo.detail(aid, {"M-001"})["sensor_event"])
    with pytest.raises(AlertError) as error:
        repo.action(aid, "start", {"M-001"}, "tech", {})
    assert error.value.status == 409
    repo.action(aid, "ack", {"M-001"}, "tech", {"note": "received\x00note"})
    repo.action(aid, "start", {"M-001"}, "tech", {})
    repo.action(aid, "resolve", {"M-001"}, "tech", {"resolution": "Fixed"})
    with pytest.raises(AlertError):
        repo.action(aid, "ack", {"M-001"}, "tech", {})
    feedback = repo.feedback(
        aid,
        {"M-001"},
        "tech",
        {"technician_verdict": "CONFIRMED_ISSUE", "recommendation_feedback": "NOT_AVAILABLE"},
    )
    assert feedback["alert_id"] == aid
    detail = repo.detail(aid, {"M-001"})
    assert detail["sensor_event"] == original
    assert [x["event_type"] for x in detail["history"]] == [
        "CREATED",
        "ACKNOWLEDGED",
        "IN_PROGRESS",
        "RESOLVED",
    ]
    assert repo.list({"different"})["total"] == 0
    with pytest.raises(AlertError) as error:
        repo.detail(aid, {"different"})
    assert error.value.status == 403
    with db.factory() as connection:
        assert repo.active(connection, ["M-001"]) == {"M-001": []}
    deliver_alert(db, "signal-after-close")
    assert repo.list({"M-001"})["total"] == 2


def test_concurrent_signals_and_actions_serialize_on_machine(business_database):
    db = business_database
    setup(db)
    with ThreadPoolExecutor(2) as executor:
        list(executor.map(lambda name: deliver_alert(db, name), ("a", "b")))
    repo = repository(db)
    assert repo.list({"M-001"})["total"] == 1
    aid = repo.list({"M-001"})["items"][0]["alert_id"]

    def ack():
        from backend.shared.alerts.domain import AlertError

        try:
            return repo.action(aid, "ack", {"M-001"}, "tech", {})["status"]
        except AlertError as error:
            return error.status

    with ThreadPoolExecutor(2) as executor:
        values = list(executor.map(lambda _: ack(), range(2)))
    assert sorted(map(str, values)) == ["409", "ACKNOWLEDGED"]


def test_bad_evidence_rolls_back_receipt_and_persists_conflict(business_database):
    db = business_database
    setup(db)

    class Wrong(EvidenceFixture):
        def fetch(self, payload):
            value = super().fetch(payload)
            value["outcome"]["risk_score"] = 0.1
            return value

    with pytest.raises(IdentityConflict):
        deliver_alert(db, client=Wrong())
    with db.factory() as connection:
        for table in ("alerts", "alert_events", "maintai_kafka_inbox", "alert_notifications"):
            assert connection.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"] == 0
        assert (
            connection.execute(
                "SELECT count(*) AS n FROM business_projection_conflicts"
            ).fetchone()["n"]
            == 1
        )


def test_historical_signal_severity_groups_and_escalation_false_alert(business_database):
    from .test_projections import deliver, event

    db = business_database
    projector = setup(db)
    # UI already projects a later reading. An older source signal is still actionable.
    deliver(db, projector, event("completed", 99))
    with db.factory() as connection:
        machine_state = connection.execute("SELECT state FROM machines").fetchone()["state"]
    deliver_alert(db, "historical-high")
    deliver_alert(db, "historical-critical", severity="CRITICAL")
    repo = repository(db)
    assert repo.list({"M-001"})["total"] == 2
    high = repo.list({"M-001"}, severity="HIGH")["items"][0]["alert_id"]
    source = repo.detail(high, {"M-001"})["sensor_event"]
    repo.action(high, "escalate", {"M-001"}, "tech", {"note": "Needs specialist"})
    repo.action(high, "start", {"M-001"}, "specialist", {})
    repo.action(high, "false-alert", {"M-001"}, "specialist", {"reason": "Inspection clear"})
    detail = repo.detail(high, {"M-001"})
    assert detail["sensor_event"] == source
    assert [entry["event_type"] for entry in detail["history"]] == [
        "CREATED",
        "ESCALATED",
        "IN_PROGRESS",
        "FALSE_ALERT",
    ]
    with db.factory() as connection:
        active = repo.active(connection, ["M-001"])["M-001"]
        assert high not in active and len(active) == 1
        assert connection.execute("SELECT state FROM machines").fetchone()["state"] == machine_state
        assert (
            connection.execute(
                "SELECT details FROM alert_events WHERE event_type='FALSE_ALERT'"
            ).fetchone()["details"]
            == b'{"reason":"Inspection clear"}'
        )
