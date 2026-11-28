"""Unit tests for feedback audit recording and AlertHistoryEvent contract compliance."""

import json
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path

from backend.shared.alerts.repository import AlertRepository
from jsonschema import Draft202012Validator
from referencing import Registry, Resource


def test_feedback_appends_valid_alert_history_event():
    executed = []
    now = datetime(2026, 11, 27, 2, 0, 0, tzinfo=UTC)
    alert_row = {
        "alert_id": "alt_123",
        "machine_id": "M-001",
        "source_event_id": "evt_1",
        "prediction_id": "pred_1",
        "severity": "HIGH",
        "status": "OPEN",
        "title": "High failure risk",
        "created_at": now,
        "updated_at": now,
    }

    class _Result:
        def __init__(self, row=None):
            self._row = row

        def fetchone(self):
            return self._row

    class _Conn:
        def transaction(self):
            return nullcontext()

        def execute(self, sql, params=()):
            executed.append((sql, params))
            if "SELECT * FROM alerts" in sql:
                return _Result(alert_row)
            return _Result()

    class _Factory:
        def __enter__(self):
            return _Conn()

        def __exit__(self, *_):
            return False

    repo = AlertRepository(lambda: _Factory(), clock=lambda _c: now)
    res = repo.feedback(
        "alt_123",
        {"M-001"},
        "tech_user",
        {
            "technician_verdict": "FALSE_ALERT",
            "recommendation_feedback": "UNHELPFUL",
            "comment": "Sensor recalibrated",
        },
    )
    assert res["alert_id"] == "alt_123"

    audit_calls = [p for sql, p in executed if "INSERT INTO alert_events" in sql]
    assert len(audit_calls) == 1
    ae_id, aid, event_type, from_status, to_status, actor_b, note_b, details_b, ts = audit_calls[0]
    assert aid == "alt_123"
    assert event_type == "CREATED"
    assert from_status == "OPEN"
    assert to_status == "OPEN"
    assert actor_b == b"tech_user"
    assert note_b.decode("utf-8") == "[Feedback: FALSE_ALERT / UNHELPFUL] Sensor recalibrated"
    assert json.loads(details_b.decode("utf-8"))["feedback"]["technician_verdict"] == "FALSE_ALERT"

    schemas_dir = Path(__file__).resolve().parents[3] / "contracts/v1.3/schemas"
    business_schema = json.loads((schemas_dir / "business.schema.json").read_text(encoding="utf-8"))
    common_schema = json.loads((schemas_dir / "common.schema.json").read_text(encoding="utf-8"))
    reg = Registry().with_resources(
        [
            ("common.schema.json", Resource.from_contents(common_schema)),
            ("business.schema.json", Resource.from_contents(business_schema)),
        ]
    )
    validator = Draft202012Validator(
        {"$ref": "business.schema.json#/$defs/AlertHistoryEvent"},
        registry=reg,
    )
    validator.validate(
        {
            "alert_event_id": ae_id,
            "event_type": event_type,
            "from_status": from_status,
            "to_status": to_status,
            "actor_id": actor_b.decode("utf-8"),
            "note": note_b.decode("utf-8"),
            "timestamp": ts.isoformat(),
        }
    )
