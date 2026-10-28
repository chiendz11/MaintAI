import logging
import time

from starlette.testclient import TestClient


def test_dependency_failure_and_recovery(edge):
    app, pub, probe = edge
    with TestClient(app) as c:
        assert c.get("/health/live").status_code == 200
        assert c.get("/health/ready").status_code == 200
        probe.value = False
        deadline = time.monotonic() + 2
        while c.get("/health/ready").status_code != 503 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert c.get("/health/ready").status_code == 503
        assert c.get("/health/live").status_code == 200
        probe.value = True
        deadline = time.monotonic() + 2
        while c.get("/health/ready").status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert c.get("/health/ready").status_code == 200
    assert not pub.records


def test_metrics_are_bounded_and_secrets_not_logged(edge, payload, caplog):
    app, pub, _ = edge
    caplog.set_level(logging.INFO, logger="maintai.ingestion")
    with TestClient(app) as c:
        c.post(
            "/api/v1/sensor-readings",
            json=payload,
            headers={"Authorization": "Bearer SECRET-token"},
        )
        text = c.get("/metrics").text
        assert "maintai_ingestion_accepted_total 1.0" in text
        assert 'outcome="ACCEPTED"' in text
        assert "M-001" not in text and payload["event_id"] not in text
    assert '"service": "ingestion-api"' in caplog.text
    assert "SECRET-token" not in caplog.text


def test_stale_snapshot_is_not_ready():
    from maintai_ingestion.health import DependencyState

    state = DependencyState()
    state.kafka = state.auth_keys = True
    state.checked_at = time.monotonic() - 11
    status, body = state.response()
    assert status == 503 and not body["checks"]["kafka"]
