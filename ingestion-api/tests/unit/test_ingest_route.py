import hashlib
import json

import pytest
from maintai_messaging.errors import PublishUnavailable, PublishUncertain
from starlette.testclient import TestClient

from maintai_ingestion.app import create_app
from maintai_ingestion.errors import EdgeFailure


def test_valid_envelope_and_status_url(edge, payload):
    app, pub, _ = edge
    with TestClient(app) as client:
        r = client.post("/api/v1/sensor-readings", json=payload)
    assert r.status_code == 202
    assert r.json()["processing_status"] == "PENDING"
    assert r.headers["location"] == r.json()["status_url"]
    assert r.headers["location"].startswith("http://127.0.0.1:8003/")
    assert pub.records[0].topic == "sensor.readings" and pub.records[0].key == "M-001"
    assert pub.closed == 1


@pytest.mark.parametrize("feature", ["negative", "wrong-type"])
def test_feature_invalid_is_accepted_unchanged(edge, payload, feature):
    app, pub, _ = edge
    payload["features"]["rotational_speed_rpm"] = -5 if feature == "negative" else "bad"
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
    assert r.status_code == 202
    assert pub.records[0].payload["features"] == payload["features"]


@pytest.mark.parametrize(
    "body,status", [(b"{", 400), (b"{}", 422), (b"null", 422), (b'{"x":NaN}', 400)]
)
def test_invalid_body_quarantined_hash_only(edge, body, status):
    app, pub, _ = edge
    with TestClient(app) as c:
        r = c.post(
            "/api/v1/sensor-readings",
            content=body,
            headers={"Content-Type": "application/json", "X-Request-ID": "req-test"},
        )
    assert r.status_code == status
    event = pub.records[0]
    assert event.topic == "sensor.rejected" and event.key == "verified-source"
    assert event.payload["payload_sha256"] == hashlib.sha256(body).hexdigest()
    assert event.payload["payload_disposition"] == "HASH_ONLY"
    assert not any(k in event.payload for k in ["raw_body", "payload", "features"])
    assert r.json()["error"]["details"]["rejection_id"] == event.payload["rejection_id"]
    assert r.json()["error"]["request_id"] == "req-test"


@pytest.mark.parametrize("failure,status", [(PublishUnavailable(), 503), (PublishUncertain(), 504)])
@pytest.mark.parametrize("raw", [False, True])
def test_ack_failure_never_success_or_durable_reject(edge, payload, failure, status, raw):
    app, pub, _ = edge
    pub.failure = failure
    with TestClient(app) as c:
        r = c.post(
            "/api/v1/sensor-readings",
            content=b"{" if raw else json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
    assert r.status_code == status and r.json()["error"]["retryable"]


def test_machine_forbidden_never_publishes(edge, payload):
    app, pub, _ = edge
    payload["machine_id"] = "M-999"
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
    assert r.status_code == 403 and pub.records == []


def test_payload_correlation_is_overridden_by_request_trace(edge, payload):
    app, pub, _ = edge
    with TestClient(app) as c:
        r = c.post(
            "/api/v1/sensor-readings", json=payload, headers={"X-Correlation-ID": "trusted-cor"}
        )
    assert r.headers["x-correlation-id"] == "trusted-cor"
    assert pub.records[0].payload["correlation_id"] == "trusted-cor"


def test_body_source_identity_cannot_override_token(edge, payload):
    app, pub, _ = edge
    payload["source_identity"] = "attacker"
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
    assert r.status_code == 422 and pub.records[0].key == "verified-source"


@pytest.mark.parametrize(
    "kwargs,status",
    [
        ({"content": b"{}"}, 415),
        ({"content": b"{}", "headers": {"Content-Type": "text/plain"}}, 415),
        (
            {
                "content": b"{}",
                "headers": {"Content-Type": "application/json", "Content-Encoding": "gzip"},
            },
            415,
        ),
        ({"content": b"x" * 262145, "headers": {"Content-Type": "application/json"}}, 413),
    ],
)
def test_transport_rejected_before_publish(edge, kwargs, status):
    app, pub, _ = edge
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", **kwargs)
    assert r.status_code == status and not pub.records


def test_auth_fails_before_reading_or_publishing(settings, payload):
    class Deny:
        def verify(self, header):
            raise EdgeFailure(401, "UNAUTHORIZED")

        def check(self):
            return True

    from ..fakes import FakePublisher, Probe

    pub = FakePublisher(settings.kafka)
    app = create_app(settings, publisher_factory=lambda _: pub, verifier=Deny(), probe=Probe())
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
    assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer" and not pub.records


def test_framework_errors_and_internal_error_sanitized(edge, payload):
    app, pub, _ = edge
    pub.failure = RuntimeError("SECRET-body-token")
    with TestClient(app, raise_server_exceptions=False) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
        assert r.status_code == 500 and "SECRET" not in r.text
        for method, path, status in [
            ("get", "/missing", 404),
            ("get", "/api/v1/sensor-readings", 405),
        ]:
            r = getattr(c, method)(path)
            assert r.status_code == status and "error" in r.json() and "x-request-id" in r.headers


def test_generated_status_path_is_escaped(edge, payload):
    app, pub, _ = edge
    payload["event_id"] = "id/with space"
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
    assert "/id%2Fwith%20space/status" in r.json()["status_url"]
