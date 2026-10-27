"""Status boundary with real signed tokens and an explicit fixture issuer."""

import asyncio
import json
import time
from pathlib import Path

import httpx
import jwt
import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[3]


class StatusFixture:
    def __init__(self, status="COMPLETED", *, down=False, missing=False):
        self.status, self.down, self.missing = status, down, missing

    def get(self, event_id):
        if self.down:
            raise psycopg.OperationalError("secret connection string")
        if self.missing:
            return None
        return {
            "event_id": event_id,
            "machine_id": "M-001",
            "processing_status": self.status,
            "validation_status": "INVALID" if self.status == "INVALID" else "VALID",
            "result_id": "result-1",
            "updated_at": "2026-10-26T00:00:00Z",
            "failure_code": "ML_UNAVAILABLE" if self.status == "FAILED" else None,
        }

    def check(self):
        return not self.down


def auth():
    from processing_read_api.auth import IdentityVerifier

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = {
        **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
        "kid": "test-key",
        "alg": "RS256",
        "use": "sig",
    }
    verifier = IdentityVerifier(
        issuer="https://issuer.test",
        audience="processing-internal",
        roles_claim="roles",
        machines_claim="machines",
        fetch_jwks=lambda: {"keys": [jwk]},
    )

    def token(**changes):
        claims = {
            "iss": "https://issuer.test",
            "aud": "processing-internal",
            "sub": "business",
            "exp": int(time.time()) + 120,
            "roles": ["BUSINESS_SERVICE"],
            "machines": ["M-001"],
        }
        claims.update(changes)
        return "Bearer " + jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test-key"})

    return verifier, token


@pytest.mark.parametrize("status", ["COMPLETED", "INVALID", "FAILED"])
def test_status_preserves_durable_state_and_matches_schema(status):
    from processing_read_api.app import create_app

    verifier, token = auth()

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(StatusFixture(status), verifier)),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/v1/internal/sensor-events/e-1/status",
                headers={
                    "Authorization": token(),
                    "X-Correlation-ID": "cor-test",
                },
            )
            assert response.status_code == 200
            assert response.json()["processing_status"] == status
            assert response.headers["X-Correlation-ID"] == "cor-test"
            schema = json.loads((ROOT / "contracts/v1.3/schemas/runtime.schema.json").read_text())
            Draft202012Validator(
                schema["$defs"]["ProcessingStatus"], format_checker=FormatChecker()
            ).validate(response.json())

    asyncio.run(run())


@pytest.mark.parametrize(
    "changes,want",
    [
        ({"roles": ["VIEWER"]}, 403),
        ({"machines": ["M-002"]}, 403),
        ({"aud": "business-test"}, 401),
        ({"exp": 1}, 401),
    ],
)
def test_status_rejects_wrong_role_audience_or_machine(changes, want):
    from processing_read_api.app import create_app

    verifier, token = auth()

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(StatusFixture(), verifier)),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/v1/internal/sensor-events/e-1/status",
                headers={"Authorization": token(**changes)},
            )
            assert response.status_code == want

    asyncio.run(run())


@pytest.mark.parametrize(
    "fixture,want,code",
    [
        (StatusFixture(missing=True), 404, "EVENT_NOT_MATERIALIZED"),
        (StatusFixture(down=True), 503, "DEPENDENCY_UNAVAILABLE"),
    ],
)
def test_status_missing_and_outage_are_explicit_sanitized_errors(fixture, want, code):
    from processing_read_api.app import create_app

    verifier, token = auth()

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(fixture, verifier)), base_url="http://test"
        ) as client:
            response = await client.get(
                "/api/v1/internal/sensor-events/e-1/status", headers={"Authorization": token()}
            )
            assert response.status_code == want
            assert response.json()["error"]["code"] == code
            assert response.json()["error"]["retryable"] is True
            assert "secret" not in response.text

    asyncio.run(run())


def test_http_metrics_use_route_templates_and_exclude_health_and_scrapes():
    from processing_read_api.app import create_app

    verifier, token = auth()
    app = create_app(StatusFixture(), verifier)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for event_id in ("e-one", "e-two"):
                assert (
                    await client.get(
                        f"/api/v1/internal/sensor-events/{event_id}/status",
                        headers={"Authorization": token()},
                    )
                ).status_code == 200
            await client.get("/health/live")
            await client.get("/health/ready")
            result = await client.get("/metrics")
            assert result.status_code == 200
            assert "e-one" not in result.text and "e-two" not in result.text
            registry = app.state.metrics.registry
            labels = {
                "route": "/api/v1/internal/sensor-events/{event_id:path}/status",
                "method": "GET",
                "status": "200",
            }
            assert registry.get_sample_value("maintai_http_requests_total", labels) == 2
            assert not any(
                "health" in str(s.labels) or "/metrics" in str(s.labels)
                for m in registry.collect()
                for s in m.samples
            )

    asyncio.run(run())


def test_failed_metric_collector_preserves_http_result(monkeypatch):
    from processing_read_api.app import create_app

    verifier, token = auth()
    app = create_app(StatusFixture(), verifier)

    def broken(*args, **kwargs):
        raise RuntimeError("collector failed")

    monkeypatch.setattr(app.state.metrics, "http", broken)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                "/api/v1/internal/sensor-events/e-one/status", headers={"Authorization": token()}
            )
            assert result.status_code == 200
            assert result.json()["processing_status"] == "COMPLETED"

    asyncio.run(run())


def test_metric_labels_use_catalog_unmatched_and_unhandled_values():
    from processing_read_api.app import create_app

    verifier, _ = auth()
    app = create_app(StatusFixture(), verifier)

    @app.get("/fixture-error")
    def fixture_error():
        raise RuntimeError("fixture failure")

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
        ) as http:
            assert (await http.get("/unknown-fixture-id")).status_code == 404
            assert (await http.get("/fixture-error")).status_code == 500
        registry = app.state.metrics.registry
        assert (
            registry.get_sample_value(
                "maintai_http_requests_total",
                {
                    "route": "__unmatched__",
                    "method": "GET",
                    "status": "404",
                },
            )
            == 1
        )
        assert (
            registry.get_sample_value(
                "maintai_http_requests_total",
                {
                    "route": "/fixture-error",
                    "method": "GET",
                    "status": "UNHANDLED",
                },
            )
            == 1
        )

    asyncio.run(run())
