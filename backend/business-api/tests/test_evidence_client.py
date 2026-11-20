"""Evidence reads use a service token, exact IDs and one total wall deadline."""

import asyncio
import importlib
import json
import time

import httpx
import pytest
from backend.shared.alerts.evidence import EvidenceClient
from maintai_messaging.errors import IdentityConflict


def fixture():
    return importlib.import_module("backend.business-consumer.tests.test_alert_lifecycle")


def test_evidence_client_reads_status_then_exact_result_with_service_credential(tmp_path):
    credential = tmp_path / "service"
    credential.write_text("service-token")
    calls = []

    def handle(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer service-token"
        if request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={
                    "event_id": "evt_fixture_1",
                    "machine_id": "M-001",
                    "result_id": "res_fixture_1",
                    "processing_status": "COMPLETED",
                    "validation_status": "VALID",
                },
            )
        return httpx.Response(200, json=fixture().evidence())

    client = EvidenceClient(
        "https://processing.test", credential, transport=httpx.MockTransport(handle)
    )
    try:
        value = client.fetch(fixture().signal().payload)
        assert value["result_id"] == "res_fixture_1"
        assert calls[1].url.path == "/api/v1/internal/predictions/res_fixture_1"
        assert all(r.headers["X-Correlation-ID"] == "fixture" for r in calls)
    finally:
        client.close()


def test_evidence_deadline_bounds_both_requests_together(tmp_path):
    credential = tmp_path / "service"
    credential.write_text("service-token")

    async def delayed(request):
        await asyncio.sleep(0.09)
        return httpx.Response(
            200,
            json={
                "event_id": "evt_fixture_1",
                "machine_id": "M-001",
                "result_id": "res_fixture_1",
                "processing_status": "COMPLETED",
                "validation_status": "VALID",
                "updated_at": "2026-11-20T00:00:00Z",
                "failure_code": None,
            },
        )

    client = EvidenceClient(
        "https://processing.test",
        credential,
        deadline_seconds=0.12,
        transport=httpx.MockTransport(delayed),
    )
    start = time.monotonic()
    try:
        with pytest.raises(RuntimeError):
            client.fetch(fixture().signal().payload)
        assert time.monotonic() - start < 0.3
    finally:
        client.close()


def test_signal_identity_is_never_repaired_from_another_prediction(tmp_path):
    credential = tmp_path / "service"
    credential.write_text("service-token")

    def wrong(request):
        return httpx.Response(
            200,
            json={
                "event_id": "another",
                "machine_id": "M-001",
                "result_id": "res_fixture_1",
                "processing_status": "COMPLETED",
                "validation_status": "VALID",
            },
        )

    client = EvidenceClient(
        "https://processing.test", credential, transport=httpx.MockTransport(wrong)
    )
    try:
        with pytest.raises(IdentityConflict):
            client.fetch(fixture().signal().payload)
    finally:
        client.close()


def test_evidence_and_status_clients_use_oauth_provider_with_refresh_and_401_retry(tmp_path):
    from backend.shared.service_auth import ServiceOAuthConfig, ServiceOAuthTokenProvider
    from business_processing_client import ProcessingStatusClient

    secret_file = tmp_path / "client_secret"
    secret_file.write_text("oauth-secret-value")
    issued = []
    now = [100.0]

    def idp(request):
        body = request.content.decode()
        assert "client_id=maintai-processing" in body
        assert "client_secret=oauth-secret-value" in body
        token = f"oauth-tok-{len(issued) + 1}"
        issued.append(token)
        return httpx.Response(200, json={"access_token": token, "expires_in": 60})

    provider = ServiceOAuthTokenProvider(
        ServiceOAuthConfig(
            token_url="http://keycloak:8080/realms/maintai/protocol/openid-connect/token",
            client_id="maintai-processing",
            secret_file=str(secret_file),
        ),
        transport=httpx.MockTransport(idp),
        clock=lambda: now[0],
    )

    seen_auth = []
    force_401_once = [True]

    def api_handler(request):
        seen_auth.append(request.headers["Authorization"])
        if request.url.path.endswith("/status") and force_401_once[0]:
            force_401_once[0] = False
            return httpx.Response(401, json={"error": {"code": "UNAUTHORIZED"}})
        if request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={
                    "event_id": "evt_fixture_1",
                    "machine_id": "M-001",
                    "result_id": "res_fixture_1",
                    "processing_status": "COMPLETED",
                    "validation_status": "VALID",
                },
            )
        return httpx.Response(200, json=fixture().evidence())

    client = EvidenceClient(
        "http://processing-read-api:8004",
        token_provider=provider,
        transport=httpx.MockTransport(api_handler),
    )
    try:
        # First call gets oauth-tok-1 -> 401 -> refreshes to oauth-tok-2 -> succeeds.
        value = client.fetch(fixture().signal().payload)
        assert value["result_id"] == "res_fixture_1"
        assert seen_auth == [
            "Bearer oauth-tok-1",
            "Bearer oauth-tok-2",
            "Bearer oauth-tok-2",
        ]
        # Cached within lifetime: reuses oauth-tok-2.
        client.fetch(fixture().signal().payload)
        assert len(issued) == 2
        # Expired clock triggers proactive refresh -> oauth-tok-3.
        now[0] += 120.0
        client.fetch(fixture().signal().payload)
        assert len(issued) == 3
        assert seen_auth[-1] == "Bearer oauth-tok-3"
    finally:
        client.close()

    # Verify ProcessingStatusClient reuses cached OAuth token and retries once on 401.
    class StreamBody(httpx.AsyncByteStream):
        def __init__(self, payload):
            self.raw = json.dumps(payload).encode()

        async def __aiter__(self):
            yield self.raw

    status_calls = []
    reject_once = [True]

    def status_handler(request):
        status_calls.append(request.headers["Authorization"])
        if reject_once[0]:
            reject_once[0] = False
            return httpx.Response(401, json={"error": {"code": "UNAUTHORIZED"}})
        return httpx.Response(
            200,
            stream=StreamBody(
                {
                    "event_id": "evt_fixture_1",
                    "machine_id": "M-001",
                    "result_id": "res_fixture_1",
                    "processing_status": "COMPLETED",
                    "validation_status": "VALID",
                    "updated_at": "2026-11-20T00:00:00Z",
                    "failure_code": None,
                }
            ),
            headers={"Content-Type": "application/json"},
        )

    async def check_status():
        async with httpx.AsyncClient(transport=httpx.MockTransport(status_handler)) as http:
            status_client = ProcessingStatusClient(
                http, base_url="http://processing-read-api:8004", token_provider=provider
            )
            payload = await status_client.get("evt_fixture_1", correlation_id="cor-oauth")
            assert payload["result_id"] == "res_fixture_1"

    asyncio.run(check_status())
    assert len(issued) == 4
    assert status_calls == ["Bearer oauth-tok-3", "Bearer oauth-tok-4"]
