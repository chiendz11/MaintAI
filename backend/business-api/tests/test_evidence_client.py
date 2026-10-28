"""Evidence reads use a service token, exact IDs and one total wall deadline."""

import asyncio
import importlib
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
