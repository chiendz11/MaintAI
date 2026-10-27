"""Public status uses its own service credential, bounded HTTP and caller machine scope."""

import asyncio
import json

import httpx
import pytest
from api.app import create_app

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


def status():
    return {
        "event_id": "e-1",
        "machine_id": "M-001",
        "processing_status": "COMPLETED",
        "validation_status": "VALID",
        "result_id": "result-1",
        "updated_at": "2026-10-26T00:00:00Z",
        "failure_code": None,
    }


class Body(httpx.AsyncByteStream):
    def __init__(self, raw):
        self.raw = raw

    async def __aiter__(self):
        for start in range(0, len(self.raw), 4096):
            yield self.raw[start : start + 4096]


def response(code, value, *, headers=None):
    return httpx.Response(
        code,
        stream=Body(json.dumps(value).encode()),
        headers=headers or {"Content-Type": "application/json"},
    )


@pytest.mark.parametrize("roles", [["SENSOR_SERVICE"], ["VIEWER"]])
def test_public_status_uses_separate_service_identity_and_preserves_correlation(
    signing_key,
    tmp_path,
    roles,
):
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt\n")

    async def run():
        def upstream(request):
            assert request.headers["Authorization"] == "Bearer fixture-service-jwt"
            assert request.headers["X-Correlation-ID"] == "cor-test"
            assert "X-Request-ID" not in request.headers
            assert request.url.path == "/api/v1/internal/sensor-events/e-1/status"
            return response(200, status())

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            app = create_app(RegistryFixture(), verifier(signing_key), status_client=client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as caller:
                headers = {
                    "Authorization": token(signing_key, roles=roles),
                    "X-Correlation-ID": "cor-test",
                }
                result = await caller.get("/api/v1/sensor-events/e-1/status", headers=headers)
                assert result.status_code == 200
                assert result.json() == status()
                assert result.headers["X-Correlation-ID"] == "cor-test"
                registry = await caller.get("/api/v1/machines/M-001", headers=headers)
                assert registry.status_code == (403 if roles == ["SENSOR_SERVICE"] else 200)

    asyncio.run(run())


def test_public_status_checks_caller_machine_before_returning_body(signing_key, tmp_path):
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: response(200, status()))
        ) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            app = create_app(RegistryFixture(), verifier(signing_key), status_client=client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as caller:
                result = await caller.get(
                    "/api/v1/sensor-events/e-1/status",
                    headers={
                        "Authorization": token(signing_key, machines=["M-002"]),
                    },
                )
                assert result.status_code == 403
                assert "result-1" not in result.text

    asyncio.run(run())


@pytest.mark.parametrize(
    "upstream_code,value,want,code",
    [
        (401, {}, 503, "DEPENDENCY_UNAVAILABLE"),
        (403, {}, 503, "DEPENDENCY_UNAVAILABLE"),
        (503, {}, 503, "DEPENDENCY_UNAVAILABLE"),
        (504, {}, 504, "DEPENDENCY_TIMEOUT"),
        (
            404,
            {
                "error": {
                    "code": "EVENT_NOT_MATERIALIZED",
                    "message": "not materialized",
                    "retryable": False,
                }
            },
            404,
            "EVENT_NOT_MATERIALIZED",
        ),
        (404, {"error": {"code": "EVENT_NOT_MATERIALIZED"}}, 503, "DEPENDENCY_UNAVAILABLE"),
        (404, {}, 503, "DEPENDENCY_UNAVAILABLE"),
        (200, {"machine_id": "M-001"}, 503, "DEPENDENCY_UNAVAILABLE"),
        (200, {"unbounded": "x" * 70000}, 503, "DEPENDENCY_UNAVAILABLE"),
    ],
)
def test_status_dependency_errors_are_explicit_and_never_fake_pending(
    tmp_path,
    upstream_code,
    value,
    want,
    code,
):
    from api.auth import RegistryFailure
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: response(upstream_code, value))
        ) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            with pytest.raises(RegistryFailure) as caught:
                await client.get("e-1", correlation_id="cor-test")
            assert (caught.value.status, caught.value.code) == (want, code)

    asyncio.run(run())


def test_status_timeout_is_504_and_opaque_event_id_is_encoded(tmp_path):
    from api.auth import RegistryFailure
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")

    async def run():
        def upstream(request):
            assert b"e%2Fone%20%E6%A9%9F" in request.url.raw_path
            raise httpx.ReadTimeout("fixture timeout", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            with pytest.raises(RegistryFailure) as caught:
                await client.get("e/one 機", correlation_id="cor-test")
            assert (caught.value.status, caught.value.code) == (504, "DEPENDENCY_TIMEOUT")

    asyncio.run(run())


@pytest.mark.parametrize("event_id,encoded", [(".", "%2E"), ("..", "%2E%2E")])
def test_status_preserves_dot_only_opaque_event_ids(tmp_path, event_id, encoded):
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")
    value = {**status(), "event_id": event_id}

    async def run():
        def upstream(request):
            assert request.url.raw_path == (
                f"/api/v1/internal/sensor-events/{encoded}/status".encode()
            )
            return response(200, value)

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            assert await client.get(event_id, correlation_id="cor-test") == value

    asyncio.run(run())


def test_facade_returns_503_for_excessively_nested_upstream_json(signing_key, tmp_path):
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")
    raw = b"[" * 20000 + b"0" + b"]" * 20000

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, stream=Body(raw), headers={"Content-Type": "application/json"}
                )
            )
        ) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            app = create_app(RegistryFixture(), verifier(signing_key), status_client=client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as caller:
                result = await caller.get(
                    "/api/v1/sensor-events/e-1/status",
                    headers={"Authorization": token(signing_key)},
                )
                assert result.status_code == 503
                assert result.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"
                assert result.json()["error"]["retryable"] is True

    asyncio.run(run())


@pytest.mark.parametrize("upstream_code,want", [(404, 404), (504, 504)])
def test_facade_eventual_missing_and_timeout_errors_are_retryable(
    signing_key, tmp_path, upstream_code, want
):
    from business_processing_client import ProcessingStatusClient

    credential = tmp_path / "service-token"
    credential.write_text("fixture-service-jwt")

    async def run():
        def upstream(request):
            if upstream_code == 504:
                raise httpx.ReadTimeout("fixture timeout", request=request)
            return response(
                404,
                {
                    "error": {
                        "code": "EVENT_NOT_MATERIALIZED",
                        "message": "not materialized",
                        "retryable": True,
                    }
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            client = ProcessingStatusClient(
                http, base_url="https://processing.test", token_file=credential
            )
            app = create_app(RegistryFixture(), verifier(signing_key), status_client=client)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as caller:
                result = await caller.get(
                    "/api/v1/sensor-events/e-1/status",
                    headers={
                        "Authorization": token(signing_key),
                    },
                )
                assert result.status_code == want
                assert result.json()["error"]["retryable"] is True

    asyncio.run(run())
