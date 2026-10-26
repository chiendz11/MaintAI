"""Registry HTTP fixture follows existing Business GET; no Business code changes."""

import asyncio
import gzip
import json
from pathlib import Path

import httpx
import pytest
from processing.shared.data_quality.machine_registry import HTTPMachineRegistry

ROOT = Path(__file__).resolve().parents[3]
DETAIL = json.loads((ROOT / "contracts/v1.3/examples/http/backend/machine-detail.json").read_text())
NOT_FOUND = json.loads(
    (ROOT / "contracts/v1.3/examples/http/errors/machine_not_found.json").read_text()
)


def lookup(handler, *, twice=False, **kwargs):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            registry = HTTPMachineRegistry(
                http,
                base_url="http://127.0.0.1:8000",
                schema_directory=ROOT / "contracts/v1.3/schemas",
                local_fixture=True,
                **kwargs,
            )
            result = await registry.lookup("M-001", correlation_id="corr-1")
            if twice:
                result = await registry.lookup("M-001", correlation_id="corr-2")
            return result

    return asyncio.run(run())


def test_provisioned_machine_is_found_using_existing_business_route():
    def handler(request):
        assert request.url.path == "/api/v1/machines/M-001"
        assert request.headers["X-Correlation-ID"] == "corr-1"
        return httpx.Response(200, json=DETAIL)

    assert lookup(handler).status == "FOUND"


def test_only_typed_not_found_marks_machine_unprovisioned():
    assert lookup(lambda request: httpx.Response(404, json=NOT_FOUND)).status == "NOT_FOUND"


@pytest.mark.parametrize(
    "status,payload",
    [
        (401, NOT_FOUND),
        (403, NOT_FOUND),
        (503, NOT_FOUND),
        (404, {}),
        (200, {"machine_id": "M-001"}),
        (200, {**DETAIL, "machine_id": "wrong"}),
    ],
)
def test_outage_auth_or_malformed_response_is_not_not_found(status, payload):
    result = lookup(lambda request: httpx.Response(status, json=payload))
    assert result.status == "UNAVAILABLE"


def test_cache_is_bounded_and_explicit():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=DETAIL)

    assert lookup(handler, twice=True, cache_seconds=1, max_cache_entries=1).status == "FOUND"
    assert len(calls) == 1


def test_expired_cache_cannot_hide_registry_outage():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=DETAIL) if len(calls) == 1 else httpx.Response(503)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            registry = HTTPMachineRegistry(
                http,
                base_url="http://127.0.0.1:8000",
                schema_directory=ROOT / "contracts/v1.3/schemas",
                local_fixture=True,
                cache_seconds=0.01,
            )
            assert (await registry.lookup("M-001", correlation_id="corr")).status == "FOUND"
            await asyncio.sleep(0.02)
            return await registry.lookup("M-001", correlation_id="corr")

    assert asyncio.run(run()).status == "UNAVAILABLE"


def test_lookup_has_total_deadline():
    async def handler(request):
        await asyncio.sleep(10)
        return httpx.Response(200, json=DETAIL)

    result = lookup(handler, timeout_seconds=0.02)
    assert result.status == "UNAVAILABLE"
    assert result.failure_code == "REGISTRY_TIMEOUT"


@pytest.mark.parametrize(
    "compressed",
    [b"not-gzip", gzip.compress(b"x" * (8 * 1024 * 1024))],
    ids=["malformed", "expansion"],
)
def test_encoded_registry_response_cannot_allocate_unbounded_decompression(compressed):
    class CountingStream(httpx.AsyncByteStream):
        reads = 0

        async def __aiter__(self):
            self.reads += 1
            yield compressed

    stream = CountingStream()
    result = lookup(
        lambda request: httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=stream)
    )
    assert result.status == "UNAVAILABLE"
    assert result.failure_code == "REGISTRY_INVALID_RESPONSE"
    assert stream.reads == 0
