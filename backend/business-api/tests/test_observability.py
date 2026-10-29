"""Business HTTP metrics use finite templates and never change response semantics."""

import asyncio
import logging

import httpx
from api.app import create_app

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


def test_business_http_metrics_exclude_probes_and_do_not_label_machine_ids(signing_key):
    app = create_app(RegistryFixture(), verifier(signing_key))

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (
                await client.get(
                    "/api/v1/machines/M-001",
                    headers={
                        "Authorization": token(signing_key),
                    },
                )
            ).status_code == 200
            await client.get("/health/live")
            result = await client.get("/metrics")
            assert result.status_code == 200
            assert "M-001" not in result.text
            value = app.state.metrics.registry.get_sample_value(
                "maintai_http_requests_total",
                {
                    "route": "/api/v1/machines/{machine_id:path}",
                    "method": "GET",
                    "status": "200",
                },
            )
            assert value == 1

    asyncio.run(run())


def test_business_instrumentation_failure_does_not_change_success(signing_key, monkeypatch):
    app = create_app(RegistryFixture(), verifier(signing_key))

    def broken(*args, **kwargs):
        raise RuntimeError("instrumentation failed")

    monkeypatch.setattr(app.state.metrics, "http", broken)
    monkeypatch.setattr(logging.getLogger("maintai.business.registry"), "info", broken)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                "/api/v1/machines/M-001",
                headers={
                    "Authorization": token(signing_key),
                },
            )
            assert result.status_code == 200

    asyncio.run(run())


def test_business_unmatched_route_uses_catalog_label(signing_key):
    app = create_app(RegistryFixture(), verifier(signing_key))

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as http:
            assert (await http.get("/unknown-fixture-id")).status_code == 404
        assert (
            app.state.metrics.registry.get_sample_value(
                "maintai_http_requests_total",
                {
                    "route": "__unmatched__",
                    "method": "GET",
                    "status": "404",
                },
            )
            == 1
        )

    asyncio.run(run())
