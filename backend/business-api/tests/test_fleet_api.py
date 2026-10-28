"""Fleet routing, scoped reads, pagination and explicit missing-source failures."""

import asyncio

import httpx
import pytest
from api.app import create_app
from backend.shared.projections.views import MachineViewUnavailable

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


class Views:
    def __init__(self, *, down=False):
        self.calls = []
        self.down = down

    def fleet(self, machines, **page):
        self.calls.append((machines, page))
        if self.down:
            raise MachineViewUnavailable("private source failure")
        return {"items": [], "total": 0}

    def detail(self, machine):
        self.calls.append(machine)
        raise MachineViewUnavailable("private source failure")

    def check(self):
        return True


def test_fleet_passes_verified_scope_and_pagination_before_repository_read(signing_key):
    async def run():
        views = Views()
        app = create_app(RegistryFixture(), verifier(signing_key), projections=views)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            headers = {"Authorization": token(signing_key, machines=["M-001", "M-003"])}
            result = await client.get(
                "/api/v1/machines?state=HIGH&limit=20&offset=2", headers=headers
            )
            assert result.status_code == 200
            assert result.json() == {"items": [], "total": 0}
            assert views.calls == [
                (frozenset({"M-001", "M-003"}), {"state": "HIGH", "limit": 20, "offset": 2})
            ]
            await client.get("/api/v1/machines", headers=headers)
            assert views.calls[-1][1] == {"state": None, "limit": 50, "offset": 0}

    asyncio.run(run())


@pytest.mark.parametrize("query", ["limit=0", "limit=201", "offset=-1", "state=INVALID_DATA"])
def test_fleet_rejects_invalid_pagination_and_state_without_database_read(signing_key, query):
    async def run():
        views = Views()
        app = create_app(RegistryFixture(), verifier(signing_key), projections=views)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                "/api/v1/machines?" + query, headers={"Authorization": token(signing_key)}
            )
            assert result.status_code == 422
            assert result.json()["error"]["code"] == "VALIDATION_FAILED"
            assert views.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("claims,want", [({"roles": ["SENSOR_SERVICE"]}, 403), ({"exp": 1}, 401)])
def test_fleet_auth_failure_prevents_database_read(signing_key, claims, want):
    async def run():
        views = Views()
        app = create_app(RegistryFixture(), verifier(signing_key), projections=views)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                "/api/v1/machines", headers={"Authorization": token(signing_key, **claims)}
            )
            assert result.status_code == want
            assert views.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("path", ["/api/v1/machines", "/api/v1/machines/M-001"])
def test_missing_required_read_source_returns_sanitized_retryable_503(signing_key, path):
    async def run():
        app = create_app(RegistryFixture(), verifier(signing_key), projections=Views(down=True))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(path, headers={"Authorization": token(signing_key)})
            assert result.status_code == 503
            assert result.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"
            assert result.json()["error"]["retryable"] is True
            assert "private" not in result.text
            assert (
                app.state.metrics.registry.get_sample_value(
                    "maintai_dependency_errors_total",
                    {"dependency": "business_db", "error_type": "UNAVAILABLE"},
                )
                is None
            )

    asyncio.run(run())
