"""Registry-only API behavior; projections remain outside M2.R."""

import asyncio
import json
from pathlib import Path

import httpx
import psycopg
import pytest
from api.app import create_app
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from .test_registry_auth import token, verifier

ROOT = Path(__file__).resolve().parents[3]


class RegistryFixture:
    def __init__(self):
        self.lookups = []
        self.down = False

    def get(self, machine):
        self.lookups.append(machine)
        if self.down:
            raise psycopg.OperationalError("secret database URL")
        if machine == "M-001":
            return {
                "machine_id": machine,
                "display_name": "Fixture machine",
                "state": "UNKNOWN",
                "last_seen_at": None,
            }
        return None

    def check(self):
        return not self.down


@pytest.mark.parametrize(
    "machine,claims,status,code",
    [
        ("M-001", {}, 200, None),
        ("missing", {"machines": ["missing"]}, 404, "MACHINE_NOT_FOUND"),
        ("M-002", {}, 403, "FORBIDDEN"),
        ("M-001", {"roles": ["SENSOR_SERVICE"]}, 403, "FORBIDDEN"),
        ("M-001", {"exp": 1}, 401, "UNAUTHORIZED"),
    ],
)
def test_get_machine_enforces_auth_scope_and_baseline_schema(
    signing_key, machine, claims, status, code
):
    async def run():
        repository = RegistryFixture()
        app = create_app(repository, verifier(signing_key))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/api/v1/machines/" + machine,
                headers={
                    "Authorization": token(signing_key, **claims),
                    "X-Correlation-ID": "cor-test",
                },
            )
        assert response.status_code == status
        assert response.headers["X-Correlation-ID"] == "cor-test"
        if code:
            assert response.json()["error"]["code"] == code
        if status in (401, 403):
            assert repository.lookups == []
        if status == 200:
            directory = ROOT / "contracts/v1.3/schemas"
            schemas = Registry().with_resources(
                [
                    (p.as_uri(), Resource.from_contents(json.loads(p.read_text())))
                    for p in directory.glob("*.schema.json")
                ]
            )
            Draft202012Validator(
                {"$ref": (directory / "business.schema.json").as_uri() + "#/$defs/MachineDetail"},
                registry=schemas,
            ).validate(response.json())
            assert response.json()["state"] == "UNKNOWN"
            assert response.json()["latest_prediction"] is None
            assert response.json()["data_status"] is None

    asyncio.run(run())


def test_database_outage_returns_sanitized_dependency_failure(signing_key):
    async def run():
        repository = RegistryFixture()
        repository.down = True
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(repository, verifier(signing_key))),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/v1/machines/M-001", headers={"Authorization": token(signing_key)}
            )
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"
            assert "secret" not in response.text
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).status_code == 503

    asyncio.run(run())


@pytest.mark.parametrize("down", [False, True])
def test_business_probes_match_existing_health_contract(signing_key, down):
    async def run():
        repository = RegistryFixture()
        repository.down = down
        common = json.loads((ROOT / "contracts/v1.3/schemas/common.schema.json").read_text())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(repository, verifier(signing_key))),
            base_url="http://test",
        ) as http:
            for path, name in (("live", "Liveness"), ("ready", "Readiness")):
                result = await http.get("/health/" + path)
                assert result.status_code == (503 if path == "ready" and down else 200)
                Draft202012Validator(common["$defs"][name]).validate(result.json())

    asyncio.run(run())
