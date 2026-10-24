"""Real PostgreSQL migrations/provisioning with no production seed data."""

import asyncio
from pathlib import Path

import httpx
import pytest
from api.app import create_app
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.registry import RegistryRepository
from processing.shared.data_quality.machine_registry import HTTPMachineRegistry

from .test_registry_auth import token, verifier

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def test_registry_migration_is_repeatable_and_initially_empty(registry_database):
    upgrade(registry_database.dsn, schema=registry_database.schema)
    upgrade(registry_database.dsn, schema=registry_database.schema)
    repo = RegistryRepository(registry_database.factory)
    assert repo.check()
    assert repo.get("M-001") is None
    with registry_database.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM machines").fetchone()["n"] == 0


def test_explicit_provisioning_updates_metadata_without_resetting_state(registry_database):
    upgrade(registry_database.dsn, schema=registry_database.schema)
    repo = RegistryRepository(registry_database.factory)
    machines = [{"machine_id": "M-001", "display_name": "Machine one"}]
    repo.provision(machines)
    repo.provision(machines)
    with registry_database.factory() as conn:
        conn.execute("UPDATE machines SET state='CRITICAL' WHERE machine_id='M-001'")
    repo.provision([{**machines[0], "display_name": "Renamed"}])
    assert repo.get("M-001")["display_name"] == "Renamed"
    assert repo.get("M-001")["state"] == "CRITICAL"
    with registry_database.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM machines").fetchone()["n"] == 1


@pytest.mark.parametrize(
    "bad",
    [
        {"machine_id": "", "display_name": "Empty"},
        {"machine_id": "bad\x00id", "display_name": "NUL"},
        {"machine_id": "M-002", "display_name": "Two", "state": "NORMAL"},
    ],
)
def test_invalid_manifest_cannot_partially_provision_machines(registry_database, bad):
    upgrade(registry_database.dsn, schema=registry_database.schema)
    repo = RegistryRepository(registry_database.factory)
    with pytest.raises(ValueError):
        repo.provision([{"machine_id": "M-001", "display_name": "One"}, bad])
    assert repo.get("M-001") is None


def test_processing_adapter_reads_business_implementation_with_real_database(
    registry_database, signing_key
):
    upgrade(registry_database.dsn, schema=registry_database.schema)
    repo = RegistryRepository(registry_database.factory)
    repo.provision([{"machine_id": "M-001", "display_name": "Test-only machine"}])
    authorization = token(signing_key, machines=["M-001", "missing"])

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(repo, verifier(signing_key)))
        ) as http:
            registry = HTTPMachineRegistry(
                http,
                base_url="https://business.test",
                schema_directory=ROOT / "contracts/v1.3/schemas",
                bearer_token=authorization.removeprefix("Bearer "),
            )
            assert (await registry.lookup("M-001", correlation_id="cor-test")).status == "FOUND"
            assert (
                await registry.lookup("missing", correlation_id="cor-test")
            ).status == "NOT_FOUND"
            assert (
                await registry.lookup("outside-scope", correlation_id="cor-test")
            ).status == "UNAVAILABLE"
            assert repo.get("missing") is None

    asyncio.run(run())


def test_opaque_slash_machine_ids_work_through_processing_adapter(registry_database, signing_key):
    upgrade(registry_database.dsn, schema=registry_database.schema)
    repo = RegistryRepository(registry_database.factory)
    repo.provision([{"machine_id": "cell/press-1", "display_name": "Slash fixture"}])
    authorization = token(signing_key, machines=["cell/press-1", "missing/press"])

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(repo, verifier(signing_key)))
        ) as http:
            client = HTTPMachineRegistry(
                http,
                base_url="https://business.test",
                schema_directory=ROOT / "contracts/v1.3/schemas",
                bearer_token=authorization.removeprefix("Bearer "),
            )
            assert (
                await client.lookup("cell/press-1", correlation_id="cor-test")
            ).status == "FOUND"
            assert (
                await client.lookup("missing/press", correlation_id="cor-test")
            ).status == "NOT_FOUND"
            assert (
                await client.lookup("other/press", correlation_id="cor-test")
            ).status == "UNAVAILABLE"

    asyncio.run(run())
