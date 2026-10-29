"""Live snapshot is separate from the baseline registry DTO and supports opaque IDs."""

import asyncio
from urllib.parse import quote

import httpx
import pytest
from api.app import create_app

from .test_registry_auth import token, verifier


class Registry:
    def get(self, machine_id):
        return {
            "machine_id": machine_id,
            "display_name": "Fixture",
            "state": "UNKNOWN",
            "last_seen_at": None,
        }

    def check(self):
        return True


class Projections:
    def detail(self, machine_id):
        return {
            **Registry().get(machine_id),
            "data_status": None,
            "latest_sensor": None,
            "latest_prediction": None,
            "active_alerts": [],
        }

    def live(self, machine_id):
        if machine_id == "missing":
            return None
        return {
            "machine_id": machine_id,
            "telemetry": None,
            "latest_valid_prediction": None,
            "latest_processing": None,
            "as_of": "2026-10-26T00:00:00Z",
        }


@pytest.mark.parametrize("machine_id", ["M-001", "cell/press", "cell/live", "space 機", "live"])
def test_live_and_detail_routes_preserve_encoded_opaque_machine_ids(signing_key, machine_id):
    async def run():
        app = create_app(Registry(), verifier(signing_key), projections=Projections())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            headers = {"Authorization": token(signing_key, machines=[machine_id])}
            path = "/api/v1/machines/" + quote(machine_id, safe="")
            detail = await client.get(path, headers=headers)
            assert detail.status_code == 200
            assert detail.json()["machine_id"] == machine_id
            assert "display_name" in detail.json()
            live = await client.get(path + "/live", headers=headers)
            assert live.status_code == 200
            assert live.json()["machine_id"] == machine_id
            assert "display_name" not in live.json()
            assert live.json()["latest_valid_prediction"] is None

    asyncio.run(run())


@pytest.mark.parametrize(
    "machine,claims,want",
    [
        ("missing", {"machines": ["missing"]}, 404),
        ("M-002", {}, 403),
        ("M-001", {"roles": ["SENSOR_SERVICE"]}, 403),
    ],
)
def test_live_requires_existing_machine_and_registry_read_permissions(
    signing_key, machine, claims, want
):
    async def run():
        app = create_app(Registry(), verifier(signing_key), projections=Projections())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                f"/api/v1/machines/{machine}/live",
                headers={
                    "Authorization": token(signing_key, **claims),
                },
            )
            assert result.status_code == want

    asyncio.run(run())
