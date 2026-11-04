"""Public brief paths preserve opaque Alert IDs and existing action routes."""

import asyncio
from urllib.parse import quote

import httpx
import pytest
from api.app import create_app

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


@pytest.mark.parametrize("identifier", ["one", "máy/brief", "brief", "a/ack"])
def test_brief_resource_does_not_shadow_encoded_detail_ids(signing_key, identifier):
    class Alerts:
        metrics = None

        def detail(self, aid, machines):
            return {"id": aid}

    class Briefs:
        def facade(self, aid, machines):
            return {"id": aid, "copilot_status": "NOT_REQUESTED"}

        def enqueue(self, aid, machines, actor, key, intent):
            assert key == "same-key"
            return {"id": aid, "job_id": "job"}

    async def run():
        app = create_app(RegistryFixture(), verifier(signing_key), alerts=Alerts(), briefs=Briefs())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            viewer = {"Authorization": token(signing_key)}
            tech = {
                "Authorization": token(signing_key, roles=["TECHNICIAN"]),
                "Idempotency-Key": "same-key",
            }
            url = "/api/v1/alerts/" + quote(identifier, safe="")
            assert (await c.get(url, headers=viewer)).json() == {"id": identifier}
            assert (await c.get(url + "/brief", headers=viewer)).json()["id"] == identifier
            assert (
                await c.post(url + "/brief", headers=viewer, json={"intent": "MAINTENANCE_BRIEF"})
            ).status_code == 403
            response = await c.post(
                url + "/brief", headers=tech, json={"intent": "MAINTENANCE_BRIEF"}
            )
            assert response.status_code == 202 and response.json()["id"] == identifier

    asyncio.run(run())
