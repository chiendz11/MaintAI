"""History routes authorize the decoded machine without stealing encoded detail IDs."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
from api.app import create_app
from api.history import HistoryFacade
from business_processing_client.history_schemas import HistorySchemas

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


def test_history_dispatch_enforces_profile_and_machine_scope(signing_key):
    class Facade:
        async def machine(self, identity, machine, resource, request):
            return {"machine": machine, "resource": resource}

        async def aclose(self):
            pass

    async def run():
        app = create_app(RegistryFixture(), verifier(signing_key), history=Facade())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            headers = {
                "Authorization": token(signing_key),
                "X-MaintAI-History-Profile": "M6-HISTORY-01/1.0.0",
            }
            response = await client.get("/api/v1/machines/M-001/predictions", headers=headers)
            assert response.status_code == 200
            assert response.json() == {"machine": "M-001", "resource": "predictions"}
            assert (
                await client.get("/api/v1/machines/M-002/predictions", headers=headers)
            ).status_code == 403
            assert (
                await client.get(
                    "/api/v1/machines/M-001/predictions",
                    headers={"Authorization": headers["Authorization"]},
                )
            ).status_code == 400
            # Literal fixed suffixes select resources; encoded slash belongs to the ID.
            detail = await client.get(
                "/api/v1/machines/M-001%2Fpredictions",
                headers={
                    **headers,
                    "Authorization": token(signing_key, machines=["M-001/predictions"]),
                },
            )
            assert detail.status_code == 404

    asyncio.run(run())


def test_facade_validates_provider_page_and_machine_identity(signing_key):
    root = Path(__file__).resolve().parents[3]
    page = json.loads((root / "contracts/v1.3/examples/m6/processing-page.json").read_text())
    seen = []

    class Provider:
        async def request(self, actor, method, resource, **kwargs):
            seen.append((actor.subject, resource))
            return SimpleNamespace(value=page, status=200)

        async def aclose(self):
            pass

    facade = HistoryFacade(Provider(), HistorySchemas(root / "contracts/v1.3/schemas"))

    async def run():
        app = create_app(RegistryFixture(), verifier(signing_key), history=facade)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            headers = {
                "Authorization": token(signing_key),
                "X-MaintAI-History-Profile": "M6-HISTORY-01/1.0.0",
            }
            result = await client.get("/api/v1/machines/M-001/predictions", headers=headers)
            assert result.status_code == 200, result.text
            assert result.json() == page
            wrong_machine = {**headers, "Authorization": token(signing_key, machines=["different"])}
            result = await client.get(
                "/api/v1/machines/different/predictions", headers=wrong_machine
            )
            assert result.status_code == 503
        assert seen[0][1] == "machines/M-001/predictions"

    asyncio.run(run())
