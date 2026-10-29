"""Public Alert endpoints validate roles, actor identity, body and immutable DTOs."""

import asyncio
import importlib

import httpx
import pytest
from api.app import create_app
from backend.shared.alerts.repository import AlertRepository
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository

from .test_registry_auth import token, verifier

pytestmark = pytest.mark.integration


def test_alert_http_lifecycle_and_fleet_are_backed_by_owner_data(registry_database, signing_key):
    db = registry_database
    upgrade(db.dsn, schema=db.schema)
    registry = RegistryRepository(db.factory)
    registry.provision([{"machine_id": "M-001", "display_name": "One"}])
    helper = importlib.import_module("backend.business-consumer.tests.test_alert_lifecycle")
    helper.deliver_alert(db)
    alerts = AlertRepository(db.factory)
    app = create_app(
        registry,
        verifier(signing_key),
        alerts=alerts,
        projections=ProjectionRepository(db.factory, alert_reader=alerts.active),
    )

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            viewer = {"Authorization": token(signing_key)}
            tech = {"Authorization": token(signing_key, roles=["TECHNICIAN"])}
            page = (await c.get("/api/v1/alerts", headers=viewer)).json()
            assert page["total"] == 1
            aid = page["items"][0]["alert_id"]
            url = "/api/v1/alerts/" + aid
            assert (await c.get("/api/v1/machines", headers=viewer)).json()["items"][0][
                "active_alert_count"
            ] == 1
            assert (
                await c.post(url + "/ack", headers=viewer, json={"actor_id": "reader"})
            ).status_code == 403
            assert (
                await c.post(url + "/ack", headers=tech, json={"actor_id": "forged"})
            ).status_code == 403
            assert (
                await c.post(url + "/ack", headers=tech, json={"actor_id": "reader", "extra": 1})
            ).status_code == 422
            assert (
                await c.post(url + "/ack", headers=tech, json={"actor_id": "reader"})
            ).status_code == 200
            assert (
                await c.post(url + "/ack", headers=tech, json={"actor_id": "reader"})
            ).status_code == 409
            assert (
                await c.post(url + "/resolve", headers=tech, json={"actor_id": "reader"})
            ).status_code == 422
            assert (
                await c.post(
                    url + "/resolve",
                    headers=tech,
                    json={"actor_id": "reader", "resolution": "Fixed"},
                )
            ).status_code == 200
            assert (
                await c.post(
                    url + "/feedback",
                    headers=tech,
                    json={
                        "technician_verdict": "CONFIRMED_ISSUE",
                        "recommendation_feedback": "USEFUL",
                    },
                )
            ).status_code == 201
            detail = (await c.get(url, headers=viewer)).json()
            assert detail["alert"]["status"] == "RESOLVED"
            assert len(detail["history"]) == 3
            fleet = (await c.get("/api/v1/machines", headers=viewer)).json()
            assert fleet["items"][0]["active_alert_count"] == 0
            assert (
                await c.get(url, headers={"Authorization": token(signing_key, machines=["other"])})
            ).status_code == 403
            assert (
                await c.get(
                    "/api/v1/alerts",
                    headers={"Authorization": token(signing_key, machines=["other"])},
                )
            ).json()["total"] == 0
            assert (
                await c.get(url, headers={"Authorization": token(signing_key, exp=1)})
            ).status_code == 401
            assert (await c.get("/api/v1/alerts?limit=201", headers=viewer)).status_code == 422
            assert (await c.get("/api/v1/alerts?machine_id=missing", headers=viewer)).json()[
                "total"
            ] == 0
            assert (await c.get("/api/v1/alerts/missing", headers=viewer)).status_code == 404

    asyncio.run(run())
