"""SSE authorizes before streaming and retains a verified expiry deadline."""

import asyncio
import time

import httpx
import pytest
from api.app import create_app
from business_sse.hub import NotificationHub, SSESettings

from .test_registry_api import RegistryFixture
from .test_registry_auth import token, verifier


def test_identity_exposes_verified_expiry_for_long_lived_stream(signing_key):
    expiry = int(time.time()) + 90
    identity = verifier(signing_key).verify(token(signing_key, exp=expiry))
    assert identity.expires_at == expiry


@pytest.mark.parametrize("expiry", [True, 10**500])
def test_unusable_token_expiry_is_unauthorized_instead_of_server_error(signing_key, expiry):
    from api.auth import RegistryFailure

    with pytest.raises(RegistryFailure) as caught:
        verifier(signing_key).verify(token(signing_key, exp=expiry))
    assert caught.value.status == 401


def test_readiness_explains_missing_listener(signing_key):
    async def run():
        hub = NotificationHub(RegistryFixture(), dsn="unused")
        app = create_app(RegistryFixture(), verifier(signing_key), notifications=hub)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get("/health/ready")
            assert result.status_code == 503
            assert result.json()["checks"]["notifications"] is False

    asyncio.run(run())


@pytest.mark.parametrize(
    "claims,machine,want",
    [
        ({"exp": 1}, "M-001", 401),
        ({"roles": ["SENSOR_SERVICE"]}, "M-001", 403),
        ({}, "M-002", 403),
        ({}, "M-001", 503),
    ],
)
def test_sse_denies_before_handshake_when_auth_scope_or_listener_unavailable(
    signing_key, claims, machine, want
):
    async def run():
        hub = NotificationHub(RegistryFixture(), dsn="unused")
        app = create_app(RegistryFixture(), verifier(signing_key), notifications=hub)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            result = await client.get(
                "/api/v1/events/stream",
                params={"machine_id": machine},
                headers={"Authorization": token(signing_key, **claims)},
            )
            assert result.status_code == want
            assert result.json()["error"]["code"] in {
                "UNAUTHORIZED",
                "FORBIDDEN",
                "DEPENDENCY_UNAVAILABLE",
            }
            assert not hub.subscriptions

    asyncio.run(run())


def test_stream_cancels_at_expiry_and_releases_admission_budget():
    async def run():
        hub = NotificationHub(RegistryFixture(), dsn="unused", settings=SSESettings())
        hub.ready = True
        subscription = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 0.02)
        assert await hub.next_frame(subscription) is None
        assert subscription.closed
        assert not hub.subscriptions

    asyncio.run(run())


def test_saturated_sse_admission_is_retryable(signing_key):
    async def run():
        hub = NotificationHub(
            RegistryFixture(), dsn="unused", settings=SSESettings(max_connections=1)
        )
        hub.ready = True
        occupied = hub.subscribe({"M-001"}, expires_at=time.time() + 60)
        app = create_app(RegistryFixture(), verifier(signing_key), notifications=hub)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                result = await client.get(
                    "/api/v1/events/stream?machine_id=M-001",
                    headers={"Authorization": token(signing_key)},
                )
                assert result.status_code == 429
                assert result.json()["error"]["retryable"] is True
        finally:
            hub.unsubscribe(occupied)

    asyncio.run(run())
