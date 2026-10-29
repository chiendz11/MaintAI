"""Bounded subscriptions isolate slow clients and preserve scoped delivery."""

import asyncio
import json
import time

import pytest
from backend.shared.notifications import machine_key
from business_sse.hub import NotificationHub, SSESettings


class Repository:
    def __init__(self):
        self.calls = []

    def live(self, machine):
        self.calls.append(machine)
        return {
            "machine_id": machine,
            "telemetry": {
                "event_id": "e-1",
                "stream_epoch": "z",
                "sequence_number": 2**80 + 1,
                "server_accepted_at": "2026-10-28T00:00:00Z",
                "received_at": "2026-10-28T00:00:00Z",
                "features_raw": {"raw": "\x00value"},
                "validation_status": "UNVERIFIED",
            },
            "latest_valid_prediction": None,
            "latest_processing": None,
            "as_of": "2026-10-28T00:00:00Z",
        }


def test_hub_reads_once_and_fans_out_only_to_authorized_subscribers():
    async def run():
        repository = Repository()
        hub = NotificationHub(repository, dsn="unused", settings=SSESettings(coalesce_seconds=0))
        hub.ready = True
        first = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
        second = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
        forbidden = hub.subscribe(frozenset({"M-002"}), expires_at=time.time() + 60)
        hub.invalidate(machine_key("M-001"))
        await hub.flush()
        assert repository.calls == ["M-001"]
        frame = await hub.next_frame(first, timeout=0.1)
        assert frame == await hub.next_frame(second, timeout=0.1)
        assert b"1208925819614629174706177" in frame
        value = json.loads(frame.split(b"data: ", 1)[1])
        assert value["type"] == "telemetry.received"
        assert value["data"]["sequence_number"] == 2**80 + 1
        assert await hub.next_frame(forbidden, timeout=0.01) == b": heartbeat\n\n"
        hub.invalidate(machine_key("M-001"))
        await hub.flush()
        assert await hub.next_frame(first, timeout=0.01) == b": heartbeat\n\n"
        await hub.stop()
        assert hub.pending_bytes == 0

    asyncio.run(run())


def test_slow_subscriber_is_closed_without_dropping_fast_subscriber():
    async def run():
        repository = Repository()
        hub = NotificationHub(
            repository,
            dsn="unused",
            settings=SSESettings(coalesce_seconds=0, connection_bytes=1200, total_bytes=5000),
        )
        hub.ready = True
        slow = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
        fast = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
        original = repository.live
        sequence = 0

        def changed(machine):
            nonlocal sequence
            sequence += 1
            value = original(machine)
            value["telemetry"]["sequence_number"] = sequence
            value["telemetry"]["event_id"] = f"e-{sequence}"
            return value

        repository.live = changed
        for _ in range(6):
            hub.invalidate(machine_key("M-001"))
            await hub.flush()
            assert await hub.next_frame(fast, timeout=0.1)
        assert slow.closed
        assert not fast.closed
        await hub.stop()
        assert hub.pending_bytes == 0

    asyncio.run(run())


def test_expired_identity_and_admission_limit_fail_closed():
    from api.auth import RegistryFailure

    hub = NotificationHub(Repository(), dsn="unused", settings=SSESettings(max_connections=1))
    with pytest.raises(RegistryFailure) as failure:
        hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
    assert failure.value.status == 503
    hub.ready = True
    with pytest.raises(RegistryFailure) as failure:
        hub.subscribe(frozenset({"M-001"}), expires_at=time.time() - 1)
    assert failure.value.status == 401
    first = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
    with pytest.raises(RegistryFailure) as failure:
        hub.subscribe(frozenset({"M-002"}), expires_at=time.time() + 60)
    assert failure.value.status == 429
    hub.unsubscribe(first)
    assert hub.pending_bytes == 0


def test_snapshot_for_another_machine_never_crosses_subscription_scope():
    async def run():
        repository = Repository()
        original = repository.live

        def wrong(machine):
            value = original(machine)
            value["machine_id"] = "M-002"
            return value

        repository.live = wrong
        hub = NotificationHub(repository, dsn="unused", settings=SSESettings(coalesce_seconds=0))
        hub.ready = True
        subscription = hub.subscribe(frozenset({"M-001"}), expires_at=time.time() + 60)
        hub.invalidate(machine_key("M-001"))
        await hub.flush()
        assert subscription.closed
        assert await hub.next_frame(subscription) is None
        await hub.stop()

    asyncio.run(run())
