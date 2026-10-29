"""A gap during awaited SQL must not kill the SSE pump or leak queue budgets."""

import asyncio
import json
import time
from types import SimpleNamespace

from backend.shared.notifications import machine_key
from business_sse.hub import NotificationHub


def test_disconnect_during_alert_read_can_recover_on_the_next_subscription():
    async def run():
        loop = asyncio.get_running_loop()
        hub = NotificationHub(None, dsn="unused")

        def read(identifier, machine):
            loop.call_soon_threadsafe(hub.disconnect_all, "NOTIFY_LOST")
            return {
                "schema_version": "1.0.0",
                "type": "alert.created",
                "machine_id": machine,
                "occurred_at": "2026-10-28T00:00:00Z",
                "data": {
                    "alert_id": "a",
                    "machine_id": machine,
                    "severity": "HIGH",
                    "status": "OPEN",
                    "updated_at": "2026-10-28T00:00:00Z",
                },
            }

        hub.alert_repository = SimpleNamespace(notification=read)
        hub.ready = True
        subscription = hub.subscribe({"M-001"}, expires_at=time.time() + 60)
        for identifier in ("a" * 32, "b" * 32):
            hub.invalidate_alert(
                json.dumps(
                    {
                        "version": 1,
                        "machine_key": machine_key("M-001"),
                        "notification_id": identifier,
                    }
                )
            )
        await hub.flush_alerts()
        assert subscription.closed and hub.pending_bytes == 0
        assert not hub.alert_hints

    asyncio.run(run())


def test_alert_hint_overflow_closes_stream_and_releases_budget():
    hub = NotificationHub(None, dsn="unused")
    hub.ready = True
    subscription = hub.subscribe({"M-001"}, expires_at=time.time() + 60)
    for number in range(129):
        hub.invalidate_alert(
            json.dumps(
                {
                    "version": 1,
                    "machine_key": machine_key("M-001"),
                    "notification_id": f"{number:032x}",
                }
            )
        )
    assert subscription.closed and not hub.alert_hints and hub.pending_bytes == 0
