import asyncio
import json
import time

from backend.shared.notifications import machine_key
from business_sse.hub import NotificationHub


def test_m7_fanout_scopes_brief_and_delivers_global_drift():
    async def run():
        class Reader:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def execute(self, *args):
                return self

            def fetchone(self):
                return {"event": json.dumps(event).encode()}

        hub = NotificationHub(None, dsn="unused", m7_factory=Reader)
        hub.ready = True
        first = hub.subscribe({"M-001"}, expires_at=time.time() + 60)
        other = hub.subscribe({"M-002"}, expires_at=time.time() + 60)
        event = {
            "schema_version": "1.0.0",
            "type": "brief.ready",
            "machine_id": "M-001",
            "occurred_at": "2026-11-03T00:00:00Z",
            "data": {"job_id": "job", "alert_id": "alert", "updated_at": "2026-11-03T00:00:00Z"},
        }
        hint = {"version": 1, "machine_key": machine_key("M-001"), "notification_id": "a" * 32}
        hub.invalidate_m7(json.dumps(hint))
        await hub.flush_m7()
        assert b"brief.ready" in await hub.next_frame(first, timeout=0.1)
        assert other.queue.empty()
        event = {
            **event,
            "type": "drift.updated",
            "machine_id": None,
            "data": {"report_id": "r", "updated_at": "2026-11-03T00:00:00Z"},
        }
        hint["machine_key"] = "0" * 64
        hub.invalidate_m7(json.dumps(hint))
        await hub.flush_m7()
        assert b"drift.updated" in await hub.next_frame(first, timeout=0.1)
        assert b"drift.updated" in await hub.next_frame(other, timeout=0.1)

    asyncio.run(run())
