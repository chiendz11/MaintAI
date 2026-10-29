"""Alert snapshots fan out to both API replicas without collapsing lifecycle changes."""

import asyncio
import json
import time
from uuid import uuid4

import pytest
from backend.shared.alerts.notifications import parse
from backend.shared.alerts.repository import AlertRepository
from backend.shared.persistence.projections import ProjectionRepository
from business_sse.hub import NotificationHub, SSESettings

from .test_alert_lifecycle import deliver_alert, repository
from .test_projections import setup

pytestmark = pytest.mark.integration


def test_two_replicas_receive_exact_committed_alert_snapshots(business_database):
    db = business_database
    setup(db)
    channel = "m5_" + uuid4().hex
    alerts = AlertRepository(db.factory, channel=channel)

    async def run():
        settings = SSESettings(channel="m4_" + uuid4().hex, alert_channel=channel)
        hubs = [
            NotificationHub(
                ProjectionRepository(db.factory),
                dsn=db.dsn,
                settings=settings,
                alert_repository=alerts,
            )
            for _ in range(2)
        ]
        # The fixture schema is explicit, unlike the runtime public schema.
        from psycopg.conninfo import make_conninfo

        for hub in hubs:
            hub.dsn = make_conninfo(db.dsn, options="-csearch_path=" + db.schema)
            await hub.start()
        try:
            deadline = time.monotonic() + 5
            while not all(hub.ready for hub in hubs):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)
            subscriptions = [hub.subscribe({"M-001"}, expires_at=time.time() + 60) for hub in hubs]
            foreign = hubs[0].subscribe({"M-002"}, expires_at=time.time() + 60)
            from types import SimpleNamespace

            from backend.shared.alerts.handler import AlertHandler

            from .test_alert_lifecycle import EvidenceFixture, signal

            event = signal()
            AlertHandler(db.factory, "business-alert-v1", EvidenceFixture(), repository=alerts)(
                event,
                SimpleNamespace(topic=event.topic, partition=0, offset=1, value=event.wire_bytes),
            )
            aid = alerts.list({"M-001"})["items"][0]["alert_id"]
            alerts.action(aid, "ack", {"M-001"}, "tech", {})
            alerts.action(aid, "resolve", {"M-001"}, "tech", {"resolution": "Fixed"})
            for hub, subscription in zip(hubs, subscriptions, strict=True):
                states = []
                while len(states) < 3:
                    frame = await asyncio.wait_for(hub.next_frame(subscription, timeout=0.2), 5)
                    if frame and b"data: " in frame:
                        value = json.loads(frame.split(b"data: ", 1)[1])
                        states.append(value["data"]["status"])
                assert states == ["OPEN", "ACKNOWLEDGED", "RESOLVED"]
            assert await hubs[0].next_frame(foreign, timeout=0.01) == b": heartbeat\n\n"
        finally:
            await asyncio.gather(*(hub.stop() for hub in hubs))

    asyncio.run(run())


def test_notification_snapshot_survives_later_mutations(business_database):
    db = business_database
    setup(db)
    deliver_alert(db)
    repo = repository(db)
    aid = repo.list({"M-001"})["items"][0]["alert_id"]
    with db.factory() as connection:
        first = connection.execute("SELECT notification_id FROM alert_notifications").fetchone()[
            "notification_id"
        ]
    repo.action(aid, "ack", {"M-001"}, "tech", {})
    assert repo.notification(first, "M-001")["data"]["status"] == "OPEN"
    assert repo.notification(first, "M-002") is None


@pytest.mark.parametrize("raw", ["[]", "1", "null", '{"version":true}', '[{"a":1}]'])
def test_invalid_alert_hints_are_rejected_without_killing_listener(raw):
    with pytest.raises(ValueError):
        parse(raw)
