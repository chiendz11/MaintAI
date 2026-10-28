"""PostgreSQL delivers invalidation hints only after durable projection commit."""

import json
import os
from uuid import uuid4

import psycopg
import pytest
from backend.shared.notifications import Notifier

from .test_projections import deliver, event, setup

pytestmark = pytest.mark.integration


@pytest.fixture
def notify_database(business_database):
    if os.environ.get("MAINTAI_M4_INTEGRATION") != "1":
        pytest.skip("Enable MAINTAI_M4_INTEGRATION")
    return business_database


def test_notify_is_post_commit_and_rollback_has_no_hint(notify_database):
    db = notify_database
    channel = "m4_" + uuid4().hex
    notifier = Notifier(channel)
    with psycopg.connect(db.dsn, autocommit=True) as listener:
        listener.execute(psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel)))
        with db.factory() as writer:
            notifier(writer, "M-001")
            assert list(listener.notifies(timeout=0.05, stop_after=1)) == []
            writer.rollback()
        assert list(listener.notifies(timeout=0.05, stop_after=1)) == []
        with db.factory() as writer:
            notifier(writer, "machine/" + "機" * 10000)
        messages = list(listener.notifies(timeout=1, stop_after=1))
        assert len(messages) == 1
        payload = json.loads(messages[0].payload)
        assert set(payload) == {"version", "machine_key"}
        assert payload["version"] == 1
        assert len(payload["machine_key"]) == 64
        assert len(messages[0].payload) < 200


def test_projector_notifies_visible_axes_but_not_duplicate_or_stale(notify_database):
    from backend.shared.projections.projector import Projector

    db = notify_database
    setup(db)
    channel = "m4_" + uuid4().hex
    projector = Projector(notify=Notifier(channel))
    with psycopg.connect(db.dsn, autocommit=True) as listener:
        listener.execute(psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel)))
        deliver(db, projector, event("sensor-reading", 12))
        assert len(list(listener.notifies(timeout=1, stop_after=1))) == 1
        deliver(db, projector, event("sensor-reading", 12))
        assert list(listener.notifies(timeout=0.05, stop_after=1)) == []
        deliver(db, projector, event("sensor-reading", 11), 2)
        assert list(listener.notifies(timeout=0.05, stop_after=1)) == []
        deliver(db, projector, event("completed", 12))
        assert len(list(listener.notifies(timeout=1, stop_after=1))) == 1
        deliver(db, projector, event("invalid", 13))
        assert len(list(listener.notifies(timeout=1, stop_after=1))) == 1


@pytest.mark.parametrize("channel", ["", "space name", "x" * 64, '";SELECT 1--'])
def test_notification_channel_is_a_bounded_identifier(channel):
    with pytest.raises(ValueError):
        Notifier(channel)
