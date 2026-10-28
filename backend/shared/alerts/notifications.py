"""Immutable per-mutation snapshots; NOTIFY contains a small generated identity."""

import json
import re
from uuid import uuid4

from maintai_messaging.contracts import canonical_json

from backend.shared.notifications import Notifier, machine_key

ALERT_CHANNEL = "maintai_business_alert_v1"


def emit(connection, alert, timestamp, *, created=False, channel=ALERT_CHANNEL):
    Notifier(channel)
    identifier = uuid4().hex
    value = {
        "schema_version": "1.0.0",
        "type": "alert.created" if created else "alert.updated",
        "machine_id": alert["machine_id"],
        "occurred_at": timestamp.isoformat(),
        "data": {
            **{k: alert[k] for k in ("alert_id", "machine_id", "severity", "status")},
            "updated_at": timestamp.isoformat(),
        },
    }
    connection.execute(
        """INSERT INTO alert_notifications(notification_id,machine_id,alert_id,event,created_at)
        VALUES(%s,%s,%s,%s,%s)""",
        (identifier, alert["machine_id"], alert["alert_id"], canonical_json(value), timestamp),
    )
    connection.execute(
        "SELECT pg_notify(%s,%s)",
        (
            channel,
            json.dumps(
                {
                    "version": 1,
                    "machine_key": machine_key(alert["machine_id"]),
                    "notification_id": identifier,
                }
            ),
        ),
    )


def parse(raw):
    if len(raw.encode()) > 256:
        raise ValueError("Alert hint exceeds bound")
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "machine_key", "notification_id"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or not isinstance(value["machine_key"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["machine_key"])
        or not isinstance(value["notification_id"], str)
        or not re.fullmatch(r"[0-9a-f]{32}", value["notification_id"])
    ):
        raise ValueError("Invalid alert hint")
    return value["machine_key"], value["notification_id"]
