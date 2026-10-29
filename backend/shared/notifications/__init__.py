"""Small transactional invalidations; PostgreSQL broadcasts only after commit."""

import hashlib
import json
import re
from dataclasses import dataclass

DEFAULT_CHANNEL = "maintai_business_live_v1"


def machine_key(machine_id):
    return hashlib.sha256(machine_id.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Notifier:
    channel: str = DEFAULT_CHANNEL

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.channel):
            raise ValueError("Notification channel must be a bounded lowercase identifier")

    def __call__(self, connection, machine_id):
        connection.execute(
            "SELECT pg_notify(%s,%s)",
            (self.channel, json.dumps({"version": 1, "machine_key": machine_key(machine_id)})),
        )


def parse_hint(raw):
    if len(raw.encode("utf-8")) > 256:
        raise ValueError("Oversized notification hint")
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "machine_key"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or not isinstance(value["machine_key"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["machine_key"])
    ):
        raise ValueError("Invalid notification hint")
    return value["machine_key"]
