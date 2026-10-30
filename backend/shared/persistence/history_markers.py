"""Immutable aggregate/marker membership in the Business owner's database."""

import base64
import hashlib
import hmac
import re
from datetime import UTC, datetime
from uuid import uuid4

from api.auth import RegistryFailure
from maintai_messaging.contracts import canonical_json, strict_json

PROFILE = "M6-HISTORY-01/1.0.0"


def scope(actor):
    return hashlib.sha256(canonical_json(sorted(actor.machines))).hexdigest()


class MarkerSnapshots:
    def __init__(self, factory, keys, *, active, clock=None):
        if active not in keys or any(
            not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key)
            or not isinstance(value, bytes)
            or len(value) < 32
            for key, value in keys.items()
        ):
            raise ValueError("Explicit versioned marker cursor keys are required")
        self.factory, self.keys, self.active = factory, dict(keys), active
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def encode(raw):
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    @staticmethod
    def decode(raw):
        return base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True)

    def token(self, identifier, after, actor):
        body = (
            self.active
            + "."
            + self.encode(
                canonical_json(
                    {
                        "snapshot": identifier,
                        "after": after,
                        "actor": actor.subject,
                        "scope": scope(actor),
                        "profile": PROFILE,
                    }
                )
            )
        )
        return (
            body + "." + self.encode(hmac.digest(self.keys[self.active], body.encode(), "sha256"))
        )

    def create(self, actor, machine, query, base, markers):
        if len(markers) > 100000:
            raise RegistryFailure(422, "QUERY_TOO_LARGE")
        identifier = "hbs_" + uuid4().hex
        payload = canonical_json({"base": base, "markers": markers})
        if len(payload) > 64 * 1024 * 1024:
            raise RegistryFailure(422, "QUERY_TOO_LARGE")
        with self.factory() as connection:
            connection.execute(
                "INSERT INTO history_marker_snapshots(snapshot_id,actor,scope_sha256,machine_id,"
                "query,payload,payload_sha256,as_of,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    identifier,
                    actor.subject,
                    scope(actor),
                    machine,
                    canonical_json(query),
                    payload,
                    hashlib.sha256(payload).hexdigest(),
                    base["as_of"],
                    base["expires_at"],
                ),
            )
        return self.page(actor, machine, query, self.token(identifier, 0, actor))

    def page(self, actor, machine, query, token):
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError("Invalid cursor")
            key, body, signature = token.split(".")
            if key not in self.keys or not hmac.compare_digest(
                self.decode(signature),
                hmac.digest(self.keys[key], (key + "." + body).encode(), "sha256"),
            ):
                raise ValueError("Invalid signature")
            value = strict_json(self.decode(body))
            if (
                value["actor"] != actor.subject
                or value["scope"] != scope(actor)
                or value["profile"] != PROFILE
            ):
                raise ValueError("Cursor identity differs")
            after = value["after"]
            if type(after) is not int or after < 0:
                raise ValueError("Invalid cursor position")
        except Exception:
            raise RegistryFailure(400, "INVALID_CURSOR") from None
        with self.factory() as connection:
            row = connection.execute(
                "SELECT * FROM history_marker_snapshots WHERE snapshot_id=%s", (value["snapshot"],)
            ).fetchone()
        if row is None or row["expires_at"] <= self.clock():
            raise RegistryFailure(410, "SNAPSHOT_EXPIRED")
        if (
            row["actor"] != actor.subject
            or row["scope_sha256"] != scope(actor)
            or row["machine_id"] != machine
        ):
            raise RegistryFailure(400, "INVALID_CURSOR")
        frozen = strict_json(bytes(row["query"]))
        if any(key not in frozen or frozen[key] != item for key, item in query.items()):
            raise RegistryFailure(400, "INVALID_CURSOR")
        payload = bytes(row["payload"])
        if hashlib.sha256(payload).hexdigest() != row["payload_sha256"]:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        stored = strict_json(payload)
        markers = stored["markers"]
        return {
            **stored["base"],
            "snapshot_id": row["snapshot_id"],
            "markers": markers[after : after + 100],
            "markers_next_cursor": self.token(row["snapshot_id"], after + 100, actor)
            if after + 100 < len(markers)
            else None,
        }
