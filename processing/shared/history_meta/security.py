"""Actor-bound M6 delegation and authenticated cursors, separate from M5 auth."""

import base64
import hashlib
import hmac
import re
import time
from dataclasses import dataclass

import jwt
from maintai_messaging.contracts import canonical_json, strict_json

PROFILE = "M6-HISTORY-01/1.0.0"


class HistoryError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(code)


def digest(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


@dataclass(frozen=True)
class Actor:
    subject: str
    machines: frozenset[str]
    roles: frozenset[str]

    @property
    def scope(self):
        return digest(sorted(self.machines))


class DelegationVerifier:
    def __init__(self, *, issuer, fetch_jwks):
        from processing_read_api.auth import IdentityVerifier

        self.keys = IdentityVerifier(
            issuer=issuer,
            audience="processing-history",
            roles_claim="roles",
            machines_claim="machines",
            fetch_jwks=fetch_jwks,
        )
        self.issuer = issuer

    def verify(self, token, service):
        if not isinstance(token, str) or not 1 <= len(token) <= 16384:
            raise HistoryError(401, "UNAUTHORIZED")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256" or not isinstance(header.get("kid"), str):
                raise ValueError("Invalid signing header")
            keys = self.keys._load()
            matches = [key for key in keys if key.key_id == header["kid"]]
            if not matches:
                matches = [
                    key for key in self.keys._load(force=True) if key.key_id == header["kid"]
                ]
            if len(matches) != 1:
                raise ValueError("Unknown signing key")
            claims = jwt.decode(
                token,
                matches[0].key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience="processing-history",
                leeway=5,
                options={
                    "require": [
                        "iss",
                        "aud",
                        "sub",
                        "azp",
                        "iat",
                        "nbf",
                        "exp",
                        "jti",
                        "roles",
                        "machines",
                    ]
                },
            )
            if (
                any(type(claims[k]) is not int for k in ("iat", "nbf", "exp"))
                or not 0 < claims["exp"] - claims["iat"] <= 60
                or claims["nbf"] > claims["exp"]
                or claims["azp"] != service.subject
                or not isinstance(claims["sub"], str)
                or not 1 <= len(claims["sub"]) <= 256
                or not isinstance(claims["jti"], str)
                or not claims["jti"]
            ):
                raise ValueError("Invalid delegation identity")
            machines, roles = claims["machines"], claims["roles"]
            if (
                not isinstance(machines, list)
                or not machines
                or not all(
                    isinstance(m, str) and m and m != "*" and "\x00" not in m for m in machines
                )
                or not isinstance(roles, list)
                or not roles
                or not all(r in {"VIEWER", "TECHNICIAN", "ADMIN"} for r in roles)
                or not set(machines) <= service.machines
            ):
                raise ValueError("Delegation scope exceeds service scope")
            return Actor(claims["sub"], frozenset(machines), frozenset(roles))
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise HistoryError(401, "UNAUTHORIZED") from None
        except Exception:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE") from None


class CursorKeys:
    def __init__(self, keys, *, active, clock=time.time):
        if (
            active not in keys
            or not keys
            or any(
                not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key)
                or not isinstance(value, bytes)
                or len(value) < 32
                for key, value in keys.items()
            )
        ):
            raise ValueError("Explicit cursor key versions of at least 32 bytes are required")
        self.keys, self.active, self.clock = dict(keys), active, clock

    @staticmethod
    def _encode(raw):
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    @staticmethod
    def _decode(raw):
        return base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True)

    def encode(self, payload):
        body = self._encode(canonical_json({**payload, "profile": PROFILE}))
        message = self.active + "." + body
        return (
            message
            + "."
            + self._encode(hmac.digest(self.keys[self.active], message.encode(), "sha256"))
        )

    def decode(self, token, *, actor, scope):
        try:
            if not isinstance(token, str) or len(token) > 8192:
                raise ValueError("Invalid cursor size")
            key, body, signature = token.split(".")
            message = key + "." + body
            if key not in self.keys or not hmac.compare_digest(
                self._decode(signature), hmac.digest(self.keys[key], message.encode(), "sha256")
            ):
                raise ValueError("Invalid cursor signature")
            value = strict_json(self._decode(body))
            if (
                value.get("profile") != PROFILE
                or value.get("actor") != actor
                or value.get("scope") != scope
            ):
                raise ValueError("Cursor identity differs")
            if type(value.get("exp")) not in (int, float):
                raise ValueError("Invalid cursor expiry")
            if value["exp"] <= self.clock():
                raise HistoryError(410, "SNAPSHOT_EXPIRED")
            return value
        except HistoryError:
            raise
        except Exception:
            raise HistoryError(400, "INVALID_CURSOR") from None
