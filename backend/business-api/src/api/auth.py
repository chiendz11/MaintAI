"""OIDC verification for existing Business read roles and explicit machine scopes."""

import asyncio
import json
import threading
import time
from dataclasses import dataclass

import httpx
import jwt


class RegistryFailure(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(code)


@dataclass(frozen=True)
class Identity:
    subject: str
    machines: frozenset[str]

    def authorize(self, machine_id):
        if machine_id not in self.machines:
            raise RegistryFailure(403, "FORBIDDEN")


def jwks_fetcher(url):
    """Bound the entire fetch and raw body; reject compressed key documents."""

    async def fetch_async():
        async with asyncio.timeout(2):
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as client:
                async with client.stream(
                    "GET", url, timeout=2, headers={"Accept-Encoding": "identity"}
                ) as response:
                    response.raise_for_status()
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise ValueError("Unsupported JWKS encoding")
                    raw = bytearray()
                    async for part in response.aiter_raw(chunk_size=16384):
                        if len(raw) + len(part) > 131072:
                            raise ValueError("JWKS exceeds bound")
                        raw.extend(part)
                    return json.loads(raw)

    return lambda: asyncio.run(fetch_async())


class IdentityVerifier:
    def __init__(
        self,
        *,
        issuer,
        audience,
        roles_claim,
        machines_claim,
        fetch_jwks,
        cache_seconds=300,
        clock=time.monotonic,
    ):
        if not all(
            isinstance(v, str) and v.strip()
            for v in (issuer, audience, roles_claim, machines_claim)
        ):
            raise ValueError("Explicit issuer, audience and claim mapping are required")
        if type(cache_seconds) is not int or not 1 <= cache_seconds <= 3600:
            raise ValueError("JWKS cache must have a bounded lifetime")
        self.issuer, self.audience = issuer, audience
        self.roles_claim, self.machines_claim = roles_claim, machines_claim
        self.fetch_jwks, self.cache_seconds, self.clock = fetch_jwks, cache_seconds, clock
        self._keys, self._fetched_at, self._last_attempt = [], float("-inf"), float("-inf")
        self._lock = threading.Lock()

    def _load(self, *, force=False):
        if not self._lock.acquire(timeout=2):
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        try:
            now = self.clock()
            fresh = bool(self._keys) and now - self._fetched_at < self.cache_seconds
            if fresh and (not force or now - self._last_attempt < 5):
                return self._keys
            if now - self._last_attempt < 1:
                if fresh:
                    return self._keys
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
            self._last_attempt = now
            try:
                value = self.fetch_jwks()
                if not isinstance(value, dict) or not isinstance(value.get("keys"), list):
                    raise ValueError("Invalid key document")
                if not 1 <= len(value["keys"]) <= 128:
                    raise ValueError("Invalid key count")
                keys = [
                    key
                    for key in jwt.PyJWKSet.from_dict(value).keys
                    if key.algorithm_name == "RS256" and key.public_key_use in (None, "sig")
                ]
                if not keys:
                    raise ValueError("No usable signing keys")
            except Exception:
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
            self._keys, self._fetched_at = keys, self.clock()
            return keys
        finally:
            self._lock.release()

    def check(self):
        try:
            return bool(self._load())
        except RegistryFailure:
            return False

    def verify(self, authorization, *, allowed_roles=None):
        if not authorization or len(authorization) > 16384:
            raise RegistryFailure(401, "UNAUTHORIZED")
        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise RegistryFailure(401, "UNAUTHORIZED")
        try:
            header = jwt.get_unverified_header(parts[1])
            if header.get("alg") != "RS256" or not isinstance(header.get("kid"), str):
                raise RegistryFailure(401, "UNAUTHORIZED")
            keys = self._load()
            matches = [key for key in keys if key.key_id == header["kid"]]
            if not matches:
                matches = [key for key in self._load(force=True) if key.key_id == header["kid"]]
            if len(matches) != 1:
                raise RegistryFailure(401, "UNAUTHORIZED")
            claims = jwt.decode(
                parts[1],
                matches[0].key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self.audience,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
            subject = claims["sub"]
            if not isinstance(subject, str) or not 1 <= len(subject) <= 256:
                raise RegistryFailure(401, "UNAUTHORIZED")
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise RegistryFailure(401, "UNAUTHORIZED") from None
        roles, machines = claims.get(self.roles_claim), claims.get(self.machines_claim)
        if (
            not isinstance(roles, list)
            or not all(isinstance(r, str) for r in roles)
            or not set(roles).intersection(
                {"VIEWER", "TECHNICIAN", "ADMIN"} if allowed_roles is None else allowed_roles
            )
            or not isinstance(machines, list)
            or not machines
            or not all(isinstance(m, str) and m and m != "*" and "\x00" not in m for m in machines)
        ):
            raise RegistryFailure(403, "FORBIDDEN")
        return Identity(subject, frozenset(machines))
