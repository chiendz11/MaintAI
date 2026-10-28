"""JWT verification with a bounded, expiring JWKS cache and explicit claim mapping."""

import asyncio
import json
import threading
import time

import httpx
import jwt
from maintai_messaging.ingestion import SourceIdentity

from .errors import EdgeFailure


def jwks_fetcher(url, timeout):
    async def fetch_async():
        async with asyncio.timeout(timeout):
            async with httpx.AsyncClient(
                timeout=timeout, follow_redirects=False, trust_env=False
            ) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(raw) + len(chunk) > 131072:
                            raise ValueError("JWKS exceeds bound")
                        raw.extend(chunk)
                    return json.loads(raw)

    def fetch():
        # Called in a worker thread; the wall deadline includes connect and all body chunks.
        return asyncio.run(fetch_async())

    return fetch


class LocalFixtureVerifier:
    def __init__(self, subject, machines):
        self.identity = SourceIdentity(subject, machines)

    def verify(self, authorization):
        return self.identity

    def check(self):
        return True


class OidcIdentityVerifier:
    def __init__(
        self,
        *,
        issuer,
        audience,
        algorithms,
        roles_claim,
        machines_claim,
        fetch_jwks,
        cache_seconds=300,
        clock=time.monotonic,
        lock_timeout=2,
        observability=None,
    ):
        self.issuer, self.audience, self.algorithms = issuer, audience, algorithms
        self.roles_claim, self.machines_claim = roles_claim, machines_claim
        self.fetch_jwks, self.cache_seconds, self.clock = fetch_jwks, cache_seconds, clock
        self._keys = []
        self._fetched_at = float("-inf")
        self._last_attempt = float("-inf")
        self._lock = threading.Lock()
        self.lock_timeout = lock_timeout
        self.observability = observability

    def _load(self, *, force=False):
        if not self._lock.acquire(timeout=self.lock_timeout):
            raise EdgeFailure(503, "DEPENDENCY_UNAVAILABLE", retryable=True)
        try:
            now = self.clock()
            fresh = bool(self._keys) and now - self._fetched_at < self.cache_seconds
            if fresh and (not force or now - self._last_attempt < 5):
                return self._keys
            # Bound retries on unknown kid and provider failure; never use expired keys.
            if now - self._last_attempt < 1:
                if fresh:
                    return self._keys
                raise EdgeFailure(503, "DEPENDENCY_UNAVAILABLE", retryable=True)
            self._last_attempt = now
            try:
                document = self.fetch_jwks()
                if not isinstance(document, dict) or not 1 <= len(document.get("keys", [])) <= 128:
                    raise ValueError("Invalid JWKS")
                keys = jwt.PyJWKSet.from_dict(document).keys
                keys = [
                    k
                    for k in keys
                    if k.public_key_use in {None, "sig"} and k.algorithm_name in self.algorithms
                ]
                if not keys:
                    raise ValueError("No usable signing keys")
            except Exception as error:
                if self.observability is not None:
                    self.observability.record_dependency_error("oidc", error)
                self._last_attempt = self.clock()
                raise EdgeFailure(503, "DEPENDENCY_UNAVAILABLE", retryable=True) from None
            self._keys, self._fetched_at = keys, self.clock()
            return keys
        finally:
            self._lock.release()

    def check(self):
        try:
            return bool(self._load())
        except EdgeFailure:
            return False

    def verify(self, authorization):
        if not authorization or len(authorization) > 16384:
            raise EdgeFailure(401, "UNAUTHORIZED")
        pieces = authorization.split()
        if len(pieces) != 2 or pieces[0].lower() != "bearer":
            raise EdgeFailure(401, "UNAUTHORIZED")
        token = pieces[1]
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") not in self.algorithms or not isinstance(header.get("kid"), str):
                raise EdgeFailure(401, "UNAUTHORIZED")
            keys = self._load()
            matches = [
                k for k in keys if k.key_id == header["kid"] and k.algorithm_name == header["alg"]
            ]
            if not matches:
                matches = [
                    k
                    for k in self._load(force=True)
                    if k.key_id == header["kid"] and k.algorithm_name == header["alg"]
                ]
            if len(matches) != 1:
                raise EdgeFailure(401, "UNAUTHORIZED")
            claims = jwt.decode(
                token,
                matches[0].key,
                algorithms=list(self.algorithms),
                issuer=self.issuer,
                audience=self.audience,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
            subject = claims["sub"]
            if not isinstance(subject, str) or not 1 <= len(subject) <= 256:
                raise EdgeFailure(401, "UNAUTHORIZED")
        except (jwt.PyJWTError, ValueError, TypeError, KeyError):
            raise EdgeFailure(401, "UNAUTHORIZED") from None
        roles = claims.get(self.roles_claim)
        machines = claims.get(self.machines_claim)
        if (
            not isinstance(roles, list)
            or not all(isinstance(x, str) for x in roles)
            or "SENSOR_SERVICE" not in roles
            or not isinstance(machines, list)
            or not machines
            or not all(isinstance(x, str) and x and x != "*" for x in machines)
        ):
            raise EdgeFailure(403, "FORBIDDEN")
        return SourceIdentity(subject, frozenset(machines))
