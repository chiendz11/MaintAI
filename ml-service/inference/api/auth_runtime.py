"""Tải JWKS có giới hạn và quản lý cache khóa xác minh."""

import asyncio
import json
import threading
import time
from urllib.parse import urlsplit

import httpx
import jwt

from auth_core import AuthFailure, ServiceTokenVerifier


class AuthDependencyUnavailable(Exception):
    """Không thể xác minh vì nguồn khóa chưa sẵn sàng."""


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def make_jwks_fetcher(url, *, tls_verify=True):
    """Chỉ dùng URL cấu hình phía server; không đọc URL từ token."""
    parsed = urlsplit(url)

    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("JWKS URL must use HTTPS without credentials or fragment")

    if tls_verify is False:
        raise ValueError("TLS verification must remain enabled")

    async def fetch_async():
        # Deadline tổng bao gồm kết nối và đọc toàn bộ response.
        async with asyncio.timeout(1.0):
            async with httpx.AsyncClient(
                verify=tls_verify,
                trust_env=False,
                follow_redirects=False,
                timeout=1.0,
            ) as client:
                async with client.stream(
                    "GET",
                    url,
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                ) as response:
                    if response.status_code != 200:
                        raise ValueError("JWKS endpoint did not return 200")

                    encoding = response.headers.get(
                        "Content-Encoding", "identity"
                    ).lower()
                    if encoding != "identity":
                        raise ValueError("Encoded JWKS response is unsupported")

                    body = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=16384):
                        body.extend(chunk)
                        if len(body) > 131072:
                            raise ValueError("JWKS response exceeds size limit")

                    return json.loads(
                        bytes(body),
                        object_pairs_hook=_unique_object,
                    )

    def fetch():
        # Hàm sync: gọi từ worker thread, không gọi trực tiếp trên event loop.
        return asyncio.run(fetch_async())

    return fetch


class CachedServiceVerifier:
    def __init__(
        self,
        *,
        issuer,
        audience,
        roles_claim,
        fetch_jwks,
        cache_seconds=300,
        clock=time.monotonic,
    ):
        for name, value in {
            "issuer": issuer,
            "audience": audience,
            "roles_claim": roles_claim,
        }.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Missing auth configuration: {name}")

        if (
            type(cache_seconds) is not int
            or not 1 <= cache_seconds <= 3600
        ):
            raise ValueError("Cache lifetime must be between 1 and 3600 seconds")
        if not callable(fetch_jwks):
            raise ValueError("Missing JWKS fetcher")

        self._configuration = {
            "issuer": issuer,
            "audience": audience,
            "roles_claim": roles_claim,
        }
        self._fetch = fetch_jwks
        self._clock = clock
        self._ttl = cache_seconds
        self._lock = threading.Lock()
        self._verifier = None
        self._kids = frozenset()
        self._loaded_at = float("-inf")
        self._last_attempt = float("-inf")

    def _fresh(self, now):
        return (
            self._verifier is not None
            and now - self._loaded_at < self._ttl
        )

    def _load(self, requested_kid=None):
        if not self._lock.acquire(timeout=0.1):
            raise AuthDependencyUnavailable("JWKS refresh is busy")

        try:
            now = self._clock()
            fresh = self._fresh(now)

            if fresh:
                if requested_kid is None or requested_kid in self._kids:
                    return self._verifier

                # Không cho token với kid ngẫu nhiên gây tải JWKS liên tục.
                if now - self._last_attempt < 5:
                    return self._verifier

            # Sau lỗi nguồn khóa, hạn chế thử lại trong vòng một giây.
            if now - self._last_attempt < 1:
                raise AuthDependencyUnavailable("JWKS refresh is cooling down")

            self._last_attempt = now

            try:
                document = self._fetch()
                verifier = ServiceTokenVerifier(
                    **self._configuration,
                    jwks=document,
                )
            except Exception:
                # Không đưa URL, response hoặc token vào exception trả ra ngoài.
                raise AuthDependencyUnavailable(
                    "JWKS could not be loaded"
                ) from None

            # Thay toàn bộ snapshot sau khi kiểm tra thành công.
            self._verifier = verifier
            self._kids = frozenset(
                raw["kid"]
                for raw in document["keys"]
                if (
                    raw.get("kty") == "RSA"
                    and raw.get("alg", "RS256") == "RS256"
                    and raw.get("use", "sig") == "sig"
                )
            )
            self._loaded_at = self._clock()
            return verifier
        finally:
            self._lock.release()

    def check(self):
        """Readiness của nguồn khóa; không phụ thuộc model."""
        try:
            self._load()
            return True
        except AuthDependencyUnavailable:
            return False

    def verify(self, authorization):
        # Từ chối credentials hỏng trước khi gọi nguồn khóa.
        if (
            not isinstance(authorization, str)
            or not authorization
            or len(authorization) > 16384
        ):
            raise AuthFailure(401, "UNAUTHORIZED")

        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise AuthFailure(401, "UNAUTHORIZED")

        try:
            header = jwt.get_unverified_header(parts[1])
        except (jwt.PyJWTError, ValueError, TypeError):
            raise AuthFailure(401, "UNAUTHORIZED") from None

        kid = header.get("kid")
        if (
            header.get("alg") != "RS256"
            or not isinstance(kid, str)
            or not 1 <= len(kid) <= 256
            or "crit" in header
        ):
            raise AuthFailure(401, "UNAUTHORIZED")

        verifier = self._load(requested_kid=kid)
        return verifier.verify(authorization)