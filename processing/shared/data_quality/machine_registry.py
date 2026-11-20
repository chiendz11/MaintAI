"""Read-only Business registry adapter with typed absence and bounded cache.

No Business implementation, provisioning or service role is created here.
Shared deployment requires reviewed Business service access and credentials.
"""

from __future__ import annotations

import asyncio
import ipaddress
import math
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import httpx
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import strict_json
from maintai_messaging.errors import ContractViolation
from referencing import Registry, Resource


@dataclass
class OAuthClientCredentials:
    """Thread/task-safe cached OAuth2 client_credentials token provider."""

    token_url: str
    client_id: str
    client_secret: str = field(repr=False)
    scope: str | None = None
    timeout_seconds: float = 2.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _token: str | None = field(default=None, init=False, repr=False)
    _expires_at: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        url = httpx.URL(self.token_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.host
            or url.userinfo
            or url.query
            or url.fragment
        ):
            raise ValueError("Registry OAuth token URL must be an explicit HTTP(S) URL")
        if (
            not isinstance(self.client_id, str)
            or not self.client_id.strip()
            or "\r" in self.client_id
            or "\n" in self.client_id
        ):
            raise ValueError("Invalid Registry OAuth client_id")
        if (
            not isinstance(self.client_secret, str)
            or not self.client_secret.strip()
            or "\r" in self.client_secret
            or "\n" in self.client_secret
        ):
            raise ValueError("Invalid Registry OAuth client_secret")
        if self.scope is not None and (
            not isinstance(self.scope, str) or "\r" in self.scope or "\n" in self.scope
        ):
            raise ValueError("Invalid Registry OAuth scope")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 10
        ):
            raise ValueError("Registry OAuth timeout must be positive and bounded")

    async def get_token(self, http: httpx.AsyncClient, *, force_refresh: bool = False) -> str:
        async with self._lock:
            if (
                not force_refresh
                and self._token is not None
                and time.monotonic() < self._expires_at
            ):
                return self._token
            form = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            if self.scope:
                form["scope"] = self.scope
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    response = await http.post(
                        self.token_url,
                        data=form,
                        headers={"Accept": "application/json"},
                        timeout=self.timeout_seconds,
                        follow_redirects=False,
                    )
                if response.status_code != 200:
                    raise RuntimeError("REGISTRY_AUTH_ERROR")
                payload = strict_json(response.content)
                token = payload.get("access_token") if isinstance(payload, dict) else None
                if (
                    not isinstance(token, str)
                    or not token.strip()
                    or "\r" in token
                    or "\n" in token
                ):
                    raise RuntimeError("REGISTRY_AUTH_ERROR")
                raw_expires = payload.get("expires_in", 60)
                expires_in = (
                    float(raw_expires)
                    if type(raw_expires) in (int, float)
                    and math.isfinite(raw_expires)
                    and raw_expires > 0
                    else 60.0
                )
                ttl = (
                    max(5.0, min(expires_in - 30.0, 3600.0))
                    if expires_in >= 60.0
                    else max(1.0, expires_in / 2.0)
                )
                self._token = token
                self._expires_at = time.monotonic() + ttl
                return token
            except (TimeoutError, httpx.HTTPError, ContractViolation, ValueError):
                self._token = None
                self._expires_at = 0.0
                raise RuntimeError("REGISTRY_AUTH_ERROR") from None


@dataclass(frozen=True)
class MachineLookupResult:
    status: Literal["FOUND", "NOT_FOUND", "UNAVAILABLE"]
    failure_code: str | None = None


INTERNAL_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "kafka",
    "postgres",
    "keycloak",
    "business-api",
    "business-consumer",
    "processing-worker",
    "processing-read-api",
    "processing-outbox-relay",
    "ingestion-api",
    "sensor",
    "storage-consumer",
    "ml-service",
    "ml-service-shadow",
    "mlflow",
    "rag-service",
    "frontend",
    "simulator-ui",
    "idp-gateway",
}


def is_loopback(host: str) -> bool:
    if not host:
        return False
    lowered = host.strip().lower()
    if (
        lowered in INTERNAL_HOSTS
        or lowered.endswith(".svc.cluster.local")
        or lowered.endswith(".local")
        or lowered.endswith(".localhost")
        or lowered.endswith(".internal")
    ):
        return True
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class HTTPMachineRegistry:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        schema_directory: Path,
        bearer_token: str | None = None,
        oauth_credentials: OAuthClientCredentials | None = None,
        local_fixture: bool = False,
        timeout_seconds: float = 1,
        cache_seconds: float = 0,
        max_cache_entries: int = 1000,
    ):
        url = httpx.URL(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.host
            or url.userinfo
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("Business registry URL must be an explicit HTTP(S) origin")
        loopback = is_loopback(url.host)
        if local_fixture and not loopback:
            raise ValueError("Registry fixture is loopback-only")
        if bearer_token is not None and oauth_credentials is not None:
            raise ValueError("Specify either bearer_token or oauth_credentials, not both")
        if (
            oauth_credentials is not None
            and local_fixture
            and not is_loopback(httpx.URL(oauth_credentials.token_url).host or "")
        ):
            raise ValueError("Registry OAuth token URL must be loopback in local_fixture mode")
        if not bearer_token and oauth_credentials is None and not local_fixture:
            raise ValueError("Business service credentials are required")
        if bearer_token is not None and (
            not isinstance(bearer_token, str)
            or not bearer_token.strip()
            or "\r" in bearer_token
            or "\n" in bearer_token
        ):
            raise ValueError("Invalid Business credential")
        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 2
        ):
            raise ValueError("Registry deadline must be positive and bounded")
        if (
            type(cache_seconds) not in (int, float)
            or not math.isfinite(cache_seconds)
            or cache_seconds < 0
        ):
            raise ValueError("Registry cache TTL must be nonnegative and finite")
        if type(max_cache_entries) is not int or max_cache_entries < 1:
            raise ValueError("Registry cache capacity must be positive")
        directory = Path(schema_directory).resolve()
        resources = [
            (path.as_uri(), Resource.from_contents(strict_json(path.read_bytes())))
            for path in directory.glob("*.schema.json")
        ]
        schemas = Registry().with_resources(resources)

        def validator(filename, definition):
            return Draft202012Validator(
                {"$ref": f"{(directory / filename).as_uri()}#/$defs/{definition}"},
                registry=schemas,
                format_checker=FormatChecker(),
            )

        self.detail = validator("business.schema.json", "MachineDetail")
        self.error = validator("common.schema.json", "ErrorEnvelope")
        self.http, self.base_url = http, str(url).rstrip("/")
        self.timeout_seconds, self.cache_seconds = timeout_seconds, cache_seconds
        self.max_cache_entries = max_cache_entries
        self._cache = OrderedDict()
        self._headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}
        self._oauth = oauth_credentials

    async def lookup(self, machine_id: str, *, correlation_id: str) -> MachineLookupResult:
        if not isinstance(machine_id, str) or not machine_id:
            raise ValueError("Machine identity is required")
        if (
            not isinstance(correlation_id, str)
            or not correlation_id
            or "\r" in correlation_id
            or "\n" in correlation_id
        ):
            raise ValueError("Valid correlation identity is required")
        cached = self._cache.get(machine_id)
        if cached and cached[0] > time.monotonic():
            self._cache.move_to_end(machine_id)
            return cached[1]
        self._cache.pop(machine_id, None)
        url = self.base_url + "/api/v1/machines/" + quote(machine_id, safe="")
        base_headers = {
            **self._headers,
            "X-Correlation-ID": correlation_id,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        try:
            result: MachineLookupResult | None = None
            for attempt in range(2 if self._oauth is not None else 1):
                headers = dict(base_headers)
                if self._oauth is not None:
                    try:
                        token = await self._oauth.get_token(self.http, force_refresh=(attempt == 1))
                    except RuntimeError:
                        return MachineLookupResult("UNAVAILABLE", "REGISTRY_AUTH_ERROR")
                    headers["Authorization"] = f"Bearer {token}"
                async with asyncio.timeout(self.timeout_seconds):
                    async with self.http.stream(
                        "GET",
                        url,
                        headers=headers,
                        timeout=self.timeout_seconds,
                        follow_redirects=False,
                    ) as response:
                        if response.status_code == 401 and self._oauth is not None and attempt == 0:
                            continue
                        if response.status_code not in {200, 404}:
                            return MachineLookupResult("UNAVAILABLE", "REGISTRY_HTTP_ERROR")
                        encoding = (
                            response.headers.get("Content-Encoding", "identity").strip().lower()
                        )
                        if encoding not in {"", "identity"}:
                            return MachineLookupResult("UNAVAILABLE", "REGISTRY_INVALID_RESPONSE")
                        raw = bytearray()
                        if response.is_stream_consumed:
                            if len(response.content) > 65536:
                                return MachineLookupResult(
                                    "UNAVAILABLE", "REGISTRY_INVALID_RESPONSE"
                                )
                            raw.extend(response.content)
                        else:
                            async for chunk in response.aiter_raw(chunk_size=16384):
                                if len(raw) + len(chunk) > 65536:
                                    return MachineLookupResult(
                                        "UNAVAILABLE", "REGISTRY_INVALID_RESPONSE"
                                    )
                                raw.extend(chunk)
                        payload = strict_json(bytes(raw))
                        if (
                            response.status_code == 200
                            and self.detail.is_valid(payload)
                            and payload["machine_id"] == machine_id
                        ):
                            result = MachineLookupResult("FOUND")
                        elif (
                            response.status_code == 404
                            and self.error.is_valid(payload)
                            and payload["error"]["code"] == "MACHINE_NOT_FOUND"
                        ):
                            result = MachineLookupResult("NOT_FOUND")
                        else:
                            return MachineLookupResult("UNAVAILABLE", "REGISTRY_INVALID_RESPONSE")
                break
            assert result is not None
            if self.cache_seconds:
                self._cache[machine_id] = (time.monotonic() + self.cache_seconds, result)
                while len(self._cache) > self.max_cache_entries:
                    self._cache.popitem(last=False)
            return result
        except (TimeoutError, httpx.TimeoutException):
            return MachineLookupResult("UNAVAILABLE", "REGISTRY_TIMEOUT")
        except httpx.TransportError:
            return MachineLookupResult("UNAVAILABLE", "REGISTRY_CONNECTION_FAILED")
        except (ContractViolation, httpx.DecodingError):
            return MachineLookupResult("UNAVAILABLE", "REGISTRY_INVALID_RESPONSE")
