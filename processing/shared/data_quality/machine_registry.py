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
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import httpx
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import strict_json
from maintai_messaging.errors import ContractViolation
from referencing import Registry, Resource


@dataclass(frozen=True)
class MachineLookupResult:
    status: Literal["FOUND", "NOT_FOUND", "UNAVAILABLE"]
    failure_code: str | None = None


class HTTPMachineRegistry:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        schema_directory: Path,
        bearer_token: str | None = None,
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
        try:
            loopback = url.host == "localhost" or ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            loopback = False
        if local_fixture and not loopback:
            raise ValueError("Registry fixture is loopback-only")
        if not bearer_token and not local_fixture:
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
        headers = {
            **self._headers,
            "X-Correlation-ID": correlation_id,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with self.http.stream(
                    "GET",
                    url,
                    headers=headers,
                    timeout=self.timeout_seconds,
                    follow_redirects=False,
                ) as response:
                    if response.status_code not in {200, 404}:
                        return MachineLookupResult("UNAVAILABLE", "REGISTRY_HTTP_ERROR")
                    encoding = response.headers.get("Content-Encoding", "identity").strip().lower()
                    if encoding not in {"", "identity"}:
                        return MachineLookupResult("UNAVAILABLE", "REGISTRY_INVALID_RESPONSE")
                    raw = bytearray()
                    if response.is_stream_consumed:
                        if len(response.content) > 65536:
                            return MachineLookupResult("UNAVAILABLE", "REGISTRY_INVALID_RESPONSE")
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
