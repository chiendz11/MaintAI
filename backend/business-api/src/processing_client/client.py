"""Bounded status reads using a service credential, never a caller token or SQL."""

import asyncio
import json
from pathlib import Path
from urllib.parse import quote

import httpx
from api.auth import RegistryFailure
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from maintai_messaging.contracts import default_contracts


class ProcessingStatusClient:
    def __init__(self, client, *, base_url, token_file):
        self.client, self.base_url, self.token_file = client, base_url.rstrip("/"), Path(token_file)
        schema = json.loads(
            (default_contracts().root / "v1.3/schemas/runtime.schema.json").read_text()
        )
        self.validator = Draft202012Validator(
            schema["$defs"]["ProcessingStatus"], format_checker=FormatChecker()
        )
        self.error_validator = Draft202012Validator(schema["$defs"]["Error"])

    def _token(self):
        if not self.token_file.is_file():
            raise ValueError("Service credential must be a regular file")
        with self.token_file.open("rb") as source:
            raw = source.read(16385)
        if len(raw) > 16377:
            raise ValueError("Service credential exceeds authorization bound")
        token = raw.decode("ascii").strip()
        if not token or not token.isprintable() or any(c.isspace() for c in token):
            raise ValueError("Invalid service credential")
        return token

    async def get(self, event_id, *, correlation_id):
        try:
            async with asyncio.timeout(3):
                token = await asyncio.to_thread(self._token)
                encoded_id = quote(event_id, safe="")
                if event_id in (".", ".."):
                    encoded_id = encoded_id.replace(".", "%2E")
                url = self.base_url + "/api/v1/internal/sensor-events/" + encoded_id + "/status"
                async with self.client.stream(
                    "GET",
                    url,
                    timeout=2,
                    follow_redirects=False,
                    headers={
                        "Authorization": "Bearer " + token,
                        "X-Correlation-ID": correlation_id,
                        "Accept-Encoding": "identity",
                    },
                ) as response:
                    if response.status_code == 504:
                        raise RegistryFailure(504, "DEPENDENCY_TIMEOUT")
                    if (
                        response.headers.get("Content-Encoding", "identity").lower() != "identity"
                        or response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                        != "application/json"
                    ):
                        raise ValueError("Unsupported status response representation")
                    body = bytearray()
                    async for part in response.aiter_raw(chunk_size=8192):
                        if len(body) + len(part) > 65536:
                            raise ValueError("Status response exceeds bound")
                        body.extend(part)
                    value = json.loads(body)
                    if (
                        response.status_code == 404
                        and isinstance(value, dict)
                        and (
                            isinstance(value.get("error"), dict)
                            and value["error"].get("code") == "EVENT_NOT_MATERIALIZED"
                        )
                    ):
                        self.error_validator.validate(value)
                        raise RegistryFailure(404, "EVENT_NOT_MATERIALIZED")
                    if response.status_code != 200:
                        raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
                    self.validator.validate(value)
                    if value["event_id"] != event_id:
                        raise ValueError("Status response belongs to another event")
                    return value
        except (TimeoutError, httpx.TimeoutException):
            raise RegistryFailure(504, "DEPENDENCY_TIMEOUT") from None
        except (httpx.HTTPError, OSError, ValueError, ValidationError, RecursionError):
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None

    async def aclose(self):
        await self.client.aclose()
