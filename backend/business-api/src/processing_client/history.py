"""Bounded Processing facade; caller JWTs and redirects never reach providers."""

import asyncio
import tempfile
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from api.auth import RegistryFailure
from maintai_messaging.contracts import canonical_json, strict_json

from .client import ProcessingStatusClient

PROFILE = "M6-HISTORY-01/1.0.0"


def opaque(value):
    encoded = quote(value, safe="")
    return encoded.replace(".", "%2E") if value in {".", ".."} else encoded


@dataclass
class HistoryResponse:
    status: int
    value: dict | None = None
    file: object | None = None
    media_type: str | None = None


class ProcessingHistoryClient:
    def __init__(self, client, *, base_url, token_file, signer):
        self.client, self.base_url, self.signer = client, base_url.rstrip("/"), signer
        self.credentials = ProcessingStatusClient(client, base_url=base_url, token_file=token_file)

    async def request(
        self,
        actor,
        method,
        resource,
        *,
        query=None,
        value=None,
        idempotency_key=None,
        correlation_id,
    ):
        output = None
        try:
            download = resource.endswith("/download")
            async with asyncio.timeout(120 if download else 12):
                token = await asyncio.to_thread(self.credentials._token)
                delegated = await asyncio.to_thread(self.signer.sign, actor)
                headers = {
                    "Authorization": "Bearer " + token,
                    "X-MaintAI-History-Profile": PROFILE,
                    "X-MaintAI-History-Delegation": delegated,
                    "X-Correlation-ID": correlation_id,
                    "Accept-Encoding": "identity",
                }
                if idempotency_key is not None:
                    headers["Idempotency-Key"] = idempotency_key
                if value is not None:
                    headers["Content-Type"] = "application/json"
                async with self.client.stream(
                    method,
                    self.base_url + "/api/v1/internal/" + resource,
                    params=query,
                    content=canonical_json(value) if value is not None else None,
                    headers=headers,
                    timeout=5,
                    follow_redirects=False,
                ) as response:
                    if response.status_code == 504:
                        raise RegistryFailure(504, "DEPENDENCY_TIMEOUT")
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise ValueError("Unsupported upstream encoding")
                    media = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
                    if response.status_code not in {200, 202}:
                        body = bytearray()
                        async for chunk in response.aiter_bytes(16384):
                            if len(body) + len(chunk) > 65536:
                                raise ValueError("Error representation exceeds bound")
                            body.extend(chunk)
                        error = strict_json(bytes(body))
                        self.credentials.error_validator.validate(error)
                        if response.status_code in {400, 403, 404, 409, 410, 413, 422}:
                            raise RegistryFailure(response.status_code, error["error"]["code"])
                        raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
                    if media == "application/json" and not download:
                        body = bytearray()
                        async for chunk in response.aiter_bytes(16384):
                            if len(body) + len(chunk) > 2 * 1024 * 1024:
                                raise ValueError("History response exceeds bound")
                            body.extend(chunk)
                        return HistoryResponse(response.status_code, value=strict_json(bytes(body)))
                    if response.status_code != 200 or media not in {
                        "text/csv",
                        "application/vnd.apache.parquet",
                    }:
                        raise ValueError("Unsupported history response")
                    if not download and (method != "POST" or media != "text/csv"):
                        raise ValueError("Unexpected streamed representation")
                    limit = 512 * 1024 * 1024 if download else 10 * 1024 * 1024
                    output = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
                    total = 0
                    async for chunk in response.aiter_bytes(65536):
                        total += len(chunk)
                        if total > limit:
                            raise ValueError("History artifact exceeds bound")
                        output.write(chunk)
                    length = response.headers.get("Content-Length")
                    if length is not None and int(length) != total:
                        raise ValueError("Truncated artifact")
                    output.seek(0)
                    result = HistoryResponse(200, file=output, media_type=media)
                    output = None
                    return result
        except RegistryFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise RegistryFailure(504, "DEPENDENCY_TIMEOUT") from None
        except Exception:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        finally:
            if output is not None:
                output.close()

    async def aclose(self):
        await self.client.aclose()
