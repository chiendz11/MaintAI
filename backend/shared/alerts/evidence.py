"""One three-second wall budget for credential, status, point-read and validation."""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path
from urllib.parse import quote

import httpx
from maintai_messaging.contracts import strict_json
from maintai_messaging.errors import ContractViolation, IdentityConflict

from backend.shared.service_auth import resolve_bearer_token

from .domain import validate_evidence


class EvidenceClient:
    def __init__(
        self,
        base_url,
        token_file=None,
        *,
        token_provider=None,
        transport=None,
        deadline_seconds=3,
    ):
        if token_file is None and token_provider is None:
            raise ValueError("Evidence credential file or OAuth token provider is required")
        self.base_url = base_url.rstrip("/")
        self.token_file = Path(token_file) if token_file is not None else None
        self.token_provider = token_provider
        self.transport, self.budget = transport, deadline_seconds
        if not 0 < self.budget <= 3:
            raise ValueError("Evidence deadline must not exceed three seconds")
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="alert-evidence")
        self.admission = threading.BoundedSemaphore(1)

    def _token(self, *, force_refresh=False, timeout=None):
        return resolve_bearer_token(
            token_file=self.token_file,
            token_provider=self.token_provider,
            force_refresh=force_refresh,
            timeout=timeout,
        )

    async def _fetch(self, signal, deadline):
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            token = await asyncio.to_thread(
                self._token,
                timeout=min(2, max(0.05, deadline - time.monotonic())),
            )
            async with httpx.AsyncClient(transport=self.transport, trust_env=False) as client:

                async def get(path):
                    nonlocal token
                    for attempt in range(2 if self.token_provider is not None else 1):
                        async with client.stream(
                            "GET",
                            self.base_url + path,
                            follow_redirects=False,
                            timeout=min(2, max(0.001, deadline - time.monotonic())),
                            headers={
                                "Authorization": "Bearer " + token,
                                "X-Correlation-ID": signal["correlation_id"],
                                "Accept-Encoding": "identity",
                            },
                        ) as response:
                            if (
                                response.status_code == 401
                                and self.token_provider is not None
                                and attempt == 0
                            ):
                                token = await asyncio.to_thread(
                                    self._token,
                                    force_refresh=True,
                                    timeout=min(2, max(0.05, deadline - time.monotonic())),
                                )
                                continue
                            if (
                                response.status_code != 200
                                or response.headers.get("Content-Encoding", "identity")
                                != "identity"
                                or response.headers.get("Content-Type", "").split(";")[0]
                                != "application/json"
                            ):
                                raise RuntimeError("Processing evidence unavailable")
                            raw = bytearray()
                            async for chunk in response.aiter_bytes():
                                raw.extend(chunk)
                                if len(raw) > 262144:
                                    raise ValueError("Evidence response exceeds bound")
                            return strict_json(bytes(raw))

                def segment(value):
                    return (
                        quote(value, safe="").replace(".", "%2E")
                        if value in {".", ".."}
                        else quote(value, safe="")
                    )

                status = await get(
                    "/api/v1/internal/sensor-events/" + segment(signal["event_id"]) + "/status"
                )
                if (
                    status.get("event_id") != signal["event_id"]
                    or status.get("machine_id") != signal["machine_id"]
                    or status.get("processing_status") != "COMPLETED"
                    or status.get("validation_status") != "VALID"
                    or not isinstance(status.get("result_id"), str)
                    or not status["result_id"]
                ):
                    raise IdentityConflict("Signal status evidence differs")
                value = await get("/api/v1/internal/predictions/" + segment(status["result_id"]))
                if value.get("result_id") != status["result_id"]:
                    raise IdentityConflict("Processing point-read returned another result")
                return validate_evidence(signal, value)

    def fetch(self, signal):
        started = time.monotonic()
        if not self.admission.acquire(blocking=False):
            raise RuntimeError("Evidence reader is already busy")
        future = self.executor.submit(
            lambda: asyncio.run(self._fetch(signal, started + self.budget))
        )
        future.add_done_callback(lambda _: self.admission.release())
        try:
            value = future.result(timeout=max(0, self.budget - (time.monotonic() - started)))
            if time.monotonic() - started > self.budget:
                raise RuntimeError("Evidence deadline exceeded")
            return value
        except ContractViolation:
            raise IdentityConflict("Processing evidence is not valid JSON") from None
        except (FutureTimeout, TimeoutError, httpx.HTTPError, OSError, ValueError) as exc:
            future.cancel()
            raise RuntimeError("Processing evidence unavailable") from exc

    def close(self):
        self.executor.shutdown(wait=False, cancel_futures=True)
