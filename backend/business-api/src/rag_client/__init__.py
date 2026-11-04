"""Bounded service-to-service RAG calls; durable retries belong to the owner job runner."""

import ipaddress
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from backend.shared.copilot.schemas import valid
from maintai_messaging.contracts import strict_json


class ProviderFailure(Exception):
    def __init__(self, code, *, retryable=False):
        self.code, self.retryable = code, retryable
        super().__init__(code)


def validate_response(value, alert_id):
    if not valid("maintenance-brief", value) or value["alert_id"] != alert_id:
        raise ProviderFailure("RAG_CONTRACT_INVALID")
    citations = value["citations"]
    identifiers = [item["citation_id"] for item in citations]
    retrieved = set(value["retrieval"]["retrieved_chunk_ids"])
    if (
        len(identifiers) != len(set(identifiers))
        or any(item["chunk_id"] not in retrieved for item in citations)
        or any(
            not step["citation_ids"] or not set(step["citation_ids"]) <= set(identifiers)
            for step in value["checklist"]
        )
    ):
        raise ProviderFailure("RAG_CITATIONS_INVALID")
    return value


class RAGClient:
    def __init__(self, base_url, token_file, *, local=False, transport=None):
        origin = urlparse(base_url)
        try:
            loopback = (
                origin.hostname == "localhost" or ipaddress.ip_address(origin.hostname).is_loopback
            )
        except ValueError:
            loopback = False
        if (
            origin.scheme not in {"http", "https"}
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.query
            or origin.fragment
            or origin.path not in {"", "/"}
            or (origin.scheme != "https" and not (local and loopback))
        ):
            raise ValueError("RAG requires an explicit HTTPS origin or loopback test origin")
        self.base_url, self.token_file = base_url.rstrip("/"), Path(token_file)
        self.client = httpx.Client(transport=transport, trust_env=False, follow_redirects=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.client.close()

    def generate(self, payload, *, job_id, timeout=60):
        started = time.monotonic()
        try:
            with self.token_file.open("rb") as source:
                raw = source.read(16385)
            token = raw.decode("ascii").strip()
            if (
                len(raw) > 16384
                or not token
                or not token.isprintable()
                or any(c.isspace() for c in token)
            ):
                raise ValueError("Invalid service credential")
            with self.client.stream(
                "POST",
                self.base_url + "/api/v1/briefs",
                json=payload,
                headers={
                    "Authorization": "Bearer " + token,
                    "Idempotency-Key": job_id,
                    "X-Correlation-ID": job_id,
                    "Accept-Encoding": "identity",
                },
                timeout=max(0.001, min(timeout, 60)),
            ) as response:
                if response.status_code != 200:
                    raise ProviderFailure(
                        "RAG_UNAVAILABLE",
                        retryable=(response.status_code == 429 or response.status_code >= 500),
                    )
                if (
                    response.headers.get("Content-Type", "").split(";")[0] != "application/json"
                    or response.headers.get("Content-Encoding", "identity") != "identity"
                ):
                    raise ProviderFailure("RAG_CONTRACT_INVALID")
                body = bytearray()
                for part in response.iter_bytes():
                    if time.monotonic() - started >= timeout:
                        raise ProviderFailure("RAG_TIMEOUT", retryable=True)
                    body.extend(part)
                    if len(body) > 262144:
                        raise ProviderFailure("RAG_RESPONSE_TOO_LARGE")
                value = strict_json(bytes(body))
                return validate_response(value, payload["alert"]["alert_id"])
        except ProviderFailure:
            raise
        except httpx.HTTPError:
            raise ProviderFailure("RAG_UNAVAILABLE", retryable=True) from None
        except Exception:
            raise ProviderFailure("RAG_CONTRACT_INVALID") from None
