"""Shared OAuth2 client-credentials and bearer-file helpers for internal service calls."""

from __future__ import annotations

import ipaddress
import math
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import httpx
from maintai_messaging.contracts import strict_json
from maintai_messaging.errors import ContractViolation

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
    "ml-service",
    "ml-service-shadow",
    "mlflow",
    "sensor",
    "simulator-ui",
    "storage-consumer",
    "minio",
    "rag-service",
}

MAX_CREDENTIAL_FILE_READ = 16385
MAX_CREDENTIAL_BYTES = 16377
MAX_TOKEN_RESPONSE_BYTES = 65536


def loopback(host: str | None) -> bool:
    """Return whether host refers to loopback or a reviewed cluster-internal service name."""
    if not host:
        return False
    if (
        host in INTERNAL_HOSTS
        or host.endswith(".svc.cluster.local")
        or host.endswith(".local")
        or host.endswith(".localhost")
        or host.endswith(".internal")
    ):
        return True
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_bearer_token(token: str) -> str:
    value = token.strip()
    raw = value.encode("ascii")
    if (
        len(raw) > MAX_CREDENTIAL_BYTES
        or not value
        or not value.isprintable()
        or any(ch.isspace() for ch in value)
    ):
        raise ValueError("Invalid service credential")
    return value


def read_credential_file(path: Path) -> str:
    """Read and validate a bounded printable ASCII credential or client secret file."""
    if not path.is_file():
        raise ValueError("Service credential must be a regular file")
    with path.open("rb") as source:
        raw = source.read(MAX_CREDENTIAL_FILE_READ)
    if len(raw) > MAX_CREDENTIAL_BYTES:
        raise ValueError("Service credential exceeds authorization bound")
    return _validate_bearer_token(raw.decode("ascii"))


@dataclass(frozen=True)
class ServiceOAuthConfig:
    """Bounded OAuth2 client-credentials settings for Business-to-Processing requests."""

    token_url: str
    client_id: str
    secret_file: str = field(repr=False)
    scope: str | None = None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str],
        *,
        prefix: str = "BUSINESS_PROCESSING_OAUTH_",
        local: bool,
    ) -> ServiceOAuthConfig | None:
        token_url = env.get(prefix + "TOKEN_URL", "").strip()
        client_id = env.get(prefix + "CLIENT_ID", "").strip()
        secret_file = env.get(prefix + "SECRET_FILE", "").strip()
        scope_raw = env.get(prefix + "SCOPE", "").strip()
        if not any((token_url, client_id, secret_file, scope_raw)):
            return None
        if not token_url or not client_id or not secret_file:
            raise ValueError(
                f"{prefix}TOKEN_URL, {prefix}CLIENT_ID and {prefix}SECRET_FILE "
                "must all be set together"
            )
        parsed = urlparse(token_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Invalid Processing OAuth token URL")
        if local and not loopback(parsed.hostname):
            raise ValueError("Local test Processing OAuth token URL must target internal/loopback")
        if not local and parsed.scheme != "https":
            raise ValueError("Shared Processing OAuth token URL requires HTTPS")
        if not client_id.isprintable() or "\r" in client_id or "\n" in client_id:
            raise ValueError("Invalid Processing OAuth client_id")
        scope = scope_raw or None
        if scope is not None and (not scope.isprintable() or "\r" in scope or "\n" in scope):
            raise ValueError("Invalid Processing OAuth scope")
        return cls(token_url=token_url, client_id=client_id, secret_file=secret_file, scope=scope)


class ServiceOAuthTokenProvider:
    """Thread-safe cached OAuth2 client_credentials bearer provider."""

    def __init__(
        self,
        config: ServiceOAuthConfig,
        *,
        timeout_seconds: float = 2.0,
        transport: httpx.BaseTransport | None = None,
        clock=time.monotonic,
    ) -> None:
        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
        ):
            raise ValueError("OAuth token timeout must be positive and bounded")
        self.config = config
        self.timeout_seconds = float(timeout_seconds)
        self.transport = transport
        self._clock = clock
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0

    def get_token(self, *, force_refresh: bool = False, timeout: float | None = None) -> str:
        with self._lock:
            now = self._clock()
            if not force_refresh and self._token is not None and now < self._expires_at:
                return self._token
            secret = read_credential_file(Path(self.config.secret_file))
            budget = (
                self.timeout_seconds
                if timeout is None
                else max(0.05, min(self.timeout_seconds, float(timeout)))
            )
            form = {
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": secret,
            }
            if self.config.scope:
                form["scope"] = self.config.scope
            with httpx.Client(
                transport=self.transport,
                trust_env=False,
                follow_redirects=False,
            ) as client:
                response = client.post(
                    self.config.token_url,
                    data=form,
                    headers={"Accept": "application/json", "Accept-Encoding": "identity"},
                    timeout=budget,
                )
            if response.status_code != 200:
                raise RuntimeError("Processing OAuth token request failed")
            encoding = response.headers.get("Content-Encoding", "identity").lower()
            if encoding != "identity":
                raise ValueError("Unsupported OAuth token response encoding")
            if len(response.content) > MAX_TOKEN_RESPONSE_BYTES:
                raise ValueError("OAuth token response exceeds bound")
            try:
                payload = strict_json(response.content)
            except ContractViolation as exc:
                raise ValueError("Invalid OAuth token response") from exc
            if not isinstance(payload, dict) or not isinstance(payload.get("access_token"), str):
                raise ValueError("OAuth token response missing access_token")
            token = _validate_bearer_token(payload["access_token"])
            expires_in = payload.get("expires_in")
            if type(expires_in) in (int, float) and math.isfinite(expires_in) and expires_in > 0:
                margin = min(60.0, max(5.0, float(expires_in) * 0.2))
                ttl = max(1.0, float(expires_in) - margin)
            else:
                ttl = 60.0
            self._token = token
            self._expires_at = self._clock() + ttl
            return token


def resolve_bearer_token(
    *,
    token_file: Path | None = None,
    token_provider: ServiceOAuthTokenProvider | None = None,
    force_refresh: bool = False,
    timeout: float | None = None,
) -> str:
    """Resolve an active bearer token from an OAuth provider or static file."""
    if token_provider is not None:
        return token_provider.get_token(force_refresh=force_refresh, timeout=timeout)
    if token_file is None:
        raise ValueError("Service credential file or OAuth token provider is required")
    return read_credential_file(token_file)
