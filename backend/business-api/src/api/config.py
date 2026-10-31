"""Explicit Business DB/OIDC settings; local tests still require signed tokens."""

import ipaddress
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

from business_sse.hub import SSESettings

from .history_config import HistorySettings


def loopback(host):
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class Settings:
    database_url: str = field(repr=False)
    issuer: str
    audience: str
    jwks_url: str
    roles_claim: str
    machines_claim: str
    host: str
    port: int
    processing_read_url: str | None = None
    processing_token_file: str | None = field(default=None, repr=False)
    sse: SSESettings = field(default_factory=SSESettings)
    history: HistorySettings | None = None

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        required = (
            "BUSINESS_DATABASE_URL",
            "BUSINESS_OIDC_ISSUER",
            "BUSINESS_OIDC_AUDIENCE",
            "BUSINESS_OIDC_JWKS_URL",
            "BUSINESS_ROLES_CLAIM",
            "BUSINESS_MACHINES_CLAIM",
        )
        if any(not env.get(key, "").strip() for key in required):
            raise ValueError("Business requires explicit DB, OIDC and claim mapping")
        db, issuer, audience, jwks, roles, machines = (env[key] for key in required)
        database = urlparse(db)
        if database.scheme not in {"postgres", "postgresql"} or not database.hostname:
            raise ValueError("Business owner PostgreSQL URL is required")
        local = env.get("BUSINESS_PROFILE", "shared") == "local-test"
        if env.get("BUSINESS_PROFILE", "shared") not in {"shared", "local-test"}:
            raise ValueError("Unsupported Business profile")
        host, port = env.get("BUSINESS_HOST", "0.0.0.0"), int(env.get("BUSINESS_PORT", "8003"))
        ipaddress.ip_address(host)
        if not 1 <= port <= 65535:
            raise ValueError("Invalid Business port")
        if local and (not loopback(database.hostname) or not loopback(host)):
            raise ValueError("Local test Business binds to loopback with a local DB")
        for value in (issuer, jwks):
            url = urlparse(value)
            if (
                not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or url.scheme not in {"http", "https"}
            ):
                raise ValueError("Invalid explicit OIDC URL")
            if local and not loopback(url.hostname):
                raise ValueError("Local test issuer/JWKS must use loopback")
            if not local and url.scheme != "https":
                raise ValueError("Shared OIDC requires HTTPS")
        processing_url = env.get("BUSINESS_PROCESSING_READ_URL", "").strip()
        processing_token = env.get("BUSINESS_PROCESSING_TOKEN_FILE", "").strip()
        if bool(processing_url) != bool(processing_token):
            raise ValueError("Processing status requires both service origin and credential file")
        if processing_url:
            url = urlparse(processing_url)
            if (
                not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or url.scheme not in {"http", "https"}
            ):
                raise ValueError("Invalid Processing service origin")
            if local and not loopback(url.hostname):
                raise ValueError("Local test Processing origin must use loopback")
            if not local and url.scheme != "https":
                raise ValueError("Shared Processing service origin requires HTTPS")
        return cls(
            db,
            issuer,
            audience,
            jwks,
            roles,
            machines,
            host,
            port,
            processing_url or None,
            processing_token or None,
            SSESettings.from_env(env),
            HistorySettings.from_env(env, processing_url=processing_url, local=local),
        )
