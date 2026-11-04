"""Explicit read-only Processing DB/OIDC settings; signed tokens are required."""

import ipaddress
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

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
    history: HistorySettings | None = None

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        required = (
            "PROCESSING_READ_DATABASE_URL",
            "PROCESSING_READ_OIDC_ISSUER",
            "PROCESSING_READ_OIDC_AUDIENCE",
            "PROCESSING_READ_OIDC_JWKS_URL",
            "PROCESSING_READ_ROLES_CLAIM",
            "PROCESSING_READ_MACHINES_CLAIM",
        )
        if any(not env.get(key, "").strip() for key in required):
            raise ValueError("Processing Read API requires explicit DB, OIDC and claim mapping")
        db, issuer, audience, jwks, roles, machines = (env[key] for key in required)
        database = urlparse(db)
        if database.scheme not in {"postgres", "postgresql"} or not database.hostname:
            raise ValueError("Processing Read API owner PostgreSQL URL is required")
        local = env.get("PROCESSING_READ_PROFILE", "shared") == "local-test"
        if env.get("PROCESSING_READ_PROFILE", "shared") not in {"shared", "local-test"}:
            raise ValueError("Unsupported Processing Read API profile")
        host, port = (
            env.get("PROCESSING_READ_HOST", "0.0.0.0"),
            int(env.get("PROCESSING_READ_PORT", "8004")),
        )
        ipaddress.ip_address(host)
        if not 1 <= port <= 65535:
            raise ValueError("Invalid Processing Read API port")
        if local and (not loopback(database.hostname) or not loopback(host)):
            raise ValueError("Local test Processing Read API binds to loopback with a local DB")
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
        return cls(
            db,
            issuer,
            audience,
            jwks,
            roles,
            machines,
            host,
            port,
            HistorySettings.from_env(env, db, local=local),
        )
