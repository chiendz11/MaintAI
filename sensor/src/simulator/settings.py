"""Validated configuration for the single durable Sensor controller."""

import ipaddress
import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from simulator.config import SimulatorConfig


def loopback(host: str | None) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host or "").is_loopback
    except ValueError:
        return False


def safe_url(value: str, *, local: bool = False) -> bool:
    url = urlsplit(value)
    return bool(
        url.hostname
        and not url.username
        and not url.password
        and not url.fragment
        and not url.query
        and (url.scheme == "https" or (local and url.scheme == "http" and loopback(url.hostname)))
    )


class RuntimeSettings(SimulatorConfig):
    state_path: Path = Path("sensor/output/state.sqlite3")
    host: str = "127.0.0.1"
    port: int = Field(default=8005, ge=1, le=65535)
    auth_mode: Literal["jwt", "local"] = "jwt"
    jwt_issuer: str = ""
    jwt_audience: str = "simulator-control"
    jwks_url: str = ""
    roles_claim: str = "roles"
    machines_claim: str = "machine_ids"
    control_scope: str = "simulator:control"
    cors_origins: list[str] = Field(default_factory=list)
    ingestion_auth: Literal["token_file", "oauth", "none"] = "token_file"
    ingestion_token_file: Path | None = None
    oauth_token_url: str = ""
    oauth_client_id: str = ""
    oauth_secret_file: Path | None = None
    oauth_scope: str = "sensor:write"
    spool_max_records: int = Field(default=10000, ge=1)
    spool_max_bytes: int = Field(default=32_000_000, ge=1024)
    spool_ttl_seconds: float = Field(default=86400, gt=0)
    retry_max_seconds: float = Field(default=60, ge=1, le=300)
    send_workers: int = Field(default=4, ge=1, le=32)
    initial_state: Literal["RUNNING", "PAUSED"] = "RUNNING"
    max_control_body_bytes: int = Field(default=4096, ge=256, le=65536)
    receipt_max_count: int = Field(default=100000, ge=1)

    @model_validator(mode="after")
    def secure_runtime(self):
        local = self.auth_mode == "local"
        if local and not loopback(self.host):
            raise ValueError("Local anonymous control must bind to loopback")
        if not local and (not safe_url(self.jwt_issuer) or not safe_url(self.jwks_url)):
            raise ValueError("Shared runtime requires HTTPS issuer and JWKS URL")
        if not safe_url(str(self.backend_base_url), local=local):
            raise ValueError("Ingestion requires HTTPS (HTTP loopback only in local mode)")
        if self.ingestion_auth == "none" and not (local and loopback(self.backend_base_url.host)):
            raise ValueError("Anonymous ingestion is limited to explicit local mode and loopback")
        if self.ingestion_auth == "token_file" and self.ingestion_token_file is None:
            raise ValueError("Ingestion token file is required")
        if self.ingestion_auth == "oauth" and not (
            safe_url(self.oauth_token_url, local=local)
            and self.oauth_client_id
            and self.oauth_secret_file
        ):
            raise ValueError("OAuth needs approved token URL, client ID and secret file")
        if self.interval_ms <= 0:
            raise ValueError("Durable runtime requires a positive per-machine interval")
        for origin in self.cors_origins:
            parsed = urlsplit(origin)
            if not safe_url(origin, local=local) or parsed.path not in ("", "/") or origin == "*":
                raise ValueError("CORS must contain explicit approved origins")
        return self

    @classmethod
    def from_env(cls, overrides: dict | None = None):
        import json

        values = SimulatorConfig.from_env().model_dump()
        values["loop_dataset"] = os.environ.get("SIM_LOOP_DATASET", "true")
        for name in cls.model_fields.keys() - SimulatorConfig.model_fields.keys():
            env = os.environ.get(f"SIM_{name.upper()}")
            if env is not None:
                values[name] = json.loads(env) if name == "cors_origins" else env
        values.update({k: v for k, v in (overrides or {}).items() if v is not None})
        return cls.model_validate(values)
