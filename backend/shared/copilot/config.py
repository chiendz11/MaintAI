"""Explicit opt-in settings shared by the two Business runtimes."""

import ipaddress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


def origin(value, *, local):
    parsed = urlparse(value)
    try:
        loopback = (
            parsed.hostname == "localhost" or ipaddress.ip_address(parsed.hostname).is_loopback
        )
    except ValueError:
        loopback = False
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed.scheme not in {"http", "https"}
        or (parsed.scheme != "https" and not (local and loopback))
    ):
        raise ValueError("Service origins require HTTPS or explicit loopback tests")
    return value.rstrip("/")


@dataclass(frozen=True)
class M7Settings:
    rag_url: str
    rag_token_file: str
    cursor_key_file: str
    bucket: str
    region: str
    endpoint: str | None = None
    model_version: str | None = None
    ml_url: str | None = None
    local: bool = False

    @classmethod
    def from_env(cls, env, *, local):
        flag = env.get("BUSINESS_M7_ENABLED", "false").lower()
        if flag not in {"true", "false"}:
            raise ValueError("BUSINESS_M7_ENABLED must be true or false")
        if flag == "false":
            return None
        names = (
            "BUSINESS_RAG_URL",
            "BUSINESS_RAG_TOKEN_FILE",
            "BUSINESS_DRIFT_CURSOR_KEY_FILE",
            "BUSINESS_DRIFT_BUCKET",
            "BUSINESS_DRIFT_S3_REGION",
        )
        if any(not env.get(name, "").strip() for name in names):
            raise ValueError("M7 requires RAG credentials, catalog cursor key, bucket and region")
        url, token, key, bucket, region = (env[name] for name in names)
        url = origin(url, local=local)
        for filename in (token, key):
            if not Path(filename).is_file() or Path(filename).stat().st_size > 16384:
                raise ValueError("M7 credentials must be bounded regular files")
        if len(Path(key).read_bytes()) < 32:
            raise ValueError("M7 cursor key requires at least 32 bytes")
        endpoint = env.get("BUSINESS_DRIFT_S3_ENDPOINT") or None
        ml = env.get("BUSINESS_ML_URL") or None
        if endpoint:
            endpoint = origin(endpoint, local=local)
        if ml:
            ml = origin(ml, local=local)
        return cls(
            url,
            token,
            key,
            bucket,
            region,
            endpoint,
            env.get("BUSINESS_MONITORING_MODEL_VERSION") or None,
            ml,
            local,
        )
