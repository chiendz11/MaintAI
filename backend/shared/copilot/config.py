"""Explicit opt-in settings shared by the two Business runtimes."""

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from backend.shared.service_auth import ServiceOAuthConfig, loopback


def origin(value, *, local):
    parsed = urlparse(value)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed.scheme not in {"http", "https"}
        or (parsed.scheme != "https" and not (local and loopback(parsed.hostname)))
    ):
        raise ValueError("Service origins require HTTPS or explicit loopback tests")
    return value.rstrip("/")


@dataclass(frozen=True)
class M7Settings:
    rag_url: str
    rag_token_file: str | None
    cursor_key_file: str
    bucket: str
    region: str
    endpoint: str | None = None
    model_version: str | None = None
    ml_url: str | None = None
    local: bool = False
    rag_oauth: ServiceOAuthConfig | None = None

    @classmethod
    def from_env(cls, env, *, local):
        flag = env.get("BUSINESS_M7_ENABLED", "false").lower()
        if flag not in {"true", "false"}:
            raise ValueError("BUSINESS_M7_ENABLED must be true or false")
        if flag == "false":
            return None
        names = (
            "BUSINESS_RAG_URL",
            "BUSINESS_DRIFT_CURSOR_KEY_FILE",
            "BUSINESS_DRIFT_BUCKET",
            "BUSINESS_DRIFT_S3_REGION",
        )
        if any(not env.get(name, "").strip() for name in names):
            raise ValueError("M7 requires RAG credentials, catalog cursor key, bucket and region")
        url, key, bucket, region = (env[name] for name in names)
        url = origin(url, local=local)
        rag_oauth = ServiceOAuthConfig.from_env(env, prefix="BUSINESS_RAG_OAUTH_", local=local)
        token = env.get("BUSINESS_RAG_TOKEN_FILE", "").strip() or None
        if rag_oauth is None and token is None:
            raise ValueError("M7 requires RAG credentials, catalog cursor key, bucket and region")
        files_to_check = (token, key) if token is not None else (key,)
        for filename in files_to_check:
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
            rag_oauth=rag_oauth,
        )
