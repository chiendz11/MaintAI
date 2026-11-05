"""Generation serving and recovery execution are separate explicit opt-ins."""

from dataclasses import dataclass, field
from urllib.parse import urlparse

from backend.shared.copilot.config import origin


def flag(env, name):
    value = env.get(name, "false").lower()
    if value not in {"true", "false"}:
        raise ValueError(name + " must be true or false")
    return value == "true"


def generations_enabled(env):
    return flag(env, "BUSINESS_PROJECTION_GENERATIONS_ENABLED")


@dataclass(frozen=True)
class RebuildSettings:
    database_url: str = field(repr=False)
    bucket: str
    region: str
    endpoint: str | None = None

    @classmethod
    def from_env(cls, env, *, local):
        if not flag(env, "BUSINESS_REBUILD_ENABLED"):
            return None
        names = (
            "BUSINESS_REBUILD_DATABASE_URL",
            "BUSINESS_REBUILD_RAW_BUCKET",
            "BUSINESS_REBUILD_S3_REGION",
            "BUSINESS_PROCESSING_READ_URL",
            "BUSINESS_PROCESSING_TOKEN_FILE",
        )
        if not generations_enabled(env) or any(not env.get(name, "").strip() for name in names):
            raise ValueError(
                "Rebuild requires generations, separate owner DB, Storage and Processing settings"
            )
        dsn = urlparse(env[names[0]])
        live = urlparse(env.get("BUSINESS_DATABASE_URL", ""))
        if dsn.scheme not in {"postgres", "postgresql"} or not dsn.hostname:
            raise ValueError("Explicit Business rebuild owner PostgreSQL URL is required")
        if (dsn.hostname, dsn.port, dsn.path) != (live.hostname, live.port, live.path):
            raise ValueError("Rebuild must access the same Business owner database")
        if not local and (not dsn.username or dsn.username == live.username):
            raise ValueError("Shared rebuild execution requires a distinct owner role")
        endpoint = env.get("BUSINESS_REBUILD_S3_ENDPOINT") or None
        if endpoint:
            endpoint = origin(endpoint, local=local)
        return cls(env[names[0]], env[names[1]], env[names[2]], endpoint)
