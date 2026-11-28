"""Business Consumer needs its own DB and Kafka, without API OIDC or ML settings."""

import ipaddress
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

from backend.shared.copilot.config import M7Settings
from backend.shared.notifications import DEFAULT_CHANNEL, Notifier
from backend.shared.projections.config import RebuildSettings, generations_enabled
from backend.shared.service_auth import INTERNAL_HOSTS, ServiceOAuthConfig, loopback  # noqa: F401
from maintai_messaging.config import KafkaSettings


@dataclass(frozen=True)
class Settings:
    database_url: str = field(repr=False)
    kafka: KafkaSettings
    host: str
    port: int
    notification_channel: str = DEFAULT_CHANNEL
    alert_channel: str = "maintai_business_alert_v1"
    processing_read_url: str | None = None
    processing_token_file: str | None = field(default=None, repr=False)

    m7: M7Settings | None = None
    projection_generations: bool = False
    rebuild: RebuildSettings | None = None
    processing_oauth: ServiceOAuthConfig | None = None
    run_workers: bool = True

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        database_url = env.get("BUSINESS_DATABASE_URL", "")
        database = urlparse(database_url)
        if database.scheme not in {"postgres", "postgresql"} or not database.hostname:
            raise ValueError("Explicit Business owner PostgreSQL URL is required")
        profile = env.get("BUSINESS_CONSUMER_PROFILE", "shared")
        if profile not in {"shared", "local-test"}:
            raise ValueError("Unsupported Business Consumer profile")
        host = env.get("BUSINESS_CONSUMER_HOST", "0.0.0.0")
        ipaddress.ip_address(host)
        port = int(env.get("BUSINESS_CONSUMER_PORT", "8083"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid Consumer probe port")
        kafka = KafkaSettings.from_env(env)
        if profile == "local-test":
            if (
                not (loopback(host) or host == "0.0.0.0")
                or not loopback(database.hostname)
                or not kafka.local_mode
            ):
                raise ValueError("Local test Consumer requires loopback DB/bind and local Kafka")
        elif kafka.local_mode:
            raise ValueError("Shared Consumer cannot enable local Kafka mode")
        channel = Notifier(env.get("BUSINESS_NOTIFY_CHANNEL", DEFAULT_CHANNEL)).channel
        alert_channel = Notifier(
            env.get("BUSINESS_ALERT_NOTIFY_CHANNEL", "maintai_business_alert_v1")
        ).channel
        if alert_channel == channel:
            raise ValueError("Alert and telemetry channels must differ")
        url, token = (
            env.get("BUSINESS_PROCESSING_READ_URL", "").strip(),
            env.get("BUSINESS_PROCESSING_TOKEN_FILE", "").strip(),
        )
        local = profile == "local-test"
        processing_oauth = ServiceOAuthConfig.from_env(env, local=local)
        has_credential = bool(token) or processing_oauth is not None
        if bool(url) != has_credential:
            raise ValueError("Evidence requires both service origin and credentials")
        if profile == "shared" and not url:
            raise ValueError("Shared Alert Consumer requires Processing evidence credentials")
        if url:
            value = urlparse(url)
            if (
                value.scheme not in {"http", "https"}
                or not value.hostname
                or value.username
                or value.password
                or value.query
                or value.fragment
                or value.path not in {"", "/"}
                or (profile == "shared" and value.scheme != "https")
                or (local and not loopback(value.hostname))
            ):
                raise ValueError("Invalid Processing evidence origin")
        m7 = M7Settings.from_env(env, local=local)
        run_workers_raw = env.get("BUSINESS_CONSUMER_RUN_WORKERS", "true").lower()
        if run_workers_raw not in {"true", "false"}:
            raise ValueError("BUSINESS_CONSUMER_RUN_WORKERS must be true or false")
        return cls(
            database_url,
            kafka,
            host,
            port,
            channel,
            alert_channel,
            url or None,
            token or None,
            m7,
            generations_enabled(env),
            RebuildSettings.from_env(env, local=local),
            processing_oauth,
            run_workers_raw == "true",
        )


@dataclass(frozen=True)
class WorkerSettings:
    """Configuration for standalone Business background worker (M7 + rebuild, no Kafka)."""

    database_url: str = field(repr=False)
    host: str
    port: int
    processing_read_url: str | None = None
    processing_token_file: str | None = field(default=None, repr=False)
    m7: M7Settings | None = None
    projection_generations: bool = False
    rebuild: RebuildSettings | None = None
    processing_oauth: ServiceOAuthConfig | None = None

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        database_url = env.get("BUSINESS_DATABASE_URL", "")
        database = urlparse(database_url)
        if database.scheme not in {"postgres", "postgresql"} or not database.hostname:
            raise ValueError("Explicit Business owner PostgreSQL URL is required")
        profile = env.get("BUSINESS_WORKER_PROFILE", env.get("BUSINESS_CONSUMER_PROFILE", "shared"))
        if profile not in {"shared", "local-test"}:
            raise ValueError("Unsupported Business Worker profile")
        host = env.get("BUSINESS_WORKER_HOST", "0.0.0.0")
        ipaddress.ip_address(host)
        port = int(env.get("BUSINESS_WORKER_PORT", "8084"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid Worker probe port")
        local = profile == "local-test"
        if local and (not (loopback(host) or host == "0.0.0.0") or not loopback(database.hostname)):
            raise ValueError("Local test Worker requires loopback DB and bind host")
        url, token = (
            env.get("BUSINESS_PROCESSING_READ_URL", "").strip(),
            env.get("BUSINESS_PROCESSING_TOKEN_FILE", "").strip(),
        )
        processing_oauth = ServiceOAuthConfig.from_env(env, local=local)
        if url:
            has_credential = bool(token) or processing_oauth is not None
            if not has_credential:
                raise ValueError("Processing read URL requires service credentials")
            value = urlparse(url)
            if (
                value.scheme not in {"http", "https"}
                or not value.hostname
                or value.username
                or value.password
                or value.query
                or value.fragment
                or value.path not in {"", "/"}
                or (profile == "shared" and value.scheme != "https")
                or (local and not loopback(value.hostname))
            ):
                raise ValueError("Invalid Processing evidence origin")
        return cls(
            database_url=database_url,
            host=host,
            port=port,
            processing_read_url=url or None,
            processing_token_file=token or None,
            m7=M7Settings.from_env(env, local=local),
            projection_generations=generations_enabled(env),
            rebuild=RebuildSettings.from_env(env, local=local),
            processing_oauth=processing_oauth,
        )
