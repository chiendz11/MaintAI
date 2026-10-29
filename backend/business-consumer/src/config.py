"""Business Consumer needs its own DB and Kafka, without API OIDC or ML settings."""

import ipaddress
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

from backend.shared.notifications import DEFAULT_CHANNEL, Notifier
from maintai_messaging.config import KafkaSettings


def loopback(host):
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


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
            if not loopback(host) or not loopback(database.hostname) or not kafka.local_mode:
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
            env.get("BUSINESS_PROCESSING_READ_URL", ""),
            env.get("BUSINESS_PROCESSING_TOKEN_FILE", ""),
        )
        if bool(url) != bool(token):
            raise ValueError("Evidence requires both service origin and credential file")
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
                or (profile == "local-test" and not loopback(value.hostname))
            ):
                raise ValueError("Invalid Processing evidence origin")
        return cls(
            database_url, kafka, host, port, channel, alert_channel, url or None, token or None
        )
