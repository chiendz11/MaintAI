"""Transport-only relay configuration; never loads model identity fixtures."""

import math
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

from consumer.config import loopback
from consumer.observability import probe_host
from maintai_messaging.config import KafkaSettings


@dataclass(frozen=True)
class RelaySettings:
    profile: str
    kafka: KafkaSettings
    database_url: str = field(repr=False)
    health_port: int
    lease_seconds: float
    batch_size: int
    health_host: str = "127.0.0.1"
    execution_mode: str = "legacy"
    poll_interval_seconds: float = 0.25

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        mode = env.get("OUTBOX_EXECUTION_MODE", "legacy")
        if mode not in {"legacy", "ordered"}:
            raise ValueError("Unsupported relay mode")
        profile = env.get("PROCESSING_PROFILE", "shared")
        if profile not in {"local-fixture", "shared"}:
            raise ValueError("Explicit supported relay profile is required")
        kafka = KafkaSettings.from_env(env)
        dsn = env.get("PROCESSING_DATABASE_URL", "")
        database = urlparse(dsn)
        if database.scheme not in {"postgres", "postgresql"} or not database.hostname:
            raise ValueError("Processing owner PostgreSQL URL is required")
        if profile == "local-fixture":
            if not kafka.local_mode or not loopback(database.hostname):
                raise ValueError("Fixture relay requires local Kafka and database")
            if any(
                not loopback(server.rsplit(":", 1)[0].strip("[]"))
                for server in kafka.bootstrap_servers.split(",")
            ):
                raise ValueError("Fixture relay requires a loopback broker")
        elif kafka.local_mode or env.get("PROCESSING_MODEL_IDENTITY_FIXTURE"):
            raise ValueError("Fixture settings cannot be used in shared relay")
        port = int(env.get("PROCESSING_HEALTH_PORT", "8082"))

        def setting(name, legacy, default):
            if name in env and legacy in env and env[name] != env[legacy]:
                raise ValueError("Conflicting relay configuration aliases")
            return env.get(name, env.get(legacy, default))

        lease = float(
            setting("OUTBOX_CLAIM_LEASE_SECONDS", "PROCESSING_OUTBOX_LEASE_SECONDS", "120")
        )
        batch = int(setting("OUTBOX_BATCH_SIZE", "PROCESSING_OUTBOX_BATCH_SIZE", "1"))
        interval = float(env.get("OUTBOX_POLL_INTERVAL_MS", "250")) / 1000
        if not math.isfinite(interval) or not 0.01 <= interval <= 10:
            raise ValueError("Relay poll interval must be bounded")
        if not 1 <= port <= 65535 or not 1 <= batch <= 100 or not math.isfinite(lease):
            raise ValueError("Invalid relay probe or lease budget")
        if lease <= (batch if mode == "legacy" else 1) * (kafka.delivery_timeout_ms / 1000 + 25):
            raise ValueError("Relay lease is too short for its complete batch")
        return cls(
            profile,
            kafka,
            dsn,
            port,
            lease,
            batch,
            health_host=probe_host(env),
            execution_mode=mode,
            poll_interval_seconds=interval,
        )
