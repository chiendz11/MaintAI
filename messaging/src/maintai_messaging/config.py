"""One validated configuration for local clients and Strimzi SASL/TLS clients."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .errors import ConfigurationError


@dataclass(frozen=True)
class KeyedSettings:
    """Opt-in Processing budgets, separate from default Business/Storage config."""

    max_active_keys: int = 8
    max_pending_records: int = 512
    max_pending_bytes: int = 16777216
    max_pending_per_key: int = 64
    max_uncommitted_per_partition: int = 128
    resume_low_watermark: float = 0.5
    handler_timeout_seconds: float = 15
    drain_timeout_seconds: float = 30

    def __post_init__(self):
        for name in (
            "max_active_keys",
            "max_pending_records",
            "max_pending_bytes",
            "max_pending_per_key",
            "max_uncommitted_per_partition",
        ):
            if not isinstance(getattr(self, name), int) or getattr(self, name) < 1:
                raise ConfigurationError("Keyed capacity settings must be positive integers")
        if self.max_active_keys > self.max_pending_records:
            raise ConfigurationError("Active keys cannot exceed the receipt budget")
        if not 0 < self.resume_low_watermark < 1:
            raise ConfigurationError("Resume watermark must be between zero and one")
        for value in (self.handler_timeout_seconds, self.drain_timeout_seconds):
            if not math.isfinite(value) or not 0 < value <= 120:
                raise ConfigurationError("Keyed deadlines must be finite and at most 120 seconds")

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        integers = {
            "max_active_keys": "PROCESSING_MAX_ACTIVE_KEYS",
            "max_pending_records": "PROCESSING_MAX_PENDING_RECORDS",
            "max_pending_bytes": "PROCESSING_MAX_PENDING_BYTES",
            "max_pending_per_key": "PROCESSING_MAX_PENDING_PER_KEY",
            "max_uncommitted_per_partition": "PROCESSING_MAX_UNCOMMITTED_PER_PARTITION",
        }
        defaults = cls()
        values = {key: int(env.get(name, getattr(defaults, key))) for key, name in integers.items()}
        for key, name in {
            "resume_low_watermark": "PROCESSING_RESUME_LOW_WATERMARK",
            "handler_timeout_seconds": "PROCESSING_HANDLER_TIMEOUT_SECONDS",
            "drain_timeout_seconds": "PROCESSING_DRAIN_TIMEOUT_SECONDS",
        }.items():
            values[key] = float(env.get(name, getattr(defaults, key)))
        return cls(**values)


def flag(value: str) -> bool:
    if value.lower() not in {"true", "false"}:
        raise ConfigurationError("Boolean settings must be true or false")
    return value.lower() == "true"


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "127.0.0.1:19092"
    client_id: str = "maintai"
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "SCRAM-SHA-512"
    username: str = ""
    password: str = field(default="", repr=False)
    ca_location: str = ""
    local_mode: bool = False
    group_prefix: str = ""
    delivery_timeout_ms: int = 10000
    poll_interval_seconds: float = 0.1
    max_payload_bytes: int = 262144
    producer_git_sha: str = ""

    def __post_init__(self):
        if not self.bootstrap_servers or not self.client_id:
            raise ConfigurationError("Kafka bootstrap servers and client ID are required")
        if self.security_protocol == "PLAINTEXT":
            if not self.local_mode:
                raise ConfigurationError("PLAINTEXT requires explicit KAFKA_LOCAL_MODE=true")
        elif self.security_protocol == "SASL_SSL":
            if self.sasl_mechanism != "SCRAM-SHA-512":
                raise ConfigurationError("Use the reviewed SCRAM-SHA-512 protocol")
            if not self.username or not self.password:
                raise ConfigurationError("SASL username/password are required")
            if not self.ca_location or not Path(self.ca_location).is_file():
                raise ConfigurationError("A readable Kafka TLS CA file is required")
        else:
            raise ConfigurationError("Supported Kafka protocols: SASL_SSL and local PLAINTEXT")
        if not 1000 <= self.delivery_timeout_ms <= 120000:
            raise ConfigurationError("Delivery timeout must be between 1000 and 120000 ms")
        if not 0.01 <= self.poll_interval_seconds <= 1:
            raise ConfigurationError("Poll interval must be between 0.01 and 1 second")
        if not 1024 <= self.max_payload_bytes <= 524288:
            raise ConfigurationError("Payload limit must be between 1 KiB and 512 KiB")
        if self.group_prefix and not self.local_mode:
            raise ConfigurationError(
                "Custom group prefixes are local-only; production uses topic ACLs"
            )
        if self.producer_git_sha and not re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}", self.producer_git_sha
        ):
            raise ConfigurationError("Producer Git SHA must be an actual Git object ID")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> KafkaSettings:
        env = os.environ if env is None else env
        for name in ["KAFKA_ENABLE_AUTO_COMMIT", "KAFKA_ENABLE_AUTO_OFFSET_STORE"]:
            if flag(env.get(name, "false")):
                raise ConfigurationError(f"{name} must remain false")
        if env.get("KAFKA_ACKS", "all") not in {"all", "-1"}:
            raise ConfigurationError("Kafka producers require acks=all")
        if not flag(env.get("KAFKA_ENABLE_IDEMPOTENCE", "true")):
            raise ConfigurationError("Kafka idempotent production is required")
        return cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
            client_id=env.get("KAFKA_CLIENT_ID", "maintai"),
            security_protocol=env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=env.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            username=env.get("KAFKA_SASL_USERNAME", ""),
            password=env.get("KAFKA_SASL_PASSWORD", ""),
            ca_location=env.get("KAFKA_SSL_CA_LOCATION", ""),
            local_mode=flag(env.get("KAFKA_LOCAL_MODE", "false")),
            group_prefix=env.get("KAFKA_GROUP_PREFIX", ""),
            delivery_timeout_ms=int(env.get("KAFKA_DELIVERY_TIMEOUT_MS", "10000")),
            poll_interval_seconds=float(env.get("KAFKA_POLL_INTERVAL_SECONDS", "0.1")),
            max_payload_bytes=int(env.get("KAFKA_MAX_PAYLOAD_BYTES", "262144")),
            producer_git_sha=env.get("PRODUCER_GIT_SHA", ""),
        )

    def client_config(self) -> dict:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol == "SASL_SSL":
            config.update(
                {
                    "sasl.mechanism": self.sasl_mechanism,
                    "sasl.username": self.username,
                    "sasl.password": self.password,
                    "ssl.ca.location": self.ca_location,
                    "enable.ssl.certificate.verification": True,
                    "ssl.endpoint.identification.algorithm": "https",
                }
            )
        return config

    def producer_config(self) -> dict:
        return {
            **self.client_config(),
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "message.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": min(self.delivery_timeout_ms, 5000),
            "linger.ms": 0,
        }

    def consumer_config(self, group_id: str) -> dict:
        return {
            **self.client_config(),
            "group.id": self.group_prefix + group_id,
            "allow.auto.create.topics": False,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "partition.assignment.strategy": "range",
            "max.poll.interval.ms": 300000,
            "queued.max.messages.kbytes": 1024,
        }
