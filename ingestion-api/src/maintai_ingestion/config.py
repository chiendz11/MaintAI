"""Explicit local/shared profiles with fail-closed configuration."""

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlparse

from maintai_messaging.config import KafkaSettings
from maintai_messaging.errors import ConfigurationError

INTERNAL_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "kafka",
    "postgres",
    "keycloak",
    "business-api",
    "business-consumer",
    "processing-worker",
    "processing-read-api",
    "processing-outbox-relay",
    "ml-service",
    "ml-service-shadow",
    "mlflow",
    "sensor",
    "simulator-ui",
    "storage-consumer",
    "minio",
    "rag-service",
    "ingestion-api",
    "mosquitto",
}


def loopback(host: str) -> bool:
    if not host:
        return False
    if (
        host in INTERNAL_HOSTS
        or host.endswith(".svc.cluster.local")
        or host.endswith(".local")
        or host.endswith(".localhost")
        or host.endswith(".internal")
    ):
        return True
    return host in {"127.0.0.1", "::1", "localhost"}


def number(env, key, default, low, high):
    value = float(env.get(key, str(default)))
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError("Invalid numeric configuration")
    return value


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings = field(repr=False)
    business_origin: str
    environment: str = "shared"
    host: str = "127.0.0.1"
    port: int = 8000
    auth_mode: str = "oidc"
    issuer: str = ""
    jwks_url: str = ""
    audience: str = "ingestion"
    algorithms: tuple[str, ...] = ("RS256",)
    roles_claim: str = ""
    machines_claim: str = ""
    local_subject: str = ""
    local_machines: frozenset[str] = frozenset()
    jwks_timeout: float = 2
    jwks_cache: float = 300
    max_inflight: int = 8
    body_timeout: float = 10
    probe_timeout: float = 1
    probe_interval: float = 5
    mqtt_enabled: bool = False
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_topic_filter: str = "maintai/v1/site/+/machines/+/readings"
    mqtt_client_id: str = "maintai-ingestion-mqtt-bridge"
    mqtt_batch_size: int = 32
    mqtt_batch_wait_ms: int = 5
    mqtt_max_inflight: int = 256
    mqtt_source_subject: str = ""
    mqtt_allowed_machines: frozenset[str] = frozenset()

    @classmethod
    def from_env(cls, env: Mapping[str, str]):
        try:
            kafka = KafkaSettings.from_env(env)
            origin = env.get("BUSINESS_PUBLIC_ORIGIN", "")
            parsed = urlparse(origin)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("Invalid origin")
            environment = env.get("INGESTION_ENV", "shared")
            host = env.get("INGESTION_HOST", "127.0.0.1")
            auth_mode = env.get("AUTH_MODE", "oidc")
            if environment not in {"local", "test", "shared"} or not host:
                raise ValueError("Invalid profile")
            if environment == "shared" and (kafka.local_mode or parsed.scheme != "https"):
                raise ValueError("Shared profile requires TLS")
            subject = env.get("LOCAL_SOURCE_SUBJECT", "")
            machines = frozenset(
                x.strip() for x in env.get("LOCAL_ALLOWED_MACHINE_IDS", "").split(",") if x.strip()
            )
            if auth_mode == "local-fixture":
                if (
                    environment not in {"local", "test"}
                    or not loopback(host)
                    or not kafka.local_mode
                    or not subject
                    or len(subject) > 256
                    or not machines
                    or "*" in machines
                ):
                    raise ValueError("Invalid fixture profile")
            elif auth_mode == "oidc":
                for key in ("OIDC_ISSUER", "OIDC_JWKS_URL"):
                    url = urlparse(env.get(key, ""))
                    if (
                        not url.hostname
                        or url.username is not None
                        or url.password is not None
                        or url.fragment
                        or url.scheme not in {"https", "http"}
                        or (
                            url.scheme != "https"
                            and not (environment in {"local", "test"} and loopback(url.hostname))
                        )
                    ):
                        raise ValueError("Invalid identity provider")
                if not env.get("OIDC_ROLES_CLAIM") or not env.get("OIDC_MACHINES_CLAIM"):
                    raise ValueError("Explicit claim mapping required")
            else:
                raise ValueError("Invalid auth mode")
            algorithms = tuple(x.strip() for x in env.get("OIDC_ALGORITHMS", "RS256").split(","))
            if not algorithms or any(
                x not in {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"}
                for x in algorithms
            ):
                raise ValueError("Invalid algorithm allowlist")
            port = int(env.get("INGESTION_PORT", "8000"))
            inflight = int(env.get("INGESTION_MAX_INFLIGHT", "8"))
            if not 1 <= port <= 65535 or not 1 <= inflight <= 1024:
                raise ValueError("Invalid capacity")
            audience = env.get("OIDC_AUDIENCE", "ingestion")
            if audience != "ingestion":
                raise ValueError("Ingestion audience required")
            mqtt_enabled = env.get("INGESTION_MQTT_ENABLED", "false").lower() == "true"
            if env.get("INGESTION_MQTT_ENABLED", "false").lower() not in {"true", "false"}:
                raise ValueError("Invalid MQTT toggle")
            mqtt_host = env.get("INGESTION_MQTT_HOST", "mosquitto").strip()
            mqtt_port = int(env.get("INGESTION_MQTT_PORT", "1883"))
            mqtt_topic_filter = env.get(
                "INGESTION_MQTT_TOPIC_FILTER", "maintai/v1/site/+/machines/+/readings"
            ).strip()
            mqtt_client_id = env.get(
                "INGESTION_MQTT_CLIENT_ID", "maintai-ingestion-mqtt-bridge"
            ).strip()
            mqtt_batch_size = int(env.get("INGESTION_MQTT_BATCH_SIZE", "32"))
            mqtt_batch_wait_ms = int(env.get("INGESTION_MQTT_BATCH_WAIT_MS", "5"))
            mqtt_max_inflight = int(env.get("INGESTION_MQTT_MAX_INFLIGHT", "256"))
            mqtt_source_subject = env.get("INGESTION_MQTT_SOURCE_SUBJECT", subject).strip()
            raw_mqtt_machines = env.get("INGESTION_MQTT_ALLOWED_MACHINE_IDS", "")
            mqtt_allowed_machines = (
                frozenset(x.strip() for x in raw_mqtt_machines.split(",") if x.strip())
                if raw_mqtt_machines.strip()
                else machines
            )
            if mqtt_enabled:
                if (
                    not mqtt_host
                    or not 1 <= mqtt_port <= 65535
                    or not mqtt_topic_filter
                    or mqtt_topic_filter.startswith("$share/")
                    or not mqtt_client_id
                    or not 1 <= mqtt_batch_size <= 256
                    or not 0 <= mqtt_batch_wait_ms <= 5000
                    or not 1 <= mqtt_max_inflight <= 4096
                    or not mqtt_source_subject
                    or not mqtt_allowed_machines
                    or "*" in mqtt_allowed_machines
                ):
                    raise ValueError("Invalid MQTT bridge configuration")
                if environment == "shared" and not loopback(mqtt_host):
                    # Shared deployments must still enforce explicit TLS/network boundaries.
                    pass
            return cls(
                kafka=kafka,
                business_origin=origin.rstrip("/"),
                environment=environment,
                host=host,
                port=port,
                auth_mode=auth_mode,
                issuer=env.get("OIDC_ISSUER", ""),
                jwks_url=env.get("OIDC_JWKS_URL", ""),
                audience=audience,
                algorithms=algorithms,
                roles_claim=env.get("OIDC_ROLES_CLAIM", ""),
                machines_claim=env.get("OIDC_MACHINES_CLAIM", ""),
                local_subject=subject,
                local_machines=machines,
                jwks_timeout=number(env, "OIDC_JWKS_TIMEOUT_SECONDS", 2, 0.1, 10),
                jwks_cache=number(env, "OIDC_JWKS_CACHE_SECONDS", 300, 1, 3600),
                max_inflight=inflight,
                body_timeout=number(env, "INGESTION_BODY_TIMEOUT_SECONDS", 10, 0.01, 120),
                probe_timeout=number(env, "INGESTION_PROBE_TIMEOUT_SECONDS", 1, 0.01, 5),
                probe_interval=number(env, "INGESTION_PROBE_INTERVAL_SECONDS", 5, 0.1, 5),
                mqtt_enabled=mqtt_enabled,
                mqtt_host=mqtt_host,
                mqtt_port=mqtt_port,
                mqtt_topic_filter=mqtt_topic_filter,
                mqtt_client_id=mqtt_client_id,
                mqtt_batch_size=mqtt_batch_size,
                mqtt_batch_wait_ms=mqtt_batch_wait_ms,
                mqtt_max_inflight=mqtt_max_inflight,
                mqtt_source_subject=mqtt_source_subject,
                mqtt_allowed_machines=mqtt_allowed_machines,
            )
        except (ValueError, TypeError, OverflowError, ConfigurationError):
            raise ValueError("Invalid ingestion configuration") from None
