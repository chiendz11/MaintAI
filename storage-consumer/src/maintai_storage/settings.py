"""Offline configuration validation; no AWS credentials or endpoints are contacted."""

from __future__ import annotations

import ipaddress
import math
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from maintai_messaging.config import KafkaSettings, flag
from maintai_messaging.errors import ConfigurationError


def number(env, name, default, minimum, maximum, *, integer=False):
    try:
        value = int(env.get(name, default)) if integer else float(env.get(name, default))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"Invalid {name}") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} outside supported bounds")
    return value


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings
    bucket: str
    prefix: str = "raw-sensor"
    endpoint_url: str | None = None
    region: str | None = None
    allow_unversioned: bool = False
    archive_outcomes: bool = False
    addressing_style: str = "auto"
    host: str = "0.0.0.0"
    port: int = 8008
    retry_attempts: int = 5
    retry_initial_seconds: float = 1
    retry_max_seconds: float = 15
    connect_timeout_seconds: float = 3
    read_timeout_seconds: float = 10
    probe_interval_seconds: float = 15
    probe_timeout_seconds: float = 3
    readiness_max_age_seconds: float = 60
    liveness_max_age_seconds: float = 90
    catalog_enabled: bool = False
    catalog_prefix: str = "raw-catalog/v1"
    catalog_interval_seconds: float = 5
    catalog_batch_records: int = 100
    catalog_poll_seconds: float = 0.5
    catalog_heartbeat_seconds: float = 30

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        try:
            kafka = KafkaSettings.from_env({"KAFKA_CLIENT_ID": "maintai-storage", **env})
        except (TypeError, ValueError) as exc:
            raise ConfigurationError("Invalid Kafka numeric configuration") from exc
        bucket = env.get("S3_RAW_BUCKET", "")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
            raise ConfigurationError("S3_RAW_BUCKET must be a bucket name, not a URL")
        try:
            ipaddress.ip_address(bucket)
        except ValueError:
            pass
        else:
            raise ConfigurationError("Bucket name cannot be an IP address")
        if ".." in bucket or ".-" in bucket or "-." in bucket:
            raise ConfigurationError("Invalid bucket name")
        prefix = env.get("S3_RAW_PREFIX", "raw-sensor")
        if not re.fullmatch(r"[A-Za-z0-9_./=-]{1,200}", prefix) or any(
            part in {"", ".", ".."} for part in prefix.split("/")
        ):
            raise ConfigurationError("S3_RAW_PREFIX must be a stable relative object prefix")
        catalog_prefix = env.get("STORAGE_CATALOG_PREFIX", "raw-catalog/v1")
        if not re.fullmatch(r"[A-Za-z0-9_./=-]{1,200}", catalog_prefix) or any(
            part in {"", ".", ".."} for part in catalog_prefix.split("/")
        ):
            raise ConfigurationError("Invalid STORAGE_CATALOG_PREFIX")
        if (
            prefix == catalog_prefix
            or prefix.startswith(catalog_prefix + "/")
            or catalog_prefix.startswith(prefix + "/")
        ):
            raise ConfigurationError("Raw and catalog prefixes must not overlap")
        endpoint = env.get("S3_ENDPOINT_URL") or None
        if endpoint:
            try:
                url = urlsplit(endpoint)
                valid = (
                    url.scheme in {"http", "https"}
                    and url.hostname
                    and not url.username
                    and not url.password
                    and not url.query
                    and not url.fragment
                    and url.path in {"", "/"}
                    and (url.port is None or 1 <= url.port <= 65535)
                    and not any(c.isspace() for c in endpoint)
                )
            except ValueError as exc:
                raise ConfigurationError("Invalid S3 endpoint URL") from exc
            if not valid or (url.scheme != "https" and not kafka.local_mode):
                raise ConfigurationError("S3 endpoint requires TLS outside explicit local mode")
        allow = flag(env.get("S3_ALLOW_UNVERSIONED", "false"))
        if allow and not kafka.local_mode:
            raise ConfigurationError("Unversioned buckets are explicit local-only fixtures")
        style = env.get("S3_ADDRESSING_STYLE", "auto")
        if style not in {"auto", "path", "virtual"}:
            raise ConfigurationError("Invalid S3_ADDRESSING_STYLE")
        host = env.get("STORAGE_HTTP_HOST", "0.0.0.0")
        try:
            ipaddress.IPv4Address(host)
        except ipaddress.AddressValueError as exc:
            raise ConfigurationError("STORAGE_HTTP_HOST must be an IPv4 bind address") from exc
        result = cls(
            kafka=kafka,
            bucket=bucket,
            prefix=prefix,
            endpoint_url=endpoint,
            region=env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or None,
            allow_unversioned=allow,
            archive_outcomes=flag(env.get("STORAGE_ARCHIVE_OUTCOMES", "false")),
            addressing_style=style,
            host=host,
            port=number(env, "STORAGE_HTTP_PORT", 8008, 1, 65535, integer=True),
            retry_attempts=number(env, "STORAGE_RETRY_ATTEMPTS", 5, 1, 10, integer=True),
            retry_initial_seconds=number(env, "STORAGE_RETRY_INITIAL_SECONDS", 1, 0.01, 30),
            retry_max_seconds=number(env, "STORAGE_RETRY_MAX_SECONDS", 15, 0.01, 60),
            connect_timeout_seconds=number(env, "S3_CONNECT_TIMEOUT_SECONDS", 3, 0.1, 10),
            read_timeout_seconds=number(env, "S3_READ_TIMEOUT_SECONDS", 10, 0.1, 30),
            probe_interval_seconds=number(env, "STORAGE_PROBE_INTERVAL_SECONDS", 15, 0.1, 60),
            probe_timeout_seconds=number(env, "STORAGE_PROBE_TIMEOUT_SECONDS", 3, 0.1, 10),
            readiness_max_age_seconds=number(env, "STORAGE_READINESS_MAX_AGE_SECONDS", 60, 1, 180),
            liveness_max_age_seconds=number(env, "STORAGE_LIVENESS_MAX_AGE_SECONDS", 90, 2, 300),
            catalog_enabled=flag(env.get("STORAGE_CATALOG_ENABLED", "false")),
            catalog_prefix=catalog_prefix,
            catalog_interval_seconds=number(env, "STORAGE_CATALOG_INTERVAL_SECONDS", 5, 0.1, 60),
            catalog_batch_records=number(
                env, "STORAGE_CATALOG_BATCH_RECORDS", 100, 1, 100, integer=True
            ),
            catalog_poll_seconds=number(env, "STORAGE_CATALOG_POLL_SECONDS", 0.5, 0.1, 5),
            catalog_heartbeat_seconds=number(env, "STORAGE_CATALOG_HEARTBEAT_SECONDS", 30, 1, 60),
        )
        if result.catalog_enabled and (result.allow_unversioned or result.archive_outcomes):
            raise ConfigurationError("M6 catalog requires versioning and raw-only archive groups")
        if result.retry_initial_seconds > result.retry_max_seconds:
            raise ConfigurationError("Retry initial delay exceeds maximum delay")
        if result.readiness_max_age_seconds <= result.probe_interval_seconds:
            raise ConfigurationError("Readiness freshness must exceed probe interval")
        # A failed dependency check must not itself trip liveness.
        probe_budget = (
            result.connect_timeout_seconds
            + result.read_timeout_seconds
            + result.probe_timeout_seconds
            + result.probe_interval_seconds
            + 5
        )
        if result.liveness_max_age_seconds <= probe_budget:
            raise ConfigurationError("Liveness budget too short for bounded dependency probes")
        return result

    @property
    def roles(self):
        roles = ["storage-accepted", "storage-rejected"]
        if self.archive_outcomes:
            roles.append("storage-outcomes")
        return tuple(roles)

    def create_s3_client(self):
        import boto3
        from botocore.config import Config

        # Use the standard credential chain (IRSA, profiles, env); never a URL credential.
        # Ignore AWS_ENDPOINT_URL overrides so the endpoint validated above is authoritative.
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            config=Config(
                connect_timeout=self.connect_timeout_seconds,
                read_timeout=self.read_timeout_seconds,
                retries={"total_max_attempts": 1, "mode": "standard"},
                s3={"addressing_style": self.addressing_style},
                ignore_configured_endpoint_urls=True,
            ),
        )
