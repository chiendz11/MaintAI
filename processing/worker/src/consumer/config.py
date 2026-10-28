"""Explicit local-fixture runtime until the serving-bound identity handoff freezes.

HTTP clients and domain callbacks support reviewed provider contracts. This entry
point deliberately cannot turn an example identity file into shared deployment.
"""

import ipaddress
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from urllib.parse import urlparse

from maintai_messaging.config import KafkaSettings, KeyedSettings
from maintai_messaging.contracts import strict_json
from maintai_messaging.outcomes import ModelIdentity
from processing.shared.data_quality.validator import FeatureValidator
from processing.shared.risk_engine.policy import RiskPolicy

from .observability import probe_host


def loopback(host):
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class WorkerSettings:
    profile: str
    kafka: KafkaSettings
    database_url: str = field(repr=False)
    business_url: str
    ml_url: str
    schema_directory: Path
    validator: FeatureValidator
    policy: RiskPolicy
    identity_fixture: ModelIdentity
    health_port: int
    health_host: str = "127.0.0.1"
    registry_token: str | None = field(default=None, repr=False)
    execution_mode: str = "serial"
    keyed_limits: KeyedSettings = field(default_factory=KeyedSettings)

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        mode = env.get("PROCESSING_EXECUTION_MODE", "serial")
        if mode not in {"serial", "keyed"}:
            raise ValueError("Unsupported Processing execution mode")
        if env.get("PROCESSING_PROFILE") != "local-fixture":
            raise ValueError(
                "Shared worker requires the reviewed registry/serving-identity handoff"
            )
        kafka = KafkaSettings.from_env(env)
        if not kafka.local_mode or not kafka.group_prefix or not kafka.producer_git_sha:
            raise ValueError(
                "Fixture worker requires local Kafka, a group prefix and producer Git identity"
            )
        if any(
            not loopback(server.rsplit(":", 1)[0].strip("[]"))
            for server in kafka.bootstrap_servers.split(",")
        ):
            raise ValueError("Fixture worker requires a loopback broker")
        dsn = env.get("PROCESSING_DATABASE_URL", "")
        business, ml = env.get("BUSINESS_BASE_URL", ""), env.get("ML_BASE_URL", "")
        db = urlparse(dsn)
        if db.scheme not in {"postgres", "postgresql"} or not loopback(db.hostname):
            raise ValueError("Fixture worker requires an isolated loopback PostgreSQL URL")
        for url in (business, ml):
            parsed = urlparse(url)
            if (
                parsed.scheme not in {"http", "https"}
                or not loopback(parsed.hostname)
                or parsed.username
                or parsed.query
                or parsed.fragment
                or parsed.path not in {"", "/"}
            ):
                raise ValueError("Fixture providers must use loopback HTTP(S) origins")
        try:
            schemas = Path(env["PROCESSING_SCHEMA_DIRECTORY"]).resolve()
            for name in ("features", "common", "business", "prediction"):
                if not (schemas / f"{name}.schema.json").is_file():
                    raise ValueError("HTTP contract schemas are missing")
            validator = FeatureValidator.from_file(
                schemas / "features.schema.json",
                version=env.get("PROCESSING_VALIDATOR_VERSION", "sensor-validator-1.0.0"),
            )
            policy = RiskPolicy.from_file(Path(env["PROCESSING_RISK_POLICY_FILE"]))
            fixture = strict_json(Path(env["PROCESSING_MODEL_IDENTITY_FIXTURE"]).read_bytes())
            identity = ModelIdentity(
                **{item.name: fixture[item.name] for item in fields(ModelIdentity)}
            )
            port = int(env.get("PROCESSING_HEALTH_PORT", "8081"))
            if not 1 <= port <= 65535:
                raise ValueError("Invalid health port")
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "Invalid Processing schemas, policy, fixture identity or probe config"
            ) from exc
        return cls(
            "local-fixture",
            kafka,
            dsn,
            business,
            ml,
            schemas,
            validator,
            policy,
            identity,
            port,
            health_host=probe_host(env),
            registry_token=env.get("BUSINESS_BEARER_TOKEN"),
            execution_mode=mode,
            keyed_limits=KeyedSettings.from_env(env),
        )
