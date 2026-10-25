"""Fixture runtime is explicit, isolated and cannot silently become shared serving."""

import json
from pathlib import Path

import pytest
from consumer.config import WorkerSettings

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def worker_env(tmp_path):
    policy = tmp_path / "fixture-policy.json"
    policy.write_text(
        json.dumps(
            {
                "policy_version": "fixture-only",
                "thresholds": {
                    "watch": 0.4,
                    "high": 0.65,
                    "critical": 0.85,
                },
                "create_alert_for": ["HIGH", "CRITICAL"],
            }
        )
    )
    return {
        "PROCESSING_PROFILE": "local-fixture",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:39092",
        "KAFKA_GROUP_PREFIX": "m2-fixture-",
        "PRODUCER_GIT_SHA": "1" * 40,
        "PROCESSING_DATABASE_URL": "postgresql://fixture:secret@127.0.0.1:35432/fixture",
        "BUSINESS_BASE_URL": "http://127.0.0.1:48000",
        "ML_BASE_URL": "http://127.0.0.1:48001",
        "PROCESSING_SCHEMA_DIRECTORY": str(ROOT / "contracts/v1.3/schemas"),
        "PROCESSING_RISK_POLICY_FILE": str(policy),
        "PROCESSING_MODEL_IDENTITY_FIXTURE": str(ROOT / "contracts/kafka/examples/completed.json"),
    }


def test_local_fixture_is_explicit_and_secrets_are_not_in_config_repr(worker_env):
    config = WorkerSettings.from_env(worker_env)
    assert config.profile == "local-fixture"
    assert config.policy.policy_version == "fixture-only"
    assert "secret" not in repr(config)


@pytest.mark.parametrize(
    "changes",
    [
        {"PROCESSING_PROFILE": "shared"},
        {"PROCESSING_PROFILE": ""},
        {"KAFKA_GROUP_PREFIX": ""},
        {"KAFKA_BOOTSTRAP_SERVERS": "shared-broker:9092"},
        {"ML_BASE_URL": "http://shared-ml:8001"},
        {"BUSINESS_BASE_URL": "http://shared-business:8000"},
        {"PROCESSING_DATABASE_URL": "postgresql://fixture:secret@shared-db/processing_db"},
        {"PRODUCER_GIT_SHA": ""},
        {"PROCESSING_RISK_POLICY_FILE": "/missing"},
        {"PROCESSING_MODEL_IDENTITY_FIXTURE": "/missing"},
    ],
)
def test_invalid_or_shared_fixture_configuration_is_rejected(worker_env, changes):
    worker_env.update(changes)
    with pytest.raises(ValueError):
        WorkerSettings.from_env(worker_env)


def test_keyed_limits_are_explicit_and_invalid_mode_fails(worker_env):
    settings = WorkerSettings.from_env(
        {**worker_env, "PROCESSING_EXECUTION_MODE": "keyed", "PROCESSING_MAX_ACTIVE_KEYS": "2"}
    )
    assert settings.execution_mode == "keyed"
    assert settings.keyed_limits.max_active_keys == 2
    with pytest.raises(ValueError):
        WorkerSettings.from_env({**worker_env, "PROCESSING_EXECUTION_MODE": "pretend"})
