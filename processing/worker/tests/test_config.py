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


def test_registry_oauth_parses_secret_file_and_hides_secret_from_repr(worker_env, tmp_path):
    secret_file = tmp_path / "client_secret"
    secret_file.write_text("oauth-super-secret\n", encoding="utf-8")
    settings = WorkerSettings.from_env(
        {
            **worker_env,
            "BUSINESS_OAUTH_TOKEN_URL": "http://keycloak:8080/realms/maintai/protocol/openid-connect/token",
            "BUSINESS_OAUTH_CLIENT_ID": "maintai-registry",
            "BUSINESS_OAUTH_SECRET_FILE": str(secret_file),
            "BUSINESS_OAUTH_SCOPE": "openid",
        }
    )
    assert settings.registry_oauth is not None
    assert settings.registry_oauth.client_id == "maintai-registry"
    assert settings.registry_oauth.scope == "openid"
    assert "oauth-super-secret" not in repr(settings)
    assert "oauth-super-secret" not in repr(settings.registry_oauth)


def test_registry_oauth_validation_rejects_invalid_combinations(worker_env, tmp_path):
    secret_file = tmp_path / "client_secret"
    secret_file.write_text("oauth-secret", encoding="utf-8")
    empty_file = tmp_path / "empty_secret"
    empty_file.write_text("   \n", encoding="utf-8")
    valid_oauth = {
        "BUSINESS_OAUTH_TOKEN_URL": "http://127.0.0.1:8080/token",
        "BUSINESS_OAUTH_CLIENT_ID": "maintai-registry",
        "BUSINESS_OAUTH_SECRET_FILE": str(secret_file),
    }

    with pytest.raises(ValueError):
        WorkerSettings.from_env({**worker_env, "BUSINESS_OAUTH_CLIENT_ID": "only-id"})
    with pytest.raises(ValueError):
        WorkerSettings.from_env({**worker_env, "BUSINESS_OAUTH_SCOPE": "openid"})
    with pytest.raises(ValueError):
        WorkerSettings.from_env(
            {**worker_env, **valid_oauth, "BUSINESS_BEARER_TOKEN": "static-token"}
        )
    with pytest.raises(ValueError):
        WorkerSettings.from_env(
            {**worker_env, **valid_oauth, "BUSINESS_OAUTH_TOKEN_URL": "http://shared-kc:8080/token"}
        )
    with pytest.raises(ValueError):
        WorkerSettings.from_env(
            {**worker_env, **valid_oauth, "BUSINESS_OAUTH_SECRET_FILE": str(tmp_path / "missing")}
        )
    with pytest.raises(ValueError):
        WorkerSettings.from_env(
            {**worker_env, **valid_oauth, "BUSINESS_OAUTH_SECRET_FILE": str(empty_file)}
        )


def test_hybrid_ordering_settings_default_safe_and_validated(worker_env):
    default_cfg = WorkerSettings.from_env(worker_env)
    assert default_cfg.keyed_limits.hybrid_ordering_enabled is False
    assert default_cfg.keyed_limits.max_inflight_per_key == 1
    assert default_cfg.keyed_limits.effective_max_total_inflight == 8
    assert default_cfg.unavailable_alert_disposition == "BLOCKED"

    hybrid_cfg = WorkerSettings.from_env(
        {
            **worker_env,
            "PROCESSING_EXECUTION_MODE": "keyed",
            "PROCESSING_HYBRID_ORDERING_ENABLED": "true",
            "PROCESSING_MAX_ACTIVE_KEYS": "8",
            "PROCESSING_MAX_INFLIGHT_PER_KEY": "2",
            "PROCESSING_MAX_TOTAL_INFLIGHT": "8",
            "PROCESSING_UNAVAILABLE_ALERT_DISPOSITION": "NO_SIGNAL",
        }
    )
    assert hybrid_cfg.keyed_limits.hybrid_ordering_enabled is True
    assert hybrid_cfg.keyed_limits.max_inflight_per_key == 2
    assert hybrid_cfg.keyed_limits.effective_max_total_inflight == 8
    assert hybrid_cfg.unavailable_alert_disposition == "NO_SIGNAL"

    with pytest.raises(ValueError):
        WorkerSettings.from_env(
            {**worker_env, "PROCESSING_UNAVAILABLE_ALERT_DISPOSITION": "IGNORE"}
        )
