"""Relay profile keeps fixture configuration isolated from shared Kafka."""

import pytest
from relay.config import RelaySettings


def env():
    return {
        "PROCESSING_PROFILE": "local-fixture",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:39092",
        "PROCESSING_DATABASE_URL": "postgresql://fixture:secret@127.0.0.1:35432/fixture",
    }


def test_relay_fixture_config_does_not_expose_credentials():
    settings = RelaySettings.from_env(env())
    assert settings.batch_size == 1
    assert "secret" not in repr(settings)


@pytest.mark.parametrize(
    "change",
    [
        {"PROCESSING_DATABASE_URL": "postgresql://fixture:secret@shared-db/processing_db"},
        {"KAFKA_BOOTSTRAP_SERVERS": "shared-kafka:9092"},
        {"PROCESSING_OUTBOX_BATCH_SIZE": "0"},
        {"PROCESSING_OUTBOX_LEASE_SECONDS": "nan"},
    ],
)
def test_unsafe_fixture_or_invalid_budgets_are_rejected(change):
    settings = env()
    settings.update(change)
    with pytest.raises(ValueError):
        RelaySettings.from_env(settings)


def test_sdk_relay_budget_names_are_used_and_conflicting_aliases_rejected():
    config = {
        **env(),
        "OUTBOX_EXECUTION_MODE": "ordered",
        "OUTBOX_CLAIM_LEASE_SECONDS": "60",
        "OUTBOX_BATCH_SIZE": "3",
        "OUTBOX_POLL_INTERVAL_MS": "500",
    }
    settings = RelaySettings.from_env(config)
    assert (settings.lease_seconds, settings.batch_size, settings.poll_interval_seconds) == (
        60,
        3,
        0.5,
    )
    with pytest.raises(ValueError):
        RelaySettings.from_env({**config, "PROCESSING_OUTBOX_BATCH_SIZE": "2"})
