"""Explicit owner DB and OIDC configuration; no Kafka dependency."""

import pytest


def environment():
    return {
        "PROCESSING_READ_DATABASE_URL": "postgresql://reader@127.0.0.1/test",
        "PROCESSING_READ_OIDC_ISSUER": "http://127.0.0.1:18080/issuer",
        "PROCESSING_READ_OIDC_AUDIENCE": "processing-internal",
        "PROCESSING_READ_OIDC_JWKS_URL": "http://127.0.0.1:18080/keys",
        "PROCESSING_READ_ROLES_CLAIM": "roles",
        "PROCESSING_READ_MACHINES_CLAIM": "machines",
        "PROCESSING_READ_PROFILE": "local-test",
        "PROCESSING_READ_HOST": "127.0.0.1",
    }


def test_read_api_config_does_not_require_worker_ml_or_kafka_configuration():
    from processing_read_api.config import Settings

    settings = Settings.from_env(environment())
    assert settings.audience == "processing-internal"
    assert settings.host == "127.0.0.1"
    assert "reader@" not in repr(settings)


@pytest.mark.parametrize(
    "changes",
    [
        {"PROCESSING_READ_DATABASE_URL": ""},
        {"PROCESSING_READ_OIDC_AUDIENCE": ""},
        {"PROCESSING_READ_PROFILE": "shared"},
        {"PROCESSING_READ_HOST": "0.0.0.0"},
        {"PROCESSING_READ_DATABASE_URL": "postgresql://reader@remote.invalid/test"},
    ],
)
def test_read_api_config_fails_closed_for_missing_or_unsafe_settings(changes):
    from processing_read_api.config import Settings

    with pytest.raises(ValueError):
        Settings.from_env({**environment(), **changes})
