import pytest

from maintai_ingestion.config import AppSettings


def env(**overrides):
    return {
        "INGESTION_ENV": "test",
        "INGESTION_HOST": "127.0.0.1",
        "AUTH_MODE": "local-fixture",
        "LOCAL_SOURCE_SUBJECT": "fixture",
        "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
        "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:8003",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        **overrides,
    }


def test_valid_local_settings():
    s = AppSettings.from_env(env())
    assert s.kafka.max_payload_bytes == 262144
    assert s.local_machines == frozenset({"M-001"})


@pytest.mark.parametrize(
    "overrides",
    [
        {"BUSINESS_PUBLIC_ORIGIN": ""},
        {"BUSINESS_PUBLIC_ORIGIN": "http://user:secret@host"},
        {"BUSINESS_PUBLIC_ORIGIN": "https://host/path"},
        {"BUSINESS_PUBLIC_ORIGIN": "https://host/?x=1"},
        {"INGESTION_ENV": "shared"},
        {"INGESTION_HOST": "0.0.0.0"},
        {"KAFKA_LOCAL_MODE": "false"},
        {"LOCAL_ALLOWED_MACHINE_IDS": ""},
        {"INGESTION_MAX_INFLIGHT": "0"},
        {"INGESTION_BODY_TIMEOUT_SECONDS": "nan"},
        {"INGESTION_PORT": "70000"},
    ],
)
def test_unsafe_configuration_fails(overrides):
    with pytest.raises(ValueError):
        AppSettings.from_env(env(**overrides))


def test_oidc_shared_requires_explicit_provider_mapping():
    with pytest.raises(ValueError):
        AppSettings.from_env(env(AUTH_MODE="oidc"))


def test_settings_do_not_expose_secret():
    s = AppSettings.from_env(env())
    assert "password" not in repr(s)
