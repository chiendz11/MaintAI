import socket
from types import SimpleNamespace

import pytest
from maintai_messaging.errors import ConfigurationError

from maintai_storage.cli import main
from maintai_storage.settings import Settings

BASE = {
    "KAFKA_LOCAL_MODE": "true",
    "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
    "S3_RAW_BUCKET": "fixture-bucket",
}


@pytest.mark.parametrize(
    "key,value",
    [
        ("S3_RAW_BUCKET", ""),
        ("S3_RAW_BUCKET", "s3://a-bucket"),
        ("S3_RAW_BUCKET", "127.0.0.1"),
        ("S3_RAW_PREFIX", "../raw"),
        ("S3_RAW_PREFIX", "/raw"),
        ("S3_RAW_PREFIX", "a//b"),
        ("S3_ENDPOINT_URL", "https://user:password@example.com"),
        ("S3_ENDPOINT_URL", "https://example.com/?secret=token"),
        ("S3_ENDPOINT_URL", "ftp://example.com"),
        ("S3_ENDPOINT_URL", "https://s3:bad"),
        ("S3_ALLOW_UNVERSIONED", "1"),
        ("STORAGE_ARCHIVE_OUTCOMES", "yes"),
        ("STORAGE_RETRY_ATTEMPTS", "0"),
        ("STORAGE_RETRY_ATTEMPTS", "1.5"),
        ("STORAGE_RETRY_INITIAL_SECONDS", "NaN"),
        ("S3_READ_TIMEOUT_SECONDS", "inf"),
        ("STORAGE_HTTP_PORT", "70000"),
        ("STORAGE_HTTP_HOST", ""),
        ("STORAGE_READINESS_MAX_AGE_SECONDS", "1"),
        ("STORAGE_LIVENESS_MAX_AGE_SECONDS", "10"),
        ("S3_ADDRESSING_STYLE", "unsafe"),
        ("KAFKA_ENABLE_AUTO_COMMIT", "true"),
        ("KAFKA_POLL_INTERVAL_SECONDS", "secret-value"),
    ],
)
def test_invalid_settings_fail_offline_without_echoing_values(key, value):
    with pytest.raises(ConfigurationError) as error:
        Settings.from_env({**BASE, key: value})
    assert "password" not in str(error.value)
    assert "secret-value" not in str(error.value)


def test_aws_default_endpoint_and_optional_role():
    settings = Settings.from_env({**BASE, "STORAGE_ARCHIVE_OUTCOMES": "true"})
    assert settings.endpoint_url is None
    assert settings.roles == ("storage-accepted", "storage-rejected", "storage-outcomes")
    assert settings.allow_unversioned is False


def test_local_minio_is_explicit_and_may_still_require_versioning():
    settings = Settings.from_env({**BASE, "S3_ENDPOINT_URL": "http://127.0.0.1:9000"})
    assert not settings.allow_unversioned
    assert settings.endpoint_url.startswith("http:")


@pytest.mark.parametrize(
    "override",
    [
        {"S3_ALLOW_UNVERSIONED": "true"},
        {"S3_ENDPOINT_URL": "http://127.0.0.1:9000"},
    ],
)
def test_unversioned_and_http_are_rejected_outside_local_mode(monkeypatch, override):
    # Avoid requiring a production Kafka CA just to validate S3-specific guards.
    monkeypatch.setattr(
        "maintai_storage.settings.KafkaSettings.from_env",
        lambda _: SimpleNamespace(local_mode=False),
    )
    with pytest.raises(ConfigurationError):
        Settings.from_env({**BASE, **override})


def test_verify_config_never_opens_network_or_logs_credentials(monkeypatch, capsys):
    for name in list(__import__("os").environ):
        if name.startswith(("KAFKA_", "S3_", "STORAGE_")):
            monkeypatch.delenv(name)
    for name, value in BASE.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "SECRET_DO_NOT_LOG")
    monkeypatch.setattr(socket, "create_connection", lambda *_a, **_k: pytest.fail("Network I/O"))
    monkeypatch.setattr(Settings, "create_s3_client", lambda _: pytest.fail("S3 client created"))
    assert main(["verify-config"]) == 0
    output = capsys.readouterr().out
    assert '"network_checked": false' in output
    assert "SECRET_DO_NOT_LOG" not in output


def test_invalid_config_cli_returns_failure_without_secrets(monkeypatch, capsys):
    monkeypatch.setenv("S3_RAW_BUCKET", "SECRET_DO_NOT_LOG")
    assert main(["verify-config"]) == 1
    assert "SECRET_DO_NOT_LOG" not in capsys.readouterr().err
