"""Actual botocore request/response models with stubs, without an AWS account."""

import os
from dataclasses import replace

import pytest
from botocore.stub import Stubber

from maintai_storage.settings import Settings


def test_client_uses_validated_endpoint_and_one_retry_layer(settings, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fixture-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    # An ambient SDK override cannot bypass the endpoint's TLS validation.
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://do-not-use.invalid")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://do-not-use.invalid")
    client = settings.create_s3_client()
    try:
        assert client.meta.endpoint_url.startswith("https://")
        assert "do-not-use" not in client.meta.endpoint_url
        assert client.meta.config.retries["total_max_attempts"] == 1
        assert client.meta.config.connect_timeout == settings.connect_timeout_seconds
        with Stubber(client) as stub:
            stub.add_response(
                "get_bucket_versioning", {"Status": "Enabled"}, {"Bucket": settings.bucket}
            )
            assert client.get_bucket_versioning(Bucket=settings.bucket)["Status"] == "Enabled"
            stub.assert_no_pending_responses()
    finally:
        client.close()


def test_local_endpoint_and_standard_credential_chain(settings, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "fixture-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
    local = replace(settings, endpoint_url="http://127.0.0.1:9000", addressing_style="path")
    client = local.create_s3_client()
    try:
        assert client.meta.endpoint_url == "http://127.0.0.1:9000"
        assert client.meta.config.s3["addressing_style"] == "path"
    finally:
        client.close()


@pytest.mark.parametrize("name", ["S3_RAW_BUCKET", "S3_ENDPOINT_URL"])
def test_verify_configuration_does_not_require_aws_variables(name, monkeypatch):
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    env = {
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "S3_RAW_BUCKET": "fixture-bucket",
    }
    if name == "S3_ENDPOINT_URL":
        env[name] = "http://127.0.0.1:9000"
    assert Settings.from_env(env).bucket == "fixture-bucket"
