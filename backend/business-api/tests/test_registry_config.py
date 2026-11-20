"""Startup must fail closed rather than invent OIDC or DB defaults."""

import pytest
from api.config import Settings


def environment():
    return {
        "BUSINESS_PROFILE": "local-test",
        "BUSINESS_DATABASE_URL": "postgresql://test:secret@127.0.0.1/test",
        "BUSINESS_HOST": "127.0.0.1",
        "BUSINESS_OIDC_ISSUER": "http://127.0.0.1:49000",
        "BUSINESS_OIDC_AUDIENCE": "business-test",
        "BUSINESS_OIDC_JWKS_URL": "http://127.0.0.1:49000/jwks",
        "BUSINESS_ROLES_CLAIM": "roles",
        "BUSINESS_MACHINES_CLAIM": "machines",
    }


def test_explicit_config_keeps_database_secret_out_of_repr():
    config = Settings.from_env(environment())
    assert config.port == 8003
    assert "secret" not in repr(config)


@pytest.mark.parametrize(
    "changes",
    [
        {"BUSINESS_PROFILE": "shared"},
        {"BUSINESS_DATABASE_URL": ""},
        {"BUSINESS_OIDC_AUDIENCE": ""},
        {"BUSINESS_MACHINES_CLAIM": ""},
        {"BUSINESS_HOST": "8.8.8.8"},
        {"BUSINESS_PORT": "0"},
        {"BUSINESS_OIDC_JWKS_URL": "http://shared-oidc/jwks"},
    ],
)
def test_invalid_or_unreviewed_configuration_is_rejected(changes):
    env = {**environment(), **changes}
    with pytest.raises(ValueError):
        Settings.from_env(env)


def test_status_dependency_requires_paired_origin_and_credential_file(tmp_path):
    token_file = tmp_path / "service-token"
    env = {
        **environment(),
        "BUSINESS_PROCESSING_READ_URL": "http://127.0.0.1:8004",
        "BUSINESS_PROCESSING_TOKEN_FILE": str(token_file),
    }
    config = Settings.from_env(env)
    assert config.processing_read_url == "http://127.0.0.1:8004"
    assert config.processing_token_file == str(token_file)
    for changes in (
        {"BUSINESS_PROCESSING_READ_URL": ""},
        {"BUSINESS_PROCESSING_TOKEN_FILE": ""},
        {"BUSINESS_PROCESSING_READ_URL": "http://unreviewed.invalid:8004"},
    ):
        with pytest.raises(ValueError):
            Settings.from_env({**env, **changes})


def test_processing_oauth_replaces_static_token_file_and_redacts_secret():
    oauth_env = {
        **environment(),
        "BUSINESS_PROCESSING_READ_URL": "http://processing-read-api:8004",
        "BUSINESS_PROCESSING_OAUTH_TOKEN_URL": (
            "http://keycloak:8080/realms/maintai/protocol/openid-connect/token"
        ),
        "BUSINESS_PROCESSING_OAUTH_CLIENT_ID": "maintai-processing",
        "BUSINESS_PROCESSING_OAUTH_SECRET_FILE": "/credentials/processing/secret-super-sensitive",
    }
    config = Settings.from_env(oauth_env)
    assert config.processing_read_url == "http://processing-read-api:8004"
    assert config.processing_token_file is None
    assert config.processing_oauth is not None
    assert config.processing_oauth.client_id == "maintai-processing"
    assert "secret-super-sensitive" not in repr(config)
    for bad in (
        {"BUSINESS_PROCESSING_OAUTH_SECRET_FILE": ""},
        {"BUSINESS_PROCESSING_OAUTH_CLIENT_ID": ""},
        {"BUSINESS_PROCESSING_OAUTH_TOKEN_URL": "http://external-idp.example/token"},
        {"BUSINESS_PROCESSING_READ_URL": ""},
    ):
        with pytest.raises(ValueError):
            Settings.from_env({**oauth_env, **bad})


def test_local_profile_accepts_rfc6761_localhost_subdomain_issuer():
    config = Settings.from_env(
        {
            **environment(),
            "BUSINESS_OIDC_ISSUER": "https://auth.maintai.localhost/realms/maintai",
        }
    )
    assert config.issuer == "https://auth.maintai.localhost/realms/maintai"
