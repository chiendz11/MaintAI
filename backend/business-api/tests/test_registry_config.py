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
        {"BUSINESS_HOST": "0.0.0.0"},
        {"BUSINESS_PORT": "0"},
        {"BUSINESS_OIDC_JWKS_URL": "http://shared-oidc/jwks"},
    ],
)
def test_invalid_or_unreviewed_configuration_is_rejected(changes):
    env = {**environment(), **changes}
    with pytest.raises(ValueError):
        Settings.from_env(env)
