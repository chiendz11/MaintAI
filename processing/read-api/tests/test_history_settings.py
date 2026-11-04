"""M6 config never repurposes the source credential as a metadata writer."""

import base64
import json

import pytest
from processing_read_api.config import Settings

from .test_settings import environment


def configured(tmp_path):
    key = tmp_path / "cursor.json"
    key.write_text(
        json.dumps({"active": "one", "keys": {"one": base64.b64encode(b"k" * 32).decode()}})
    )
    return {
        **environment(),
        "PROCESSING_HISTORY_ENABLED": "true",
        "PROCESSING_HISTORY_METADATA_DATABASE_URL": "postgresql://metadata@127.0.0.1/test",
        "PROCESSING_HISTORY_DELEGATION_ISSUER": "http://127.0.0.1:18081/business",
        "PROCESSING_HISTORY_DELEGATION_JWKS_URL": "http://127.0.0.1:18081/keys",
        "PROCESSING_HISTORY_CURSOR_KEY_FILE": str(key),
    }


def test_history_opt_in_requires_distinct_owner_credentials(tmp_path):
    settings = Settings.from_env(configured(tmp_path))
    assert settings.history is not None
    assert "metadata@" not in repr(settings.history)
    assert settings.history.meta_schema == "history_meta"


@pytest.mark.parametrize(
    "change",
    [
        {"PROCESSING_HISTORY_METADATA_DATABASE_URL": "postgresql://reader@127.0.0.1/test"},
        {
            "PROCESSING_HISTORY_METADATA_DATABASE_URL": "postgresql://metadata@127.0.0.1/another_owner"
        },
        {"PROCESSING_HISTORY_CURSOR_KEY_FILE": ""},
        {"PROCESSING_HISTORY_DELEGATION_ISSUER": "http://remote.invalid"},
        {"PROCESSING_HISTORY_META_SCHEMA": "a; DROP DATABASE"},
    ],
)
def test_history_bad_config_fails_closed(tmp_path, change):
    with pytest.raises(ValueError):
        Settings.from_env({**configured(tmp_path), **change})


def test_legacy_read_api_does_not_require_m6_credentials():
    assert Settings.from_env(environment()).history is None
