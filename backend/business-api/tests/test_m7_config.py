import pytest


def test_opt_in_requires_complete_explicit_configuration(tmp_path):
    from backend.shared.copilot.config import M7Settings

    assert M7Settings.from_env({}, local=True) is None
    with pytest.raises(ValueError):
        M7Settings.from_env({"BUSINESS_M7_ENABLED": "true"}, local=True)
    key = tmp_path / "key"
    key.write_bytes(b"x" * 32)
    token = tmp_path / "token"
    token.write_text("token")
    env = {
        "BUSINESS_M7_ENABLED": "true",
        "BUSINESS_RAG_URL": "http://127.0.0.1:8002",
        "BUSINESS_RAG_TOKEN_FILE": str(token),
        "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
        "BUSINESS_DRIFT_BUCKET": "reports",
        "BUSINESS_DRIFT_S3_ENDPOINT": "http://127.0.0.1:39000",
        "BUSINESS_DRIFT_S3_REGION": "us-east-1",
    }
    assert M7Settings.from_env(env, local=True).bucket == "reports"
    with pytest.raises(ValueError):
        M7Settings.from_env(env, local=False)
