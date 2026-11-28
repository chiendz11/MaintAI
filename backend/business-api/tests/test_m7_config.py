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

    internal_env = {
        **env,
        "BUSINESS_RAG_URL": "http://rag-service:8002",
        "BUSINESS_DRIFT_S3_ENDPOINT": "http://minio:9000",
        "BUSINESS_ML_URL": "http://ml-service-mirror:8001",
    }
    parsed = M7Settings.from_env(internal_env, local=True)
    assert parsed.rag_url == "http://rag-service:8002"
    assert parsed.endpoint == "http://minio:9000"
    assert parsed.ml_url == "http://ml-service-mirror:8001"
    with pytest.raises(ValueError):
        M7Settings.from_env(internal_env, local=False)


def test_m7_settings_accepts_oauth_without_static_token_file(tmp_path):
    from backend.shared.copilot.config import M7Settings

    secret = tmp_path / "rag-oauth-secret"
    secret.write_text("oauth-secret", encoding="ascii")
    key = tmp_path / "cursor-key"
    key.write_bytes(b"k" * 32)
    env = {
        "BUSINESS_M7_ENABLED": "true",
        "BUSINESS_RAG_URL": "https://rag.maintai.example.com",
        "BUSINESS_RAG_OAUTH_TOKEN_URL": "https://cognito-idp.ap-southeast-1.amazonaws.com/oauth2/token",
        "BUSINESS_RAG_OAUTH_CLIENT_ID": "maintai-business-worker",
        "BUSINESS_RAG_OAUTH_SECRET_FILE": str(secret),
        "BUSINESS_RAG_OAUTH_SCOPE": "https://api.maintai.example.com/rag:brief",
        "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
        "BUSINESS_DRIFT_BUCKET": "maintai-drift",
        "BUSINESS_DRIFT_S3_REGION": "ap-southeast-1",
    }
    parsed = M7Settings.from_env(env, local=False)
    assert parsed is not None
    assert parsed.rag_token_file is None
    assert parsed.rag_oauth is not None
    assert parsed.rag_oauth.client_id == "maintai-business-worker"
    assert parsed.rag_oauth.scope == "https://api.maintai.example.com/rag:brief"


def test_m7_settings_requires_either_oauth_or_token_file(tmp_path):
    import pytest
    from backend.shared.copilot.config import M7Settings

    key = tmp_path / "cursor-key"
    key.write_bytes(b"k" * 32)
    with pytest.raises(ValueError, match="RAG credentials"):
        M7Settings.from_env(
            {
                "BUSINESS_M7_ENABLED": "true",
                "BUSINESS_RAG_URL": "https://rag.maintai.example.com",
                "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
                "BUSINESS_DRIFT_BUCKET": "maintai-drift",
                "BUSINESS_DRIFT_S3_REGION": "ap-southeast-1",
            },
            local=False,
        )
