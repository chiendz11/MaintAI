import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from main import app

VALID_REQUEST = {
    "alert": {
        "alert_id": "alt_auth_1",
        "machine_id": "M-001",
        "severity": "HIGH",
        "status": "OPEN",
        "risk_score": 0.91,
        "policy_version": "demo-risk-v1",
    },
    "prediction": {
        "prediction_id": "pred_auth_1",
        "prediction": "FAILURE",
        "failure_probability": 0.91,
        "model_version": "logreg-baseline-v0",
    },
    "sensor_context": {
        "product_type": "L",
        "air_temperature_k": 304.0,
        "process_temperature_k": 314.0,
        "rotational_speed_rpm": 1350.0,
        "torque_nm": 65.0,
        "tool_wear_min": 210.0,
    },
    "intent": "MAINTENANCE_BRIEF",
}


def test_briefs_rejects_missing_or_wrong_bearer_token(tmp_path, monkeypatch):
    token_file = tmp_path / "rag-token"
    token_file.write_text("secret-service-token\n", encoding="ascii")
    monkeypatch.setenv("RAG_AUTH_TOKEN_FILE", str(token_file))

    client = TestClient(app)
    r_missing = client.post("/api/v1/briefs", json=VALID_REQUEST)
    assert r_missing.status_code == 401
    assert r_missing.json()["error"]["code"] == "UNAUTHORIZED"

    r_wrong = client.post(
        "/api/v1/briefs",
        json=VALID_REQUEST,
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert r_wrong.status_code == 401

    mock_pipeline = SimpleNamespace(
        run=lambda req: {
            "brief_id": "brief_1",
            "alert_id": req.alert.alert_id,
            "grounded_status": "INSUFFICIENT_EVIDENCE",
            "summary": "No evidence",
            "uncertainty": "Missing matching chunk",
            "checklist": [],
            "citations": [],
            "retrieval": {"top_k": 3, "retrieved_chunk_ids": []},
            "generator": None,
            "generated_at": None,
        }
    )
    with patch("api.routes.get_pipeline", return_value=mock_pipeline):
        r_ok = client.post(
            "/api/v1/briefs",
            json=VALID_REQUEST,
            headers={"Authorization": "Bearer secret-service-token"},
        )
    assert r_ok.status_code == 200


def test_prod_requires_auth_token_file(monkeypatch):
    monkeypatch.delenv("RAG_AUTH_TOKEN_FILE", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")

    r = TestClient(app).post("/api/v1/briefs", json=VALID_REQUEST)
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "RAG_UNAVAILABLE"


def _load_vector_store(monkeypatch):
    if "langchain_core.documents" not in sys.modules:
        class _Doc:
            def __init__(self, page_content="", metadata=None):
                self.page_content = page_content
                self.metadata = metadata or {}

        monkeypatch.setitem(sys.modules, "langchain_core", SimpleNamespace())
        monkeypatch.setitem(sys.modules, "langchain_core.documents", SimpleNamespace(Document=_Doc))
    import importlib

    return importlib.import_module("knowledge.vector_store")


def test_vector_store_applies_cosine_distance_threshold(monkeypatch):
    vs_mod = _load_vector_store(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@127.0.0.1:5432/knowledge_db")
    monkeypatch.delenv("RAG_MAX_COSINE_DISTANCE", raising=False)

    captured = []
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.fetchall.return_value = []

    def _exec(sql, params):
        captured.append((sql, params))

    cur.execute.side_effect = _exec
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cur

    with patch.object(vs_mod.psycopg, "connect", return_value=conn):
        store = vs_mod.PostgresVectorStore()
        result = store.search([0.1] * 384, top_k=3)
        assert result == []
        sql, params = captured[-1]
        assert "(embedding <=> %s::vector) <= %s" in sql
        assert params[3] == pytest.approx(0.65)
        assert params[4] == 3

        monkeypatch.setenv("RAG_MAX_COSINE_DISTANCE", "0.42")
        cur.fetchall.return_value = [
            (7, "Inspect bearing", {"document_id": "doc-1", "document_version": "v1", "section": "4.2", "page": "12"}, 0.31)
        ]
        hits = store.search([0.1] * 384, top_k=2)
        assert len(hits) == 1
        assert hits[0].metadata["chunk_id"] == "7"
        assert captured[-1][1][3] == pytest.approx(0.42)

        monkeypatch.setenv("RAG_MAX_COSINE_DISTANCE", "2.5")
        with pytest.raises(ValueError, match="RAG_MAX_COSINE_DISTANCE"):
            store.search([0.1] * 384)


def _build_rsa_pair():
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _sample_brief_request() -> dict:
    return {
        "alert": {
            "alert_id": "alt-oidc-1",
            "machine_id": "M-001",
            "severity": "HIGH",
            "status": "OPEN",
            "risk_score": 0.85,
            "policy_version": "1.0.0",
        },
        "prediction": {
            "prediction_id": "pred-1",
            "prediction": "FAILURE_RISK",
            "failure_probability": 0.85,
            "model_version": "1.0.0",
        },
        "sensor_context": {
            "product_type": "M",
            "air_temperature_k": 300.0,
            "process_temperature_k": 310.0,
            "rotational_speed_rpm": 1400.0,
            "torque_nm": 45.0,
            "tool_wear_min": 180.0,
        },
        "intent": "MAINTENANCE_BRIEF",
    }


def _mock_brief_pipeline_result(alert_id: str) -> dict:
    return {
        "brief_id": "brief-oidc-1",
        "alert_id": alert_id,
        "grounded_status": "GROUNDED",
        "summary": "Bearing wear detected.",
        "uncertainty": "Low uncertainty.",
        "checklist": [{"step": 1, "action": "Check bearing", "citation_ids": ["cit-1"]}],
        "citations": [
            {
                "citation_id": "cit-1",
                "document_id": "doc-1",
                "document_version": "1",
                "section": "sec-1",
                "page": 1,
                "chunk_id": "chunk-1",
            }
        ],
        "retrieval": {"top_k": 1, "retrieved_chunk_ids": ["chunk-1"]},
        "generator": {"provider": "ollama", "model": "qwen2.5"},
        "generated_at": "2026-01-01T00:00:00Z",
    }


def test_oidc_m2m_jwt_verification_keycloak_and_cognito(monkeypatch):
    import time
    from types import SimpleNamespace

    import jwt
    from api import routes
    from fastapi.testclient import TestClient
    from main import app

    private_key, public_key = _build_rsa_pair()
    issuer = "https://auth.maintai.example.com/realms/maintai"
    jwks_url = issuer + "/protocol/openid-connect/certs"

    routes._jwk_client.cache_clear()
    monkeypatch.setenv("RAG_OIDC_JWKS_URL", jwks_url)
    monkeypatch.setenv("RAG_OIDC_ISSUER", issuer)
    monkeypatch.setenv("RAG_OIDC_AUDIENCE", "maintai-rag")
    monkeypatch.setenv("RAG_OIDC_REQUIRED_SCOPE", "rag:brief")
    monkeypatch.setattr(
        routes,
        "_jwk_client",
        lambda _url: SimpleNamespace(
            get_signing_key_from_jwt=lambda _tok: SimpleNamespace(key=public_key)
        ),
    )
    monkeypatch.setattr(
        routes,
        "get_pipeline",
        lambda: SimpleNamespace(run=lambda req: _mock_brief_pipeline_result(req.alert.alert_id)),
    )

    now = int(time.time())
    valid_jwt = jwt.encode(
        {
            "iss": issuer,
            "aud": "maintai-rag",
            "sub": "svc-business-worker",
            "scope": "profile rag:brief",
            "iat": now,
            "exp": now + 300,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "kid-1"},
    )
    wrong_scope_jwt = jwt.encode(
        {
            "iss": issuer,
            "aud": "maintai-rag",
            "sub": "svc-other",
            "scope": "profile",
            "iat": now,
            "exp": now + 300,
        },
        private_key,
        algorithm="RS256",
    )
    expired_jwt = jwt.encode(
        {
            "iss": issuer,
            "aud": "maintai-rag",
            "sub": "svc-business-worker",
            "scope": "rag:brief",
            "iat": now - 400,
            "exp": now - 60,
        },
        private_key,
        algorithm="RS256",
    )

    client = TestClient(app)
    payload = _sample_brief_request()

    assert client.post("/api/v1/briefs", json=payload).status_code == 401
    assert (
        client.post(
            "/api/v1/briefs",
            json=payload,
            headers={"Authorization": f"Bearer {expired_jwt}"},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/briefs",
            json=payload,
            headers={"Authorization": f"Bearer {wrong_scope_jwt}"},
        ).status_code
        == 403
    )
    ok_res = client.post(
        "/api/v1/briefs",
        json=payload,
        headers={"Authorization": f"Bearer {valid_jwt}"},
    )
    assert ok_res.status_code == 200
    assert ok_res.json()["alert_id"] == "alt-oidc-1"

    # Cognito M2M tokens omit "aud" and enforce custom resource-server scope.
    monkeypatch.delenv("RAG_OIDC_AUDIENCE", raising=False)
    monkeypatch.setenv("RAG_OIDC_REQUIRED_SCOPE", "https://api.maintai.example.com/rag:brief")
    cognito_jwt = jwt.encode(
        {
            "iss": issuer,
            "sub": "cognito-client-id",
            "token_use": "access",
            "scope": "https://api.maintai.example.com/rag:brief",
            "exp": now + 300,
        },
        private_key,
        algorithm="RS256",
    )
    cognito_res = client.post(
        "/api/v1/briefs",
        json=payload,
        headers={"Authorization": f"Bearer {cognito_jwt}"},
    )
    assert cognito_res.status_code == 200

    # JWKS connection failure maps cleanly to retryable 503 RAG_UNAVAILABLE.
    def _raise_jwks(_tok):
        raise jwt.PyJWKClientConnectionError("jwks unreachable")

    monkeypatch.setattr(
        routes,
        "_jwk_client",
        lambda _url: SimpleNamespace(get_signing_key_from_jwt=_raise_jwks),
    )
    outage_res = client.post(
        "/api/v1/briefs",
        json=payload,
        headers={"Authorization": f"Bearer {cognito_jwt}"},
    )
    assert outage_res.status_code == 503
    assert outage_res.json()["error"]["retryable"] is True
