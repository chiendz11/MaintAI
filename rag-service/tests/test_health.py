from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from main import app


def test_liveness_has_no_provider_or_database_dependency():
    with patch("main.get_generator", side_effect=AssertionError("liveness must not call Groq")):
        response = TestClient(app).get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "UP"


def test_readiness_degraded_without_dependencies():
    with (
        patch("main.psycopg.connect", side_effect=OSError("offline")),
        patch("main.get_generator", return_value=SimpleNamespace(is_available=lambda: False)),
        patch("ingestion.embedding.model_available_locally", return_value=False),
    ):
        response = TestClient(app).get("/health/ready")
    assert response.status_code == 503
    assert not response.json()["checks"]["llm_provider"]


def test_readiness_healthy_with_database_model_and_embedding_cache():
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        True,
    )
    with (
        patch("main.psycopg.connect", return_value=connection),
        patch("main.get_generator", return_value=SimpleNamespace(is_available=lambda: True)),
        patch("ingestion.embedding.model_available_locally", return_value=True),
    ):
        response = TestClient(app).get("/health/ready")
    assert response.status_code == 200
    assert all(response.json()["checks"].values())


def test_startup_refuses_missing_provider_key(monkeypatch):
    import pytest

    monkeypatch.delenv("GROQ_API_KEY")
    with pytest.raises(ValueError, match="GROQ_API_KEY"), TestClient(app):
        pass
