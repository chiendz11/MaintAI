"""Isolated credentials/cache defaults; no live Groq requests or model downloads."""

import pytest

from llm_generator.generator import get_generator


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch, tmp_path):
    monkeypatch.delenv("GROQ_API_KEY_FILE", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "unit-test-not-a-real-key")
    monkeypatch.setenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    monkeypatch.setenv("GROQ_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("GROQ_MAX_COMPLETION_TOKENS", "2048")
    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture:fixture@localhost/knowledge_db")
    monkeypatch.setenv(
        "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    monkeypatch.setenv("SENTENCE_TRANSFORMERS_HOME", str(tmp_path / "cache"))
    get_generator.cache_clear()
    yield
    get_generator.cache_clear()
