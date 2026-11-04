from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from langchain_core.documents import Document

from api.schemas import MaintenanceBriefRequest
from llm_generator.generator import LLMProviderError
from main import app
from pipeline import RAGPipeline
from test_schemas import REQUEST


class FakeGenerator:
    provider = "groq"
    model = "fixture-model"

    def __init__(self, chunk="chunk-001"):
        self.chunk = chunk
        self.calls = 0

    def generate_json(self, prompt):
        self.calls += 1
        return {
            "summary": "Kiểm tra thông gió",
            "uncertainty": "Cần xác minh",
            "checklist": [{"step": 1, "action": "Kiểm tra quạt", "chunk_ids": [self.chunk]}],
        }


def test_groq_metadata_and_citations_survive_pipeline_validation():
    docs = [
        Document(
            page_content="Kiểm tra quạt",
            metadata={
                "chunk_id": "chunk-001",
                "document_id": "manual",
                "document_version": "1",
                "section": "maintenance",
                "page": 1,
            },
        )
    ]
    generator = FakeGenerator()
    result = RAGPipeline(
        retriever=SimpleNamespace(search=lambda **kwargs: docs), generator=generator
    ).run(MaintenanceBriefRequest.model_validate(REQUEST))
    assert result.grounded_status.value == "GROUNDED"
    assert result.generator.provider == "groq"
    assert result.citations[0].chunk_id == "chunk-001"
    assert generator.calls == 1


def test_no_evidence_skips_generation_and_fabricated_citations_are_rejected():
    generator = FakeGenerator()
    result = RAGPipeline(
        retriever=SimpleNamespace(search=lambda **kwargs: []), generator=generator
    ).run(MaintenanceBriefRequest.model_validate(REQUEST))
    assert result.grounded_status.value == "INSUFFICIENT_EVIDENCE"
    assert generator.calls == 0
    docs = [
        Document(
            page_content="context",
            metadata={
                "chunk_id": "chunk-001",
                "document_id": "manual",
                "document_version": "1",
                "section": "maintenance",
                "page": 1,
            },
        )
    ]
    result = RAGPipeline(
        retriever=SimpleNamespace(search=lambda **kwargs: docs),
        generator=FakeGenerator("invented-chunk"),
    ).run(MaintenanceBriefRequest.model_validate(REQUEST))
    assert result.grounded_status.value == "VALIDATION_FAILED"
    assert not result.checklist and not result.citations


@pytest.mark.parametrize(
    "reason,retryable",
    [("AUTHENTICATION_FAILED", False), ("RATE_LIMITED", True), ("TIMEOUT", True)],
)
def test_http_errors_preserve_safe_provider_reason_and_retryability(
    monkeypatch, reason, retryable, caplog
):
    def fail(_):
        raise LLMProviderError(reason, retryable=retryable)

    monkeypatch.setattr("api.routes.get_pipeline", lambda: SimpleNamespace(run=fail))
    response = TestClient(app).post("/api/v1/briefs", json=REQUEST)
    assert response.status_code == 503
    error = response.json()["error"]
    assert error["code"] == "RAG_UNAVAILABLE"
    assert error["retryable"] is retryable
    assert error["details"] == {"dependency": "groq", "reason": reason}
    assert "unit-test-not-a-real-key" not in response.text + caplog.text
