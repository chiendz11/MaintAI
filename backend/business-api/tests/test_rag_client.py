"""Canonical RAG responses remain distinct from provider protocol failures."""

import copy
import json
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[3]


def brief():
    return json.loads(
        (ROOT / "contracts/v1.3/examples/http/backend/copilot-grounded.json").read_text()
    )["brief"]


def test_grounded_and_non_grounded_validate_without_inventing_provenance():
    from rag_client import validate_response

    value = brief()
    assert validate_response(value, value["alert_id"]) == value
    value.update(grounded_status="INSUFFICIENT_EVIDENCE", checklist=[], citations=[])
    value.pop("generator", None)
    value.pop("generated_at", None)
    assert validate_response(value, value["alert_id"]) == value


@pytest.mark.parametrize("mutation", ["alert", "chunk", "citation", "null"])
def test_invalid_provider_response_is_rejected(mutation):
    from rag_client import ProviderFailure, validate_response

    value = copy.deepcopy(brief())
    if mutation == "alert":
        value["alert_id"] = "another-alert"
    if mutation == "chunk":
        value["citations"][0]["chunk_id"] = "not-retrieved"
    if mutation == "citation":
        value["checklist"][0]["citation_ids"] = ["missing"]
    if mutation == "null":
        value["generator"] = None
    with pytest.raises(ProviderFailure) as error:
        validate_response(value, brief()["alert_id"])
    assert not error.value.retryable


def test_client_posts_once_and_bounds_failure(tmp_path):
    from rag_client import ProviderFailure, RAGClient

    key = tmp_path / "token"
    key.write_text("service-token")
    calls = []

    def respond(request):
        calls.append(request)
        assert request.headers["authorization"] == "Bearer service-token"
        return httpx.Response(503)

    with RAGClient(
        "http://127.0.0.1:18002", key, local=True, transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(ProviderFailure) as error:
            client.generate({"alert": {"alert_id": "a"}}, job_id="job", timeout=1)
    assert error.value.retryable
    assert len(calls) == 1


@pytest.mark.parametrize(
    "status,retryable",
    [(401, False), (403, False), (422, False), (429, True), (503, True), (307, False)],
)
def test_http_failure_classification_has_no_hidden_post_retry(tmp_path, status, retryable):
    from rag_client import ProviderFailure, RAGClient

    key = tmp_path / "token"
    key.write_text("service-token")
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(status)

    with RAGClient(
        "http://127.0.0.1", key, local=True, transport=httpx.MockTransport(respond)
    ) as client:
        with pytest.raises(ProviderFailure) as error:
            client.generate({"alert": {"alert_id": "a"}}, job_id="job", timeout=1)
    assert error.value.retryable is retryable
    assert len(calls) == 1


def test_oversized_provider_response_is_terminal(tmp_path):
    from rag_client import ProviderFailure, RAGClient

    key = tmp_path / "token"
    key.write_text("service-token")
    with RAGClient(
        "http://127.0.0.1",
        key,
        local=True,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"Content-Type": "application/json"}, content=b"x" * 262145
            )
        ),
    ) as client:
        with pytest.raises(ProviderFailure) as error:
            client.generate({"alert": {"alert_id": "a"}}, job_id="job", timeout=1)
    assert error.value.code == "RAG_RESPONSE_TOO_LARGE"
    assert not error.value.retryable


def test_rag_client_allows_cluster_dns_only_when_local(tmp_path):
    from rag_client import RAGClient

    key = tmp_path / "token"
    key.write_text("service-token")
    with RAGClient("http://rag-service:8002", key, local=True) as client:
        assert client.base_url == "http://rag-service:8002"
    with pytest.raises(ValueError):
        RAGClient("http://rag-service:8002", key, local=False)
    with RAGClient("https://rag.maintai.example.com", key, local=False) as prod_client:
        assert prod_client.base_url == "https://rag.maintai.example.com"


def test_rag_client_requires_token_file_or_oauth_provider():
    import pytest
    from rag_client import RAGClient

    with pytest.raises(ValueError, match="credential file or OAuth token provider"):
        RAGClient("http://127.0.0.1:8002", None, local=True)


def test_rag_client_uses_oauth_provider_and_retries_once_on_401(tmp_path):
    import httpx
    from backend.shared.service_auth import ServiceOAuthConfig, ServiceOAuthTokenProvider
    from rag_client import ProviderFailure, RAGClient

    secret = tmp_path / "client_secret"
    secret.write_text("worker-secret", encoding="ascii")
    now = [100.0]
    issued = []

    def idp_handler(request):
        body = request.content.decode()
        assert "grant_type=client_credentials" in body
        assert "client_id=maintai-business-worker" in body
        assert "client_secret=worker-secret" in body
        assert "scope=rag%3Abrief" in body
        token = f"jwt-rag-{len(issued) + 1}"
        issued.append(token)
        return httpx.Response(200, json={"access_token": token, "expires_in": 60})

    provider = ServiceOAuthTokenProvider(
        ServiceOAuthConfig(
            token_url="http://127.0.0.1:8080/realms/maintai/protocol/openid-connect/token",
            client_id="maintai-business-worker",
            secret_file=str(secret),
            scope="rag:brief",
        ),
        transport=httpx.MockTransport(idp_handler),
        clock=lambda: now[0],
    )

    seen_auth = []
    reject_first = [True]

    def rag_handler(request):
        seen_auth.append(request.headers["Authorization"])
        if reject_first[0]:
            reject_first[0] = False
            return httpx.Response(401, json={"error": {"code": "UNAUTHORIZED"}})
        return httpx.Response(
            200,
            json={
                "brief_id": "brief-1",
                "alert_id": "alt-1",
                "grounded_status": "GROUNDED",
                "summary": "Check bearing lubrication.",
                "uncertainty": "Low uncertainty.",
                "checklist": [{"step": 1, "action": "Inspect bearing", "citation_ids": ["cit-1"]}],
                "citations": [
                    {
                        "citation_id": "cit-1",
                        "document_id": "manual-1",
                        "document_version": "v1",
                        "section": "3.1",
                        "page": 4,
                        "chunk_id": "chunk-1",
                    }
                ],
                "retrieval": {"top_k": 3, "retrieved_chunk_ids": ["chunk-1"]},
                "generator": {"provider": "ollama", "model": "qwen2.5"},
                "generated_at": "2026-01-01T00:00:00Z",
            },
        )

    payload = {"alert": {"alert_id": "alt-1"}}
    with RAGClient(
        "http://rag-service:8002",
        None,
        token_provider=provider,
        local=True,
        transport=httpx.MockTransport(rag_handler),
    ) as client:
        result = client.generate(payload, job_id="job-1")
    assert result["brief_id"] == "brief-1"
    assert seen_auth == ["Bearer jwt-rag-1", "Bearer jwt-rag-2"]

    failing_provider = ServiceOAuthTokenProvider(
        ServiceOAuthConfig(
            token_url="http://127.0.0.1:8080/realms/maintai/protocol/openid-connect/token",
            client_id="maintai-business-worker",
            secret_file=str(secret),
        ),
        transport=httpx.MockTransport(lambda req: httpx.Response(503, text="idp down")),
    )
    with RAGClient(
        "http://127.0.0.1:8002",
        None,
        token_provider=failing_provider,
        local=True,
        transport=httpx.MockTransport(rag_handler),
    ) as failing_client:
        try:
            failing_client.generate(payload, job_id="job-2")
        except ProviderFailure as exc:
            assert exc.code == "RAG_UNAVAILABLE"
            assert exc.retryable is True
        else:
            raise AssertionError("Expected ProviderFailure on IdP outage")
