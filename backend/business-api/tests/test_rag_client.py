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
