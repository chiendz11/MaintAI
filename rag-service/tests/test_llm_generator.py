"""Groq wire/error tests use MockTransport, never provider credentials or quota."""

import json

import httpx
import pytest

from llm_generator.generator import LLMGenerator, LLMProviderError

MODEL = "llama-3.3-70b-versatile"
ANSWER = {
    "summary": "Kiểm tra làm mát",
    "uncertainty": "Cần kiểm tra thực tế",
    "checklist": [{"step": 1, "action": "Kiểm tra thông gió", "chunk_ids": ["101"]}],
}


def completion(content=None, **overrides):
    return {
        "model": MODEL,
        "choices": [
            {
                "finish_reason": "stop",
                "message": {"content": json.dumps(ANSWER) if content is None else content},
            }
        ],
        **overrides,
    }


def generator(handler, **kwargs):
    return LLMGenerator(transport=httpx.MockTransport(handler), **kwargs)


def test_groq_json_request_uses_authenticated_chat_completions_and_local_validation():
    calls = []

    def handler(request):
        calls.append(request)
        assert str(request.url) == "https://api.groq.com/openai/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer unit-test-not-a-real-key"
        payload = json.loads(request.content)
        assert payload["model"] == MODEL
        assert payload["response_format"] == {"type": "json_object"}
        assert payload["max_completion_tokens"] == 2048
        assert payload["stream"] is False
        assert "JSON" in payload["messages"][0]["content"]
        assert payload["messages"][1] == {"role": "user", "content": "Explain maintenance"}
        return httpx.Response(200, json=completion())

    client = generator(handler)
    assert client.generate_json("Explain maintenance") == ANSWER
    assert client.provider == "groq"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "status,reason,retryable",
    [
        (401, "AUTHENTICATION_FAILED", False),
        (403, "AUTHENTICATION_FAILED", False),
        (429, "RATE_LIMITED", True),
        (500, "UPSTREAM_UNAVAILABLE", True),
        (503, "UPSTREAM_UNAVAILABLE", True),
        (400, "REQUEST_REJECTED", False),
        (404, "REQUEST_REJECTED", False),
    ],
)
def test_provider_http_errors_are_sanitized_and_not_automatically_retried(
    status, reason, retryable
):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"error": "unit-test-not-a-real-key private-prompt"})

    with pytest.raises(LLMProviderError) as caught:
        generator(handler).generate_json("private-prompt")
    assert caught.value.reason == reason
    assert caught.value.retryable is retryable
    assert len(calls) == 1
    assert "private-prompt" not in str(caught.value)
    assert "unit-test-not-a-real-key" not in str(caught.value)


def test_timeout_does_not_repeat_uncertain_generation():
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("private provider diagnostics", request=request)

    with pytest.raises(LLMProviderError) as caught:
        generator(handler).generate_json("maintenance")
    assert (caught.value.reason, caught.value.retryable) == ("TIMEOUT", True)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "content",
    [
        "not JSON",
        "[]",
        '{"summary":"x","summary":"y"}',
        '{"summary":"x","uncertainty":"x","checklist":NaN}',
        '{"summary":123,"uncertainty":"x","checklist":[]}',
        '{"summary":"x","uncertainty":"x","checklist":{}}',
    ],
)
def test_json_mode_does_not_replace_our_output_validation(content):
    with pytest.raises(LLMProviderError) as caught:
        generator(lambda request: httpx.Response(200, json=completion(content))).generate_json(
            "maintenance"
        )
    assert caught.value.reason == "INVALID_RESPONSE"


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"model": MODEL, "choices": []},
        completion(model="unreviewed-model"),
        {"model": MODEL, "choices": [{"message": {"content": None}}]},
    ],
)
def test_malformed_chat_envelopes_and_model_mismatch_are_rejected(body):
    with pytest.raises(LLMProviderError):
        generator(lambda request: httpx.Response(200, json=body)).generate_json("maintenance")


def test_truncated_json_is_not_accepted_as_a_complete_brief():
    body = completion()
    body["choices"][0]["finish_reason"] = "length"
    with pytest.raises(LLMProviderError) as caught:
        generator(lambda request: httpx.Response(200, json=body)).generate_json("maintenance")
    assert caught.value.reason == "OUTPUT_TRUNCATED"


def test_readiness_uses_cached_get_models_not_completions():
    calls, clock = [], [0.0]

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        assert request.url.path == "/openai/v1/models"
        return httpx.Response(200, json={"data": [{"id": MODEL}]})

    client = generator(handler, clock=lambda: clock[0])
    assert client.is_available()
    clock[0] = 10
    assert client.is_available()
    assert len(calls) == 1
    clock[0] = 31
    assert client.is_available()
    assert len(calls) == 2


def test_readiness_recovers_after_a_provider_failure():
    clock, calls = [0.0], []

    def handler(request):
        calls.append(request)
        return (
            httpx.Response(503)
            if len(calls) == 1
            else httpx.Response(200, json={"data": [{"id": MODEL}]})
        )

    client = generator(handler, clock=lambda: clock[0])
    assert not client.is_available()
    clock[0] = 2
    assert not client.is_available()
    assert len(calls) == 1
    clock[0] = 6
    assert client.is_available()


def test_redirects_never_forward_credentials_to_another_origin():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(307, headers={"Location": "https://other.invalid/steal"})

    with pytest.raises(LLMProviderError):
        generator(handler).generate_json("maintenance")
    assert len(calls) == 1


def test_text_generation_and_prompt_validation():
    calls = []

    def handler(request):
        calls.append(request)
        assert "response_format" not in json.loads(request.content)
        return httpx.Response(200, json=completion("Maintenance text"))

    client = generator(handler)
    assert client.generate("maintenance") == "Maintenance text"
    with pytest.raises(ValueError):
        client.generate_json(" ")
    assert len(calls) == 1
