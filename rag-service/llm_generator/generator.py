"""Groq Chat Completions over HTTPS with locally validated JSON output."""

import json
import threading
import time
from functools import lru_cache

import httpx

from llm_generator.config import GROQ_BASE_URL, GroqSettings


class LLMProviderError(RuntimeError):
    def __init__(self, reason, *, retryable):
        self.reason = reason
        self.retryable = retryable
        super().__init__("Groq generation unavailable: " + reason)


def json_object(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    def nonfinite(_):
        raise ValueError("Non-finite JSON number")

    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    if not isinstance(value, dict):
        raise ValueError("Expected JSON object")
    json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return value


class LLMGenerator:
    provider = "groq"

    def __init__(self, *, settings=None, transport=None, clock=time.monotonic):
        self.settings = settings or GroqSettings.from_env()
        self.model = self.settings.model
        self.transport = transport
        self.clock = clock
        self._ready_until = 0
        self._ready = False
        self._ready_lock = threading.Lock()

    def _client(self, timeout):
        return httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(5, timeout)),
            transport=self.transport,
            follow_redirects=False,
            headers={"Authorization": "Bearer " + self.settings.api_key},
        )

    def is_available(self):
        """Cached credential/model check; never generate or spend completion tokens."""
        with self._ready_lock:
            if self.clock() < self._ready_until:
                return self._ready
            self._ready = False
            try:
                with self._client(min(3, self.settings.timeout_seconds)) as client:
                    response = client.get(GROQ_BASE_URL + "/models")
                    response.raise_for_status()
                    models = json_object(response.content).get("data")
                    self._ready = isinstance(models, list) and any(
                        isinstance(model, dict) and model.get("id") == self.model
                        for model in models
                    )
            except (httpx.HTTPError, ValueError, OSError):
                pass
            self._ready_until = self.clock() + (30 if self._ready else 5)
            return self._ready

    def _generate(self, prompt, *, structured):
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("Prompt must be a non-empty string")
        system = "Use only the supplied maintenance evidence. Do not invent citations or change machine/alert state."
        if structured:
            system += " Return only a JSON object matching the format requested by the user."
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt.strip()},
            ],
            "temperature": 0.1,
            "max_completion_tokens": self.settings.max_completion_tokens,
            "stream": False,
        }
        if structured:
            payload["response_format"] = {"type": "json_object"}
        try:
            with self._client(self.settings.timeout_seconds) as client:
                response = client.post(GROQ_BASE_URL + "/chat/completions", json=payload)
                response.raise_for_status()
        except httpx.TimeoutException:
            raise LLMProviderError("TIMEOUT", retryable=True) from None
        except httpx.HTTPStatusError as error:
            status = error.response.status_code
            if status in {401, 403}:
                raise LLMProviderError("AUTHENTICATION_FAILED", retryable=False) from None
            if status == 429:
                raise LLMProviderError("RATE_LIMITED", retryable=True) from None
            if status >= 500:
                raise LLMProviderError("UPSTREAM_UNAVAILABLE", retryable=True) from None
            raise LLMProviderError("REQUEST_REJECTED", retryable=False) from None
        except (httpx.RequestError, OSError):
            raise LLMProviderError("CONNECTION_FAILED", retryable=True) from None
        # Do not retry an uncertain generation request or expose provider bodies.
        try:
            if len(response.content) > 1_000_000:
                raise ValueError("Oversized provider response")
            data = json_object(response.content)
            choice = data["choices"][0]
            if choice.get("finish_reason") == "length":
                raise LLMProviderError("OUTPUT_TRUNCATED", retryable=False)
            content = choice["message"]["content"]
            if data.get("model") != self.model:
                raise ValueError("Serving model differs from the configured model")
            if not isinstance(content, str) or not content.strip():
                raise ValueError("Empty provider response")
            return content.strip()
        except (ValueError, KeyError, TypeError, IndexError, AttributeError):
            raise LLMProviderError("INVALID_RESPONSE", retryable=False) from None

    def generate(self, prompt):
        return self._generate(prompt, structured=False)

    def generate_json(self, prompt):
        answer = self._generate(prompt, structured=True)
        try:
            result = json_object(answer)
            if not isinstance(result.get("summary"), str) or not result["summary"].strip():
                raise ValueError("Missing summary")
            if not isinstance(result.get("uncertainty"), str) or not isinstance(
                result.get("checklist"), list
            ):
                raise ValueError("Invalid uncertainty/checklist")
            return result
        except (ValueError, TypeError):
            raise LLMProviderError("INVALID_RESPONSE", retryable=False) from None


@lru_cache(maxsize=1)
def get_generator():
    return LLMGenerator()
