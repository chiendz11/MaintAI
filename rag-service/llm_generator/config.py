"""Groq runtime inputs; credentials are never part of public metadata."""

import math
import os
from dataclasses import dataclass, field
from pathlib import Path

GROQ_BASE_URL = "https://api.groq.com/openai/v1"


@dataclass(frozen=True)
class GroqSettings:
    api_key: str = field(repr=False)
    model: str
    timeout_seconds: float = 60
    max_completion_tokens: int = 2048

    @classmethod
    def from_env(cls):
        value = os.getenv("GROQ_API_KEY", "").strip()
        filename = os.getenv("GROQ_API_KEY_FILE", "").strip()
        if value and filename:
            raise ValueError("Set only one of GROQ_API_KEY or GROQ_API_KEY_FILE")
        if filename:
            try:
                with Path(filename).open(encoding="utf-8") as stream:
                    value = stream.read(16385).strip()
            except (OSError, UnicodeError):
                raise ValueError("GROQ_API_KEY_FILE is not readable") from None
        if (
            not value
            or value == "CHANGE_ME"
            or len(value) > 16384
            or any(char.isspace() for char in value)
        ):
            raise ValueError("A valid GROQ_API_KEY or GROQ_API_KEY_FILE is required")
        model = os.getenv("GROQ_MODEL", "").strip()
        if not model or any(char.isspace() for char in model):
            raise ValueError("GROQ_MODEL must be explicitly configured")
        try:
            timeout = float(os.getenv("GROQ_TIMEOUT_SECONDS", "60"))
            tokens = int(os.getenv("GROQ_MAX_COMPLETION_TOKENS", "2048"))
        except ValueError:
            raise ValueError("Groq timeout/token budget must be numeric") from None
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("GROQ_TIMEOUT_SECONDS must be positive and finite")
        if not 1 <= tokens <= 32768:
            raise ValueError("GROQ_MAX_COMPLETION_TOKENS must be between 1 and 32768")
        return cls(value, model, timeout, tokens)
