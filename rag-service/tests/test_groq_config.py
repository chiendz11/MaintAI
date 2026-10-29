import pytest

from llm_generator.config import GroqSettings
from runtime_config import validate_runtime_config


def test_env_key_is_never_in_settings_repr():
    settings = GroqSettings.from_env()
    assert settings.model == "llama-3.3-70b-versatile"
    assert "unit-test-not-a-real-key" not in repr(settings)


def test_mounted_secret_file_is_supported(monkeypatch, tmp_path):
    secret = tmp_path / "key"
    secret.write_text("fixture-file-key\n")
    monkeypatch.delenv("GROQ_API_KEY")
    monkeypatch.setenv("GROQ_API_KEY_FILE", str(secret))
    assert GroqSettings.from_env().api_key == "fixture-file-key"


@pytest.mark.parametrize(
    "name,value",
    [
        ("GROQ_API_KEY", ""),
        ("GROQ_API_KEY", "CHANGE_ME"),
        ("GROQ_API_KEY", "key\nembedded"),
        ("GROQ_MODEL", ""),
        ("GROQ_TIMEOUT_SECONDS", "NaN"),
        ("GROQ_TIMEOUT_SECONDS", "0"),
        ("GROQ_MAX_COMPLETION_TOKENS", "0"),
        ("GROQ_MAX_COMPLETION_TOKENS", "40000"),
    ],
)
def test_invalid_provider_configuration_fails_before_requests(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        GroqSettings.from_env()


def test_ambiguous_credentials_and_missing_secret_file_are_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("GROQ_API_KEY_FILE", str(tmp_path / "missing"))
    with pytest.raises(ValueError, match="only one"):
        GroqSettings.from_env()
    monkeypatch.delenv("GROQ_API_KEY")
    with pytest.raises(ValueError, match="not readable"):
        GroqSettings.from_env()


def test_runtime_no_longer_requires_ollama(monkeypatch):
    for name in ["OLLAMA_BASE_URL", "OLLAMA_MODEL", "OLLAMA_TIMEOUT"]:
        monkeypatch.delenv(name, raising=False)
    assert validate_runtime_config()
