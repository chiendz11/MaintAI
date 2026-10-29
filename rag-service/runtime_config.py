import math
import os
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


def validate_runtime_config():
    required = (
        "DATABASE_URL",
        "OLLAMA_BASE_URL",
        "OLLAMA_MODEL",
        "EMBEDDING_MODEL",
    )

    missing = [
        key for key in required
        if not os.getenv(key, "").strip()
    ]

    if missing:
        raise ValueError(
            "Missing runtime configuration: " + ", ".join(missing)
        )

    database = urlparse(os.environ["DATABASE_URL"])

    if database.scheme not in ("postgresql", "postgres") or not database.hostname:
        raise ValueError(
            "DATABASE_URL must be a valid PostgreSQL URL"
        )

    ollama = urlparse(os.environ["OLLAMA_BASE_URL"])

    if ollama.scheme not in ("http", "https") or not ollama.hostname:
        raise ValueError(
            "OLLAMA_BASE_URL must be a valid HTTP(S) URL"
        )

    try:
        timeout = float(os.getenv("OLLAMA_TIMEOUT", "120"))

        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError()

    except ValueError:
        raise ValueError(
            "OLLAMA_TIMEOUT must be a positive finite number"
        ) from None

    cache = Path(
        os.getenv("SENTENCE_TRANSFORMERS_HOME")
        or os.getenv("HF_HOME")
        or str(Path.home() / ".cache" / "huggingface")
    )

    try:
        cache.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ValueError(
            f"Embedding cache directory is not writable: {cache}"
        ) from exc

    if not cache.is_dir() or not os.access(cache, os.W_OK):
        raise ValueError(
            f"Embedding cache directory is not writable: {cache}"
        )

    return True
