import os
from pathlib import Path
import numpy as np

DEFAULT_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def model_name():
    return os.getenv("EMBEDDING_MODEL", DEFAULT_MODEL)


def model_available_locally():
    # Only a lightweight filesystem check for readiness; no download.
    cache = Path(
        os.getenv("SENTENCE_TRANSFORMERS_HOME")
        or os.getenv("HF_HOME")
        or Path.home() / ".cache" / "huggingface"
    )
    model = model_name()
    return (
        Path(model).is_dir()
        or (cache / ("models--" + model.replace("/", "--"))).is_dir()
        or (cache / model.replace("/", "_")).is_dir()
    )


class Embedder:
    def __init__(self):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(
            model_name(),
            revision=os.getenv("EMBEDDING_REVISION") or None,
            cache_folder=os.getenv("SENTENCE_TRANSFORMERS_HOME") or os.getenv("HF_HOME"),
            local_files_only=os.getenv("MODEL_LOCAL_FILES_ONLY", "false").lower() == "true",
        )

    def encode(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, 384), dtype="float32")
        embeddings = self.model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        return embeddings.astype("float32")
