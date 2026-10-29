
from unittest.mock import patch

from knowledge.vector_store import PostgresVectorStore


def test_vector_store_initialization():
    with patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://example:example@localhost:5432/test"},
    ):
        store = PostgresVectorStore()

    assert store.connection_string
