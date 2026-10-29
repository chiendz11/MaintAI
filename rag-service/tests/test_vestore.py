
from unittest.mock import MagicMock, patch

from langchain_core.documents import Document

from knowledge.vector_store import PostgresVectorStore


def test_vector_store_add():
    document = Document(
        page_content="Check motor ventilation.",
        metadata={
            "document_id": "manual-001",
            "document_version": "1.0",
            "section": "Maintenance",
            "page": 1,
        },
    )

    embedding = [0.0] * 384

    with patch("psycopg.connect") as mock_connect:
        connection = MagicMock()
        cursor = MagicMock()

        mock_connect.return_value.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor

        store = PostgresVectorStore()
        store.add([document], [embedding])

        assert mock_connect.called
