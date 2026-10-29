from unittest.mock import MagicMock, patch

from langchain_core.documents import Document

from metadata_filter.filter import MetadataFilter
from vector_retriever.retriever import VectorRetriever


def test_retrieval_flow():
    query = "motor overheating troubleshooting maintenance"

    filters = MetadataFilter().build(
        source="pdffile.pdf"
    )

    expected_documents = [
        Document(
            page_content="Check the motor cooling system.",
            metadata={
                "chunk_id": "chunk-001",
                "page": 13,
                "source": "pdffile.pdf",
            },
        )
    ]

    with patch(
        "vector_retriever.retriever.PostgresVectorStore"
    ) as mock_store_class:
        mock_store = MagicMock()
        mock_store.search.return_value = expected_documents
        mock_store_class.return_value = mock_store

        retriever = VectorRetriever()

        documents = retriever.search(
            query=query,
            top_k=3,
            filters=filters,
        )

    assert len(documents) == 1
    assert len(documents) <= 3

    for doc in documents:
        assert isinstance(doc.metadata["chunk_id"], str)
        assert "page" in doc.metadata
        assert doc.metadata["source"] == "pdffile.pdf"

    mock_store.search.assert_called_once()
