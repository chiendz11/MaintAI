
from langchain_core.documents import Document

from vector_retriever.retriever import VectorRetriever


def test_retriever_search():
    retriever = VectorRetriever.__new__(VectorRetriever)

    class FakeEmbedder:
        def encode(self, texts):
            return [[0.0] * 384 for _ in texts]

    class FakeStore:
        def search(self, query_embedding, top_k=3, filters=None):
            return [
                Document(
                    page_content="Check motor ventilation.",
                    metadata={
                        "chunk_id": "1",
                        "document_id": "manual-001",
                        "document_version": "1.0",
                        "section": "Maintenance",
                        "page": 1,
                    },
                )
            ]

    retriever.embedder = FakeEmbedder()
    retriever.vector_store = FakeStore()

    results = retriever.search(
        query="motor overheating",
        top_k=3,
    )

    assert len(results) == 1
    assert results[0].metadata["chunk_id"] == "1"
