
from typing import Any

from langchain_core.documents import Document

from ingestion.embedding import Embedder
from knowledge.vector_store import PostgresVectorStore


class VectorRetriever:
    def __init__(self):
        self.embedder = Embedder()
        self.vector_store = PostgresVectorStore()

    def search(
        self,
        query: str,
        top_k: int = 3,
        filters: dict[str, Any] | None = None
    ) -> list[Document]:

        if not query or not query.strip():
            return []

        if top_k <= 0:
            return []

        # Chuyển câu truy vấn thành vector
        embeddings = self.embedder.encode([query])
        query_embedding = embeddings[0]

        # Truy vấn PostgreSQL với bộ lọc metadata
        documents = self.vector_store.search(
            query_embedding=query_embedding,
            top_k=top_k,
            filters=filters
        )

        return documents
