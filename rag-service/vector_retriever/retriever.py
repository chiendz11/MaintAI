
from typing import List

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
        top_k: int = 3
    ) -> List[Document]:

        if not query or not query.strip():
            return []

        if top_k <= 0:
            return []

        # 1. Chuyển câu hỏi thành embedding
        embeddings = self.embedder.encode([query])

        # 2. Lấy vector đầu tiên
        query_embedding = embeddings[0]

        # 3. Tìm các chunks liên quan trong PostgreSQL
        documents = self.vector_store.search(
            query_embedding=query_embedding,
            top_k=top_k
        )

        return documents
