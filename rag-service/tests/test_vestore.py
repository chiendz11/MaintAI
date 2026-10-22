

from langchain_core.documents import Document

from ingestion.embedding import Embedder
from knowledge.vector_store import PostgresVectorStore


# 1. Tạo một đoạn văn bản thử nghiệm
documents = [
    Document(
        page_content="Động cơ quá nhiệt có thể do thông gió kém.",
        metadata={
            "source": "test",
            "page": 1
        }
    )
]

# 2. Biến đoạn văn bản thành dãy số
embedder = Embedder()

texts = [doc.page_content for doc in documents]
embeddings = embedder.encode(texts)

# 3. Lưu vào PostgreSQL
store = PostgresVectorStore()
store.add(documents, embeddings)

print("Kiểm tra hoàn tất!")
