
import os
import json
import psycopg
import numpy as np

from dotenv import load_dotenv
from langchain_core.documents import Document

load_dotenv()


class PostgresVectorStore:
    def __init__(self):
        self.connection_string = os.getenv("DATABASE_URL")
        if not self.connection_string:
            raise ValueError("Chưa cấu hình DATABASE_URL")
    @staticmethod
    def _vector_to_string(vector):
        values = np.asarray(vector, dtype=np.float32).reshape(-1)
        if values.size != 384:
            raise ValueError(
                f"Embedding phải có 384 chiều, nhận được {values.size}"
            )
        if not np.all(np.isfinite(values)):
            raise ValueError("Embedding chứa giá trị không hợp lệ")
        return "[" + ",".join(map(str, values.tolist())) + "]"
    def add(self, documents, embeddings):
        if len(documents) != len(embeddings):
            raise ValueError(
                "Số documents và embeddings không khớp"
            )
        if not documents:
            return
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                for doc, vector in zip(documents, embeddings):
                    cursor.execute(
                        """
                        INSERT INTO knowledge_chunks
                            (content, embedding, metadata)
                        VALUES (%s, %s::vector, %s::jsonb)
                        """,
                        (
                            doc.page_content,
                            self._vector_to_string(vector),
                            json.dumps(
                                doc.metadata,
                                ensure_ascii=False
                            )
                        )
                    )
        print(f"Đã lưu {len(documents)} đoạn vào PostgreSQL")
    def search(
        self,
        query_embedding,
        top_k: int = 3,
        filters: dict | None = None
    ) -> list[Document]:
        if top_k <= 0:
            return []
        vector_str = self._vector_to_string(query_embedding)
        filter_json = json.dumps(
            filters or {},
            ensure_ascii=False
        )
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                        SELECT
                            id,
                            content,
                            metadata,
                            embedding <=> %s::vector AS distance
                        FROM knowledge_chunks
                        WHERE metadata @> %s::jsonb
                        AND jsonb_typeof(metadata) = 'object'
                        AND NULLIF(BTRIM(metadata->>'document_id'), '') IS NOT NULL
                        AND NULLIF(BTRIM(metadata->>'document_version'), '') IS NOT NULL
                        AND NULLIF(BTRIM(metadata->>'section'), '') IS NOT NULL
                        AND NULLIF(BTRIM(metadata->>'page'), '') IS NOT NULL
                        ORDER BY distance ASC
                        LIMIT %s
                    """,
                    (
                        vector_str,
                        filter_json,
                        top_k
                    )
                )
                rows = cursor.fetchall()
        documents = []
        for chunk_id, content, metadata, distance in rows:
            document = Document(
                page_content=content,
                metadata={
                    **(metadata or {}),
                    "chunk_id": str(chunk_id),
                    "distance": float(distance)
                }
            )
            documents.append(document)
        return documents
       
    def document_exists(
        self,
        document_id: str,
        document_version: str,
    ) -> bool:
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM knowledge_chunks
                        WHERE metadata @> %s::jsonb
                    )
                    """,
                    (
                        json.dumps({
                            "document_id": document_id,
                            "document_version": document_version,
                        }),
                    ),
                )
                return bool(cursor.fetchone()[0])

