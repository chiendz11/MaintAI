import os
import psycopg
import json


class PostgresVectorStore:
    def __init__(self):
        self.connection_string = os.getenv("DATABASE_URL")

        if not self.connection_string:
            raise ValueError("Chưa cấu hình DATABASE_URL")

    def add(self, documents,embeddings):
        if len(documents) != len(embeddings):
            raise ValueError("Số documents và embeddings không khớp")
        if not documents:
            return
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as cusor:
                for doc, vector in zip(documents, embeddings):
                    cusor.execute("""
                INSERT INTO knowledge_chunks
                    (content, embedding, metadata)
                VALUES (%s, %s::vector, %s::jsonb)
                """,(doc.page_content,"[" + ",".join(map(str, vector)) + "]",json.dumps(doc.metadata, ensure_ascii=False)
                        ))

        print(f"Đã lưu {len(documents)} đoạn vào PostgreSQL")     