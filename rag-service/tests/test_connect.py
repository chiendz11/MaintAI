import psycopg
from knowledge.vector_store import PostgresVectorStore

store = PostgresVectorStore()

with psycopg.connect(store.connection_string) as conn:
    print("Kết nối PostgreSQL thành công!")