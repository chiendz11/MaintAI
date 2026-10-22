from ingestion.chunking import Chunking
from ingestion.parsing import load_pdf
from ingestion.embedding import Embedder

from knowledge.vector_store import PostgresVectorStore

def ingest(path: str):
    pages = load_pdf(path)
    chunker = Chunking()
    documents = []
    for page in pages:
        chunk = chunker.load_document_from_text(
            text = page["text"],
            metadata = page["metadata"]
        )
        documents.extend(chunk)
    if not documents:
        print("fail")
        return

    embedder = Embedder()
    temp = [doc.page_content for doc in documents]
    embedding = embedder.encode(temp)

    store = PostgresVectorStore()
    store.add(documents,embedding)
if __name__ == "__main__":
    ingest("documents/pdffile.pdf")