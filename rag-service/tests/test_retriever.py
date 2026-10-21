from knowledge.pdf_parser import load_pdf
from knowledge.processor import KnowledgeProcessor
from vector_retriever.retriever import VectorRetriever

# 1. Đọc PDF
pages = load_pdf("documents/pdffile.pdf")

# 2. Chia chunks
processor = KnowledgeProcessor()
all_chunks = []

for page in pages:
    chunks = processor.load_document_from_text(
        text=page["text"],
        metadata=page["metadata"]
    )
    all_chunks.extend(chunks)

# 3. Khởi tạo VectorRetriever
retriever = VectorRetriever()

# 4. Embedding + lưu vào FAISS
retriever.add_document(all_chunks)

# 5. Thử tìm kiếm
query = "What are the causes of motor overheating?"

results = retriever.search(query, top_k=3)

for i, doc in enumerate(results, start=1):
    print(f"\n===== RESULT {i} =====")
    print("Nguồn:", doc.metadata)
    print("Nội dung:", doc.page_content[:700])