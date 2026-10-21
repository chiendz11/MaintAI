from knowledge.pdf_parser import load_pdf
from knowledge.processor import KnowledgeProcessor

pages = load_pdf("documents/pdffile.pdf")
processor = KnowledgeProcessor()
all_chunks = []
for page in pages:
    chunk = processor.load_document_from_text(text = page["text"],
                                              metadata = page ["metadata"])
    all_chunks.extend(chunk)


print(f"Tổng số trang đọc được: {len(pages)}")
print(f"Tổng số chunks: {len(all_chunks)}")

if all_chunks:
    print(all_chunks[0].page_content)
    print(all_chunks[0].metadata)

print("\n===== KIỂM TRA 5 CHUNKS ĐẦU =====")
for i, chunk in enumerate(all_chunks[:5]):
    print(f"\n--- CHUNK {i + 1} ---")
    print("Số ký tự:", len(chunk.page_content))
    print("Metadata:", chunk.metadata)
    print("Nội dung:")
    print(chunk.page_content[:500])