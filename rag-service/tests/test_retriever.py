from langchain_core.documents import Document
from vector_retriever.retriever import VectorRetriever


documents = [
    Document(
        page_content="Máy CNC lỗi E101, động cơ quá nhiệt, cần kiểm tra quạt làm mát.",
        metadata={"id": "D1"}
    ),
    Document(
        page_content="Máy CNC lỗi E202, áp suất dầu thấp, cần kiểm tra bơm dầu.",
        metadata={"id": "D2"}
    ),
    Document(
        page_content="Máy CNC cần vệ sinh và bảo dưỡng định kỳ.",
        metadata={"id": "D3"}
    )
]

retriever = VectorRetriever()

# Nạp tài liệu
retriever.add_document(documents)

# Câu hỏi
query = "Động cơ máy CNC bị nóng quá mức thì phải làm gì?"

# Tìm một tài liệu phù hợp nhất
result = retriever.search(query, top_k=1)

# In kết quả
if result:
    print("ID:", result[0].metadata["id"])
    print("Content:", result[0].page_content)
else:
    print("Không tìm thấy tài liệu.") 

