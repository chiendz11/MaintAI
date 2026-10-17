#comment o day

import os
from typing import List
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

class knowledgeProcessor:
    def __init__(self, chunk_size = 1000, chunk_overlap: int = 200):
        self.text_splitter = RescusiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=["\n\n", "\n", " ", ""]
        )
    
    def load_document_from_text(self, text: str, metadata: dict = None) -> List[Document]:
        """
        Xử lý chuỗi văn bản thô trực tiếp (ví dụ: nội dung copy từ hệ thống MaintAI).
        """
        metadata = metadata or {}
        raw_doc = Document(page_content=text, metadata=metadata)
        chunks = self.text_splitter.split_documents([raw_doc])
        return chunks

    def load_file(self, file_path: str, metadata: dict = None) -> List[Document]:
        """
        Đọc file tài liệu (hỗ trợ .txt, .md, và có thể mở rộng cho .pdf).
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy file tại đường dẫn: {file_path}")

        base_metadata = {"source": file_path}
        if metadata:
            base_metadata.update(metadata)

        # Đọc file dựa vào phần mở rộng
        if file_path.endswith((".txt", ".md")):
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            raw_doc = Document(page_content=content, metadata=base_metadata)
            chunks = self.text_splitter.split_documents([raw_doc])
            return chunks
        
        elif file_path.endswith(".pdf"):
            # Gợi ý: Bạn có thể dùng PyPDFLoader hoặc PyMuPDFLoader ở đây nếu cần đọc PDF
            raise NotImplementedError("Tính năng đọc PDF đang được phát triển, hãy tạm dùng txt/md!")
    
        else:
            raise ValueError(f"Định dạng file chưa được hỗ trợ: {file_path}")

