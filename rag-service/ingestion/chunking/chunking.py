import os 
from typing import List, Optional

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

class Chunking:
    def __init__(
            self,
            size: int = 1000,
            overlap: int = 200
    ):
        self.text_splitter = RecursiveCharacterTextSplitter(
                size=chunk_size,
                overlap=chunk_overlap,
                separators=["\n\n", "\n", " ", ""]
            
            )
    def load_document_from_text(
            self,
            text: str,
            metadata: Optional[dict] = None
        ) -> List[Document]:
            """
            Xử lý chuỗi văn bản thô trực tiếp.
            Ví dụ: nội dung nhận từ hệ thống MaintAI.
            """
    
            if not text.strip():
                return []
            metadata = metadata or {}
            raw_doc = Document(
                page_content=text,
                metadata=metadata
            )
            chunks = self.text_splitter.split_documents([raw_doc])
            return chunks