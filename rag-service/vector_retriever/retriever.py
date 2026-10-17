from typing import List

import faiss  #THƯ VIỆN TÌM KIỂM VECTOR
from langchain_core.documents import Document  #
from sentence_transformers import SentenceTransformers  #CLASSS DÙNG ĐỂ CHẠY MODEL EMBEDDING

class VectorRetriever:
    def __init__(self, model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"):
        self.model = SentenceTranformers(model_name)
        self.documents = []
        self.index = None

    def add_document(self,documents: List[Document]):
        if not documents:
            return
        texts = [doc.page_content for doc in documents]
        embedding = self.model.encode(texts, convert_to_numpy=True)

        embedding.astype("float32")  #chuan hoa dua ve dang ma faiss hay dung
        faiss.normalize_L2(embedding)   #bien do dai cac vetor ve 1 trong khong gian de de so sanh
        faiss.IndexFlatIP(384)
        self.index.add(embedding)
        self.documents.extend(documents)

    def search(self, query: str, top_k: int = 3) -> List[Document]:
        if self.index is None:
            return[]
        embedding = self.model.encode(query,convert_to_numpy = True)
        embedding.astype("float32")  #chuan hoa dua ve dang ma faiss hay dung
        faiss.normalize_L2(embedding)   #bien do dai cac vetor ve 1 trong khong gian de de so sanh
        k = min(top_k, len(self.documents1))
        score,indices = self.index.search(embedding, k)
        result = []
        for idx in indices[0]:
            if idx != -1:
                result.append(self.documents[int(idx)])
        return result
