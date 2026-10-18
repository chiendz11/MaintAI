from typing import List

import faiss
from langchain_core.documents import Document
from sentence_transformers import SentenceTransformer


class VectorRetriever:

    def __init__(
        self,
        model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    ):
        self.model = SentenceTransformer(model_name)
        self.documents = []
        self.index = None

    def add_document(self, documents: List[Document]):
        if not documents:
            return

        texts = [doc.page_content for doc in documents]

        embedding = self.model.encode(
            texts,
            convert_to_numpy=True
        )

        embedding = embedding.astype("float32")

        faiss.normalize_L2(embedding)

        if self.index is None:
            dimension = embedding.shape[1]
            self.index = faiss.IndexFlatIP(dimension)

        self.index.add(embedding)
        self.documents.extend(documents)

    def search(self, query: str, top_k: int = 3) -> List[Document]:
        if self.index is None or not self.documents:
            return []

        if not query.strip() or top_k <= 0:
            return []

        embedding = self.model.encode(
            [query],
            convert_to_numpy=True
        )

        embedding = embedding.astype("float32")

        faiss.normalize_L2(embedding)

        k = min(top_k, len(self.documents))

        scores, indices = self.index.search(embedding, k)

        results = []

        for idx in indices[0]:
            if idx != -1:
                results.append(self.documents[int(idx)])

        return results