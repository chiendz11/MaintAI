
from ingestion.embedding.embedding import Embedder

embedder = Embedder()

texts = [
    "Động cơ bị quá nhiệt",
    "Cách kiểm tra vòng bi động cơ"
]

vectors = embedder.encode(texts)

print("Kích thước:", vectors.shape)
print("5 số đầu tiên:", vectors[0][:5])
