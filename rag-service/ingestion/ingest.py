
from ingestion.chunking import Chunking
from ingestion.parsing import load_pdf
from ingestion.embedding import Embedder
from knowledge.vector_store import PostgresVectorStore


def ingest(
    path: str,
    document_id: str,
    document_version: str,
    section: str,
) -> int:

    # 1. Kiểm tra metadata đầu vào
    required_values = {
        "document_id": document_id,
        "document_version": document_version,
        "section": section,
    }

    for field_name, value in required_values.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{field_name} không được để trống"
            )

    document_id = document_id.strip()
    document_version = document_version.strip()
    section = section.strip()

    # 2. Kết nối Vector Store
    store = PostgresVectorStore()

    # Không ingest trùng tài liệu cùng phiên bản
    if store.document_exists(
        document_id,
        document_version,
    ):
        print("Tài liệu cùng phiên bản đã tồn tại.")
        return 0

    # 3. Đọc PDF
    pages = load_pdf(
        file_path=path,
        document_id=document_id,
        document_version=document_version,
        section=section,
    )

    if not pages:
        print("PDF không có nội dung văn bản.")
        return 0

    # 4. Chunking
    chunker = Chunking()
    documents = []

    for page in pages:
        chunks = chunker.load_document_from_text(
            text=page["text"],
            metadata=page["metadata"],
        )

        documents.extend(chunks)

    if not documents:
        print("Không có chunk nào được tạo.")
        return 0

    for index, doc in enumerate(documents):
        doc.metadata["chunk_index"] = index

    # 5. Validate metadata trước khi lưu
    required_fields = (
        "document_id",
        "document_version",
        "section",
        "page",
    )

    for index, doc in enumerate(documents):
        metadata = doc.metadata

        missing_fields = [
            field
            for field in required_fields
            if (
                field not in metadata
                or metadata[field] is None
                or (
                    isinstance(metadata[field], str)
                    and not metadata[field].strip()
                )
            )
        ]

        if missing_fields:
            raise ValueError(
                f"Chunk {index} thiếu metadata: "
                f"{missing_fields}"
            )

        if not doc.page_content.strip():
            raise ValueError(
                f"Chunk {index} không có nội dung"
            )

    # 6. Embedding
    embedder = Embedder()

    texts = [
        doc.page_content
        for doc in documents
    ]

    embeddings = embedder.encode(texts)

    if len(embeddings) != len(documents):
        raise ValueError(
            "Số lượng embeddings không khớp "
            "với số lượng chunks"
        )

    # 7. Lưu PostgreSQL
    inserted = store.add(documents, embeddings)

    print(
        f"Ingest thành công {len(documents)} chunks "
        f"cho tài liệu {document_id} "
        f"phiên bản {document_version}."
    )

    return inserted


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest PDF into MaintAI knowledge database"
    )

    parser.add_argument(
        "--path",
        required=True,
        help="Path to PDF file",
    )

    parser.add_argument(
        "--document-id",
        required=True,
        help="Unique document identifier",
    )

    parser.add_argument(
        "--document-version",
        required=True,
        help="Document version",
    )

    parser.add_argument(
        "--section",
        required=True,
        help="Document section",
    )

    args = parser.parse_args()

    count = ingest(
        path=args.path,
        document_id=args.document_id,
        document_version=args.document_version,
        section=args.section,
    )

    print(f"Total chunks inserted: {count}")
