
from ingestion.chunking import Chunking


def test_chunking_preserves_metadata():
    chunker = Chunking(size=100, overlap=20)

    documents = chunker.load_document_from_text(
        text="Motor maintenance instructions. " * 20,
        metadata={
            "document_id": "manual-001",
            "document_version": "1.0",
            "section": "Maintenance",
            "page": 1,
        },
    )

    assert documents

    for doc in documents:
        assert doc.page_content.strip()
        assert doc.metadata["document_id"] == "manual-001"
        assert doc.metadata["document_version"] == "1.0"
        assert doc.metadata["section"] == "Maintenance"
        assert doc.metadata["page"] == 1
