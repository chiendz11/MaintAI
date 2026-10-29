
from metadata_filter.filter import MetadataFilter
from vector_retriever.retriever import VectorRetriever


def test_retrieval_flow():
    query = "motor overheating troubleshooting maintenance"

    filters = MetadataFilter().build(
        source="pdffile.pdf"
    )

    retriever = VectorRetriever()

    documents = retriever.search(
        query=query,
        top_k=3,
        filters=filters
    )

    print(f"\nRetrieved: {len(documents)}")

    for doc in documents:
        print("Metadata:", doc.metadata)
        print("Content:", doc.page_content[:200])
        print("---")

        assert isinstance(doc.metadata["chunk_id"], str)
        assert "page" in doc.metadata

    assert len(documents) <= 3
