
from langchain_core.documents import Document
from prompt_builder.builder import PromptBuilder


def test_prompt_builder():
    documents = [
        Document(
            page_content=(
                "Check motor ventilation and cooling "
                "when overheating is suspected."
            ),
            metadata={
                "chunk_id": "101",
                "document_id": "motor-manual",
                "document_version": "1.0",
                "section": "Troubleshooting",
                "page": 12,
                "source": "manual.pdf",
            },
        )
    ]

    prompt = PromptBuilder().build(
        query="How should motor overheating be checked?",
        documents=documents,
    )

    assert '"chunk_id": "101"' in prompt
    assert "Check motor ventilation" in prompt
    assert '"chunk_ids"' in prompt
    assert "How should motor overheating be checked?" in prompt

    print("\nPROMPT BUILDER OK")
    print(prompt)


def test_empty_documents():
    try:
        PromptBuilder().build("motor overheating", [])
    except ValueError:
        return

    raise AssertionError("Phải từ chối danh sách tài liệu rỗng")
