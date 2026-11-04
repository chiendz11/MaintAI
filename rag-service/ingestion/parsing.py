
from pathlib import Path

import pymupdf


def load_pdf(
    file_path: str,
    document_id: str,
    document_version: str,
    section: str,
) -> list[dict]:

    for field_name, value in {
        "document_id": document_id,
        "document_version": document_version,
        "section": section,
    }.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{field_name} không được để trống"
            )

    pdf_path = Path(file_path)

    if not pdf_path.is_file():
        raise FileNotFoundError(
            f"Không tìm thấy PDF: {file_path}"
        )

    documents = []

    with pymupdf.open(str(pdf_path)) as pdf:
        for page_number, page in enumerate(pdf, start=1):
            text = page.get_text("text")

            if not text.strip():
                continue

            metadata = {
                "source": pdf_path.name,
                "page": page_number,
                "document_id": document_id.strip(),
                "document_version": document_version.strip(),
                "section": section.strip(),
            }

            documents.append({
                "text": text,
                "metadata": metadata,
            })

    return documents
