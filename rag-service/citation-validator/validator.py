
import re
from typing import Any

from langchain_core.documents import Document


class CitationValidator:

    CITATION_PATTERN = re.compile(
        r"\[Tài liệu\s+(\d+)\]",
        re.IGNORECASE
    )

    def validate(
        self,
        answer: str,
        documents: list[Document]
    ) -> dict[str, Any]:

        if not isinstance(answer, str):
            raise ValueError("Answer phải là chuỗi")

        if not answer.strip():
            return {
                "valid": False,
                "answer": "",
                "citations": [],
                "invalid_citations": [],
                "reason": "LLM trả về câu trả lời rỗng"
            }

        if not documents:
            return {
                "valid": False,
                "answer": answer,
                "citations": [],
                "invalid_citations": [],
                "reason": "Không có tài liệu tham khảo"
            }

        citation_numbers = [
            int(number)
            for number in self.CITATION_PATTERN.findall(answer)
        ]

        unique_numbers = list(dict.fromkeys(citation_numbers))

        valid_numbers = [
            number
            for number in unique_numbers
            if 1 <= number <= len(documents)
        ]

        invalid_numbers = [
            number
            for number in unique_numbers
            if number < 1 or number > len(documents)
        ]

        citations = []

        for number in valid_numbers:
            doc = documents[number - 1]

            citations.append({
                "reference": f"Tài liệu {number}",
                "chunk_id": doc.metadata.get("chunk_id"),
                "source": doc.metadata.get("source"),
                "page": doc.metadata.get("page")
            })

        is_valid = (
            len(valid_numbers) > 0
            and len(invalid_numbers) == 0
        )

        if invalid_numbers:
            reason = "Câu trả lời chứa trích dẫn không tồn tại"
        elif not valid_numbers:
            reason = "Câu trả lời không có trích dẫn hợp lệ"
        else:
            reason = None

        return {
            "valid": is_valid,
            "answer": answer,
            "citations": citations,
            "invalid_citations": [
                f"Tài liệu {number}"
                for number in invalid_numbers
            ],
            "reason": reason
        }
