
from langchain_core.documents import Document


class CitationValidator:
    def validate(
        self,
        answer: dict,
        documents: list[Document],
    ) -> dict:
        if not isinstance(answer, dict):
            return self._invalid("Invalid LLM response")

        checklist = answer.get("checklist")

        if not isinstance(checklist, list) or not checklist:
            return self._invalid("No supported checklist")

        document_map = {
            str(doc.metadata["chunk_id"]): doc
            for doc in documents
            if doc.metadata.get("chunk_id") is not None
        }

        citations = []
        validated_items = []
        citation_ids = {}

        for item in checklist:
            if not isinstance(item, dict):
                return self._invalid("Invalid checklist item")

            step = item.get("step")
            action = item.get("action")
            chunk_ids = item.get("chunk_ids")

            if (
                not isinstance(step, int)
                or isinstance(step, bool)
                or step < 1
                or not isinstance(action, str)
                or not action.strip()
                or not isinstance(chunk_ids, list)
                or not chunk_ids
            ):
                return self._invalid("Invalid checklist structure")

            item_citations = []

            for chunk_id in chunk_ids:
                if not isinstance(chunk_id, str) or not chunk_id.strip():
                    return self._invalid("Invalid chunk ID")

                if chunk_id not in document_map:
                    return self._invalid(
                        f"Unknown chunk ID: {chunk_id}"
                    )

                metadata = document_map[chunk_id].metadata

                required = (
                    "document_id",
                    "document_version",
                    "section",
                    "page",
                )

                if any(
                    metadata.get(key) in (None, "")
                    for key in required
                ):
                    return self._invalid(
                        f"Missing citation metadata: {chunk_id}"
                    )

                try:
                    page = int(metadata["page"])
                    if page < 1:
                        raise ValueError()
                except (TypeError, ValueError):
                    return self._invalid("Invalid page")

                if chunk_id not in citation_ids:
                    citation_id = f"cite_{len(citations) + 1}"
                    citation_ids[chunk_id] = citation_id

                    citations.append({
                        "citation_id": citation_id,
                        "document_id": str(metadata["document_id"]),
                        "document_version": str(
                            metadata["document_version"]
                        ),
                        "section": str(metadata["section"]),
                        "page": page,
                        "chunk_id": chunk_id,
                    })

                item_citations.append(citation_ids[chunk_id])

            validated_items.append({
                "step": step,
                "action": action.strip(),
                "citation_ids": list(dict.fromkeys(item_citations)),
            })

        return {
            "valid": True,
            "reason": None,
            "checklist": validated_items,
            "citations": citations,
        }

    @staticmethod
    def _invalid(reason: str) -> dict:
        return {
            "valid": False,
            "reason": reason,
            "checklist": [],
            "citations": [],
        }
