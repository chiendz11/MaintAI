import logging
from datetime import UTC, datetime
from uuid import uuid4

from api.fallback import FALLBACK_MESSAGE
from api.schemas import (
    GroundedStatus,
    MaintenanceBriefRequest,
    MaintenanceBriefResponse,
)
from citation_validator.validator import CitationValidator
from llm_generator.generator import get_generator
from prompt_builder.builder import PromptBuilder
from query_builder.builder import QueryBuilder
from vector_retriever.retriever import VectorRetriever

logger = logging.getLogger(__name__)


class RAGPipeline:
    def __init__(
        self,
        retriever=None,
        generator=None,
        top_k: int = 3,
    ):
        self.retriever = retriever or VectorRetriever()
        self.generator = generator or get_generator()
        self.top_k = top_k

        self.query_builder = QueryBuilder()
        self.prompt_builder = PromptBuilder()
        self.validator = CitationValidator()

    def run(
        self,
        request: MaintenanceBriefRequest,
    ) -> MaintenanceBriefResponse:

        # 1. Tạo câu truy vấn
        query = self.query_builder.build(request)

        # 2. Tìm tài liệu
        documents = self.retriever.search(
            query=query,
            top_k=self.top_k,
        )

        retrieved_ids = [
            str(doc.metadata["chunk_id"])
            for doc in documents
            if doc.metadata.get("chunk_id") is not None
        ]

        # 3. Tạo response mặc định
        response = {
            "brief_id": f"brief_{uuid4().hex}",
            "alert_id": request.alert.alert_id,
            "grounded_status": (GroundedStatus.INSUFFICIENT_EVIDENCE),
            "summary": FALLBACK_MESSAGE,
            "uncertainty": ("Chưa tìm được tài liệu phù hợp."),
            "checklist": [],
            "citations": [],
            "retrieval": {
                "top_k": self.top_k,
                "retrieved_chunk_ids": retrieved_ids,
            },
            "generator": None,
            "generated_at": None,
        }

        # 4. Không có tài liệu thì không gọi LLM
        if not documents:
            return MaintenanceBriefResponse.model_validate(response)

        # Chỉ sử dụng những chunk có đủ thông tin trích dẫn.
        required_fields = (
            "chunk_id",
            "document_id",
            "document_version",
            "section",
            "page",
        )

        citable_documents = [
            doc
            for doc in documents
            if all(
                doc.metadata.get(field) not in (None, "") for field in required_fields
            )
        ]

        if not citable_documents:
            response["uncertainty"] = (
                "Tài liệu tìm được chưa có đủ metadata để tạo citation hợp lệ."
            )

            return MaintenanceBriefResponse.model_validate(response)

        documents = citable_documents

        # 5. Tạo prompt
        prompt = self.prompt_builder.build(
            query=query,
            documents=documents,
        )

        # 6. Gọi Groq API; citation validation remains local.
        answer = self.generator.generate_json(prompt)

        # 7. Kiểm tra citation
        validation = self.validator.validate(
            answer=answer,
            documents=documents,
        )

        response["summary"] = answer["summary"]
        response["uncertainty"] = answer["uncertainty"]
        response["generator"] = {
            "provider": self.generator.provider,
            "model": self.generator.model,
        }
        response["generated_at"] = datetime.now(UTC)

        if validation["valid"]:
            response["grounded_status"] = GroundedStatus.GROUNDED
            response["checklist"] = validation["checklist"]
            response["citations"] = validation["citations"]

        else:
            response["grounded_status"] = GroundedStatus.VALIDATION_FAILED
            response["summary"] = FALLBACK_MESSAGE
            response["uncertainty"] = f"Validation failed: {validation['reason']}"

        # 8. Kiểm tra contract trước khi trả API
        return MaintenanceBriefResponse.model_validate(response)
