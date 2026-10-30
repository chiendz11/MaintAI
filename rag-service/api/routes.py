import logging
from uuid import uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from functools import lru_cache
from llm_generator.generator import LLMProviderError

from api.schemas import (
    MaintenanceBriefRequest,
    MaintenanceBriefResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["RAG Briefs"],
)


# Exception dùng khi RAG không khả dụng
class RAGUnavailableError(Exception):
    pass


# Hàm tạo HTTP error response
def error_response(
    status_code: int,
    code: str,
    message: str,
    retryable: bool,
    request_id: str,
    details: dict | None = None,
) -> JSONResponse:

    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "retryable": retryable,
                "details": details if details is not None else {},
                "request_id": request_id,
                "correlation_id": request_id,
            }
        },
    )


# Điểm kết nối với RAG Pipeline
@lru_cache(maxsize=1)
def get_pipeline():
    from pipeline import RAGPipeline

    return RAGPipeline()


# Endpoint tạo Maintenance Brief
@router.post(
    "/briefs",
    response_model=MaintenanceBriefResponse,
    status_code=200,
)
def create_maintenance_brief(
    request: MaintenanceBriefRequest,
):
    request_id = f"req_{uuid4().hex}"

    logger.info(
        "Received RAG request: request_id=%s alert_id=%s",
        request_id,
        request.alert.alert_id,
    )

    try:
        # Lấy pipeline
        pipeline = get_pipeline()

        # Chạy RAG Pipeline
        result = pipeline.run(request)

        # Kiểm tra response theo contract
        response = MaintenanceBriefResponse.model_validate(result)

        return response

    except LLMProviderError as error:
        logger.warning(
            "RAG provider unavailable: request_id=%s reason=%s", request_id, error.reason
        )
        return error_response(
            status_code=503,
            code="RAG_UNAVAILABLE",
            message="RAG generation provider is unavailable",
            retryable=error.retryable,
            request_id=request_id,
            details={"dependency": "groq", "reason": error.reason},
        )

    except (
        RAGUnavailableError,
        ConnectionError,
        TimeoutError,
        RuntimeError,
    ):
        logger.exception(
            "RAG unavailable: request_id=%s",
            request_id,
        )

        return error_response(
            status_code=503,
            code="RAG_UNAVAILABLE",
            message="RAG service is unavailable",
            retryable=True,
            request_id=request_id,
        )

    except Exception:
        logger.exception(
            "Unexpected RAG error: request_id=%s",
            request_id,
        )

        return error_response(
            status_code=500,
            code="INTERNAL_ERROR",
            message="Internal RAG service error",
            retryable=False,
            request_id=request_id,
        )
