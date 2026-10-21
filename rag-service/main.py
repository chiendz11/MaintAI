
import logging
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MaintAI RAG Service",
    description="Service xử lý bảo trì thông minh",
    version="1.0.0",
)

# Request schema
class AlertQueryRequest(BaseModel):
    alert_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    error_code: str = Field(min_length=1)
    query_text: str = Field(min_length=1)


# Response schema
class AlertQueryResponse(BaseModel):
    request_id: str
    alert_id: str
    machine_id: str
    error_code: str
    status: str
    explanation: str
    recommended_actions: list[str]
    sources: list[str]


# Health check
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "MaintAI RAG Service",
    }


# Maintenance explanation
@app.post(
    "/explain-maintenance",
    response_model=AlertQueryResponse,
)
def explain_maintenance(request: AlertQueryRequest):
    request_id = str(uuid4())

    logger.info(
        "Received maintenance request %s for alert %s",
        request_id,
        request.alert_id,
    )

    try:
        # Temporary response until the RAG modules are implemented.
        return AlertQueryResponse(
            request_id=request_id,
            alert_id=request.alert_id,
            machine_id=request.machine_id,
            error_code=request.error_code,
            status="not_implemented",
            explanation=(
                "RAG pipeline chưa được triển khai. "
                "Đây là response kiểm thử API."
            ),
            recommended_actions=[],
            sources=[],
        )

    except Exception as exc:
        logger.exception(
            "Request %s failed",
            request_id,
        )

        raise HTTPException(
            status_code=500,
            detail="Không thể xử lý yêu cầu bảo trì.",
        ) from exc
