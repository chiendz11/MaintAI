from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(
    title="MainAI RAG Service",
    description="Service xử lý bảo trì thông minh",
    version="1.0.0"
)

class AlertQueryRequest(BaseModel):
    alert_id: str
    machine_id: str
    error_code: str
    querytext: str = Field(min_length=1)

# Sửa Basemodel -> BaseModel, Respone -> Response
class AlertQueryResponse(BaseModel):
    alert_id: str
    machine_id: str
    error_code: str
    explanation: str
    recommended_actions: list[str]  # Sửa rec_action -> recommended_actions
    sources: list[str]              # Sửa source -> sources

@app.get("/health")
def health_check():  # Sửa typo heatlth_check cho đẹp
    return {
        "status": "ok",
        "message": "RAG Service is up and running!"
    }

@app.post(
    "/explain-maintenance",
    response_model=AlertQueryResponse,
)
def explain_maintenance(request: AlertQueryRequest):
    try:
        # TODO: Replace this placeholder with the RAG pipeline.
        explanation = (
            f"Đang phân tích mã lỗi {request.error_code} "
            f"của máy {request.machine_id}."
        )

        recommended_actions = [
            "Kiểm tra tình trạng máy.",
            "Đối chiếu mã lỗi với tài liệu bảo trì.",
        ]
        sources = []
        
        return AlertQueryResponse(
            alert_id=request.alert_id,
            machine_id=request.machine_id,
            error_code=request.error_code,
            explanation=explanation,
            recommended_actions=recommended_actions,
            sources=sources,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Không thể xử lý yêu cầu bảo trì.",
        ) from exc