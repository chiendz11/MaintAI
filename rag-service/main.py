import FastAPI, HTTPException
import BaseModel

app = FastAPI(tile = "MainAI RAG Service",
              description = "Service xử lí bảo trì thông minh")
class AlertQueryRequest( BaseModel) :
    alert_id: str
    machine_id: str
    error_code: str
    querytext: str

def  heatlth_check():
    return {"status": "ok", "message": "RAG Service is up and running!"}

def explain_maintenance(request: AlertQueryRequest):
    return{
        "alert_id": request.alert_id,
        "machine_id": request.machine_id,
        "explanation": f"Hệ thống đang phân tích mã lỗi {request.error_code}..."
    }