import logging

from fastapi import FastAPI
from api.routes import router

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="MaintAI RAG Service",
    description="Service xử lý bảo trì thông minh",
    version="1.0.0",
)

app.include_router(router)


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "MaintAI RAG Service",
    }