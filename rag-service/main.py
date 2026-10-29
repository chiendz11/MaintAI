import logging
import os
import httpx
import psycopg
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from api.routes import router
from runtime_config import validate_runtime_config

logging.basicConfig(level=logging.INFO)
app = FastAPI(title='MaintAI RAG Service', version='1.4.0')
app.include_router(router)

@app.on_event('startup')
def check_config():
    validate_runtime_config()

@app.get('/health/live')
def live():
    return {'status': 'UP', 'service': 'rag-service', 'version': '1.4.0'}

@app.get('/health/ready')
def ready():
    checks = {'database': False, 'vector_store': False, 'llm_provider': False, 'embedding_model': False}
    try:
        with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('knowledge_chunks') IS NOT NULL")
                checks['database'] = True
                checks['vector_store'] = bool(cur.fetchone()[0])
    except (psycopg.Error, OSError):
        pass
    try:
        with httpx.Client(timeout=3) as client:
            r = client.get(os.environ['OLLAMA_BASE_URL'].rstrip('/') + '/api/tags')
            r.raise_for_status()
            models = r.json().get('models', [])
            configured = os.environ['OLLAMA_MODEL']
            checks['llm_provider'] = any(m.get('name') == configured or m.get('model') == configured for m in models)
    except (httpx.HTTPError, ValueError, KeyError, OSError):
        pass
    try:
        from ingestion.embedding import model_available_locally
        checks['embedding_model'] = model_available_locally()
    except (OSError, RuntimeError, ValueError):
        pass
    healthy = all(checks.values())
    return JSONResponse(status_code=200 if healthy else 503, content={'status': 'UP' if healthy else 'DEGRADED', 'checks': checks})

@app.get('/health')
def health():
    return live()
