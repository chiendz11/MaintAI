from unittest.mock import patch
from fastapi.testclient import TestClient
from main import app

def test_liveness():
    response = TestClient(app).get('/health/live')
    assert response.status_code == 200
    assert response.json()['status'] == 'UP'

def test_readiness_degraded_without_db():
    with patch('main.psycopg.connect', side_effect=OSError('offline')):
        with patch('main.httpx.Client', side_effect=OSError('offline')):
            response = TestClient(app).get('/health/ready')
    assert response.status_code == 503
    assert response.json()['status'] == 'DEGRADED'
