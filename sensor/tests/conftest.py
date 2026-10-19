import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from jsonschema import Draft202012Validator, FormatChecker

SENSOR_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def sample_csv():
    return SENSOR_ROOT / "tests/fixtures/ai4i_sample.csv"


@pytest.fixture
def scenario_dir():
    return SENSOR_ROOT / "scenarios"


@pytest.fixture
def validator():
    schema = json.loads((SENSOR_ROOT / "tests/fixtures/sensor-wire.schema.json").read_text())
    return Draft202012Validator(schema, format_checker=FormatChecker())


class ContractBackend:
    """Test-only backend: enforce the published schema and idempotency independently."""

    def __init__(self, validator):
        self.validator = validator
        self.requests = []
        self.stored = {}
        self.predictions = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.path == "/api/v1/sensor-readings"
        assert request.headers["content-type"].startswith("application/json")
        assert request.headers["accept"] == "application/json"
        assert request.headers["x-request-id"].startswith("req_")
        correlation_id = request.headers["x-correlation-id"]
        assert correlation_id.startswith("cor_")
        payload = json.loads(request.content)
        errors = list(self.validator.iter_errors(payload))
        if errors:
            return httpx.Response(
                422,
                json={
                    "event_id": payload["event_id"],
                    "machine_id": payload["machine_id"],
                    "validation_status": "INVALID",
                    "machine_state": "INVALID",
                    "errors": [
                        {
                            "field": ".".join(map(str, error.path)),
                            "code": "INVALID_DATA",
                            "message": error.message,
                        }
                        for error in errors
                    ],
                    "prediction_id": None,
                    "alert_id": None,
                    "correlation_id": correlation_id,
                },
            )
        event_id = payload["event_id"]
        if event_id in self.stored:
            old_payload, ack = self.stored[event_id]
            if old_payload != payload:
                return httpx.Response(
                    409,
                    json={
                        "error": {"code": "EVENT_CONFLICT", "retryable": False},
                    },
                )
            return httpx.Response(200, json=ack)
        self.predictions += 1
        ack = {
            "event_id": event_id,
            "machine_id": payload["machine_id"],
            "validation_status": "VALID",
            "processing_status": "COMPLETED",
            "machine_state": "NORMAL",
            "prediction_id": f"pred_{self.predictions}",
            "alert_id": None,
            "correlation_id": correlation_id,
        }
        self.stored[event_id] = payload, ack
        return httpx.Response(200, json=ack)


@pytest.fixture
def backend(validator):
    return ContractBackend(validator)


@pytest.fixture
def http_backend(backend):
    """Real localhost HTTP connection, exercising CLI/client beyond MockTransport."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            request = httpx.Request(
                "POST",
                f"http://localhost{self.path}",
                headers=dict(self.headers),
                content=body,
            )
            response = backend.handle(request)
            self.send_response(response.status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", backend
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
