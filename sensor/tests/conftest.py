import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from jsonschema import Draft202012Validator, FormatChecker

from simulator.models import Features, sensor_event_schema
from simulator.settings import RuntimeSettings

SENSOR_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def sample_csv():
    return SENSOR_ROOT / "tests/fixtures/ai4i_sample.csv"


@pytest.fixture
def scenario_dir():
    return SENSOR_ROOT / "scenarios"


@pytest.fixture
def validator():
    schema = sensor_event_schema()
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
        schema = json.loads(
            (SENSOR_ROOT.parent / "contracts/v1.3/schemas/runtime.schema.json").read_text()
        )["$defs"]["SensorRequest"]
        if not Draft202012Validator(schema, format_checker=FormatChecker()).is_valid(payload):
            return httpx.Response(
                422,
                json={
                    "error": {
                        "code": "VALIDATION_FAILED",
                        "message": "Invalid envelope",
                        "retryable": False,
                    }
                },
            )
        event_id = payload["event_id"]
        if event_id in self.stored:
            old_payload, ack = self.stored[event_id]
            if old_payload != payload:
                return httpx.Response(
                    409, json={"error": {"code": "EVENT_CONFLICT", "retryable": False}}
                )
            return httpx.Response(202, json=ack)
        # Test-only processing observation; ingress ACK never exposes a prediction.
        try:
            Features.model_validate(payload["features"])
            self.predictions += 1
        except ValueError:
            pass
        ack = {
            "event_id": event_id,
            "machine_id": payload["machine_id"],
            "ingestion_status": "ACCEPTED",
            "processing_status": "PENDING",
            "server_accepted_at": "2026-10-20T00:00:00Z",
            "status_url": f"https://business.example.invalid/api/v1/sensor-events/{event_id}/status",
            "correlation_id": correlation_id,
        }
        self.stored[event_id] = payload, ack
        return httpx.Response(202, json=ack)


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


@pytest.fixture
def runtime_settings(sample_csv, scenario_dir, tmp_path):
    return RuntimeSettings(
        dataset_path=sample_csv,
        scenario_path=scenario_dir,
        state_path=tmp_path / "state.sqlite3",
        auth_mode="local",
        ingestion_auth="none",
        backend_base_url="http://127.0.0.1:9999",
        loop_dataset=True,
        machine_count=2,
        interval_ms=10,
    )
