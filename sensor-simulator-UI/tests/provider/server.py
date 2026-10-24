"""Browser integration fixture: real Sensor provider, fixture HTTP202 ingress.

No Kafka, cloud or external IdP is started. Ports are test-local only.
"""

import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import uvicorn

from simulator.control.api import create_app
from simulator.settings import RuntimeSettings


class Ingress(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        ack = {
            "event_id": payload["event_id"],
            "machine_id": payload["machine_id"],
            "ingestion_status": "ACCEPTED",
            "processing_status": "PENDING",
            "server_accepted_at": "2026-10-20T00:00:00Z",
            "status_url": "https://business.example.invalid/api/v1/sensor-events/x/status",
            "correlation_id": self.headers["X-Correlation-ID"],
        }
        body = json.dumps(ack).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    ingress = ThreadingHTTPServer(("127.0.0.1", 8007), Ingress)
    threading.Thread(target=ingress.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="maintai-browser-provider-") as state:
        settings = RuntimeSettings(
            dataset_path=root / "sensor/tests/fixtures/ai4i_sample.csv",
            scenario_path=root / "sensor/scenarios",
            state_path=Path(state) / "state.sqlite3",
            auth_mode="local",
            ingestion_auth="none",
            backend_base_url="http://127.0.0.1:8007",
            initial_state="PAUSED",
            loop_dataset=True,
            machine_count=2,
            cors_origins=["http://127.0.0.1:4174"],
        )
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(settings),
                host="127.0.0.1",
                port=8006,
                access_log=False,
                proxy_headers=False,
            )
        )
        # Playwright terminates its process group after the suite; Uvicorn owns signals.
        try:
            server.run()
        finally:
            ingress.shutdown()
            ingress.server_close()
