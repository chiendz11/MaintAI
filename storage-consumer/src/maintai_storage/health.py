"""Internal management endpoints; probes only read cached local state."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST

from . import __version__


class ManagementServer:
    def __init__(self, settings, state, metrics):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/metrics":
                    try:
                        body, status, content_type = metrics.render(), 200, CONTENT_TYPE_LATEST
                    except Exception:
                        body, status, content_type = b"Metrics unavailable\n", 503, "text/plain"
                elif self.path in {"/health/live", "/health/ready"}:
                    snapshot = state.snapshot(settings)
                    ok = snapshot["live" if self.path.endswith("live") else "ready"]
                    body = json.dumps(
                        {
                            "service": "storage-consumer",
                            "version": __version__,
                            "status": "UP" if ok else "DOWN",
                            **snapshot,
                        }
                    ).encode()
                    status, content_type = (200 if ok else 503), "application/json"
                else:
                    body, status, content_type = b"Not found\n", 404, "text/plain"
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass  # Do not log untrusted paths or query strings.

        self.server = ThreadingHTTPServer((settings.host, settings.port), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name="storage-management",
            daemon=True,
        )

    def start(self):
        self.thread.start()

    def close(self):
        if self.thread.is_alive():
            self.server.shutdown()
            self.thread.join()
        self.server.server_close()
