import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = json.loads((ROOT / "contracts/v1.3/schemas/runtime.schema.json").read_text())["$defs"]


@pytest.mark.parametrize(
    "body,status,schema", [(None, 202, "IngestAck"), (b"{", 400, "Error"), (b"{}", 422, "Error")]
)
def test_http_ack_and_errors_validate_frozen_schema(edge, payload, body, status, schema):
    app, _, _ = edge
    with TestClient(app) as c:
        r = (
            c.post("/api/v1/sensor-readings", json=payload)
            if body is None
            else c.post(
                "/api/v1/sensor-readings",
                content=body,
                headers={"Content-Type": "application/json"},
            )
        )
    assert r.status_code == status
    Draft202012Validator(SCHEMAS[schema], format_checker=FormatChecker()).validate(r.json())
    assert r.headers["x-request-id"] and r.headers["x-correlation-id"]


def test_operational_http_shapes_validate(edge):
    app, _, _ = edge
    schemas = json.loads((ROOT / "contracts/v1.3/schemas/ingestion.schema.json").read_text())[
        "$defs"
    ]
    with TestClient(app) as c:
        for path, kind in [("/health/live", "Liveness"), ("/health/ready", "Readiness")]:
            r = c.get(path)
            assert r.status_code == 200
            Draft202012Validator(schemas[kind]).validate(r.json())
