import json

import pytest
from maintai_messaging.ingestion import IngestResponse

from maintai_ingestion.errors import EdgeFailure
from maintai_ingestion.responses import RequestIdentity, resolve_request_identity, to_http


def test_ack_omits_sdk_schema_version_but_preserves_location():
    body = {
        "schema_version": "1.3.0",
        "event_id": "e",
        "machine_id": "m",
        "ingestion_status": "ACCEPTED",
        "processing_status": "PENDING",
        "server_accepted_at": "2026-10-23T00:00:00Z",
        "status_url": "https://business.example.invalid/api/v1/sensor-events/e/status",
        "correlation_id": "cor",
    }
    response = to_http(
        IngestResponse(202, body, {"Location": body["status_url"]}), RequestIdentity("req", "cor")
    )
    wire = json.loads(response.body)
    assert "schema_version" not in wire
    assert wire["status_url"] == response.headers["location"]
    assert body["schema_version"] == "1.3.0"


def test_gateway_error_gets_message_and_nested_trace_ids():
    r = to_http(
        IngestResponse(
            422,
            {
                "error": {"code": "VALIDATION_FAILED", "retryable": False},
                "correlation_id": "cor",
                "rejection_id": "rej",
            },
            {},
        ),
        RequestIdentity("req", "cor"),
    )
    wire = json.loads(r.body)
    assert list(wire) == ["error"]
    assert wire["error"]["request_id"] == "req"
    assert wire["error"]["details"] == {"rejection_id": "rej"}
    assert wire["error"]["message"]


def test_missing_trace_ids_are_generated():
    trace = resolve_request_identity({})
    assert trace.request_id and trace.correlation_id
    assert trace.request_id != trace.correlation_id


def test_valid_trace_ids_are_preserved():
    assert resolve_request_identity(
        {"x-request-id": "req", "x-correlation-id": "cor"}
    ) == RequestIdentity("req", "cor")


@pytest.mark.parametrize("value", ["", "x" * 257, "a\nb", "a\x00b"])
def test_invalid_header_ids_are_not_echoed(value):
    with pytest.raises(EdgeFailure):
        resolve_request_identity({"x-request-id": value})
