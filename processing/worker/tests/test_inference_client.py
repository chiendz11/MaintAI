"""Baseline HTTP contract tests; the ML service below is a fixture only."""

import asyncio
import gzip
import json
from pathlib import Path

import httpx
import pytest
from processing.shared.data_quality.validator import FeatureValidator
from processing.shared.inference_client.client import InferenceClient

ROOT = Path(__file__).resolve().parents[3]
FEATURES = {
    "product_type": "M",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551,
    "torque_nm": 42.8,
    "tool_wear_min": 108,
}
RESPONSE = {
    "prediction_id": "fixture-pred-1",
    "event_id": "evt-1",
    "prediction": "FAILURE",
    "failure_probability": 0.91,
    "model_version": "fixture-model-1",
    "preprocessor_version": "fixture-preprocessor-1",
    "model_threshold": 0.63,
    "inference_ms": 18,
    "predicted_at": "2026-10-24T00:00:00Z",
}


def run_request(handler, *, features=None, **kwargs):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            validator = FeatureValidator.from_file(
                ROOT / "contracts/v1.3/schemas/features.schema.json",
                version="sensor-validator-1.0.0",
            )
            client = InferenceClient(
                http,
                base_url="http://127.0.0.1:8001",
                feature_validator=validator,
                local_fixture=True,
                **kwargs,
            )
            return await client.predict(
                event_id="evt-1",
                machine_id="M-001",
                features=FEATURES if features is None else features,
                correlation_id="corr-1",
            )

    return asyncio.run(run())


def test_request_preserves_raw_features_and_correlates_response():
    def handler(request):
        assert request.method == "POST"
        assert str(request.url) == "http://127.0.0.1:8001/api/v1/predict"
        assert request.headers["X-Correlation-ID"] == "corr-1"
        assert json.loads(request.content) == {
            "event_id": "evt-1",
            "machine_id": "M-001",
            "features": FEATURES,
        }
        return httpx.Response(200, json=RESPONSE)

    result = run_request(handler)
    assert result.response.prediction == "FAILURE"
    assert result.response.failure_probability == 0.91
    assert result.failure_code is None
    assert result.attempts == 1


def test_invalid_features_do_not_reach_ml():
    def handler(request):
        pytest.fail("INVALID features reached ML")

    with pytest.raises(ValueError):
        run_request(handler, features={**FEATURES, "torque_nm": True})


@pytest.mark.parametrize(
    "status,code",
    [
        (400, "ML_REQUEST_REJECTED"),
        (401, "ML_AUTH_FAILED"),
        (403, "ML_AUTH_FAILED"),
        (422, "ML_REQUEST_REJECTED"),
    ],
)
def test_nontransient_http_errors_are_not_retried(status, code):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, json={"error": {"message": "secret-provider-detail"}})

    result = run_request(handler)
    assert result.response is None
    assert result.failure_code == code
    assert result.attempts == len(calls) == 1
    assert "secret-provider-detail" not in repr(result)


def test_transient_failure_has_only_one_retry_and_can_recover():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) == 1 else 200, json=RESPONSE)

    result = run_request(handler)
    assert result.response.prediction_id == "fixture-pred-1"
    assert result.attempts == len(calls) == 2


@pytest.mark.parametrize("status", [502, 503, 504])
def test_exhausted_transient_failure_has_no_prediction(status):
    result = run_request(lambda request: httpx.Response(status))
    assert result.response is None
    assert result.failure_code == "ML_UNAVAILABLE"
    assert result.attempts == 2


def test_connection_failure_is_bounded():
    def handler(request):
        raise httpx.ConnectError("credential-must-not-leak", request=request)

    result = run_request(handler)
    assert result.response is None
    assert result.failure_code == "ML_CONNECTION_FAILED"
    assert result.attempts == 2
    assert "credential-must-not-leak" not in repr(result)


def test_entire_http_attempt_has_a_deadline_even_without_socket_timeout():
    async def handler(request):
        await asyncio.sleep(10)
        return httpx.Response(200, json=RESPONSE)

    result = run_request(handler, timeout_seconds=0.02)
    assert result.failure_code == "ML_TIMEOUT"
    assert result.response is None
    assert result.attempts == 2


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "wrong-event"},
        {"prediction": "NORMAL"},
        {"failure_probability": True},
        {"failure_probability": "0.91"},
        {"failure_probability": 1.01},
        {"model_threshold": -1},
        {"inference_ms": -1},
        {"predicted_at": "2026-10-24T00:00:00"},
        {"model_version": ""},
        {"extra_lineage": "not-in-baseline"},
    ],
)
def test_invalid_response_never_becomes_success_or_normal(change):
    result = run_request(lambda request: httpx.Response(200, json={**RESPONSE, **change}))
    assert result.response is None
    assert result.failure_code == "ML_INVALID_RESPONSE"
    assert result.attempts == 1


@pytest.mark.parametrize(
    "raw",
    [
        b'{"failure_probability": NaN}',
        b'{"event_id":"evt-1","event_id":"other"}',
        b"not JSON",
    ],
)
def test_malformed_and_duplicate_key_response_is_not_retried(raw):
    result = run_request(lambda request: httpx.Response(200, content=raw))
    assert result.response is None
    assert result.failure_code == "ML_INVALID_RESPONSE"
    assert result.attempts == 1


def test_threshold_equality_is_failure():
    result = run_request(
        lambda request: httpx.Response(
            200,
            json={
                **RESPONSE,
                "failure_probability": 0.63,
            },
        )
    )
    assert result.response.prediction == "FAILURE"


def test_oversized_response_is_bounded_without_retry():
    result = run_request(
        lambda request: httpx.Response(200, content=b"x" * 1000), max_response_bytes=100
    )
    assert result.failure_code == "ML_INVALID_RESPONSE"
    assert result.attempts == 1


@pytest.mark.parametrize(
    "compressed",
    [b"not-gzip", gzip.compress(b"x" * (8 * 1024 * 1024))],
    ids=["malformed", "expansion"],
)
def test_encoded_response_is_rejected_without_consuming_or_decompressing_body(compressed):
    class CountingStream(httpx.AsyncByteStream):
        reads = 0

        async def __aiter__(self):
            self.reads += 1
            yield compressed

    stream = CountingStream()
    result = run_request(
        lambda request: httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=stream)
    )
    assert result.failure_code == "ML_INVALID_RESPONSE"
    assert result.attempts == 1
    assert stream.reads == 0
