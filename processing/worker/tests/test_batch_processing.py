"""Unit tests for Processing batch inference & prepare_batch micro-batching."""

import asyncio
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import httpx
from consumer.handler import ProcessingHandler
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.ordering import Assignment, WorkContext
from maintai_messaging.outcomes import ModelIdentity
from processing.shared.data_quality.machine_registry import MachineLookupResult
from processing.shared.data_quality.validator import FeatureValidator
from processing.shared.inference_client.client import (
    InferenceClient,
    PredictionAttempt,
    PredictionResponse,
)
from processing.shared.risk_engine.policy import RiskPolicy

ROOT = Path(__file__).resolve().parents[3]


def _validator():
    return FeatureValidator.from_file(
        ROOT / "contracts/v1.3/schemas/features.schema.json",
        version="sensor-validator-1.0.0",
    )


def test_inference_client_predict_batch_and_fallback():
    features = {
        "product_type": "M",
        "air_temperature_k": 298.1,
        "process_temperature_k": 308.6,
        "rotational_speed_rpm": 1551,
        "torque_nm": 42.8,
        "tool_wear_min": 108,
    }
    items = [
        {"event_id": "e-1", "machine_id": "M-1", "features": features, "correlation_id": "c-1"},
        {"event_id": "e-2", "machine_id": "M-1", "features": features, "correlation_id": "c-2"},
    ]

    def batch_handler(request):
        assert str(request.url).endswith("/api/v1/predict/batch")
        payload = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "prediction_id": f"pred-{item['event_id']}",
                        "event_id": item["event_id"],
                        "prediction": "NORMAL",
                        "failure_probability": 0.1,
                        "model_version": "m1",
                        "preprocessor_version": "p1",
                        "model_threshold": 0.5,
                        "inference_ms": 1.2,
                        "predicted_at": "2026-10-24T00:00:00Z",
                    }
                    for item in payload["items"]
                ]
            },
        )

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(batch_handler)) as http:
            client = InferenceClient(
                http,
                base_url="http://127.0.0.1:8001",
                feature_validator=_validator(),
                local_fixture=True,
            )
            return await client.predict_batch(items)

    res = asyncio.run(run())
    assert [r.response.event_id for r in res] == ["e-1", "e-2"]

    def fail_handler(_request):
        return httpx.Response(503)

    async def run_fail():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fail_handler)) as http:
            client = InferenceClient(
                http,
                base_url="http://127.0.0.1:8001",
                feature_validator=_validator(),
                local_fixture=True,
            )
            return await client.predict_batch(items)

    fail_res = asyncio.run(run_fail())
    assert all(r.response is None and r.failure_code == "ML_UNAVAILABLE" for r in fail_res)


def test_processing_handler_prepare_batch_preserves_order_and_skips_invalid():
    contracts = default_contracts()
    base = json.loads((contracts.directory / "examples/sensor-reading.json").read_text())

    valid_1 = contracts.decode(
        "sensor.readings", canonical_json({**base, "event_id": "e1", "sequence_number": 1})
    )
    bad = {**base, "event_id": "e2", "sequence_number": 2}
    bad["features"] = {**bad["features"], "rotational_speed_rpm": -5}
    invalid_2 = contracts.decode("sensor.readings", canonical_json(bad))
    valid_3 = contracts.decode(
        "sensor.readings", canonical_json({**base, "event_id": "e3", "sequence_number": 3})
    )
    items = [
        (ev, Record(ev.topic, 0, i, ev.key.encode(), ev.wire_bytes))
        for i, ev in enumerate([valid_1, invalid_2, valid_3])
    ]

    class FakeConn:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return None

    @contextmanager
    def conn_factory():
        yield FakeConn()

    registry_calls = []
    batch_calls = []
    completed = json.loads((contracts.directory / "examples/completed.json").read_text())
    identity = ModelIdentity(
        **{field: completed[field] for field in ModelIdentity.__dataclass_fields__}
    )
    policy = RiskPolicy.from_file(ROOT / "processing/config/risk-policies/demo-risk-v1.json")

    def fake_lookup(machine_id, **_kw):
        registry_calls.append(machine_id)
        return MachineLookupResult("FOUND", machine_id)

    def fake_predict_batch(ml_reqs):
        batch_calls.append([r["event_id"] for r in ml_reqs])
        return [
            PredictionAttempt(
                PredictionResponse(
                    prediction_id=f"pred-{r['event_id']}",
                    event_id=r["event_id"],
                    prediction="NORMAL",
                    failure_probability=0.05,
                    model_version=identity.model_version,
                    preprocessor_version=identity.preprocessor_version,
                    model_threshold=0.5,
                    inference_ms=0.8,
                    predicted_at=datetime.fromisoformat(completed["completed_at"]),
                ),
                None,
                1,
            )
            for r in ml_reqs
        ]

    handler = ProcessingHandler(
        conn_factory,
        consumer_group="maintai-processing-v1",
        validator=_validator(),
        registry_lookup=fake_lookup,
        predict=lambda **_kw: None,
        predict_batch=fake_predict_batch,
        identity_resolver=lambda _r: identity,
        policy=policy,
        producer_git_sha="c" * 40,
        contracts=contracts,
    )
    ctx = WorkContext(Assignment("maintai-processing-v1", "sensor.readings", 0, "owner", 1), 10**12)
    prepared = handler.prepare_batch(items, ctx)
    assert len(registry_calls) == 1
    assert batch_calls == [["e1", "e3"]]
    assert [p.outcome["kind"] for p in prepared] == [
        "PREDICTION_COMPLETED",
        "DATA_INVALID",
        "PREDICTION_COMPLETED",
    ]
