"""Contract checks for the B1 ingestion boundary with local dependency fixtures."""

import asyncio
import json
import unittest
from pathlib import Path

import httpx

from app.main import create_app
from app.schemas.sensor import SensorReadingRequest
from app.schemas.sensor import ValidationIssue
from app.services.data_quality import DataQualityValidator, ValidationResult
from app.services.sensor_ingestion import (
    MLTimeout,
    MLUnavailable,
    SensorIngestionService,
    StoredEvent,
)


EXAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "examples"
SENSOR_EVENT = json.loads((EXAMPLES / "backend" / "sensor-event.json").read_text())


class MemoryStore:
    """Fixture implementing the storage boundary, including per-event exclusion."""

    def __init__(self):
        self.machines = {"M-001"}
        self.events: dict[str, StoredEvent] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.record_calls = 0

    def lock(self, event_id: str):
        return self.locks.setdefault(event_id, asyncio.Lock())

    async def get(self, event_id: str):
        return self.events.get(event_id)

    async def machine_exists(self, machine_id: str):
        return machine_id in self.machines

    async def record(self, event: StoredEvent):
        self.record_calls += 1
        self.events[event.event_id] = event

    async def save_outcome(self, event_id: str, outcome):
        self.events[event_id].outcome = outcome


class CompletedProcessor:
    def __init__(self, store: MemoryStore):
        self.store = store
        self.calls = 0
        self.features = None
        self.recorded_before_call = False

    async def process(self, event: SensorReadingRequest, correlation_id: str):
        self.calls += 1
        stored = self.store.events.get(event.event_id)
        self.recorded_before_call = stored is not None and stored.validation_status == "VALID"
        self.features = event.features.model_dump()
        return {
            "event_id": event.event_id,
            "machine_id": event.machine_id,
            "validation_status": "VALID",
            "processing_status": "COMPLETED",
            "machine_state": "HIGH",
            "prediction_id": "pred_001",
            "alert_id": None,
            "correlation_id": correlation_id,
        }


class UnavailableProcessor:
    def __init__(self):
        self.calls = 0

    async def process(self, event: SensorReadingRequest, correlation_id: str):
        self.calls += 1
        raise MLUnavailable("ML inference service is unavailable")


class TimeoutProcessor:
    async def process(self, event: SensorReadingRequest, correlation_id: str):
        raise MLTimeout("ML inference timed out")


class RejectingValidator(DataQualityValidator):
    def validate(self, event: SensorReadingRequest):
        return ValidationResult(
            "INVALID",
            "sensor-validator-policy-1.0.0",
            (ValidationIssue(field="features.torque_nm", code="OUT_OF_RANGE", message="Torque exceeds policy"),),
        )


class SensorIngestionContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.processor = CompletedProcessor(self.store)
        self.service = SensorIngestionService(self.store, self.processor)
        self.app = create_app(self.service)

    async def post(self, payload, *, headers=None):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            return await client.post("/api/v1/sensor-readings", json=payload, headers=headers)

    async def test_valid_event_is_recorded_before_processing_and_returns_contract_response(self):
        response = await self.post(
            SENSOR_EVENT,
            headers={"X-Request-ID": "req_client", "X-Correlation-ID": "cor_client"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "event_id": "evt_001",
            "machine_id": "M-001",
            "validation_status": "VALID",
            "processing_status": "COMPLETED",
            "machine_state": "HIGH",
            "prediction_id": "pred_001",
            "alert_id": None,
            "correlation_id": "cor_client",
        })
        self.assertEqual(response.headers["X-Correlation-ID"], "cor_client")
        self.assertEqual(response.headers["X-Request-ID"], "req_client")
        self.assertEqual(self.store.events["evt_001"].validation_status, "VALID")
        self.assertTrue(self.processor.recorded_before_call)
        self.assertEqual(self.processor.calls, 1)
        self.assertEqual(set(self.processor.features), {
            "product_type", "air_temperature_k", "process_temperature_k",
            "rotational_speed_rpm", "torque_nm", "tool_wear_min",
        })

    async def test_duplicate_payload_returns_saved_outcome_without_second_processing(self):
        headers = {"X-Correlation-ID": "cor_retry"}
        first = await self.post(SENSOR_EVENT, headers=headers)
        second = await self.post(SENSOR_EVENT, headers=headers)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json(), first.json())
        self.assertEqual(first.json()["correlation_id"], first.headers["X-Correlation-ID"])
        self.assertTrue(first.headers["X-Request-ID"].startswith("req_"))
        self.assertEqual(self.processor.calls, 1)

    async def test_duplicate_uses_current_trace_ids_without_changing_saved_outcome(self):
        first = await self.post(SENSOR_EVENT, headers={
            "X-Request-ID": "req_first", "X-Correlation-ID": "cor_first",
        })
        second = await self.post(SENSOR_EVENT, headers={
            "X-Request-ID": "req_second", "X-Correlation-ID": "cor_second",
        })

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["correlation_id"], second.headers["X-Correlation-ID"])
        self.assertEqual(second.json()["correlation_id"], "cor_second")
        self.assertEqual(self.store.events["evt_001"].outcome.body["correlation_id"], "cor_first")
        self.assertEqual(self.processor.calls, 1)

    async def test_duplicate_dependency_error_uses_current_trace_ids(self):
        self.processor = UnavailableProcessor()
        self.app = create_app(SensorIngestionService(self.store, self.processor))
        await self.post(SENSOR_EVENT, headers={
            "X-Request-ID": "req_first", "X-Correlation-ID": "cor_first",
        })
        second = await self.post(SENSOR_EVENT, headers={
            "X-Request-ID": "req_second", "X-Correlation-ID": "cor_second",
        })

        self.assertEqual(second.status_code, 503)
        self.assertEqual(second.json()["correlation_id"], second.headers["X-Correlation-ID"])
        self.assertEqual(second.json()["error"]["correlation_id"], "cor_second")
        self.assertEqual(second.json()["error"]["request_id"], second.headers["X-Request-ID"])
        self.assertEqual(self.store.events["evt_001"].outcome.body["error"]["request_id"], "req_first")
        self.assertEqual(self.processor.calls, 1)

    async def test_conflicting_duplicate_returns_409_without_second_processing(self):
        await self.post(SENSOR_EVENT)
        changed = json.loads(json.dumps(SENSOR_EVENT))
        changed["features"]["torque_nm"] = 50.0
        response = await self.post(changed)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "EVENT_CONFLICT")
        self.assertEqual(self.processor.calls, 1)

    async def test_invalid_feature_is_saved_and_never_reaches_processor(self):
        invalid = json.loads(json.dumps(SENSOR_EVENT))
        invalid["features"]["rotational_speed_rpm"] = 0
        response = await self.post(invalid)

        self.assertEqual(response.status_code, 422)
        body = response.json()
        self.assertEqual(body["validation_status"], "INVALID")
        self.assertEqual(body["machine_state"], "INVALID")
        self.assertEqual(body["errors"][0]["field"], "features.rotational_speed_rpm")
        self.assertIsNone(body["prediction_id"])
        self.assertEqual(self.store.events["evt_001"].validation_status, "INVALID")
        self.assertEqual(self.processor.calls, 0)

    async def test_numeric_timestamp_is_rejected_as_invalid_sensor_data(self):
        invalid = {**SENSOR_EVENT, "received_at": 1791779415}
        response = await self.post(invalid)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["validation_status"], "INVALID")
        self.assertEqual(response.json()["errors"][0]["field"], "received_at")
        self.assertEqual(self.processor.calls, 0)

    async def test_repeated_invalid_event_uses_saved_outcome(self):
        invalid = json.loads(json.dumps(SENSOR_EVENT))
        invalid["features"]["rotational_speed_rpm"] = 0
        first = await self.post(invalid, headers={"X-Correlation-ID": "cor_retry"})
        second = await self.post(invalid, headers={"X-Correlation-ID": "cor_retry"})

        self.assertEqual(first.status_code, 422)
        self.assertEqual(second.json(), first.json())
        self.assertEqual(self.store.record_calls, 1)
        self.assertEqual(self.processor.calls, 0)

    async def test_repeated_invalid_event_uses_current_correlation_id(self):
        invalid = json.loads(json.dumps(SENSOR_EVENT))
        invalid["features"]["rotational_speed_rpm"] = 0
        await self.post(invalid, headers={"X-Correlation-ID": "cor_first"})
        second = await self.post(invalid, headers={"X-Correlation-ID": "cor_second"})

        self.assertEqual(second.status_code, 422)
        self.assertEqual(second.json()["correlation_id"], second.headers["X-Correlation-ID"])
        self.assertEqual(self.store.events["evt_001"].outcome.body["correlation_id"], "cor_first")
        self.assertEqual(self.store.record_calls, 1)

    async def test_validator_rejection_does_not_reach_processor(self):
        self.app = create_app(
            SensorIngestionService(self.store, self.processor, RejectingValidator())
        )
        response = await self.post(SENSOR_EVENT)

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["errors"][0]["field"], "features.torque_nm")
        self.assertEqual(self.processor.calls, 0)
        self.assertEqual(
            self.store.events["evt_001"].validator_version,
            "sensor-validator-policy-1.0.0",
        )

    async def test_missing_identity_returns_validation_envelope_without_recording(self):
        invalid = {key: value for key, value in SENSOR_EVENT.items() if key != "event_id"}
        response = await self.post(invalid, headers={"X-Correlation-ID": "cor_missing"})

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"]["code"], "VALIDATION_FAILED")
        self.assertEqual(response.json()["error"]["correlation_id"], "cor_missing")
        self.assertFalse(self.store.events)
        self.assertEqual(self.processor.calls, 0)

    async def test_malformed_json_returns_400_without_recording(self):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/sensor-readings", content=b'{"event_id":',
                headers={"Content-Type": "application/json"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")
        self.assertFalse(self.store.events)

    async def test_non_json_content_type_returns_400_without_recording(self):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/sensor-readings", content=json.dumps(SENSOR_EVENT),
                headers={"Content-Type": "text/plain"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")
        self.assertFalse(self.store.events)

    async def test_unknown_machine_returns_404_without_recording(self):
        event = {**SENSOR_EVENT, "machine_id": "M-unknown"}
        response = await self.post(event)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "MACHINE_NOT_FOUND")
        self.assertFalse(self.store.events)

    async def test_ml_unavailable_is_saved_as_unknown_and_duplicate_does_not_retry(self):
        self.processor = UnavailableProcessor()
        self.app = create_app(SensorIngestionService(self.store, self.processor))
        headers = {"X-Request-ID": "req_retry", "X-Correlation-ID": "cor_retry"}
        first = await self.post(SENSOR_EVENT, headers=headers)
        second = await self.post(SENSOR_EVENT, headers=headers)

        self.assertEqual(first.status_code, 503)
        self.assertEqual(first.json()["error"]["code"], "ML_UNAVAILABLE")
        self.assertEqual(first.json()["machine_state"], "UNKNOWN")
        self.assertIsNone(first.json()["prediction_id"])
        self.assertEqual(second.json(), first.json())
        self.assertEqual(self.processor.calls, 1)

    async def test_ml_timeout_returns_504_unknown_without_prediction(self):
        self.app = create_app(SensorIngestionService(self.store, TimeoutProcessor()))
        response = await self.post(SENSOR_EVENT)

        self.assertEqual(response.status_code, 504)
        self.assertEqual(response.json()["error"]["code"], "ML_TIMEOUT")
        self.assertEqual(response.json()["machine_state"], "UNKNOWN")
        self.assertIsNone(response.json()["prediction_id"])
        self.assertIsNone(response.json()["alert_id"])


if __name__ == "__main__":
    unittest.main()
