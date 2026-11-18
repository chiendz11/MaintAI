"""Unit and contract tests for GET /api/v1/internal/runtime-windows."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta

import httpx
import psycopg
import pytest
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import default_contracts
from processing.shared.persistence.status import (
    InvalidWindowCursorError,
    StatusRepository,
    _decode_before_cursor,
    _encode_before_cursor,
)
from processing_read_api.app import create_app

from .test_status_api import StatusFixture, auth


def _runtime_window_validator():
    contracts = default_contracts()
    return Draft202012Validator(
        {
            "$ref": (contracts.root / "v1.3/schemas/runtime.schema.json").as_uri()
            + "#/$defs/RuntimeWindow"
        },
        registry=contracts.registry,
        format_checker=FormatChecker(),
    )


def _sample_db_row(
    *,
    event_id: str = "evt-1",
    machine_id: str = "M-001",
    accepted_at: datetime = datetime(2026, 11, 18, 10, 0, tzinfo=UTC),
    model_version: str = "logreg-baseline-v0",
    dataset_version: str = "ai4i2020-local-replay-v1",
):
    accepted_iso = accepted_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    sha = "a" * 64
    model_identity = {
        "model_version": model_version,
        "preprocessor_version": "prep-v1",
        "dataset_version": dataset_version,
        "dataset_manifest_sha256": sha,
        "split_sha256": sha,
        "mlflow_run_id": "run-1",
        "model_artifact_sha256": sha,
        "preprocessor_sha256": sha,
        "feature_schema_version": "1.3.0",
        "feature_schema_sha256": sha,
        "release_id": "rel-1",
        "inference_image_digest": f"ghcr.io/maintai/ml-service@sha256:{sha}",
    }
    features = {
        "product_type": "M",
        "air_temperature_k": 300.1,
        "process_temperature_k": 310.2,
        "rotational_speed_rpm": 1500,
        "torque_nm": 42.5,
        "tool_wear_min": 120,
    }
    outcome = {
        "schema_version": "1.4.0",
        "kind": "PREDICTION_COMPLETED",
        "result_id": f"res-{event_id}",
        "event_id": event_id,
        "machine_id": machine_id,
        "stream_epoch": "1",
        "sequence_number": 10,
        "source_schema_version": "1.3.0",
        "source_topic": "sensor.readings",
        "source_partition": 0,
        "source_offset": 10,
        "source_server_accepted_at": accepted_iso,
        "occurred_at": accepted_iso,
        "correlation_id": f"cor-{event_id}",
        "producer_git_sha": "d" * 40,
        "dq_ruleset_version": "1.0.0",
        "dq_ruleset_sha256": "c" * 64,
        "validation_status": "VALID",
        "processing_status": "COMPLETED",
        "prediction_id": f"pred-{event_id}",
        "prediction": "NORMAL",
        "failure_probability": 0.12,
        "model_threshold": 0.5,
        "risk_decision_id": f"risk-{event_id}",
        "risk_score": 0.12,
        "policy_version": "1.0.0",
        "policy_sha256": "b" * 64,
        "machine_state": "NORMAL",
        "completed_at": accepted_iso,
        **model_identity,
    }
    raw_outcome = json.dumps(outcome, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return {
        "event_id": event_id,
        "machine_id": machine_id,
        "stream_epoch": "1",
        "sequence_number": 10,
        "source_schema_version": "1.3.0",
        "kafka_topic": "sensor.readings",
        "kafka_partition": 0,
        "kafka_offset": 10,
        "source_server_accepted_at": accepted_at,
        "validation_status": "VALID",
        "processing_status": "COMPLETED",
        "raw_features_json": features,
        "raw_features_wire": None,
        "result_id": f"res-{event_id}",
        "kind": "PREDICTION_COMPLETED",
        "outcome": raw_outcome,
        "outcome_sha256": hashlib.sha256(raw_outcome).hexdigest(),
        "prediction_id": f"pred-{event_id}",
        "label": "NORMAL",
        "failure_probability": 0.12,
        "model_threshold": 0.5,
        "model_identity": model_identity,
        "risk_decision_id": f"risk-{event_id}",
        "risk_score": 0.12,
        "policy_version": "1.0.0",
        "policy_sha256": "b" * 64,
        "machine_state": "NORMAL",
    }


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        self.calls.append((sql, params))
        return _FakeResult(self._rows)


class _WindowStatusFixture(StatusFixture):
    def __init__(self, rows=None, **kwargs):
        super().__init__(**kwargs)
        self.rows = rows if rows is not None else [_sample_db_row()]
        self.calls = []

    def runtime_window(
        self,
        *,
        machines,
        model_version,
        dataset_version,
        window_start,
        window_end,
        before=None,
        limit=500,
    ):
        self.calls.append(
            {
                "machines": set(machines),
                "model_version": model_version,
                "dataset_version": dataset_version,
                "window_start": window_start,
                "window_end": window_end,
                "before": before,
                "limit": limit,
            }
        )
        if self.down:
            raise psycopg.OperationalError("db down")
        if before == "bad-cursor":
            raise InvalidWindowCursorError("INVALID_CURSOR")
        conn = _FakeConnection(self.rows)
        return StatusRepository(lambda: conn).runtime_window(
            machines=machines,
            model_version=model_version,
            dataset_version=dataset_version,
            window_start=window_start,
            window_end=window_end,
            before=before,
            limit=limit,
        )


def test_cursor_roundtrip_and_invalid_cursor_rejection():
    ts = datetime(2026, 11, 18, 10, 15, 30, 123456, tzinfo=UTC)
    token = _encode_before_cursor(ts, "evt-42")
    decoded_ts, decoded_id = _decode_before_cursor(token)
    assert decoded_ts == ts
    assert decoded_id == "evt-42"
    for bad in ("", "not-base64!", "e30", "a" * 1025):
        with pytest.raises(InvalidWindowCursorError):
            _decode_before_cursor(bad)


def test_repository_runtime_window_paginates_and_matches_schema():
    row_1 = _sample_db_row(
        event_id="evt-2",
        accepted_at=datetime(2026, 11, 18, 10, 5, tzinfo=UTC),
    )
    row_2 = _sample_db_row(
        event_id="evt-1",
        accepted_at=datetime(2026, 11, 18, 10, 0, tzinfo=UTC),
    )
    conn = _FakeConnection([row_1, row_2])
    repo = StatusRepository(lambda: conn)
    window = repo.runtime_window(
        machines=frozenset({"M-001"}),
        model_version="logreg-baseline-v0",
        dataset_version="ai4i2020-local-replay-v1",
        window_start=datetime(2026, 11, 18, 9, 0, tzinfo=UTC),
        window_end=datetime(2026, 11, 18, 11, 0, tzinfo=UTC),
        limit=1,
    )
    _runtime_window_validator().validate(window)
    assert len(window["items"]) == 1
    assert window["items"][0]["source_event_id"] == "evt-2"
    assert window["next_before"] is not None
    cursor_ts, cursor_id = _decode_before_cursor(window["next_before"])
    assert cursor_ts == datetime(2026, 11, 18, 10, 5, tzinfo=UTC)
    assert cursor_id == "evt-2"
    _, sql_params = conn.calls[0]
    assert sql_params[-1] == 2


@pytest.mark.parametrize("role", ["DRIFT_SERVICE", "BUSINESS_SERVICE"])
def test_http_runtime_windows_accepts_drift_and_business_roles(role):
    verifier, token = auth()
    fixture = _WindowStatusFixture()
    app = create_app(fixture, verifier)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/api/v1/internal/runtime-windows",
                params={
                    "model_version": "logreg-baseline-v0",
                    "dataset_version": "ai4i2020-local-replay-v1",
                    "window_start": "2026-11-18T09:00:00Z",
                    "window_end": "2026-11-18T11:00:00Z",
                },
                headers={"Authorization": token(roles=[role], machines=["M-001", "M-002"])},
            )
            assert response.status_code == 200, response.text
            body = response.json()
            _runtime_window_validator().validate(body)
            assert body["next_before"] is None
            assert body["items"][0]["outcome"]["kind"] == "PREDICTION_COMPLETED"
            assert fixture.calls[0]["machines"] == {"M-001", "M-002"}

    asyncio.run(run())


def test_http_runtime_windows_error_semantics():
    verifier, token = auth()
    fixture = _WindowStatusFixture(rows=[])
    app = create_app(fixture, verifier)
    base_params = {
        "model_version": "logreg-baseline-v0",
        "dataset_version": "ai4i2020-local-replay-v1",
        "window_start": "2026-11-18T09:00:00Z",
        "window_end": "2026-11-18T11:00:00Z",
    }

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            unauthorized = await client.get("/api/v1/internal/runtime-windows", params=base_params)
            assert unauthorized.status_code == 401
            forbidden = await client.get(
                "/api/v1/internal/runtime-windows",
                params=base_params,
                headers={"Authorization": token(roles=["VIEWER"])},
            )
            assert forbidden.status_code == 403
            assert forbidden.json()["error"]["code"] == "FORBIDDEN"

            bad_cursor = await client.get(
                "/api/v1/internal/runtime-windows",
                params={**base_params, "before": "bad-cursor"},
                headers={"Authorization": token(roles=["DRIFT_SERVICE"])},
            )
            assert bad_cursor.status_code == 400
            assert bad_cursor.json()["error"]["code"] == "INVALID_CURSOR"

            reversed_window = await client.get(
                "/api/v1/internal/runtime-windows",
                params={
                    **base_params,
                    "window_start": "2026-11-18T11:00:00Z",
                    "window_end": "2026-11-18T09:00:00Z",
                },
                headers={"Authorization": token(roles=["DRIFT_SERVICE"])},
            )
            assert reversed_window.status_code == 422
            assert reversed_window.json()["error"]["code"] == "VALIDATION_FAILED"

            naive_ts = await client.get(
                "/api/v1/internal/runtime-windows",
                params={**base_params, "window_start": "2026-11-18T09:00:00"},
                headers={"Authorization": token(roles=["DRIFT_SERVICE"])},
            )
            assert naive_ts.status_code == 422
            assert naive_ts.json()["error"]["code"] == "VALIDATION_FAILED"

            too_large = await client.get(
                "/api/v1/internal/runtime-windows",
                params={
                    **base_params,
                    "window_start": "2026-10-01T00:00:00Z",
                    "window_end": (
                        datetime(2026, 10, 1, tzinfo=UTC) + timedelta(days=32)
                    ).isoformat(),
                },
                headers={"Authorization": token(roles=["DRIFT_SERVICE"])},
            )
            assert too_large.status_code == 422
            assert too_large.json()["error"]["code"] == "QUERY_TOO_LARGE"

            down_app = create_app(_WindowStatusFixture(down=True), verifier)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=down_app), base_url="http://test"
            ) as down_client:
                dep_err = await down_client.get(
                    "/api/v1/internal/runtime-windows",
                    params=base_params,
                    headers={"Authorization": token(roles=["DRIFT_SERVICE"])},
                )
                assert dep_err.status_code == 503
                assert dep_err.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"

    asyncio.run(run())
