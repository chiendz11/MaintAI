from datetime import UTC, timedelta
from urllib.parse import parse_qs

import httpx
import pytest
from pydantic import ValidationError

from drift_monitor.adapters.processing_api import ProcessingApiError, ProcessingApiWindowSource
from drift_monitor.config import DriftConfig
from drift_monitor.models import DriftStatus
from drift_monitor.worker import run_worker


def _history_item(sample, *, model_version="fixture-model-v1", dataset_version="runtime-ds-v1"):
    accepted = sample.received_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": "1.0.0",
        "result_id": f"res-{sample.event_id}",
        "source_event_id": sample.event_id,
        "machine_id": "M-001",
        "source_server_accepted_at": accepted,
        "features_raw": sample.features.model_dump(mode="json"),
        "raw_ref": None,
        "risk_snapshot": {
            "risk_decision_id": f"risk-{sample.event_id}",
            "machine_state": "NORMAL",
            "policy_version": "1.0.0",
            "risk_score": 0.1,
        },
        "outcome": {
            "schema_version": "1.4.0",
            "kind": "PREDICTION_COMPLETED",
            "validation_status": "VALID",
            "processing_status": "COMPLETED",
            "prediction": sample.prediction_label or "NORMAL",
            "model_version": model_version,
            "dataset_version": dataset_version,
        },
    }


def _runtime_window(items, *, next_before=None):
    return {
        "items": items,
        "model_version": "fixture-model-v1",
        "dataset_version": "runtime-ds-v1",
        "window_start": "2026-10-11T00:00:00Z",
        "window_end": "2026-10-12T01:00:00Z",
        "snapshot_at": "2026-10-12T01:00:00Z",
        "next_before": next_before,
        "snapshot_id": "rw_test123",
    }


def test_config_rejects_partial_oidc_and_loads_processing_env(config, tmp_path, monkeypatch):
    with pytest.raises(ValidationError):
        DriftConfig(
            reference_path=config.reference_path,
            model_version="fixture-model-v1",
            dataset_version="fixture-ai4i-v1",
            processing_read_url="https://processing-read-api.internal",
        )
    secret_file = tmp_path / "client-secret"
    secret_file.write_text("super-secret-value")
    with pytest.raises(ValidationError):
        DriftConfig(
            reference_path=config.reference_path,
            model_version="fixture-model-v1",
            dataset_version="fixture-ai4i-v1",
            processing_read_url="https://processing-read-api.internal",
            oidc_token_url="https://idp.internal/token",
            oidc_client_id="maintai-processing",
        )

    monkeypatch.setenv("DRIFT_MODEL_VERSION", "fixture-model-v1")
    monkeypatch.setenv("DRIFT_DATASET_VERSION", "fixture-ai4i-v1")
    monkeypatch.setenv("DRIFT_REFERENCE_PATH", str(config.reference_path))
    monkeypatch.setenv("DRIFT_PROCESSING_READ_URL", "https://processing-read-api.internal")
    monkeypatch.setenv("DRIFT_RUNTIME_DATASET_VERSION", "runtime-ds-v1")
    monkeypatch.setenv("DRIFT_WINDOW_LOOKBACK_HOURS", "12")
    monkeypatch.setenv("DRIFT_OIDC_TOKEN_URL", "https://idp.internal/token")
    monkeypatch.setenv("DRIFT_OIDC_CLIENT_ID", "maintai-processing")
    monkeypatch.setenv("DRIFT_OIDC_CLIENT_SECRET_FILE", str(secret_file))

    loaded = DriftConfig.load(None)
    assert loaded.processing_read_url == "https://processing-read-api.internal"
    assert loaded.runtime_dataset_version == "runtime-ds-v1"
    assert loaded.window_lookback_hours == 12.0


def test_processing_api_paginates_oidc_and_sorts_samples(config, samples, tmp_path):
    secret_file = tmp_path / "client-secret"
    secret_file.write_text("client-secret-123\n")
    cfg = config.model_copy(
        update={
            "processing_read_url": "https://processing-read-api.internal",
            "runtime_dataset_version": "runtime-ds-v1",
            "window_size": 3,
            "minimum_samples": 1,
            "oidc_token_url": "https://keycloak.internal/token",
            "oidc_client_id": "maintai-processing",
            "oidc_client_secret_file": secret_file,
        }
    )

    recorded_requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            body = parse_qs(request.content.decode("utf-8"))
            assert body == {
                "grant_type": ["client_credentials"],
                "client_id": ["maintai-processing"],
                "client_secret": ["client-secret-123"],
            }
            return httpx.Response(200, json={"access_token": "jwt-token-xyz"})

        assert request.headers["Authorization"] == "Bearer jwt-token-xyz"
        assert request.url.path == "/api/v1/internal/runtime-windows"
        recorded_requests.append(dict(request.url.params))
        if "before" not in request.url.params:
            return httpx.Response(
                200,
                json=_runtime_window(
                    [
                        _history_item(samples[2]),
                        _history_item(samples[1]),
                    ],
                    next_before="cursor-page-2",
                ),
            )
        assert request.url.params["before"] == "cursor-page-2"
        assert request.url.params["limit"] == "1"
        return httpx.Response(200, json=_runtime_window([_history_item(samples[0])]))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = ProcessingApiWindowSource(cfg, client=client).load_window(
            model_version=cfg.model_version,
            dataset_version="runtime-ds-v1",
            window_size=3,
            window_end=samples[2].received_at + timedelta(seconds=1),
        )

    assert [row.event_id for row in rows] == [
        samples[0].event_id,
        samples[1].event_id,
        samples[2].event_id,
    ]
    assert len(recorded_requests) == 2
    assert recorded_requests[0]["dataset_version"] == "runtime-ds-v1"
    assert recorded_requests[0]["limit"] == "3"


def test_processing_api_rejects_repeated_cursor_and_token_file(config, samples, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("static-jwt-token\n")
    cfg = config.model_copy(
        update={
            "processing_read_url": "https://processing-read-api.internal",
            "processing_token_file": token_file,
            "window_size": 3,
            "minimum_samples": 1,
        }
    )

    def repeated_cursor_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer static-jwt-token"
        return httpx.Response(
            200,
            json=_runtime_window([_history_item(samples[0])], next_before="stuck-cursor"),
        )

    with httpx.Client(transport=httpx.MockTransport(repeated_cursor_handler)) as client:
        with pytest.raises(ProcessingApiError, match="Processing runtime window"):
            ProcessingApiWindowSource(cfg, client=client).load_window(
                model_version=cfg.model_version,
                dataset_version=cfg.dataset_version,
                window_size=3,
                window_end=samples[0].received_at + timedelta(seconds=1),
            )


def test_run_worker_records_error_report_when_processing_api_fails(config, tmp_path, monkeypatch):
    token_file = tmp_path / "token"
    token_file.write_text("secret-token-should-not-leak")
    cfg = config.model_copy(
        update={
            "processing_read_url": "https://processing-read-api.internal",
            "processing_token_file": token_file,
        }
    )

    class MemoryRepo:
        def __init__(self):
            self.reports = {}
            self.checkpointed = False

        def load_checkpoint(self, worker_name):
            return None

        def load_window(self, size, model_version):
            raise AssertionError(
                "DB load_window must not be called when processing_read_url is set"
            )

        def find_report(self, report_id):
            return self.reports.get(report_id)

        def save_report(self, report):
            self.reports[report.report_id] = report
            return report

        def checkpoint(self, worker_name, report, latest, now):
            self.checkpointed = True

        def counts(self):
            return {"ERROR": len(self.reports)}

    def fail_load(self, **kwargs):
        raise ProcessingApiError("PROCESSING_API_UNAVAILABLE")

    monkeypatch.setattr(ProcessingApiWindowSource, "load_window", fail_load)
    repo = MemoryRepo()
    result = run_worker(cfg, repo)
    assert result.exit_code == 1
    assert result.report.overall_status == DriftStatus.ERROR
    assert result.report.reason == "PROCESSING_API_UNAVAILABLE"
    assert repo.checkpointed is False
    assert len(repo.reports) == 1
