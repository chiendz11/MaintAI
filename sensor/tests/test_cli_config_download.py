import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile

import pytest
from pydantic import ValidationError

from simulator import download
from simulator.cli import main
from simulator.config import SimulatorConfig


def clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("SIM_") and key != "BACKEND_BASE_URL"
    }


def test_environment_config_and_cli_override(monkeypatch):
    monkeypatch.setenv("SIM_SPEED", "4")
    monkeypatch.setenv("SIM_SEED", "13")
    monkeypatch.setenv("SIM_LOOP_DATASET", "true")
    assert SimulatorConfig.from_env().speed == 4
    assert SimulatorConfig.from_env().loop_dataset is True
    assert SimulatorConfig.from_env({"speed": 8, "seed": None}).seed == 13
    assert SimulatorConfig.from_env({"speed": 8}).speed == 8


@pytest.mark.parametrize(
    "value",
    [
        {"speed": 0},
        {"speed": float("inf")},
        {"interval_ms": -1},
        {"machine_count": 0},
        {"limit": 0},
        {"max_retries": -1},
        {"timeout_seconds": 0},
        {"start_time": "2026-10-12"},
        {"backend_base_url": "ftp://backend"},
        {"backend_base_url": "http://user:secret@backend"},
    ],
)
def test_bad_configuration_fails_before_delivery(value):
    with pytest.raises(ValidationError):
        SimulatorConfig(**value)


def test_cli_config_error_does_not_echo_credentials(capsys):
    assert main(["replay", "--backend-url", "http://user:secret@backend"]) == 2
    captured = capsys.readouterr()
    assert "configuration_error" in captured.err
    assert "secret" not in captured.err


def test_cli_dry_run_and_json_logs(sample_csv, tmp_path, validator):
    output, metrics = tmp_path / "events.jsonl", tmp_path / "metrics.prom"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator",
            "replay",
            "--dataset",
            str(sample_csv),
            "--dry-run",
            "--limit",
            "2",
            "--output",
            str(output),
            "--metrics-file",
            str(metrics),
            "--run-id",
            "cli-test",
            "--start-time",
            "2026-10-12T04:30:15Z",
        ],
        capture_output=True,
        text=True,
        env=clean_env(),
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    events = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(events) == 2
    for event in events:
        validator.validate(event)
    logs = [json.loads(line) for line in result.stderr.splitlines()]
    assert logs[-1]["event"] == "replay_completed"
    assert logs[-1]["generated"] == 2
    assert "maintai_simulator_generated_total 2" in metrics.read_text()


@pytest.mark.parametrize(
    "fault,expected_requests,expected_predictions",
    [
        ("none", 2, 2),
        ("missing_field", 2, 0),
        ("duplicate_event", 4, 2),
    ],
)
def test_cli_against_real_mock_http_backend(
    sample_csv,
    http_backend,
    fault,
    expected_requests,
    expected_predictions,
):
    url, backend = http_backend
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator",
            "replay",
            "--dataset",
            str(sample_csv),
            "--backend-url",
            url,
            "--limit",
            "2",
            "--interval-ms",
            "0",
            "--fault-mode",
            fault,
        ],
        capture_output=True,
        text=True,
        env=clean_env(),
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert len(backend.requests) == expected_requests
    assert backend.predictions == expected_predictions
    deliveries = [
        json.loads(line)
        for line in result.stderr.splitlines()
        if json.loads(line)["event"] == "sensor_delivery_attempt"
    ]
    assert all(
        {
            "event_id",
            "machine_id",
            "request_id",
            "correlation_id",
            "status_code",
            "latency_ms",
            "error",
        }
        <= set(log)
        for log in deliveries
    )


def test_download_verifies_checksum_and_preserves_existing_file(tmp_path, monkeypatch):
    data = b"fixture dataset"
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as writer:
        writer.writestr("ai4i2020.csv", data)
    monkeypatch.setattr(download, "DATASET_SHA256", hashlib.sha256(data).hexdigest())
    monkeypatch.setattr(
        download.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(archive.getvalue())
    )
    path = tmp_path / "ai4i2020.csv"
    assert download.download_dataset(path).read_bytes() == data
    path.write_bytes(b"different existing data")
    with pytest.raises(ValueError, match="Existing dataset checksum"):
        download.download_dataset(path)
    assert path.read_bytes() == b"different existing data"
    assert download.download_dataset(path, force=True).read_bytes() == data
    monkeypatch.setattr(download, "DATASET_SHA256", "invalid")
    with pytest.raises(ValueError, match="checksum mismatch"):
        download.download_dataset(path, force=True)
    assert path.read_bytes() == data
