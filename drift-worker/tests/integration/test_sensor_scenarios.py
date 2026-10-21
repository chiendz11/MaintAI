import json
import subprocess
import sys
from pathlib import Path

import pytest

from drift_monitor.config import DriftConfig
from drift_monitor.data_loader import load_jsonl
from drift_monitor.reference import build_reference
from drift_monitor.report import evaluate


@pytest.mark.parametrize(
    "scenario,expected",
    [
        ("normal", "STABLE"),
        ("sudden-drift", "CRITICAL"),
        ("gradual-drift", "CRITICAL"),
        ("categorical-drift", "WARNING"),
    ],
)
def test_real_sensor_jsonl_to_worker(scenario, expected, tmp_path):
    root = Path(__file__).resolve().parents[3]
    output = tmp_path / "runtime.jsonl"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "simulator",
            "replay",
            "--dataset",
            str(root / "sensor/tests/fixtures/ai4i_sample.csv"),
            "--loop-dataset",
            "--limit",
            "1000",
            "--scenario",
            scenario,
            "--scenario-path",
            str(root / "sensor/scenarios"),
            "--dry-run",
            "--run-id",
            f"fixture-{scenario}",
            "--start-time",
            "2026-10-12T04:30:15Z",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    rows = load_jsonl(output, "fixture-model-v1")
    reference = build_reference(
        [row.features for row in rows[:300]],
        model_version="fixture-model-v1",
        dataset_version="fixture-ai4i-v1",
        reference_source="test_fixture",
    )
    path = tmp_path / "reference.json"
    path.write_text(reference.model_dump_json())
    report = evaluate(
        DriftConfig(
            reference_path=path, model_version="fixture-model-v1", dataset_version="fixture-ai4i-v1"
        ),
        rows,
    )
    assert report.overall_status.value == expected
    assert report.current_window.sample_count == 500
    assert len(json.loads(report.model_dump_json())["features"]) == 6
