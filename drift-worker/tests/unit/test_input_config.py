import json

import pytest
from pydantic import ValidationError

from drift_monitor.config import DriftConfig
from drift_monitor.data_loader import load_jsonl


def event(sample, status="VALID", model_version="fixture-model-v1"):
    value = sample.model_dump(mode="json")
    value.pop("prediction_label")
    value["validation_status"] = status
    value["prediction"] = {"model_version": model_version, "label": "FAILURE"}
    return value


def test_jsonl_filters_invalid_old_models_and_duplicates(samples, tmp_path):
    path = tmp_path / "events.jsonl"
    values = [
        event(samples[0]),
        event(samples[1], "INVALID"),
        event(samples[2], model_version="old-model"),
        event(samples[0]),
    ]
    path.write_text("\n".join(json.dumps(value) for value in values))
    rows = load_jsonl(path, "fixture-model-v1")
    assert len(rows) == 2
    assert rows[0].prediction_label == "FAILURE"
    assert rows[1].prediction_label is None


def test_conflicting_jsonl_duplicate_rejected(samples, tmp_path):
    path = tmp_path / "events.jsonl"
    first, second = event(samples[0]), event(samples[0])
    second["features"]["torque_nm"] += 1
    path.write_text(json.dumps(first) + "\n" + json.dumps(second))
    with pytest.raises(ValueError, match="Invalid runtime JSONL"):
        load_jsonl(path, "fixture-model-v1")


def test_forbidden_feature_and_naive_clock_rejected(samples, tmp_path):
    for field in ["feature", "clock"]:
        path = tmp_path / "bad.jsonl"
        value = event(samples[0])
        if field == "feature":
            value["features"]["Machine failure"] = 1
        else:
            value["received_at"] = "2026-10-12T00:00:00"
        path.write_text(json.dumps(value))
        with pytest.raises(ValueError):
            load_jsonl(path, "fixture-model-v1")


def test_config_yaml_guide_shape_and_env_override(config, tmp_path, monkeypatch):
    path = tmp_path / "drift.yaml"
    path.write_text(
        "drift:\n  model_version: fixture-model-v1\n"
        "  dataset_version: fixture-ai4i-v1\n  reference_path: reference.json\n"
        "  psi:\n    warning: 0.12\n    critical: 0.25\n"
    )
    monkeypatch.setenv("DRIFT_WINDOW_SIZE", "600")
    loaded = DriftConfig.load(path)
    assert loaded.policy.warning == 0.12
    assert loaded.window_size == 600
    assert loaded.reference_path == tmp_path / "reference.json"


@pytest.mark.parametrize(
    "updates",
    [
        {"minimum_samples": 600},
        {"window_size": 0},
        {"policy": {"warning": 0.2, "critical": 0.1}},
        {"numeric_metric": "invented"},
        {"pushgateway_required": True},
    ],
)
def test_bad_configuration_is_actionable(config, updates):
    values = config.model_dump()
    values.update(updates)
    with pytest.raises(ValidationError):
        DriftConfig.model_validate(values)
