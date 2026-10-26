"""Identity fixtures exercise mapping, not authenticity of a live ML deployment."""

import json
from dataclasses import fields, replace
from pathlib import Path

import pytest
from maintai_messaging.outcomes import ModelIdentity
from processing.shared.inference_client.client import PredictionResponse
from processing.shared.inference_client.model_identity import resolve_model_identity

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def identity():
    sample = json.loads((ROOT / "contracts/kafka/examples/completed.json").read_text())
    return ModelIdentity(**{field.name: sample[field.name] for field in fields(ModelIdentity)})


def response(identity):
    return PredictionResponse.model_validate_json(
        json.dumps(
            {
                "prediction_id": "fixture-pred",
                "event_id": "fixture-event",
                "prediction": "FAILURE",
                "failure_probability": 0.8,
                "model_version": identity.model_version,
                "preprocessor_version": identity.preprocessor_version,
                "model_threshold": 0.5,
                "inference_ms": 1,
                "predicted_at": "2026-10-24T00:00:00Z",
            }
        )
    )


def test_baseline_response_cannot_fill_missing_identity(identity):
    with pytest.raises(ValueError):
        resolve_model_identity(response(identity), verified_identity=None)


def test_exact_identity_is_preserved(identity):
    assert resolve_model_identity(response(identity), verified_identity=identity) == identity


@pytest.mark.parametrize(
    "field,value",
    [
        ("model_version", "different"),
        ("preprocessor_version", "different"),
        ("release_id", ""),
        ("model_artifact_sha256", "guessed"),
        ("inference_image_digest", "latest"),
        ("dataset_version", ""),
    ],
)
def test_mismatch_or_incomplete_identity_cannot_become_completed(identity, field, value):
    with pytest.raises(ValueError):
        resolve_model_identity(
            response(identity), verified_identity=replace(identity, **{field: value})
        )
