
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from api.schemas import MaintenanceBriefRequest, MaintenanceBriefResponse


REQUEST = {
    "alert": {
        "alert_id": "alt_001",
        "machine_id": "M-001",
        "severity": "CRITICAL",
        "status": "OPEN",
        "risk_score": 0.91,
        "policy_version": "risk-policy-1.0.0",
    },
    "prediction": {
        "prediction_id": "pred_001",
        "prediction": "FAILURE",
        "failure_probability": 0.91,
        "model_version": "maintai-ml-1.0.0",
    },
    "sensor_context": {
        "product_type": "M",
        "air_temperature_k": 298.1,
        "process_temperature_k": 308.6,
        "rotational_speed_rpm": 1551,
        "torque_nm": 42.8,
        "tool_wear_min": 108,
    },
    "intent": "TECHNICIAN_CHECKLIST",
}


def test_request_matches_contract():
    request = MaintenanceBriefRequest.model_validate(REQUEST)
    assert request.alert.alert_id == "alt_001"


def test_invalid_intent_rejected():
    with pytest.raises(ValidationError):
        MaintenanceBriefRequest.model_validate({
            **REQUEST,
            "intent": "OTHER",
        })


def grounded():
    return {
        "brief_id": "brf_001",
        "alert_id": "alt_001",
        "grounded_status": "GROUNDED",
        "summary": "Kiểm tra điều kiện vận hành",
        "uncertainty": "Cần xác minh thực tế",
        "checklist": [
            {
                "step": 1,
                "action": "Kiểm tra torque",
                "citation_ids": ["cit_001"],
            }
        ],
        "citations": [
            {
                "citation_id": "cit_001",
                "document_id": "manual-001",
                "document_version": "2.0",
                "section": "4.2",
                "page": 18,
                "chunk_id": "chunk-123",
            }
        ],
        "retrieval": {
            "top_k": 5,
            "retrieved_chunk_ids": ["chunk-123"],
        },
        "generator": {
            "provider": "ollama",
            "model": "configured-model",
        },
        "generated_at": datetime.now(timezone.utc),
    }


def test_grounded_structure():
    response = MaintenanceBriefResponse.model_validate(grounded())
    assert response.grounded_status.value == "GROUNDED"


def test_grounded_missing_citation_rejected():
    payload = grounded()
    payload["checklist"][0]["citation_ids"] = ["unknown"]

    with pytest.raises(ValidationError):
        MaintenanceBriefResponse.model_validate(payload)


def test_grounded_unretrieved_chunk_rejected():
    payload = grounded()
    payload["retrieval"]["retrieved_chunk_ids"] = []

    with pytest.raises(ValidationError):
        MaintenanceBriefResponse.model_validate(payload)


@pytest.mark.parametrize(
    "status",
    ["INSUFFICIENT_EVIDENCE", "VALIDATION_FAILED"],
)
def test_non_grounded_results(status):
    payload = grounded()
    payload.update(
        grounded_status=status,
        checklist=[],
        citations=[],
        generator=None,
        generated_at=None,
    )

    response = MaintenanceBriefResponse.model_validate(payload)
    assert response.grounded_status.value == status


def test_non_grounded_cannot_have_actions():
    payload = grounded()
    payload["grounded_status"] = "VALIDATION_FAILED"

    with pytest.raises(ValidationError):
        MaintenanceBriefResponse.model_validate(payload)
