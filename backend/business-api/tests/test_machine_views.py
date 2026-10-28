"""Baseline machine DTOs are lossy views; live projections retain historical data."""

from copy import deepcopy

import pytest
from backend.shared.projections.views import MachineViewUnavailable, machine_detail, machine_item


def row(status="COMPLETED", *, state="HIGH", risk=0.5):
    return {
        "machine_id": "M-001",
        "display_name": "One",
        "last_seen_at": None,
        "telemetry": {
            "event_id": "e-1",
            "received_at": "2026-10-26T00:00:00Z",
            "server_accepted_at": "2026-10-26T00:00:01Z",
            "validation_status": "VALID",
            "features_raw": {"raw": "fixture"},
        },
        "processing": {"processing_status": status, "validation_status": "VALID"},
        "prediction": {
            "prediction_id": "pred-1",
            "prediction": "FAILURE",
            "failure_probability": 0.8,
            "model_version": "model-1",
            "machine_state_at_prediction": state,
        },
        "prediction_metadata": {"risk_score": risk},
        "state": state if status == "COMPLETED" else "UNKNOWN",
    }


def test_native_fleet_preserves_risk_instead_of_using_probability():
    value = machine_item(row(), ["alert-1"])
    assert value == {
        "machine_id": "M-001",
        "display_name": "One",
        "state": "HIGH",
        "latest_risk_score": 0.5,
        "active_alert_count": 1,
        "last_seen_at": "2026-10-26T00:00:01Z",
        "data_status": "VALID",
    }


def test_missing_persisted_risk_is_an_error_without_get_time_fallback():
    source = row(risk=None)
    with pytest.raises(MachineViewUnavailable):
        machine_item(source, [])
    assert machine_detail(source, ["alert-1"])["latest_prediction"]["failure_probability"] == 0.8


@pytest.mark.parametrize(
    "status,state,data_status",
    [
        ("INVALID", "INVALID", "INVALID"),
        ("FAILED", "UNKNOWN", "VALID"),
        ("PENDING", "UNKNOWN", None),
    ],
)
def test_invalid_failed_pending_do_not_present_historical_prediction_as_current(
    status, state, data_status
):
    source = row(status)
    source["state"] = state
    source["processing"]["validation_status"] = data_status
    source["telemetry"]["validation_status"] = data_status or "UNVERIFIED"
    source["telemetry"]["features_raw"] = {"bad\x00key": "raw\x00value"}
    before = deepcopy(source)
    detail = machine_detail(source, ["alert-old"])
    assert (detail["state"], detail["data_status"]) == (state, data_status)
    assert detail["latest_prediction"] is None
    assert detail["latest_sensor"]["features"] == {"bad\x00key": "raw\x00value"}
    assert detail["active_alerts"] == ["alert-old"]
    assert machine_item(source, ["alert-old"])["latest_risk_score"] is None
    assert source == before


def test_empty_machine_remains_unknown_without_fabricated_data():
    source = {
        **row(),
        "telemetry": None,
        "processing": None,
        "prediction": None,
        "prediction_metadata": None,
        "state": "UNKNOWN",
    }
    detail = machine_detail(source, [])
    assert detail["state"] == "UNKNOWN"
    assert detail["data_status"] is None
    assert detail["latest_sensor"] is None
    assert detail["latest_prediction"] is None
    assert machine_item(source, [])["last_seen_at"] is None


def test_completed_without_prediction_is_unavailable_instead_of_normal():
    source = {**row(), "prediction": None}
    with pytest.raises(MachineViewUnavailable):
        machine_detail(source, [])


def test_completed_view_does_not_label_older_unverified_raw_as_valid():
    source = row()
    source["telemetry"]["validation_status"] = "UNVERIFIED"
    assert machine_detail(source, [])["latest_sensor"] is None
    assert source["telemetry"]["features_raw"] == {"raw": "fixture"}
