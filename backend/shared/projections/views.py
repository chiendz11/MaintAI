"""Baseline machine views; historical prediction remains in the independent live DTO."""

OPERATIONAL = frozenset({"NORMAL", "WATCH", "HIGH", "CRITICAL"})


class MachineViewUnavailable(ValueError):
    """A required read source or compatibility mapping is unavailable."""


def _common(row):
    processing, prediction, telemetry = row["processing"], row["prediction"], row["telemetry"]
    state = row["state"]
    if state not in OPERATIONAL | {"INVALID", "UNKNOWN"}:
        raise MachineViewUnavailable("Missing machine state")
    if processing and processing["processing_status"] == "COMPLETED" and prediction is None:
        raise MachineViewUnavailable("Completed projection lacks prediction")
    validation = processing.get("validation_status") if processing else None
    return {
        "machine_id": row["machine_id"],
        "display_name": row["display_name"],
        "state": state,
        "data_status": validation,
        "last_seen_at": (telemetry["server_accepted_at"] if telemetry else row["last_seen_at"]),
    }


def machine_item(row, active_alerts):
    value = _common(row)
    risk = None
    if value["state"] in OPERATIONAL:
        metadata = row["prediction_metadata"] or {}
        risk = metadata.get("risk_score")
        if type(risk) not in (float, int) or not 0 <= risk <= 1:
            raise MachineViewUnavailable("Fleet requires a verified risk score")
    return {**value, "latest_risk_score": risk, "active_alert_count": len(active_alerts)}


def machine_detail(row, active_alerts):
    value = _common(row)
    telemetry, prediction = row["telemetry"], row["prediction"]
    sensor = None
    if telemetry and (value["data_status"] != "VALID" or telemetry["validation_status"] == "VALID"):
        sensor = {
            "event_id": telemetry["event_id"],
            "features": telemetry["features_raw"],
            "received_at": telemetry["received_at"],
        }
    return {
        **value,
        "latest_sensor": sensor,
        "latest_prediction": (
            {
                key: prediction[key]
                for key in ("prediction_id", "prediction", "failure_probability", "model_version")
            }
            if value["state"] in OPERATIONAL
            else None
        ),
        "active_alerts": active_alerts,
    }
