"""Frozen transitions and evidence identity; risk comes from the producer."""

import math
from datetime import datetime

from maintai_messaging.errors import IdentityConflict

ACTIVE = ("OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED")
TRANSITIONS = {
    "ack": (("OPEN",), "ACKNOWLEDGED"),
    "start": (("ACKNOWLEDGED", "ESCALATED"), "IN_PROGRESS"),
    "escalate": (("OPEN", "ACKNOWLEDGED", "IN_PROGRESS"), "ESCALATED"),
    "resolve": (("ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"), "RESOLVED"),
    "false-alert": (("OPEN", "ACKNOWLEDGED", "IN_PROGRESS"), "FALSE_ALERT"),
}


class AlertError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(code)


def transition(status, action):
    allowed, target = TRANSITIONS.get(action, ((), None))
    if status not in allowed:
        raise AlertError(409, "INVALID_ALERT_TRANSITION")
    return target


def validate_evidence(signal, value):
    from jsonschema import Draft202012Validator, FormatChecker
    from maintai_messaging.contracts import default_contracts

    contracts = default_contracts()
    validator = Draft202012Validator(
        {
            "$ref": (contracts.root / "v1.3/schemas/runtime.schema.json").as_uri()
            + "#/$defs/HistoryRecord"
        },
        registry=contracts.registry,
        format_checker=FormatChecker(),
    )
    if not validator.is_valid(value):
        raise IdentityConflict("Alert evidence schema is invalid")
    outcome, risk = value["outcome"], value["risk_snapshot"]
    if outcome["kind"] != "PREDICTION_COMPLETED" or outcome["validation_status"] != "VALID":
        raise IdentityConflict("Alert requires a valid completed prediction")
    for key in (
        "machine_id",
        "prediction_id",
        "risk_score",
        "model_version",
        "policy_version",
        "event_id",
        "correlation_id",
    ):
        if outcome[key] != signal[key]:
            raise IdentityConflict("Signal/evidence identity differs")
    if (
        value["source_event_id"] != signal["event_id"]
        or value["machine_id"] != signal["machine_id"]
        or value["result_id"] != outcome["result_id"]
        or risk is None
        or outcome["machine_state"] != signal["severity"]
    ):
        raise IdentityConflict("Evidence source or severity differs")
    if datetime.fromisoformat(value["source_server_accepted_at"]) != datetime.fromisoformat(
        outcome["source_server_accepted_at"]
    ):
        raise IdentityConflict("Evidence source time differs")
    if datetime.fromisoformat(signal["decided_at"]) != datetime.fromisoformat(
        outcome["occurred_at"]
    ):
        raise IdentityConflict("Evidence risk decision time differs")
    for key in ("risk_decision_id", "machine_state", "policy_version", "risk_score"):
        if risk.get(key) != outcome[key]:
            raise IdentityConflict("Evidence risk snapshot differs")
    features = value["features_raw"]
    expected = {
        "product_type",
        "air_temperature_k",
        "process_temperature_k",
        "rotational_speed_rpm",
        "torque_nm",
        "tool_wear_min",
    }
    if (
        set(features) != expected
        or not isinstance(features["product_type"], str)
        or features["product_type"] not in {"L", "M", "H"}
    ):
        raise IdentityConflict("Valid source features differ from the model allowlist")
    if any(
        type(features[key]) not in (int, float) or not math.isfinite(features[key])
        for key in expected - {"product_type"}
    ):
        raise IdentityConflict("Valid source features must be finite numeric values")
    if features["rotational_speed_rpm"] <= 0 or features["tool_wear_min"] < 0:
        raise IdentityConflict("Valid source feature bounds differ")
    return value
