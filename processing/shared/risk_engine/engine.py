"""Map a validated ML probability to an explicit versioned product policy."""

from dataclasses import dataclass

from .policy import RiskPolicy


@dataclass(frozen=True)
class RiskEvaluation:
    machine_state: str
    severity: str | None
    should_create_alert: bool
    risk_score: float
    policy_version: str
    policy_sha256: str
    decision_reason: str


def evaluate_risk(*, failure_probability: float, policy: RiskPolicy) -> RiskEvaluation:
    if type(failure_probability) not in (int, float) or not 0 <= failure_probability <= 1:
        raise ValueError("Risk requires a finite probability from successful inference")
    state, reason = "NORMAL", "failure_probability < watch threshold"
    for level, threshold in (
        ("CRITICAL", policy.critical),
        ("HIGH", policy.high),
        ("WATCH", policy.watch),
    ):
        if failure_probability >= threshold:
            state, reason = level, f"failure_probability >= {level.lower()} threshold"
            break
    return RiskEvaluation(
        machine_state=state,
        severity=None if state == "NORMAL" else state,
        should_create_alert=state in policy.create_alert_for,
        risk_score=failure_probability,
        policy_version=policy.policy_version,
        policy_sha256=policy.policy_sha256,
        decision_reason=reason,
    )
