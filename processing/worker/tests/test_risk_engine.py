"""Risk policy examples below are test fixtures, not deployment policy."""

import copy
import hashlib
import json
from dataclasses import replace

import pytest
from processing.shared.risk_engine.engine import evaluate_risk
from processing.shared.risk_engine.policy import RiskPolicy


@pytest.fixture
def config():
    return {
        "policy_version": "fixture-risk-policy-1",
        "thresholds": {
            "watch": 0.4,
            "high": 0.65,
            "critical": 0.85,
        },
        "create_alert_for": ["HIGH", "CRITICAL"],
    }


@pytest.mark.parametrize(
    "probability,state,alert",
    [
        (0, "NORMAL", False),
        (0.3999, "NORMAL", False),
        (0.4, "WATCH", False),
        (0.6499, "WATCH", False),
        (0.65, "HIGH", True),
        (0.8499, "HIGH", True),
        (0.85, "CRITICAL", True),
        (1, "CRITICAL", True),
    ],
)
def test_probability_boundaries_and_alert_policy(config, probability, state, alert):
    result = evaluate_risk(failure_probability=probability, policy=RiskPolicy.from_dict(config))
    assert result.machine_state == state
    assert result.should_create_alert is alert
    assert result.severity == (None if state == "NORMAL" else state)
    assert result.risk_score == probability
    assert result.policy_version == "fixture-risk-policy-1"


def test_policy_controls_watch_signals(config):
    config["create_alert_for"] = ["WATCH"]
    policy = RiskPolicy.from_dict(config)
    assert evaluate_risk(failure_probability=0.5, policy=policy).should_create_alert
    assert not evaluate_risk(failure_probability=0.9, policy=policy).should_create_alert


def test_policy_hash_and_decisions_cannot_change_by_mutating_input(config):
    expected = hashlib.sha256(
        json.dumps(
            config, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()
    policy = RiskPolicy.from_dict(config)
    config["thresholds"]["high"] = 0.99
    config["create_alert_for"].clear()
    result = evaluate_risk(failure_probability=0.7, policy=policy)
    assert result.machine_state == "HIGH"
    assert result.should_create_alert
    assert result.policy_sha256 == expected


@pytest.mark.parametrize(
    "probability", [None, "0.5", True, -0.01, 1.01, float("nan"), float("inf")]
)
def test_unavailable_or_invalid_probability_never_becomes_normal(config, probability):
    with pytest.raises(ValueError):
        evaluate_risk(failure_probability=probability, policy=RiskPolicy.from_dict(config))


@pytest.mark.parametrize(
    "change",
    [
        {"policy_version": " "},
        {"thresholds": {"watch": 0.8, "high": 0.7, "critical": 0.9}},
        {"thresholds": {"watch": 0.4, "high": 0.4, "critical": 0.9}},
        {"thresholds": {"watch": True, "high": 0.7, "critical": 0.9}},
        {"thresholds": {"watch": -0.1, "high": 0.7, "critical": 0.9}},
        {"thresholds": {"watch": 0.4, "high": 0.7, "critical": float("nan")}},
        {"thresholds": {"watch": 0.4, "high": 0.7, "critical": 1.1}},
        {"create_alert_for": ["NORMAL"]},
        {"create_alert_for": ["UNKNOWN"]},
        {"create_alert_for": ["HIGH", "HIGH"]},
        {"extra": "ignored?"},
    ],
)
def test_malformed_policy_fails_instead_of_installing_defaults(config, change):
    config.update(copy.deepcopy(change))
    with pytest.raises(ValueError):
        RiskPolicy.from_dict(config)


def test_no_default_policy_and_no_guide_alert_on_alias(config):
    with pytest.raises(ValueError):
        RiskPolicy.from_dict({})
    config["alert_on"] = config.pop("create_alert_for")
    with pytest.raises(ValueError):
        RiskPolicy.from_dict(config)


def test_direct_policy_constructor_cannot_bypass_threshold_validation():
    with pytest.raises(ValueError):
        RiskPolicy("fixture", float("nan"), float("nan"), float("nan"), ("HIGH",))


def test_replacing_thresholds_revalidates_policy(config):
    policy = RiskPolicy.from_dict(config)
    with pytest.raises(ValueError):
        replace(policy, high=float("nan"))


def test_replacing_valid_threshold_recomputes_digest(config):
    policy = RiskPolicy.from_dict(config)
    changed = replace(policy, high=0.7)
    assert changed.policy_sha256 != policy.policy_sha256
    assert evaluate_risk(failure_probability=0.67, policy=changed).machine_state == "WATCH"


def test_direct_policy_cannot_supply_forged_digest():
    with pytest.raises(TypeError):
        RiskPolicy("fixture", 0.4, 0.65, 0.85, ("HIGH",), "not-a-real-digest")
