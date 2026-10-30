"""App-owned policy catalog binds deployable bytes to the runtime decision hash."""

import hashlib
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError
from maintai_messaging.contracts import strict_json
from processing.shared.risk_engine.engine import evaluate_risk
from processing.shared.risk_engine.policy import RiskPolicy

DIRECTORY = Path(__file__).resolve().parents[2] / "config/risk-policies"


def test_catalog_binds_policy_bytes_schema_and_runtime_identity():
    manifest = strict_json((DIRECTORY / "manifest.json").read_bytes())
    assert manifest["schema_version"] == "1.0.0"
    schema_bytes = (DIRECTORY / manifest["schema_path"]).read_bytes()
    assert hashlib.sha256(schema_bytes).hexdigest() == manifest["schema_sha256"]
    validator = Draft202012Validator(strict_json(schema_bytes))
    validator.check_schema(validator.schema)
    for version, entry in manifest["policies"].items():
        path = DIRECTORY / entry["path"]
        assert path.resolve().is_relative_to(DIRECTORY.resolve())
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == entry["sha256"]
        document = strict_json(raw)
        validator.validate(document)
        policy = RiskPolicy.from_dict(document)
        assert policy.policy_version == version
        assert policy.policy_sha256 == entry["policy_sha256"]
        assert entry["usage"] == "demo"
        assert entry["calibration_status"] == "illustrative_not_calibrated"


def test_demo_policy_preserves_boundary_decisions_without_forcing_scenarios():
    policy = RiskPolicy.from_file(DIRECTORY / "demo-risk-v1.json")
    for probability, state, alert in [
        (0.06080692676105234, "NORMAL", False),
        (0.40, "WATCH", False),
        (0.65, "HIGH", True),
        (0.85, "CRITICAL", True),
    ]:
        result = evaluate_risk(failure_probability=probability, policy=policy)
        assert (result.machine_state, result.should_create_alert) == (state, alert)
        assert result.policy_sha256 == policy.policy_sha256


@pytest.mark.parametrize("change", [{"alert_on": ["HIGH"]}, {"create_alert_for": ["NORMAL"]}])
def test_published_schema_rejects_guide_aliases_and_invalid_alert_states(change):
    document = strict_json((DIRECTORY / "demo-risk-v1.json").read_bytes())
    document.update(change)
    validator = Draft202012Validator(
        strict_json((DIRECTORY / "risk-policy.schema.json").read_bytes())
    )
    with pytest.raises(ValidationError):
        validator.validate(document)
