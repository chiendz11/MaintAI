"""Feature-only validation; registry validation belongs to the next boundary."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest
from processing.shared.data_quality.validator import FeatureValidator

SCHEMA = Path(__file__).resolve().parents[3] / "contracts/v1.3/schemas/features.schema.json"


@pytest.fixture
def features():
    return {
        "product_type": "M",
        "air_temperature_k": 298.1,
        "process_temperature_k": 308.6,
        "rotational_speed_rpm": 1551,
        "torque_nm": 42.8,
        "tool_wear_min": 0,
    }


@pytest.fixture
def validator():
    return FeatureValidator.from_file(SCHEMA, version="sensor-validator-1.0.0")


def test_valid_features_are_preserved_without_preprocessing(validator, features):
    result = validator.validate(features)
    assert result.status == "VALID"
    assert result.errors == ()
    assert result.normalized_features == features
    features["torque_nm"] = 999
    assert result.normalized_features["torque_nm"] == 42.8


@pytest.mark.parametrize(
    "field",
    [
        "product_type",
        "air_temperature_k",
        "process_temperature_k",
        "rotational_speed_rpm",
        "torque_nm",
        "tool_wear_min",
    ],
)
def test_each_feature_is_required(validator, features, field):
    del features[field]
    result = validator.validate(features)
    assert result.status == "INVALID"
    assert result.normalized_features is None
    assert any(e.code == "MISSING_FIELD" for e in result.errors)


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("product_type", "X", "UNSUPPORTED_VALUE"),
        ("product_type", 1, "INVALID_TYPE"),
        ("rotational_speed_rpm", 0, "OUT_OF_RANGE"),
        ("rotational_speed_rpm", -1, "OUT_OF_RANGE"),
        ("tool_wear_min", -0.1, "OUT_OF_RANGE"),
        ("air_temperature_k", "298.1", "INVALID_TYPE"),
        ("torque_nm", True, "INVALID_TYPE"),
        ("tool_wear_min", None, "INVALID_TYPE"),
        ("torque_nm", float("nan"), "NON_FINITE"),
        ("torque_nm", float("inf"), "NON_FINITE"),
        ("air_temperature_k", float("-inf"), "NON_FINITE"),
    ],
)
def test_invalid_features_are_not_coerced(validator, features, field, value, code):
    features[field] = value
    result = validator.validate(features)
    assert result.status == "INVALID"
    assert result.normalized_features is None
    assert any(e.code == code for e in result.errors)


@pytest.mark.parametrize("field", ["event_id", "timestamp", "target", "fault_label"])
def test_extra_features_are_rejected_instead_of_dropped(validator, features, field):
    features[field] = "sensitive-value-must-not-appear-in-errors"
    result = validator.validate(features)
    assert result.status == "INVALID"
    assert any(e.code == "UNEXPECTED_FIELD" for e in result.errors)
    assert "sensitive-value" not in repr(result.errors)


@pytest.mark.parametrize("value", [None, [], "not-an-object"])
def test_non_object_is_invalid(validator, value):
    assert validator.validate(value).status == "INVALID"


def test_no_additional_temperature_or_torque_limits_are_invented(validator, features):
    features.update(air_temperature_k=-1, process_temperature_k=99999, torque_nm=-1)
    assert validator.validate(features).status == "VALID"


def test_ruleset_digest_covers_schema_and_nonfinite_rule(validator):
    rules = {
        "validator_version": "sensor-validator-1.0.0",
        "feature_schema": json.loads(SCHEMA.read_text()),
        "finite_numbers_required": True,
    }
    raw = json.dumps(
        rules, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    assert validator.ruleset_sha256 == hashlib.sha256(raw).hexdigest()


def test_empty_ruleset_version_is_configuration_error():
    with pytest.raises(ValueError):
        FeatureValidator.from_file(SCHEMA, version=" ")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("1.2"), complex(1, 2)])
@pytest.mark.parametrize("field", ["torque_nm", "rotational_speed_rpm"])
def test_python_number_types_outside_json_are_invalid(validator, features, field, value):
    features[field] = value
    result = validator.validate(features)
    assert result.status == "INVALID"
    assert result.normalized_features is None
    assert any(e.code == "INVALID_TYPE" and e.field == f"features.{field}" for e in result.errors)
