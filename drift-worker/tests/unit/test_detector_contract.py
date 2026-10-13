import json
import math
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from drift_monitor.config import Policy
from drift_monitor.detector import classify, psi
from drift_monitor.models import DriftStatus, PredictionReference
from drift_monitor.reference import histogram
from drift_monitor.report import evaluate


def test_psi_known_formula_and_zero_bins():
    assert psi([0.5, 0.5], [0.5, 0.5]) == 0
    assert psi([0.5, 0.5], [0.9, 0.1], 1e-12) == pytest.approx(0.4 * math.log(9))
    assert math.isfinite(psi([1, 0], [0, 1]))


@pytest.mark.parametrize(
    "left,right", [([1], [0.5, 0.5]), ([float("nan")], [1]), ([-1, 2], [1, 0]), ([0, 0], [1, 0])]
)
def test_psi_rejects_invalid_distributions(left, right):
    with pytest.raises(ValueError):
        psi(left, right)


@pytest.mark.parametrize(
    "score,status",
    [(0.099, DriftStatus.STABLE), (0.10, DriftStatus.WARNING), (0.20, DriftStatus.CRITICAL)],
)
def test_exact_policy_boundaries(score, status):
    assert classify(score, Policy()) == status


def test_histogram_tail_mass_not_lost():
    assert histogram([-100, 0, 5, 10, 20, 100], [0, 10, 20]) == [0.5, 0.5]


def test_normal_window_contract_and_no_forbidden_features(config, samples):
    report = evaluate(config, samples)
    assert report.overall_status == DriftStatus.STABLE
    assert len(report.features) == 6
    assert all(result.score == 0 for result in report.features)
    root = Path(__file__).resolve().parents[3]
    schema = json.loads((root / "contracts/drift-report.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        report.model_dump(mode="json")
    )
    assert {result.feature for result in report.features} == {
        "product_type",
        "air_temperature_k",
        "process_temperature_k",
        "rotational_speed_rpm",
        "torque_nm",
        "tool_wear_min",
    }


def test_one_critical_category_is_warning_not_stable(config, samples):
    shifted = [row.model_copy(deep=True) for row in samples]
    for row in shifted:
        row.features.product_type = "H"
    report = evaluate(config, shifted)
    assert report.summary.critical_features == 1
    assert report.overall_status == DriftStatus.WARNING


def test_two_critical_numeric_features_are_critical(config, samples):
    shifted = [row.model_copy(deep=True) for row in samples]
    for row in shifted:
        row.features.torque_nm += 20
        row.features.rotational_speed_rpm *= 0.5
    report = evaluate(config, shifted)
    assert report.summary.critical_features >= 2
    assert report.overall_status == DriftStatus.CRITICAL


def test_insufficient_samples_never_publish_conclusion(config, samples):
    report = evaluate(config, samples[:50])
    assert report.overall_status == DriftStatus.INSUFFICIENT_DATA
    assert report.features == []
    assert report.reason == "DRIFT_INSUFFICIENT_DATA"


@pytest.mark.parametrize("field", ["model_version", "dataset_version"])
def test_reference_mismatch_is_error(config, samples, field):
    changed = config.model_copy(update={field: "wrong-version"})
    report = evaluate(changed, samples)
    assert report.overall_status == DriftStatus.ERROR
    assert report.reason == "DRIFT_REFERENCE_NOT_FOUND"


def test_missing_reference_is_error(config, samples):
    config.reference_path.unlink()
    assert evaluate(config, samples).reason == "DRIFT_REFERENCE_NOT_FOUND"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value["features"].update({"machine_failure": {}}),
        lambda value: value["features"]["torque_nm"].update({"probabilities": [0.1]}),
        lambda value: value["features"]["torque_nm"].update({"bins": [10, 5]}),
        lambda value: value["features"]["product_type"].update({"categories": ["L", "L", "H"]}),
    ],
)
def test_invalid_reference_never_falls_back(config, samples, mutation):
    value = json.loads(config.reference_path.read_text())
    mutation(value)
    config.reference_path.write_text(json.dumps(value))
    assert evaluate(config, samples).overall_status == DriftStatus.ERROR


def test_prediction_drift_and_partial_predictions(config, reference, samples):
    reference.prediction_reference = PredictionReference(failure_rate=0.1)
    config.reference_path.write_text(reference.model_dump_json())
    predicted = [row.model_copy(update={"prediction_label": "FAILURE"}) for row in samples]
    report = evaluate(config, predicted)
    assert report.prediction_drift.current_failure_rate == 1
    assert report.prediction_drift.status == DriftStatus.CRITICAL
    assert report.overall_status == DriftStatus.WARNING
    for row in predicted[5:]:
        row.prediction_label = None
    partial = evaluate(config, predicted)
    assert partial.prediction_drift.status == DriftStatus.INSUFFICIENT_DATA
    assert partial.prediction_drift.score is None
    assert partial.overall_status == DriftStatus.STABLE


def test_idempotency_identity_tracks_input_reference_and_policy(config, samples):
    first = evaluate(config, samples)
    assert first.report_id == evaluate(config, samples).report_id
    later_predictions = [row.model_copy(update={"prediction_label": "NORMAL"}) for row in samples]
    assert evaluate(config, later_predictions).report_id != first.report_id
    changed = config.model_copy(deep=True)
    changed.policy.warning = 0.15
    assert evaluate(changed, samples).report_id != first.report_id
    reverse = list(reversed(samples))
    assert evaluate(config, reverse).report_id == first.report_id
