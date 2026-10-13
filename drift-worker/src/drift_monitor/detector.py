import numpy as np

from drift_monitor.config import Policy
from drift_monitor.models import (
    FEATURE_NAMES,
    CategoricalProfile,
    DriftStatus,
    FeatureResult,
    PredictionDrift,
    ReferenceProfile,
    RuntimeSample,
    Summary,
)
from drift_monitor.reference import histogram


def psi(reference: list[float], current: list[float], epsilon: float = 1e-6) -> float:
    expected = np.asarray(reference, dtype=float)
    actual = np.asarray(current, dtype=float)
    if expected.shape != actual.shape or expected.size < 1:
        raise ValueError("PSI distributions must have equal non-empty shapes")
    if (
        not np.all(np.isfinite(expected))
        or not np.all(np.isfinite(actual))
        or np.any(expected < 0)
        or np.any(actual < 0)
        or expected.sum() <= 0
        or actual.sum() <= 0
    ):
        raise ValueError("PSI distributions must be finite, non-negative and non-empty")
    expected = expected + epsilon
    actual = actual + epsilon
    expected /= expected.sum()
    actual /= actual.sum()
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def classify(score: float, policy: Policy) -> DriftStatus:
    if score >= policy.critical:
        return DriftStatus.CRITICAL
    if score >= policy.warning:
        return DriftStatus.WARNING
    return DriftStatus.STABLE


def feature_results(
    reference: ReferenceProfile,
    samples: list[RuntimeSample],
    policy: Policy,
) -> list[FeatureResult]:
    results = []
    for name in FEATURE_NAMES:
        profile = getattr(reference.features, name)
        values = [getattr(row.features, name) for row in samples]
        if isinstance(profile, CategoricalProfile):
            current = [values.count(category) / len(values) for category in profile.categories]
        else:
            current = histogram(values, profile.bins)
        score = psi(profile.probabilities, current, policy.epsilon)
        results.append(
            FeatureResult(
                feature=name,
                feature_type=profile.type,
                score=score,
                warning_threshold=policy.warning,
                critical_threshold=policy.critical,
                status=classify(score, policy),
            )
        )
    return results


def prediction_result(
    reference: ReferenceProfile,
    samples: list[RuntimeSample],
    policy: Policy,
    minimum_samples: int,
) -> PredictionDrift | None:
    if reference.prediction_reference is None:
        return None
    labels = [row.prediction_label for row in samples if row.prediction_label is not None]
    current = labels.count("FAILURE") / len(labels) if labels else None
    reference_rate = reference.prediction_reference.failure_rate
    score = None
    status = DriftStatus.INSUFFICIENT_DATA
    if len(labels) >= minimum_samples:
        score = psi([1 - reference_rate, reference_rate], [1 - current, current], policy.epsilon)
        status = classify(score, policy)
    return PredictionDrift(
        reference_failure_rate=reference_rate,
        current_failure_rate=current,
        sample_count=len(labels),
        score=score,
        status=status,
        warning_threshold=policy.warning,
        critical_threshold=policy.critical,
    )


def aggregate(
    results: list[FeatureResult],
    prediction: PredictionDrift | None,
    policy: Policy,
) -> tuple[DriftStatus, Summary]:
    critical = sum(result.status == DriftStatus.CRITICAL for result in results)
    warning = sum(result.status == DriftStatus.WARNING for result in results)
    summary = Summary(warning_features=warning, critical_features=critical)
    if critical >= policy.critical_if_critical_features_gte:
        return DriftStatus.CRITICAL, summary
    # A critical feature counts toward warning, too: never classify one critical feature STABLE.
    if warning + critical >= policy.warning_if_warning_features_gte or (
        prediction and prediction.status in {DriftStatus.WARNING, DriftStatus.CRITICAL}
    ):
        return DriftStatus.WARNING, summary
    return DriftStatus.STABLE, summary
