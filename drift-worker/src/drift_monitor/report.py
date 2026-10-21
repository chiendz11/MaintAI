import hashlib
import json
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from drift_monitor.config import DriftConfig
from drift_monitor.detector import aggregate, feature_results, prediction_result
from drift_monitor.models import (
    CurrentWindow,
    DriftReport,
    DriftStatus,
    ReferenceIdentity,
    ReferenceProfile,
    RuntimeSample,
)
from drift_monitor.reference import ReferenceError, load_reference


def evaluate(
    config: DriftConfig,
    samples: list[RuntimeSample],
    *,
    computed_at: datetime | None = None,
    reason: str | None = None,
    reference_profile: ReferenceProfile | None = None,
) -> DriftReport:
    now = computed_at or datetime.now(UTC)
    reference = reference_profile
    if reason is None:
        try:
            if reference is None:
                reference = load_reference(
                    config.reference_path, config.model_version, config.dataset_version
                )
            if (
                reference.model_version != config.model_version
                or reference.dataset_version != config.dataset_version
            ):
                raise ReferenceError("DRIFT_REFERENCE_NOT_FOUND")
        except ReferenceError as exc:
            reason = str(exc)
    samples = sorted(samples, key=lambda row: (row.received_at, row.event_id))[
        -config.window_size :
    ]
    policy_snapshot = {
        **config.policy.model_dump(),
        "minimum_samples": config.minimum_samples,
        "window_size": config.window_size,
        "prediction_can_raise_to_warning": True,
    }
    identity = {
        "worker": config.worker_name,
        "detector": config.detector_version,
        "model": config.model_version,
        "dataset": config.dataset_version,
        "minimum_samples": config.minimum_samples,
        "window_size": config.window_size,
        "policy": policy_snapshot,
        "reason": reason,
        "reference": reference.model_dump(mode="json") if reference else None,
        # Predictions can arrive late; same event IDs with new labels require a fresh report.
        "samples": [sample.model_dump(mode="json") for sample in samples],
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, allow_nan=False).encode()
    ).hexdigest()
    report = DriftReport(
        report_id=f"drift_{uuid5(NAMESPACE_URL, digest)}",
        computed_at=now,
        detector_version=config.detector_version,
        reference=ReferenceIdentity(
            dataset_version=config.dataset_version, model_version=config.model_version
        ),
        current_window=CurrentWindow(
            start_at=samples[0].received_at if samples else now,
            end_at=samples[-1].received_at if samples else now,
            sample_count=len(samples),
        ),
        overall_status=DriftStatus.ERROR if reason else DriftStatus.INSUFFICIENT_DATA,
        policy=policy_snapshot,
        reason=reason,
    )
    if reason or len(samples) < config.minimum_samples:
        if not reason:
            report.reason = "DRIFT_INSUFFICIENT_DATA"
        return report
    report.features = feature_results(reference, samples, config.policy)
    report.prediction_drift = prediction_result(
        reference, samples, config.policy, config.minimum_samples
    )
    report.overall_status, report.summary = aggregate(
        report.features,
        report.prediction_drift,
        config.policy,
    )
    return report
