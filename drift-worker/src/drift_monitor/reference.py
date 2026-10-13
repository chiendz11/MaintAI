import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from pydantic import ValidationError

from drift_monitor.models import (
    NUMERIC_FEATURES,
    CategoricalProfile,
    Features,
    NumericProfile,
    PredictionReference,
    ReferenceFeatures,
    ReferenceProfile,
)


class ReferenceError(ValueError):
    pass


def load_reference(path: Path, model_version: str, dataset_version: str) -> ReferenceProfile:
    try:
        reference = ReferenceProfile.model_validate_json(path.read_text())
    except (OSError, ValueError, ValidationError) as exc:
        raise ReferenceError("DRIFT_REFERENCE_NOT_FOUND") from exc
    if reference.model_version != model_version or reference.dataset_version != dataset_version:
        raise ReferenceError("DRIFT_REFERENCE_NOT_FOUND")
    return reference


def histogram(values: list[float], bins: list[float]) -> list[float]:
    """Reference bins are fixed; fold under/overflow into edge buckets, never drop samples."""
    indexes = np.searchsorted(bins, values, side="right") - 1
    indexes = np.clip(indexes, 0, len(bins) - 2)
    return (np.bincount(indexes, minlength=len(bins) - 1) / len(values)).tolist()


def build_reference(
    features: list[Features],
    *,
    model_version: str,
    dataset_version: str,
    reference_source: str,
    bin_count: int = 10,
    failure_rate: float | None = None,
) -> ReferenceProfile:
    """Offline export only. The worker never learns bins from the runtime window."""
    if not features or bin_count < 2:
        raise ValueError("Reference export requires samples and at least two bins")
    profiles = {}
    for name in NUMERIC_FEATURES:
        values = [getattr(sample, name) for sample in features]
        bins = np.unique(np.quantile(values, np.linspace(0, 1, bin_count + 1)))
        if len(bins) < 3:
            # Constant/near-discrete reference still needs edge buckets to detect shifts.
            lo, hi = min(values), max(values)
            padding = max(1.0, abs(lo) * 0.01)
            bins = np.linspace(lo - padding, hi + padding, bin_count + 1)
        edges = bins.tolist()
        profiles[name] = NumericProfile(
            type="numeric",
            bins=edges,
            probabilities=histogram(values, edges),
        )
    categories = ["L", "M", "H"]
    profiles["product_type"] = CategoricalProfile(
        type="categorical",
        categories=categories,
        probabilities=[
            sum(row.product_type == value for row in features) / len(features)
            for value in categories
        ],
    )
    return ReferenceProfile(
        dataset_version=dataset_version,
        model_version=model_version,
        created_at=datetime.now(UTC),
        sample_count=len(features),
        features=ReferenceFeatures(**profiles),
        reference_source=reference_source,
        prediction_reference=None
        if failure_rate is None
        else PredictionReference(
            failure_rate=failure_rate,
        ),
    )


def reference_features_from_jsonl(path: Path) -> list[Features]:
    # Input must be an explicitly selected training/approved reference file, not live DB data.
    rows = []
    with path.open() as stream:
        for line in stream:
            payload = json.loads(line)
            rows.append(Features.model_validate(payload.get("features", payload)))
    return rows
