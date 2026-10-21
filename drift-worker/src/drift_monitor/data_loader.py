import json
from datetime import datetime
from pathlib import Path

from drift_monitor.models import Features, RuntimeSample


def load_jsonl(path: Path, model_version: str) -> list[RuntimeSample]:
    """Offline fixture path. DB runtime filtering is handled by the repository.

    Sensor wire JSONL has no backend status; validate its features and aware
    timestamp here. Explicit INVALID records are excluded, never auto-imputed.
    """
    samples = []
    seen = {}
    with path.open() as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                event = json.loads(line)
                if event.get("validation_status", "VALID") != "VALID":
                    continue
                prediction = event.get("prediction") or {}
                label = (
                    prediction.get("label")
                    if prediction.get("model_version") == model_version
                    else None
                )
                if not isinstance(event["received_at"], str):
                    raise ValueError("received_at must be an RFC3339 string")
                sample = RuntimeSample(
                    event_id=event["event_id"],
                    received_at=datetime.fromisoformat(event["received_at"].replace("Z", "+00:00")),
                    features=Features.model_validate(event["features"]),
                    prediction_label=label,
                )
                if sample.event_id in seen:
                    if seen[sample.event_id] != sample:
                        raise ValueError("Conflicting duplicate event")
                    continue
                seen[sample.event_id] = sample
                samples.append(sample)
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"Invalid runtime JSONL at line {line_number}") from exc
    return samples
