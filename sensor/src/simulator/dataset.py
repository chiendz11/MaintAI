"""Read the six AI4I features without ever forwarding target/identifier columns."""

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from simulator.models import Features

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}


class DatasetError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetRecord:
    features: Features
    row_number: int
    # Offline evidence only. EventFactory never reads this label.
    ground_truth: int | None = None


class DatasetReader:
    def __init__(self, path: Path):
        self.path = path

    def __iter__(self) -> Iterator[DatasetRecord]:
        try:
            with self.path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                fields = reader.fieldnames or []
                missing = set(COLUMN_MAP) - set(fields)
                if missing:
                    raise DatasetError(f"Missing AI4I columns: {', '.join(sorted(missing))}")
                if len(fields) != len(set(fields)):
                    raise DatasetError("Duplicate CSV column names")
                rows = 0
                for row_number, row in enumerate(reader, start=2):
                    values = {}
                    try:
                        for column, feature in COLUMN_MAP.items():
                            raw = row[column]
                            values[feature] = raw if feature == "product_type" else float(raw)
                        features = Features.model_validate(values)
                        label = row.get("Machine failure")
                        ground_truth = None if label in (None, "") else int(label)
                        if ground_truth not in (None, 0, 1):
                            raise ValueError("Machine failure must be 0 or 1")
                    except (TypeError, ValueError, ValidationError) as exc:
                        raise DatasetError(f"Invalid AI4I row at line {row_number}: {exc}") from exc
                    rows += 1
                    yield DatasetRecord(features, row_number, ground_truth)
                if not rows:
                    raise DatasetError("AI4I dataset has no records")
        except OSError as exc:
            raise DatasetError(f"Cannot read dataset {self.path}: {exc}") from exc
