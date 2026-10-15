"""Record-level validation after transport schema parsing.

The current contract's specified checks live in SensorReadingRequest. Additional
machine-specific ranges require an approved, versioned policy before use.
"""

from dataclasses import dataclass
from typing import Literal

from app.schemas.sensor import SensorReadingRequest, ValidationIssue

VALIDATOR_VERSION = "sensor-validator-1.0.0"


@dataclass(frozen=True)
class ValidationResult:
    status: Literal["VALID", "INVALID"]
    validator_version: str
    errors: tuple[ValidationIssue, ...]


class DataQualityValidator:
    def validate(self, event: SensorReadingRequest) -> ValidationResult:
        # Pydantic has already enforced all ranges currently fixed by contract.
        return ValidationResult("VALID", VALIDATOR_VERSION, ())
