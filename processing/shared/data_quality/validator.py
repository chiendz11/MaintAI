"""Feature-only DQ: exact schema types/ranges, finite numbers, no imputation.

A VALID result here is not a complete event validation: the worker must also
check the machine registry before inference (System Contracts section 5.4).
"""

from __future__ import annotations

import copy
import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from jsonschema import Draft202012Validator, validators
from maintai_messaging.contracts import canonical_json, strict_json

_JSONFeatureValidator = validators.extend(
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine(
        "number", lambda checker, value: type(value) in (int, float)
    ),
)


@dataclass(frozen=True)
class ValidationIssue:
    field: str
    code: str
    message: str


@dataclass(frozen=True)
class DQResult:
    status: Literal["VALID", "INVALID"]
    validator_version: str
    errors: tuple[ValidationIssue, ...]
    normalized_features: dict | None


class FeatureValidator:
    """Load the actual versioned schema; never install guessed domain limits."""

    def __init__(self, schema: dict, *, version: str):
        if not isinstance(version, str) or not version.strip():
            raise ValueError("DQ ruleset version is required")
        Draft202012Validator.check_schema(schema)
        schema = copy.deepcopy(schema)
        self._validator = _JSONFeatureValidator(schema)
        self.version = version
        rules = {
            "validator_version": version,
            "feature_schema": schema,
            "finite_numbers_required": True,
        }
        self.ruleset_sha256 = hashlib.sha256(canonical_json(rules)).hexdigest()

    @classmethod
    def from_file(cls, path: Path, *, version: str) -> FeatureValidator:
        return cls(strict_json(Path(path).read_bytes()), version=version)

    def validate(self, features: object) -> DQResult:
        issues = []
        if isinstance(features, dict):
            # JSON Schema's number type can accept NaN/Infinity from direct Python
            # callers. Kafka's strict JSON boundary rejects them independently.
            for name in sorted(self._validator.schema.get("properties", {})):
                value = features.get(name)
                if isinstance(value, float) and not math.isfinite(value):
                    issues.append(
                        ValidationIssue(
                            f"features.{name}", "NON_FINITE", "Feature must be a finite number"
                        )
                    )
        codes = {
            "required": ("MISSING_FIELD", "Required feature is missing"),
            "type": ("INVALID_TYPE", "Feature has an invalid JSON type"),
            "enum": ("UNSUPPORTED_VALUE", "Feature category is not supported"),
            "minimum": ("OUT_OF_RANGE", "Feature is outside the configured range"),
            "maximum": ("OUT_OF_RANGE", "Feature is outside the configured range"),
            "exclusiveMinimum": ("OUT_OF_RANGE", "Feature is outside the configured range"),
            "exclusiveMaximum": ("OUT_OF_RANGE", "Feature is outside the configured range"),
            "additionalProperties": ("UNEXPECTED_FIELD", "Unexpected model feature"),
        }
        for error in self._validator.iter_errors(features):
            code, message = codes.get(error.validator, ("SCHEMA_VIOLATION", "Invalid feature"))
            field = ".".join(["features", *(str(part) for part in error.absolute_path)])
            issues.append(ValidationIssue(field, code, message))
        issues.sort(key=lambda issue: (issue.field, issue.code))
        return DQResult(
            status="INVALID" if issues else "VALID",
            validator_version=self.version,
            errors=tuple(issues),
            normalized_features=None if issues else copy.deepcopy(features),
        )
