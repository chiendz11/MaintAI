"""Explicit policy input; no production thresholds are supplied by this module."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

from maintai_messaging.contracts import canonical_json, strict_json


@dataclass(frozen=True)
class RiskPolicy:
    policy_version: str
    watch: float
    high: float
    critical: float
    create_alert_for: tuple[str, ...]
    policy_sha256: str = field(init=False)

    def __post_init__(self):
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("Risk policy version is required")
        limits = (self.watch, self.high, self.critical)
        if any(type(n) not in (int, float) or not 0 <= n <= 1 for n in limits):
            raise ValueError("Risk thresholds must be finite probabilities")
        if not self.watch < self.high < self.critical:
            raise ValueError("Risk thresholds must be strictly increasing")
        levels = self.create_alert_for
        if (
            not isinstance(levels, tuple)
            or any(
                not isinstance(level, str) or level not in {"WATCH", "HIGH", "CRITICAL"}
                for level in levels
            )
            or len(set(levels)) != len(levels)
        ):
            raise ValueError("create_alert_for must contain distinct immutable risk levels")
        content = {
            "policy_version": self.policy_version,
            "thresholds": {"watch": self.watch, "high": self.high, "critical": self.critical},
            "create_alert_for": list(levels),
        }
        object.__setattr__(
            self, "policy_sha256", hashlib.sha256(canonical_json(content)).hexdigest()
        )

    @classmethod
    def from_dict(cls, value: dict) -> RiskPolicy:
        if not isinstance(value, dict) or set(value) != {
            "policy_version",
            "thresholds",
            "create_alert_for",
        }:
            raise ValueError("Explicit risk policy with contract fields is required")
        thresholds = value["thresholds"]
        if not isinstance(thresholds, dict) or set(thresholds) != {"watch", "high", "critical"}:
            raise ValueError("Risk thresholds must specify watch, high and critical")
        levels = value["create_alert_for"]
        if not isinstance(levels, list):
            raise ValueError("create_alert_for must be a JSON array")
        return cls(
            value["policy_version"],
            thresholds["watch"],
            thresholds["high"],
            thresholds["critical"],
            tuple(levels),
        )

    @classmethod
    def from_file(cls, path: Path) -> RiskPolicy:
        return cls.from_dict(strict_json(Path(path).read_bytes()))
