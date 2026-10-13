import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class Policy(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    version: str = "drift-policy-1.0.0"
    warning: float = Field(default=0.10, ge=0)
    critical: float = Field(default=0.20, gt=0)
    epsilon: float = Field(default=1e-6, gt=0, lt=0.1)
    critical_if_critical_features_gte: int = Field(default=2, ge=1, le=6)
    warning_if_warning_features_gte: int = Field(default=1, ge=1, le=6)

    @model_validator(mode="after")
    def thresholds(self) -> "Policy":
        if self.warning >= self.critical:
            raise ValueError("PSI warning must be less than critical")
        return self


class DriftConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    model_version: str = Field(min_length=1)
    dataset_version: str = Field(min_length=1)
    reference_path: Path
    window_size: int = Field(default=500, ge=1)
    minimum_samples: int = Field(default=200, ge=1)
    detector_version: str = "drift-worker-1.0.0"
    numeric_metric: Literal["PSI"] = "PSI"
    categorical_metric: Literal["PSI"] = "PSI"
    policy: Policy = Field(default_factory=Policy)
    worker_name: str = Field(default="maintai-drift-worker", min_length=1)
    database_url: SecretStr | None = Field(default=None, repr=False, exclude=True)
    pushgateway_url: str | None = None
    pushgateway_required: bool = False
    metrics_job: str = "maintai-drift-worker"
    environment: str = "local"
    timeout_seconds: float = Field(default=5, gt=0)

    @model_validator(mode="after")
    def coherent_window(self) -> "DriftConfig":
        if self.minimum_samples > self.window_size:
            raise ValueError("minimum_samples cannot exceed window_size")
        if self.pushgateway_required and not self.pushgateway_url:
            raise ValueError("Required Pushgateway needs a URL")
        return self

    @classmethod
    def load(cls, path: Path | None, overrides: dict | None = None) -> "DriftConfig":
        values = {}
        if path:
            raw = yaml.safe_load(path.read_text())
            if not isinstance(raw, dict) or not isinstance(raw.get("drift"), dict):
                raise ValueError("Config YAML must contain a drift mapping")
            values = raw["drift"]
            # Resolve artifact paths relative to the YAML rather than the process cwd.
            if "reference_path" in values:
                reference = Path(values["reference_path"])
                if not reference.is_absolute():
                    values["reference_path"] = path.resolve().parent / reference
            # Accept the guide's nested policy shape as well as the explicit snapshot shape.
            policy = dict(values.pop("policy", {}))
            psi = values.pop("psi", {})
            aggregation = values.pop("aggregation", {})
            policy.update(psi)
            policy.update(aggregation)
            if policy:
                values["policy"] = policy
        env_names = {
            "model_version": "DRIFT_MODEL_VERSION",
            "dataset_version": "DRIFT_DATASET_VERSION",
            "reference_path": "DRIFT_REFERENCE_PATH",
            "database_url": "DRIFT_DATABASE_URL",
            "window_size": "DRIFT_WINDOW_SIZE",
            "minimum_samples": "DRIFT_MINIMUM_SAMPLES",
            "worker_name": "DRIFT_WORKER_NAME",
            "pushgateway_url": "DRIFT_PUSHGATEWAY_URL",
            "pushgateway_required": "DRIFT_PUSHGATEWAY_REQUIRED",
            "environment": "ENVIRONMENT",
        }
        values.update(
            {field: os.environ[name] for field, name in env_names.items() if name in os.environ}
        )
        values.update({key: value for key, value in (overrides or {}).items() if value is not None})
        return cls.model_validate(values)
