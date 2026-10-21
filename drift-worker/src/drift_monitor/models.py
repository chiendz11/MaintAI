import math
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

FeatureName = Literal[
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
]
FEATURE_NAMES = (
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
)
NUMERIC_FEATURES = FEATURE_NAMES[1:]
Probability = Annotated[float, Field(ge=0, le=1)]


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Features(WireModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    product_type: Literal["L", "M", "H"]
    air_temperature_k: float
    process_temperature_k: float
    rotational_speed_rpm: float = Field(gt=0)
    torque_nm: float
    tool_wear_min: float = Field(ge=0)


class DriftStatus(StrEnum):
    STABLE = "STABLE"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    ERROR = "ERROR"


class NumericProfile(WireModel):
    type: Literal["numeric"]
    metric: Literal["PSI"] = "PSI"
    bins: list[float] = Field(min_length=2)
    probabilities: list[Probability]

    @model_validator(mode="after")
    def valid_distribution(self) -> "NumericProfile":
        if len(self.probabilities) != len(self.bins) - 1:
            raise ValueError("Numeric probabilities must match len(bins)-1")
        if any(left >= right for left, right in zip(self.bins, self.bins[1:], strict=False)):
            raise ValueError("Reference bins must be strictly increasing")
        if not math.isclose(sum(self.probabilities), 1, abs_tol=1e-9):
            raise ValueError("Reference probabilities must sum to 1")
        return self


class CategoricalProfile(WireModel):
    type: Literal["categorical"]
    metric: Literal["PSI"] = "PSI"
    categories: list[Literal["L", "M", "H"]]
    probabilities: list[Probability]

    @model_validator(mode="after")
    def valid_distribution(self) -> "CategoricalProfile":
        if len(self.categories) != 3 or set(self.categories) != {"L", "M", "H"}:
            raise ValueError("Categories must contain each of L/M/H exactly once")
        if len(self.probabilities) != 3 or not math.isclose(
            sum(self.probabilities),
            1,
            abs_tol=1e-9,
        ):
            raise ValueError("Category probabilities must sum to 1 in reference ordering")
        return self


class ReferenceFeatures(WireModel):
    product_type: CategoricalProfile
    air_temperature_k: NumericProfile
    process_temperature_k: NumericProfile
    rotational_speed_rpm: NumericProfile
    torque_nm: NumericProfile
    tool_wear_min: NumericProfile


class PredictionReference(WireModel):
    failure_rate: Probability


class ReferenceProfile(WireModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_version: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    created_at: AwareDatetime
    sample_count: int = Field(ge=1)
    features: ReferenceFeatures
    prediction_reference: PredictionReference | None = None
    reference_source: Literal["training", "approved_reference", "test_fixture"] | None = None


class RuntimeSample(WireModel):
    event_id: str = Field(min_length=1)
    received_at: AwareDatetime
    features: Features
    prediction_label: Literal["NORMAL", "FAILURE"] | None = None


class ReferenceIdentity(WireModel):
    dataset_version: str
    model_version: str


class CurrentWindow(WireModel):
    start_at: AwareDatetime
    end_at: AwareDatetime
    sample_count: int = Field(ge=0)


class Summary(WireModel):
    total_features: Literal[6] = 6
    warning_features: int = Field(default=0, ge=0, le=6)
    critical_features: int = Field(default=0, ge=0, le=6)


class FeatureResult(WireModel):
    feature: FeatureName
    feature_type: Literal["numeric", "categorical"]
    metric: Literal["PSI"] = "PSI"
    score: float = Field(ge=0)
    warning_threshold: float
    critical_threshold: float
    status: DriftStatus


class PredictionDrift(WireModel):
    reference_failure_rate: Probability
    current_failure_rate: Probability | None
    sample_count: int = Field(ge=0)
    score: float | None = Field(default=None, ge=0)
    metric: Literal["PSI"] = "PSI"
    status: DriftStatus
    warning_threshold: float
    critical_threshold: float


class DriftReport(WireModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    report_id: str
    computed_at: AwareDatetime
    detector_version: str
    reference: ReferenceIdentity
    current_window: CurrentWindow
    overall_status: DriftStatus
    summary: Summary = Field(default_factory=Summary)
    features: list[FeatureResult] = Field(default_factory=list)
    prediction_drift: PredictionDrift | None = None
    reason: str | None = None
    policy: dict


class Checkpoint(WireModel):
    worker_name: str
    last_processed_event_at: AwareDatetime | None
    last_processed_event_id: str | None
    last_success_at: AwareDatetime
    last_report_id: str
