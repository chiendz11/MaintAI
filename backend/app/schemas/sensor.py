import re
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from app.schemas.errors import ApiError

RFC3339_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})\Z"
)


class SensorFeatures(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    product_type: Literal["L", "M", "H"]
    air_temperature_k: float
    process_temperature_k: float
    rotational_speed_rpm: float = Field(gt=0)
    torque_nm: float
    tool_wear_min: float = Field(ge=0)


class SensorReadingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    received_at: AwareDatetime = Field(strict=False)
    source: Literal["AI4I_SIMULATOR"]
    features: SensorFeatures

    @field_validator("received_at", mode="before")
    @classmethod
    def require_rfc3339_timestamp(cls, value: object) -> str:
        if not isinstance(value, str) or RFC3339_TIMESTAMP.fullmatch(value) is None:
            raise ValueError("received_at must be an RFC3339 timestamp with timezone")
        return value


class ValidationIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1)
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)


class SensorInvalidResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    validation_status: Literal["INVALID"]
    machine_state: Literal["INVALID"]
    errors: list[ValidationIssue] = Field(min_length=1)
    prediction_id: None
    alert_id: None
    correlation_id: str = Field(min_length=1)


class SensorCompletedResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    validation_status: Literal["VALID"]
    processing_status: Literal["COMPLETED"]
    machine_state: Literal["NORMAL", "WATCH", "HIGH", "CRITICAL"]
    prediction_id: str = Field(min_length=1)
    alert_id: str | None = Field(min_length=1)
    correlation_id: str = Field(min_length=1)


class SensorDependencyFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ApiError
    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    validation_status: Literal["VALID"]
    processing_status: Literal["DEPENDENCY_FAILED"]
    machine_state: Literal["UNKNOWN"]
    prediction_id: None
    alert_id: None
    correlation_id: str = Field(min_length=1)
