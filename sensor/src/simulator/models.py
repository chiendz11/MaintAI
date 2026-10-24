"""Generated readings use the six feature allowlist and SensorRequest 1.3.0.

Only contract ranges are enforced here. Machine-specific domain ranges belong
to the backend's versioned validator, not to the simulator.
"""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

PRODUCT_TYPES = ("L", "M", "H")
NUMERIC_FEATURES = (
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
)
FEATURE_NAMES = ("product_type", *NUMERIC_FEATURES)


class Features(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    product_type: Literal["L", "M", "H"]
    air_temperature_k: float
    process_temperature_k: float
    rotational_speed_rpm: float = Field(gt=0)
    torque_nm: float
    tool_wear_min: float = Field(ge=0)


class SensorEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    schema_version: Literal["1.3.0"] = "1.3.0"
    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    stream_epoch: str = Field(min_length=1)
    sequence_number: int = Field(strict=True, ge=1)
    received_at: AwareDatetime
    source: Literal["AI4I_SIMULATOR"]
    features: Features


def sensor_event_schema() -> dict:
    """Export the stricter generated-reading model (not the loose ingress DQ envelope)."""
    schema = SensorEvent.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "urn:maintai:contracts:generated-sensor-request:1.3.0"
    return schema
