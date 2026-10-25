"""Record/delivery faults; intentionally separate from valid drift transforms."""

import copy
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from simulator.models import FEATURE_NAMES


class FaultMode(StrEnum):
    NONE = "none"
    MISSING_FIELD = "missing_field"
    INVALID_TYPE = "invalid_type"
    OUT_OF_RANGE = "out_of_range"
    DUPLICATE_EVENT = "duplicate_event"
    NETWORK_TIMEOUT = "network_timeout"
    NETWORK_DISCONNECT = "network_disconnect"
    INVALID_ENVELOPE = "invalid_envelope"


@dataclass(frozen=True)
class InjectedEvent:
    payload: dict[str, Any]
    fault: FaultMode = FaultMode.NONE
    duplicate: bool = False

    @property
    def expect_rejection(self) -> bool:
        # Invalid feature values still have a valid ingress envelope (HTTP202).
        return self.fault == FaultMode.INVALID_ENVELOPE


class FaultInjector:
    def __init__(
        self,
        mode: FaultMode = FaultMode.NONE,
        *,
        every: int = 1,
        field: str = "rotational_speed_rpm",
    ):
        if every < 1:
            raise ValueError("fault_every must be positive")
        if field not in FEATURE_NAMES:
            raise ValueError(f"Unknown fault field: {field}")
        if mode == FaultMode.OUT_OF_RANGE and field not in {
            "rotational_speed_rpm",
            "tool_wear_min",
        }:
            raise ValueError("out_of_range supports rotational_speed_rpm or tool_wear_min")
        self.mode, self.every, self.field = mode, every, field

    def apply(self, payload: dict[str, Any], sequence: int) -> InjectedEvent:
        result = copy.deepcopy(payload)
        if self.mode == FaultMode.NONE or (sequence + 1) % self.every:
            return InjectedEvent(result)
        if self.mode == FaultMode.MISSING_FIELD:
            del result["features"][self.field]
        elif self.mode == FaultMode.INVALID_TYPE:
            result["features"][self.field] = 123 if self.field == "product_type" else "invalid"
        elif self.mode == FaultMode.OUT_OF_RANGE:
            result["features"][self.field] = -1
        elif self.mode == FaultMode.INVALID_ENVELOPE:
            del result["machine_id"]
        return InjectedEvent(result, self.mode, self.mode == FaultMode.DUPLICATE_EVENT)
