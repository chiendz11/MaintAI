"""Versionable YAML scenarios with independently seeded feature transforms."""

import math
import random
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from simulator.models import FEATURE_NAMES, PRODUCT_TYPES, Features


class ScenarioError(ValueError):
    pass


class ScenarioModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Shift(ScenarioModel):
    type: Literal["shift"]
    delta: float


class Scale(ScenarioModel):
    type: Literal["scale"]
    factor: float = Field(gt=0)


class LinearShift(ScenarioModel):
    type: Literal["linear_shift"]
    start_delta: float
    end_delta: float


class LinearScale(ScenarioModel):
    type: Literal["linear_scale"]
    start_factor: float = Field(gt=0)
    end_factor: float = Field(gt=0)


class CategoryMix(ScenarioModel):
    type: Literal["categorical_mix"]
    probabilities: dict[Literal["L", "M", "H"], Annotated[float, Field(ge=0, le=1)]]

    @model_validator(mode="after")
    def normalized_probabilities(self) -> "CategoryMix":
        if not math.isclose(sum(self.probabilities.values()), 1.0, abs_tol=1e-9):
            raise ValueError("Categorical probabilities must sum to 1")
        return self


Transform = Annotated[
    Shift | Scale | LinearShift | LinearScale | CategoryMix,
    Field(discriminator="type"),
]


class Phase(ScenarioModel):
    name: str = Field(min_length=1)
    events: int = Field(gt=0, strict=True)
    transforms: dict[str, Transform] = Field(default_factory=dict)

    @model_validator(mode="after")
    def known_features(self) -> "Phase":
        for feature, transform in self.transforms.items():
            if feature not in FEATURE_NAMES:
                raise ValueError(f"Forbidden/unknown scenario feature: {feature}")
            if (feature == "product_type") != isinstance(transform, CategoryMix):
                raise ValueError("product_type requires categorical_mix; numeric fields do not")
        return self


class Scenario(ScenarioModel):
    name: str = Field(min_length=1)
    seed: int = Field(strict=True)
    phases: list[Phase] = Field(min_length=1)

    @property
    def event_count(self) -> int:
        return sum(phase.events for phase in self.phases)


class ScenarioLoader:
    @staticmethod
    def load(path: Path) -> Scenario:
        try:
            return Scenario.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        except (OSError, yaml.YAMLError, ValidationError) as exc:
            raise ScenarioError(f"Invalid scenario {path}: {exc}") from exc


class ScenarioTransformer:
    def __init__(self, scenario: Scenario, *, seed: int | None = None):
        self.scenario = scenario
        self.random = random.Random(scenario.seed if seed is None else seed)

    def apply(self, features: Features, sequence: int) -> tuple[Features, str]:
        if sequence < 0:
            raise ScenarioError("sequence must be non-negative")
        offset = sequence
        for phase in self.scenario.phases:
            if offset < phase.events:
                break
            offset -= phase.events
        else:
            raise ScenarioError("Scenario is exhausted")
        progress = offset / (phase.events - 1) if phase.events > 1 else 1.0
        values = features.model_dump()
        for feature, transform in phase.transforms.items():
            if isinstance(transform, Shift):
                values[feature] += transform.delta
            elif isinstance(transform, Scale):
                values[feature] *= transform.factor
            elif isinstance(transform, LinearShift):
                values[feature] += transform.start_delta + progress * (
                    transform.end_delta - transform.start_delta
                )
            elif isinstance(transform, LinearScale):
                values[feature] *= transform.start_factor + progress * (
                    transform.end_factor - transform.start_factor
                )
            elif isinstance(transform, CategoryMix):
                weights = [transform.probabilities.get(category, 0) for category in PRODUCT_TYPES]
                values[feature] = self.random.choices(PRODUCT_TYPES, weights=weights, k=1)[0]
        try:
            return Features.model_validate(values), phase.name
        except ValidationError as exc:
            # Do not clip/impute a transform into validity or mislabel it as a fault.
            raise ScenarioError(
                f"Scenario {self.scenario.name}, phase {phase.name}, event {sequence}: "
                f"transform produced invalid features: {exc}"
            ) from exc
