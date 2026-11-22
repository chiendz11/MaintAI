"""Pluggable machine reading sources decoupled from Edge Agent spool and HTTP transport."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from simulator.dataset import DatasetReader
from simulator.ordering.store import canonical
from simulator.scenarios import ScenarioLoader, ScenarioTransformer
from simulator.settings import RuntimeSettings


@dataclass(frozen=True)
class ReadingSample:
    """Single validated domain feature reading returned by a DataSource."""

    features: dict[str, Any]
    source: str = "AI4I_SIMULATOR"


@runtime_checkable
class DataSource(Protocol):
    """Source interface separating data acquisition/simulation from Edge Agent orchestration."""

    scenarios: dict[str, Any]

    def source_identity(self, *, machine_count: int, seed: int | None) -> str: ...

    def read_sample(self, machine: dict[str, Any]) -> ReadingSample | None: ...


class CsvSimulatorSource:
    """Dataset-backed machine simulator with deterministic per-machine scenario transforms."""

    def __init__(self, settings: RuntimeSettings):
        self.dataset_path = settings.dataset_path
        self.loop_dataset = settings.loop_dataset
        self.seed = settings.seed
        self.records = list(DatasetReader(settings.dataset_path))
        self.scenarios = {
            p.stem: ScenarioLoader.load(p) for p in sorted(settings.scenario_path.glob("*.yaml"))
        }
        if settings.scenario not in self.scenarios:
            raise ValueError("Initial scenario is not in the configured allowlist")

    def source_identity(self, *, machine_count: int, seed: int | None) -> str:
        return hashlib.sha256(
            canonical(
                {
                    "dataset": hashlib.sha256(self.dataset_path.read_bytes()).hexdigest(),
                    "scenarios": {
                        name: value.model_dump(mode="json")
                        for name, value in self.scenarios.items()
                    },
                    "machines": machine_count,
                    "seed": seed,
                    "schema": "1.3.0",
                    "generation_version": "1",
                }
            ).encode()
        ).hexdigest()

    def read_sample(self, machine: dict[str, Any]) -> ReadingSample | None:
        cursor = machine["row_cursor"]
        if not self.loop_dataset and cursor >= len(self.records):
            return None
        record = self.records[cursor % len(self.records)]
        scenario = self.scenarios[machine["scenario"]]
        step = min(machine["scenario_step"], scenario.event_count - 1)
        # Derive categorical RNG from durable position, never a volatile RNG cursor.
        derived_seed = int(
            hashlib.sha256(
                (
                    f"{scenario.seed if self.seed is None else self.seed}:"
                    f"{machine['machine_id']}:{machine['scenario_step']}"
                ).encode()
            ).hexdigest()[:16],
            16,
        )
        features, _ = ScenarioTransformer(scenario, seed=derived_seed).apply(record.features, step)
        return ReadingSample(features=features.model_dump(), source="AI4I_SIMULATOR")
