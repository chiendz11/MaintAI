import os
import sys
from pathlib import Path
from typing import Literal

from pydantic import (
    AnyHttpUrl,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from simulator.fault_injection import FaultMode


def default_scenario_dir() -> Path:
    source = Path(__file__).resolve().parents[2] / "scenarios"
    return source if source.is_dir() else Path(sys.prefix) / "share/maintai-sensor/scenarios"


def default_dataset_path() -> Path:
    source = Path(__file__).resolve().parents[2] / "data"
    return (source if source.is_dir() else Path.cwd() / "data") / "ai4i2020.csv"


class SimulatorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    backend_base_url: AnyHttpUrl = AnyHttpUrl("http://localhost:8000")
    dataset_path: Path = Field(default_factory=default_dataset_path)
    interval_ms: float = Field(
        default=1000, ge=0, description="Logical interval between readings from the same machine"
    )
    speed: float = Field(default=1, gt=0)
    seed: int | None = None  # None uses the scenario's deterministic seed.
    machine_count: int = Field(default=5, ge=1)
    fault_mode: FaultMode = FaultMode.NONE
    fault_every: int = Field(default=1, ge=1)
    fault_field: str = "rotational_speed_rpm"
    scenario: str = "normal"
    scenario_path: Path = Field(default_factory=default_scenario_dir)
    scenario_file: Path | None = None
    timeout_seconds: float = Field(default=5, gt=0)
    max_retries: int = Field(default=2, ge=0, le=10)
    retry_backoff_ms: float = Field(default=100, ge=0)
    on_error: Literal["continue", "stop"] = "continue"
    limit: int | None = Field(default=None, ge=1)
    loop_dataset: bool = False
    dry_run: bool = False
    output: Path | None = None
    metrics_file: Path | None = None
    run_id: str | None = Field(default=None, min_length=1)
    start_time: AwareDatetime | None = None

    @field_validator("backend_base_url")
    @classmethod
    def safe_base_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username or value.password or value.query or value.fragment:
            raise ValueError("Backend base URL cannot contain credentials, query or fragment")
        return value

    @classmethod
    def from_env(cls, overrides: dict | None = None) -> "SimulatorConfig":
        names = {
            "backend_base_url": "BACKEND_BASE_URL",
            "dataset_path": "SIM_DATASET_PATH",
            "interval_ms": "SIM_INTERVAL_MS",
            "speed": "SIM_SPEED",
            "seed": "SIM_SEED",
            "machine_count": "SIM_MACHINE_COUNT",
            "fault_mode": "SIM_FAULT_MODE",
            "fault_every": "SIM_FAULT_EVERY",
            "fault_field": "SIM_FAULT_FIELD",
            "scenario": "SIM_SCENARIO",
            "scenario_path": "SIM_SCENARIO_PATH",
            "timeout_seconds": "SIM_TIMEOUT_SECONDS",
            "max_retries": "SIM_MAX_RETRIES",
            "retry_backoff_ms": "SIM_RETRY_BACKOFF_MS",
            "on_error": "SIM_ON_ERROR",
            "limit": "SIM_LIMIT",
            "loop_dataset": "SIM_LOOP_DATASET",
            "run_id": "SIM_RUN_ID",
            "start_time": "SIM_START_TIME",
        }
        values = {field: os.environ[name] for field, name in names.items() if name in os.environ}
        values.update({key: value for key, value in (overrides or {}).items() if value is not None})
        return cls.model_validate(values)

    @property
    def scenario_yaml(self) -> Path:
        if self.scenario_file:
            return self.scenario_file
        return self.scenario_path / f"{self.scenario.removesuffix('.yaml')}.yaml"

    @property
    def delay_seconds(self) -> float:
        # Stagger the machines evenly while keeping each machine's sampling interval.
        return self.interval_ms / 1000 / self.machine_count / self.speed
