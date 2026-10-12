import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TextIO
from uuid import uuid5

from simulator.client import BackendClient
from simulator.config import SimulatorConfig
from simulator.dataset import DatasetReader
from simulator.event_factory import EventFactory
from simulator.fault_injection import FaultInjector, FaultMode
from simulator.logging import log
from simulator.scenarios import Scenario, ScenarioTransformer


@dataclass
class ReplayStats:
    generated: int = 0
    deliveries: int = 0
    accepted: int = 0
    rejected: int = 0
    failed: int = 0
    expected_rejections: int = 0
    unexpected_results: int = 0
    retries: int = 0
    duplicates: int = 0
    interrupted: bool = False
    duration_ms: float = 0

    @property
    def exit_code(self) -> int:
        return 130 if self.interrupted else int(self.unexpected_results > 0)

    def prometheus_text(self) -> str:
        # Batch text artifact; no HTTP daemon and no event/machine ID metric labels.
        lines = [
            "# HELP maintai_simulator_deliveries_total Logical deliveries, excluding retries.",
            "# TYPE maintai_simulator_deliveries_total counter",
        ]
        for outcome in ("accepted", "rejected", "failed"):
            lines.append(
                f'maintai_simulator_deliveries_total{{outcome="{outcome}"}} '
                f"{getattr(self, outcome)}"
            )
        for name in (
            "generated",
            "retries",
            "duplicates",
            "expected_rejections",
            "unexpected_results",
        ):
            lines.extend(
                [
                    f"# TYPE maintai_simulator_{name}_total counter",
                    f"maintai_simulator_{name}_total {getattr(self, name)}",
                ]
            )
        return "\n".join(lines) + "\n"


class ReplayRunner:
    def __init__(
        self,
        config: SimulatorConfig,
        scenario: Scenario,
        *,
        client: BackendClient | None = None,
        output: TextIO | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.config, self.scenario = config, scenario
        self.client, self.output = client, output
        self.sleep, self.monotonic = sleep, monotonic
        if not config.dry_run and client is None:
            raise ValueError("HTTP replay requires a BackendClient")
        seed = scenario.seed if config.seed is None else config.seed
        self.factory = EventFactory(
            seed=seed,
            machine_count=config.machine_count,
            interval_ms=config.interval_ms,
            start_time=config.start_time,
            run_id=config.run_id,
        )
        self.transformer = ScenarioTransformer(scenario, seed=seed)
        self.injector = FaultInjector(
            config.fault_mode,
            every=config.fault_every,
            field=config.fault_field,
        )

    def run(self) -> ReplayStats:
        stats = ReplayStats()
        records = iter(DatasetReader(self.config.dataset_path))
        count = min(self.scenario.event_count, self.config.limit or self.scenario.event_count)
        started = next_slot = self.monotonic()
        log(
            "replay_started",
            run_id=self.factory.run_id,
            scenario=self.scenario.name,
            seed=self.scenario.seed if self.config.seed is None else self.config.seed,
            dry_run=self.config.dry_run,
            planned_events=count,
        )
        try:
            for sequence in range(count):
                record = next(records, None)
                if record is None:
                    if not self.config.loop_dataset:
                        log("dataset_exhausted", generated=stats.generated)
                        break
                    records = iter(DatasetReader(self.config.dataset_path))
                    record = next(records)
                features, phase_name = self.transformer.apply(record.features, sequence)
                event = self.factory.create(features, sequence)
                injected = self.injector.apply(event.model_dump(mode="json"), sequence)
                if not self.config.dry_run:
                    wait = max(0.0, next_slot - self.monotonic())
                    if wait:
                        self.sleep(wait)
                stats.generated += 1
                correlation_id = f"cor_{uuid5(self.factory.namespace, f'correlation:{sequence}')}"
                for duplicate_index in range(2 if injected.duplicate else 1):
                    if duplicate_index:
                        stats.duplicates += 1
                    if self.output is not None:
                        self.output.write(
                            json.dumps(
                                injected.payload,
                                sort_keys=True,
                                allow_nan=False,
                            )
                            + "\n"
                        )
                    if self.config.dry_run:
                        continue
                    result = self.client.send(
                        injected.payload,
                        correlation_id=correlation_id,
                        fault=injected.fault if not duplicate_index else FaultMode.NONE,
                    )
                    stats.deliveries += 1
                    stats.retries += result.attempts - 1
                    setattr(stats, result.outcome, getattr(stats, result.outcome) + 1)
                    expected = result.outcome == (
                        "rejected" if injected.expect_rejection else "accepted"
                    )
                    if injected.expect_rejection and result.outcome == "rejected":
                        stats.expected_rejections += 1
                    if not expected:
                        stats.unexpected_results += 1
                        log(
                            "unexpected_delivery_result",
                            event_id=event.event_id,
                            machine_id=event.machine_id,
                            correlation_id=correlation_id,
                            phase=phase_name,
                            fault=injected.fault.value,
                            outcome=result.outcome,
                            status_code=result.status_code,
                            error=result.error,
                        )
                        if self.config.on_error == "stop":
                            return self._finish(stats, started)
                next_slot += self.config.delay_seconds
        except KeyboardInterrupt:
            stats.interrupted = True
        finally:
            records.close()
        return self._finish(stats, started)

    def _finish(self, stats: ReplayStats, started: float) -> ReplayStats:
        stats.duration_ms = round((self.monotonic() - started) * 1000, 3)
        if self.output is not None:
            self.output.flush()
        log("replay_completed", run_id=self.factory.run_id, **asdict(stats))
        return stats
