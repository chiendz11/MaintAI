"""Independent generation and bounded HTTP dispatch inside one Sensor runtime."""

import hashlib
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from simulator.client import BackendClient
from simulator.control.auth import IngestionCredentials
from simulator.dataset import DatasetReader
from simulator.fault_injection import FaultInjector, FaultMode
from simulator.logging import log
from simulator.metrics import SensorMetrics
from simulator.ordering.store import StateStore, canonical, utc
from simulator.scenarios import ScenarioLoader, ScenarioTransformer
from simulator.settings import RuntimeSettings


class SensorEngine:
    def __init__(self, settings: RuntimeSettings, *, transport=None):
        self.settings = settings
        self.records = list(DatasetReader(settings.dataset_path))
        self.scenarios = {
            p.stem: ScenarioLoader.load(p) for p in sorted(settings.scenario_path.glob("*.yaml"))
        }
        if settings.scenario not in self.scenarios:
            raise ValueError("Initial scenario is not in the configured allowlist")
        self.epoch = str(uuid4())
        self.metrics = SensorMetrics()
        self.injector = FaultInjector(
            settings.fault_mode, every=settings.fault_every, field=settings.fault_field
        )
        self.store = StateStore(settings.state_path)
        try:
            identity = hashlib.sha256(
                canonical(
                    {
                        "dataset": hashlib.sha256(settings.dataset_path.read_bytes()).hexdigest(),
                        "scenarios": {
                            name: value.model_dump(mode="json")
                            for name, value in self.scenarios.items()
                        },
                        "machines": settings.machine_count,
                        "seed": settings.seed,
                        "schema": "1.3.0",
                        "generation_version": "1",
                    }
                ).encode()
            ).hexdigest()
            self.store.initialize(
                settings.machine_count, settings.scenario, settings.initial_state, identity
            )
            self.metrics.refresh(self.store)
        except Exception:
            self.store.close()
            raise
        self.client = BackendClient(
            str(settings.backend_base_url),
            timeout_seconds=settings.timeout_seconds,
            max_retries=0,
            transport=transport,
            credentials=IngestionCredentials(settings),
            observe=lambda outcome: self.metrics.attempts.labels(outcome).inc(),
        )
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()
        self.thread = None
        self.pool = None
        self.inflight = {}
        self.fatal = None
        self.generated = 0
        self.completed = 0
        self.closed = False
        self.last_tick = time.monotonic()
        self.deadlines = {}
        self.machine_ids = [row["machine_id"] for row in self.store.list()]

    def build(self, machine: dict) -> dict | None:
        cursor = machine["row_cursor"]
        if not self.settings.loop_dataset and cursor >= len(self.records):
            return None
        record = self.records[cursor % len(self.records)]
        scenario = self.scenarios[machine["scenario"]]
        step = min(machine["scenario_step"], scenario.event_count - 1)
        # Derive categorical RNG from durable position, never a volatile RNG cursor.
        seed = int(
            hashlib.sha256(
                (
                    f"{scenario.seed if self.settings.seed is None else self.settings.seed}:"
                    f"{machine['machine_id']}:{machine['scenario_step']}"
                ).encode()
            ).hexdigest()[:16],
            16,
        )
        features, _ = ScenarioTransformer(scenario, seed=seed).apply(record.features, step)
        payload = {
            "schema_version": "1.3.0",
            "stream_epoch": self.epoch,
            "event_id": f"evt_{uuid4()}",
            "machine_id": machine["machine_id"],
            "sequence_number": machine["sequence_number"] + 1,
            "received_at": utc(),
            "source": "AI4I_SIMULATOR",
            "features": features.model_dump(),
        }
        return self.injector.apply(payload, machine["sequence_number"]).payload

    def generate_once(self, machine_id: str) -> dict | None:
        event = self.store.generate(
            machine_id,
            self.build,
            max_records=self.settings.spool_max_records,
            max_bytes=self.settings.spool_max_bytes,
        )
        if event:
            self.metrics.generated.inc()
            self.generated += 1
        return event

    def deliver(self, row: dict):
        payload = json.loads(row["payload"])
        # Injection occurs once per durable event, so a simulated outage can recover.
        selected = payload["sequence_number"] % self.settings.fault_every == 0
        fault = self.settings.fault_mode if selected and row["attempts"] == 0 else FaultMode.NONE
        result = self.client.send(payload, fault=fault)
        if result.outcome == "accepted":
            # Delete only after an identity-checked ACK; restart retries the same payload/ID.
            self.store.finish(row["event_id"], status="accepted")
            if selected and self.settings.fault_mode == FaultMode.DUPLICATE_EVENT:
                self.client.send(payload)
            return
        if result.retryable:
            delay = (
                result.retry_after
                if result.retry_after is not None
                else min(
                    max(0.1, self.settings.retry_backoff_ms / 1000)
                    * 2 ** min(row["attempts"], 16)
                    * random.uniform(0.8, 1.2),
                    self.settings.retry_max_seconds,
                )
            )
            status = "pending"
        else:
            delay = 0
            status = (
                "auth"
                if result.status_code in (401, 403)
                or result.error == "INGESTION_CREDENTIAL_UNAVAILABLE"
                else "rejected"
            )
        self.store.finish(
            row["event_id"], status=status, error=result.error, next_at=time.time() + delay
        )

    def dispatch_once(self):
        self.pool = self.pool or ThreadPoolExecutor(
            max_workers=self.settings.send_workers, thread_name_prefix="sensor-send"
        )
        for machine_id, future in list(self.inflight.items()):
            if future.done():
                future.result()
                del self.inflight[machine_id]
                self.completed += 1
        available = self.settings.send_workers - len(self.inflight)
        if available <= 0:
            return
        for row in self.store.due(
            time.time(),
            self.settings.spool_ttl_seconds,
            available,
            exclude_machines=tuple(self.inflight),
        ):
            machine_id = row["machine_id"]
            if machine_id not in self.inflight:
                future = self.pool.submit(self.deliver, row)
                future.add_done_callback(lambda _: self.wake_event.set())
                self.inflight[machine_id] = future

    def tick(self, *, generate=True):
        now = time.monotonic()
        interval = self.settings.interval_ms / 1000 / self.settings.speed
        due_machines = []
        total = len(self.machine_ids)
        for i, machine_id in enumerate(self.machine_ids):
            deadline = self.deadlines.setdefault(machine_id, now + i * interval / total)
            if generate and now >= deadline:
                due_machines.append(machine_id)
                next_deadline = deadline + interval
                self.deadlines[machine_id] = (
                    next_deadline if next_deadline > now else now + interval
                )
        if due_machines:
            events = self.store.generate_many(
                due_machines,
                self.build,
                max_records=self.settings.spool_max_records,
                max_bytes=self.settings.spool_max_bytes,
            )
            if events:
                self.metrics.generated.inc(len(events))
                self.generated += len(events)
        self.dispatch_once()
        self.last_tick = time.monotonic()

    def run(self):
        last_log = 0
        try:
            while not self.stop_event.is_set():
                self.wake_event.clear()
                self.tick()
                if time.monotonic() - last_log >= 30:
                    summary = self.store.summary()
                    log(
                        "sensor_spool_status",
                        counts=summary["counts"],
                        buffer_used_bytes=summary["bytes"],
                        oldest_unacked_event_age=max(0, time.time() - summary["oldest"])
                        if summary["oldest"]
                        else None,
                    )
                    last_log = time.monotonic()
                if not self.stop_event.is_set():
                    self.wake_event.wait(0.005)
        except Exception as exc:
            self.fatal = type(exc).__name__
            log("sensor_runtime_failure", error_type=self.fatal)

    def start(self):
        if self.thread is not None:
            raise ValueError("Controller already started")
        self.thread = threading.Thread(target=self.run, name="sensor-controller", daemon=True)
        self.thread.start()

    def ready(self) -> bool:
        return (
            not self.closed
            and not self.fatal
            and (
                self.thread is None
                or (self.thread.is_alive() and time.monotonic() - self.last_tick < 10)
            )
        )

    def close(self):
        if self.closed:
            return
        self.stop_event.set()
        self.wake_event.set()
        if self.thread:
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                raise RuntimeError("Controller did not stop; state lock must remain held")
        if self.pool:
            self.pool.shutdown(wait=True)
        self.client.__exit__()
        self.store.close()
        self.closed = True
