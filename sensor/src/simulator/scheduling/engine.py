"""Independent generation and bounded HTTP dispatch inside one Sensor runtime."""

import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from simulator.client import BackendClient
from simulator.control.auth import IngestionCredentials
from simulator.fault_injection import FaultInjector, FaultMode
from simulator.logging import log
from simulator.metrics import SensorMetrics
from simulator.ordering.store import StateStore, utc
from simulator.settings import RuntimeSettings
from simulator.sources import CsvSimulatorSource, DataSource


class SensorEngine:
    def __init__(
        self,
        settings: RuntimeSettings,
        *,
        transport=None,
        source: DataSource | None = None,
    ):
        self.settings = settings
        self.source = CsvSimulatorSource(settings) if source is None else source
        if settings.scenario not in self.source.scenarios:
            raise ValueError("Initial scenario is not in the configured allowlist")
        self.epoch = str(uuid4())
        self.metrics = SensorMetrics()
        self.injector = FaultInjector(
            settings.fault_mode, every=settings.fault_every, field=settings.fault_field
        )
        self.store = StateStore(settings.state_path)
        try:
            identity = self.source.source_identity(
                machine_count=settings.machine_count, seed=settings.seed
            )
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
        self.cycle_id = 0
        self.cycle_deadline = None
        self._trace_lock = threading.Lock()
        self._event_traces: dict[str, dict] = {}
        self._trace_fh = None
        if settings.burst_trace_path is not None:
            settings.burst_trace_path.parent.mkdir(parents=True, exist_ok=True)
            self._trace_fh = settings.burst_trace_path.open("a", encoding="utf-8", buffering=1)
        self.machine_ids = [row["machine_id"] for row in self.store.list()]

    @property
    def records(self):
        return getattr(self.source, "records", ())

    @property
    def scenarios(self):
        return self.source.scenarios

    def build(self, machine: dict) -> dict | None:
        sample = self.source.read_sample(machine)
        if sample is None:
            return None
        payload = {
            "schema_version": "1.3.0",
            "stream_epoch": self.epoch,
            "event_id": f"evt_{uuid4()}",
            "machine_id": machine["machine_id"],
            "sequence_number": machine["sequence_number"] + 1,
            "received_at": utc(),
            "source": sample.source,
            "features": sample.features,
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

    def _record_trace(
        self,
        event_id: str,
        payload: dict,
        attempts: int,
        *,
        sent_at: str,
        ingestion_ack_at: str,
        result,
        pop: bool,
    ) -> None:
        with self._trace_lock:
            meta = self._event_traces.pop(event_id, None) if pop else self._event_traces.get(event_id)
            if self._trace_fh is None:
                return
            meta = meta or {}
            line = {
                "sampling_mode": self.settings.sampling_mode,
                "cycle_id": meta.get("cycle_id"),
                "machine_id": payload["machine_id"],
                "event_id": event_id,
                "sequence_number": payload["sequence_number"],
                "stream_epoch": payload.get("stream_epoch"),
                "sampled_at": meta.get("sampled_at") or payload.get("received_at"),
                "event_received_at": payload.get("received_at"),
                "edge_persisted_at": meta.get("edge_persisted_at"),
                "sent_at": sent_at,
                "ingestion_ack_at": ingestion_ack_at,
                "server_accepted_at": (result.response or {}).get("server_accepted_at"),
                "outcome": result.outcome,
                "status_code": result.status_code,
                "attempts": attempts,
                "send_latency_ms": round(result.latency_ms, 3),
                "error": result.error,
            }
            self._trace_fh.write(json.dumps(line, separators=(",", ":")) + "\n")

    def deliver(self, row: dict):
        payload = json.loads(row["payload"])
        # Injection occurs once per durable event, so a simulated outage can recover.
        selected = payload["sequence_number"] % self.settings.fault_every == 0
        fault = self.settings.fault_mode if selected and row["attempts"] == 0 else FaultMode.NONE
        sent_at = utc()
        result = self.client.send(payload, fault=fault)
        ingestion_ack_at = utc()
        if result.outcome == "accepted":
            # Delete only after an identity-checked ACK; restart retries the same payload/ID.
            self.store.finish(row["event_id"], status="accepted")
            self._record_trace(
                row["event_id"],
                payload,
                row["attempts"] + 1,
                sent_at=sent_at,
                ingestion_ack_at=ingestion_ack_at,
                result=result,
                pop=True,
            )
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
        self._record_trace(
            row["event_id"],
            payload,
            row["attempts"] + 1,
            sent_at=sent_at,
            ingestion_ack_at=ingestion_ack_at,
            result=result,
            pop=not result.retryable,
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
        cycle_id = None
        if self.settings.sampling_mode in ("synchronized", "burst"):
            if self.cycle_deadline is None:
                self.cycle_deadline = now
            if generate and now >= self.cycle_deadline:
                self.cycle_id += 1
                cycle_id = self.cycle_id
                due_machines = list(self.machine_ids)
                next_deadline = self.cycle_deadline + interval
                self.cycle_deadline = next_deadline if next_deadline > now else now + interval
        else:
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
            sampled_at = utc()
            events = self.store.generate_many(
                due_machines,
                self.build,
                max_records=self.settings.spool_max_records,
                max_bytes=self.settings.spool_max_bytes,
            )
            edge_persisted_at = utc()
            if events:
                if self._trace_fh is not None:
                    with self._trace_lock:
                        for ev in events:
                            self._event_traces[ev["event_id"]] = {
                                "cycle_id": cycle_id,
                                "sampled_at": sampled_at,
                                "edge_persisted_at": edge_persisted_at,
                            }
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
        if self._trace_fh is not None:
            self._trace_fh.flush()
            self._trace_fh.close()
            self._trace_fh = None
        self.client.__exit__()
        self.store.close()
        self.closed = True
