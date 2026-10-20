"""Finite diagnostic replay uses the same durable journal/delivery path as serve."""

import json
import time

from simulator.logging import log
from simulator.scheduling.engine import SensorEngine


def replay_durable(settings) -> int:
    engine = SensorEngine(settings)
    limit = settings.limit or len(engine.records)
    generated = 0
    output = None
    try:
        if settings.output:
            settings.output.parent.mkdir(parents=True, exist_ok=True)
            output = settings.output.open("w", encoding="utf-8")
        machines = engine.store.list()
        for i in range(limit):
            event = engine.generate_once(machines[i % len(machines)]["machine_id"])
            if event:
                generated += 1
                if output:
                    output.write(json.dumps(event) + "\n")
            engine.dispatch_once()
            time.sleep(settings.interval_ms / 1000 / len(machines) / settings.speed)
        deadline = time.monotonic() + max(5, settings.timeout_seconds * 3)
        while time.monotonic() < deadline:
            engine.dispatch_once()
            if not engine.inflight and not engine.store.summary()["counts"].get("pending"):
                break
            time.sleep(0.02)
        summary = engine.store.summary()
        log("replay_completed", generated=generated, spool=summary)
        return int(bool(sum(summary["counts"].values())) or generated != limit)
    finally:
        try:
            if settings.metrics_file:
                from prometheus_client import generate_latest

                engine.metrics.refresh(engine.store)
                settings.metrics_file.parent.mkdir(parents=True, exist_ok=True)
                settings.metrics_file.write_bytes(generate_latest(engine.metrics.registry))
        finally:
            if output:
                output.close()
            engine.close()
