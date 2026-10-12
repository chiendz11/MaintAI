import random
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid4, uuid5

from simulator.models import Features, SensorEvent


class EventFactory:
    def __init__(
        self,
        *,
        seed: int,
        machine_count: int,
        interval_ms: float,
        start_time: datetime | None = None,
        run_id: str | None = None,
    ):
        if machine_count < 1:
            raise ValueError("machine_count must be positive")
        if start_time is not None and start_time.utcoffset() is None:
            raise ValueError("start_time must include a timezone")
        self.random = random.Random(seed)
        self.machine_count = machine_count
        self.interval_ms = interval_ms
        self.start_time = (start_time or datetime.now(UTC)).astimezone(UTC)
        # A fresh run ID prevents a second replay from colliding with prior events.
        # Supplying run_id + start_time makes the whole payload sequence reproducible.
        self.run_id = run_id or str(uuid4())
        self.namespace = uuid5(NAMESPACE_URL, self.run_id)

    def create(self, features: Features, sequence: int) -> SensorEvent:
        return SensorEvent(
            event_id=f"evt_{uuid5(self.namespace, str(sequence))}",
            machine_id=f"M-{self.random.randint(1, self.machine_count):03d}",
            received_at=self.start_time + timedelta(milliseconds=sequence * self.interval_ms),
            source="AI4I_SIMULATOR",
            features=features,
        )
