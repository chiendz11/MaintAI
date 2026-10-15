"""Local-only ingestion app for manual HTTP checks without PostgreSQL or ML."""

import asyncio

from app.main import create_app
from app.schemas.sensor import SensorReadingRequest
from app.services.sensor_ingestion import (
    HttpOutcome,
    MLUnavailable,
    SensorIngestionService,
    StoredEvent,
)


class LocalMemoryStore:
    """One-process fixture; data disappear on restart and are not production state."""

    def __init__(self):
        self.machines = {"M-001"}
        self.events: dict[str, StoredEvent] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    def lock(self, event_id: str) -> asyncio.Lock:
        return self.locks.setdefault(event_id, asyncio.Lock())

    async def get(self, event_id: str) -> StoredEvent | None:
        return self.events.get(event_id)

    async def machine_exists(self, machine_id: str) -> bool:
        return machine_id in self.machines

    async def record(self, event: StoredEvent) -> None:
        self.events[event.event_id] = event

    async def save_outcome(self, event_id: str, outcome: HttpOutcome) -> None:
        self.events[event_id].outcome = outcome


class UnavailableML:
    async def process(self, event: SensorReadingRequest, correlation_id: str):
        raise MLUnavailable("ML service is not connected in the local fixture")


app = create_app(SensorIngestionService(LocalMemoryStore(), UnavailableML()))
