"""Accepted telemetry callback; no DQ, inference or registry provisioning."""

from backend.shared.inbox.handler import ProjectionHandler
from backend.shared.projections.projector import Projector


def build_handler(connection_factory, group, *, metrics=None, notify=None):
    return ProjectionHandler(
        connection_factory,
        group,
        topic="sensor.readings",
        metrics=metrics,
        projector=Projector(notify=notify),
    )
