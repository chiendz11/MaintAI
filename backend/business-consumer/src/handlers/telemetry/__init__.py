"""Accepted telemetry callback; no DQ, inference or registry provisioning."""

from backend.shared.inbox.handler import ProjectionHandler


def build_handler(connection_factory, group, *, metrics=None):
    return ProjectionHandler(connection_factory, group, topic="sensor.readings", metrics=metrics)
