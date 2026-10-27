"""Terminal outcome callback; does not compute model predictions or risk."""

from backend.shared.inbox.handler import ProjectionHandler


def build_handler(connection_factory, group, *, metrics=None):
    return ProjectionHandler(connection_factory, group, topic="inference.results", metrics=metrics)
