"""Terminal outcome callback; does not compute model predictions or risk."""

from backend.shared.inbox.handler import ProjectionHandler
from backend.shared.projections.projector import Projector


def build_handler(connection_factory, group, *, metrics=None, notify=None, generations=False):
    return ProjectionHandler(
        connection_factory,
        group,
        topic="inference.results",
        metrics=metrics,
        projector=Projector(notify=notify, generations=generations),
    )
