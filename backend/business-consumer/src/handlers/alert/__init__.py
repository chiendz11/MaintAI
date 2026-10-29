"""Alert Signals are an inbox-protected third loop in the existing Consumer."""

from backend.shared.alerts.handler import AlertHandler


class UnavailableEvidence:
    def fetch(self, payload):
        raise RuntimeError("Processing evidence service is not configured")


def build_handler(factory, group, *, evidence=None, metrics=None, repository=None):
    return AlertHandler(
        factory, group, evidence or UnavailableEvidence(), metrics=metrics, repository=repository
    )
