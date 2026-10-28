"""An event has a single canonical risk effect, independent of delivery retries."""

import hashlib


def risk_decision_id(event_id: str) -> str:
    return "risk_" + hashlib.sha256(event_id.encode("utf-8")).hexdigest()
