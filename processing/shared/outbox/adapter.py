"""Always queue terminal outcome; gate alert helper explicitly by policy."""


def enqueue_outcome_and_signal(connection, outbox, factory, outcome, *, risk=None):
    outbox.enqueue(connection, "inference.results", outcome)
    if risk is not None and risk.should_create_alert:
        signal = factory.alert_signal(outcome)
        if signal is None:
            raise ValueError("Policy requested a signal for a non-actionable result")
        outbox.enqueue(connection, "alert.signals", signal)
