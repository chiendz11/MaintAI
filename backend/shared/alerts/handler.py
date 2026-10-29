"""Inbox skips duplicate HTTP reads; evidence and business effects are atomic."""

import hashlib

from maintai_messaging.errors import IdentityConflict
from maintai_messaging.postgres import TransactionalHandler

from .repository import AlertRepository


class AlertHandler:
    topic = "alert.signals"

    def __init__(self, factory, consumer_group, evidence, *, repository=None, metrics=None):
        self.factory, self.consumer_group, self.evidence = factory, consumer_group, evidence
        self.repository = repository or AlertRepository(factory)
        self.metrics = metrics

    def __call__(self, event, record):
        if event.topic != self.topic:
            raise ValueError("Unexpected Alert subscription")
        applied = False
        result = None

        def callback(connection, decoded, source, outbox):
            nonlocal applied, result
            # SDK has already checked its inbox. No machine lock is held during HTTP.
            evidence = self.evidence.fetch(decoded.payload)
            result = self.repository.create_or_dedupe(connection, decoded, evidence)
            applied = True

        try:
            receipt = TransactionalHandler(self.factory, self.consumer_group, callback)(
                event, record
            )
        except IdentityConflict:
            with self.factory() as connection, connection.transaction():
                connection.execute(
                    """INSERT INTO business_projection_conflicts
                    (consumer_group,topic,source_partition,source_offset,message_sha256,error_code)
                    VALUES(%s,%s,%s,%s,%s,'ALERT_EVIDENCE_CONFLICT') ON CONFLICT DO NOTHING""",
                    (
                        self.consumer_group,
                        record.topic,
                        record.partition,
                        record.offset,
                        hashlib.sha256(event.wire_bytes).hexdigest(),
                    ),
                )
            raise
        if self.metrics is not None:
            try:
                self.metrics.committed(event, duplicate=not applied, telemetry_visible=False)
                if result is not None:
                    self.metrics.alerts.signal(*result)
            except Exception:
                pass
        return receipt
