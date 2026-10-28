"""Preserve rollback semantics; record conflicts independently, never acknowledge them."""

import hashlib

from maintai_messaging.errors import IdentityConflict
from maintai_messaging.postgres import TransactionalHandler

from backend.shared.projections.projector import Projector


class ProjectionHandler:
    def __init__(
        self, connection_factory, consumer_group, *, projector=None, topic=None, metrics=None
    ):
        self.connection_factory = connection_factory
        self.consumer_group = consumer_group
        self.projector = projector or Projector()
        self.topic, self.metrics = topic, metrics

    def __call__(self, event, record):
        if self.topic is not None and event.topic != self.topic:
            raise ValueError("Event does not belong to this Business subscription")
        applied, visible = False, False

        def callback(connection, decoded, source, outbox):
            nonlocal applied, visible
            visible = bool(self.projector(connection, decoded, source, outbox))
            applied = True

        try:
            receipt = TransactionalHandler(
                self.connection_factory,
                self.consumer_group,
                callback,
            )(event, record)
        except IdentityConflict:
            # The handler context has rolled back before this separate connection.
            # If evidence persistence fails, that failure also prevents any receipt.
            with self.connection_factory() as connection, connection.transaction():
                connection.execute(
                    """INSERT INTO business_projection_conflicts
                    (consumer_group,topic,source_partition,source_offset,message_sha256,error_code)
                    VALUES(%s,%s,%s,%s,%s,'PROJECTION_CONFLICT') ON CONFLICT DO NOTHING""",
                    (
                        self.consumer_group,
                        record.topic,
                        record.partition,
                        record.offset,
                        hashlib.sha256(event.wire_bytes).hexdigest(),
                    ),
                )
            raise
        # TransactionalHandler has committed before metrics; collectors cannot undo a receipt.
        if self.metrics is not None:
            try:
                self.metrics.committed(event, duplicate=not applied, telemetry_visible=visible)
            except Exception:
                pass
        return receipt
