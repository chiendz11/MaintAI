"""Preserve rollback semantics; record conflicts independently, never acknowledge them."""

import hashlib
import logging

from maintai_messaging.errors import IdentityConflict
from maintai_messaging.postgres import TransactionalHandler

from backend.shared.notifications import DEFAULT_CHANNEL
from backend.shared.projections.projector import Projector


class ProjectionHandler:
    def __init__(
        self,
        connection_factory,
        consumer_group,
        *,
        projector=None,
        topic=None,
        metrics=None,
        redis_bus=None,
        notification_channel=DEFAULT_CHANNEL,
    ):
        self.connection_factory = connection_factory
        self.consumer_group = consumer_group
        self.projector = projector or Projector()
        self.topic, self.metrics = topic, metrics
        self.redis_bus = redis_bus
        self.notification_channel = notification_channel

    def __call__(self, event, record):
        if self.topic is not None and event.topic != self.topic:
            raise ValueError("Event does not belong to this Business subscription")
        applied, visible = False, False
        post_commit = None

        def callback(connection, decoded, source, outbox):
            nonlocal applied, visible, post_commit
            if hasattr(self.projector, "apply"):
                reduction = self.projector.apply(
                    connection,
                    decoded,
                    source,
                    outbox,
                    pg_notify=self.redis_bus is None,
                )
                visible = bool(reduction.telemetry_visible)
                if reduction.changed:
                    post_commit = (decoded.key, reduction.rows, reduction.changed)
            else:
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
        if applied and self.redis_bus is not None and post_commit is not None:
            machine_id, rows, changed = post_commit
            try:
                self.redis_bus.update_live(machine_id, rows, changed)
                self.redis_bus.publish_live(self.notification_channel, machine_id)
            except Exception:
                logging.getLogger("maintai.business.consumer").warning(
                    "Redis projection hot-state update failed; Postgres fallback preserved"
                )
        # TransactionalHandler has committed before metrics; collectors cannot undo a receipt.
        if self.metrics is not None:
            try:
                self.metrics.committed(event, duplicate=not applied, telemetry_visible=visible)
            except Exception:
                pass
        return receipt
