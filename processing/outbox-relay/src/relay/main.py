"""Application lifecycle around the canonical leased outbox publisher."""

import argparse
import asyncio
import json
import math
import os
import signal
import sys
import threading

from maintai_messaging.errors import PublishUnavailable
from maintai_messaging.metrics import OutboxOrderMetrics
from maintai_messaging.ordering import OrderedOutboxRelay
from maintai_messaging.postgres import OutboxRelay, connect
from maintai_messaging.producer import KafkaPublisher
from processing.shared.persistence.connection import validate_execution_mode

from .config import RelaySettings
from .observability import RuntimeObservability, monitor_dependencies, start_probe_server


class ObservedPublisher:
    """Observe each ACK attempt while leaving SDK backoff/fencing unchanged."""

    def __init__(self, publisher, observer):
        self.publisher, self.observer = publisher, observer
        self.settings, self.contracts = publisher.settings, publisher.contracts

    def publish_event(self, event):
        try:
            result = self.publisher.publish_event(event)
        except PublishUnavailable:
            self.observer.publish_attempts.labels("UNACKNOWLEDGED").inc()
            raise
        self.observer.publish_attempts.labels("ACKNOWLEDGED").inc()
        return result


def create_relay(
    connection_factory,
    publisher,
    *,
    lease_seconds=120,
    batch_size=1,
    mode="legacy",
    metrics=None,
    poll_interval_seconds=0.25,
):
    if type(batch_size) is not int or not 1 <= batch_size <= 100:
        raise ValueError("Relay batch size must be between one and one hundred")
    if type(lease_seconds) not in (int, float) or not math.isfinite(lease_seconds):
        raise ValueError("Relay lease must be finite")
    # SDK connection timeout 10s + statement timeout 15s, in addition to each
    # sequential Kafka delivery. Default batch one avoids late-batch lease loss.
    if mode not in {"legacy", "ordered"}:
        raise ValueError("Explicit supported relay mode is required")
    minimum = (batch_size if mode == "legacy" else 1) * (
        publisher.settings.delivery_timeout_ms / 1000 + 25
    )
    if lease_seconds <= minimum:
        raise ValueError("Relay lease does not cover the full sequential batch budget")
    options = {
        "lease_seconds": lease_seconds,
        "batch_size": batch_size,
        "poll_interval_seconds": poll_interval_seconds,
    }
    if mode == "ordered":
        return OrderedOutboxRelay(connection_factory, publisher, metrics=metrics, **options)
    return OutboxRelay(connection_factory, publisher, **options)


async def run_relay(settings, stop, *, once=False):
    def validate():
        with connect(settings.database_url) as connection:
            validate_execution_mode(connection, ordered=settings.execution_mode == "ordered")

    await asyncio.to_thread(validate)
    observer = RuntimeObservability("relay", profile=settings.profile, stop=stop)
    server = probe_thread = monitoring = None
    try:
        if not once:
            server, probe_thread = start_probe_server(
                observer, settings.health_port, host=settings.health_host
            )
            monitoring = asyncio.create_task(
                monitor_dependencies(
                    observer, settings.kafka, settings.database_url, stop, relay=True
                )
            )
        with KafkaPublisher(settings.kafka, "outbox") as publisher:

            def connections():
                return observer.database_connection(lambda: connect(settings.database_url))

            metrics = (
                OutboxOrderMetrics(connections, observer.registry)
                if settings.execution_mode == "ordered"
                else None
            )
            runner = create_relay(
                connections,
                ObservedPublisher(publisher, observer),
                lease_seconds=settings.lease_seconds,
                batch_size=settings.batch_size,
                mode=settings.execution_mode,
                metrics=metrics,
                poll_interval_seconds=settings.poll_interval_seconds,
            )
            while not stop.is_set():
                published = await asyncio.to_thread(runner.run_once)
                observer.published.inc(published)
                if once:
                    return published
                await asyncio.sleep(settings.poll_interval_seconds)
    finally:
        stop.set()
        observer.draining = True
        if monitoring:
            monitoring.cancel()
            await asyncio.gather(monitoring, return_exceptions=True)
        if server:
            server.should_exit = True
            await asyncio.to_thread(probe_thread.join, 5)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Processing outbox relay; no schema migrations on startup"
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--mode", choices=["legacy", "ordered"])
    args = parser.parse_args(argv)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        env = dict(os.environ)
        if args.mode:
            env["OUTBOX_EXECUTION_MODE"] = args.mode
        settings = RelaySettings.from_env(env)
        published = asyncio.run(run_relay(settings, stop, once=args.once))
        if args.once:
            print(json.dumps({"published": published}))
        return 0
    except Exception as exc:
        print(f"Processing relay failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
