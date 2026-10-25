"""Local bootstrap and real consumer/relay entrypoints for application handlers."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import signal
import sys
import threading

from .admin import create_local_topics
from .config import KafkaSettings, KeyedSettings
from .contracts import ContractRegistry
from .errors import ConfigurationError


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate-contracts")
    commands.add_parser("create-local-topics")
    migrate = commands.add_parser("migrate-owner-db")
    migrate.add_argument("--owner", choices=["processing", "business"], required=True)
    migrate.add_argument("--processing-ordering", action="store_true")
    activate = commands.add_parser("activate-processing-ordering")
    activate.add_argument("--legacy-writers-drained", action="store_true", required=True)
    consume = commands.add_parser("consume")
    consume.add_argument("--role", choices=["processing", "business", "storage"])
    consume.add_argument(
        "--handler-factory", help="Import path module:function(settings, role, registry)"
    )
    consume.add_argument("--archive-outcomes", action="store_true")
    consume.add_argument("--processing-mode", choices=["serial", "keyed"])
    relay = commands.add_parser("relay")
    relay.add_argument("--once", action="store_true")
    relay.add_argument("--mode", choices=["legacy", "ordered"])
    args = parser.parse_args(argv)
    try:
        registry = ContractRegistry()
        if args.command == "validate-contracts":
            print(
                json.dumps(
                    {
                        "wire_fixtures": registry.check_examples(),
                        "topics": list(registry.topology["topics"]),
                    }
                )
            )
            return 0
        if args.command in {"migrate-owner-db", "activate-processing-ordering"}:
            from .postgres import apply_messaging_schema, connect

            owner = args.owner if args.command == "migrate-owner-db" else "processing"
            name = "PROCESSING_DATABASE_URL" if owner == "processing" else "BUSINESS_DATABASE_URL"
            dsn = os.environ.get(name) or os.environ.get("DATABASE_URL")
            if not dsn:
                raise ConfigurationError(f"{name} is required")
            with connect(dsn) as connection:
                if args.command == "migrate-owner-db":
                    if args.processing_ordering and owner != "processing":
                        raise ConfigurationError("Ordering migration is Processing-owned")
                    apply_messaging_schema(connection, registry)
                    if args.processing_ordering:
                        from .ordering import apply_processing_ordering_schema

                        apply_processing_ordering_schema(connection, registry)
                else:
                    from .ordering import activate_processing_ordering

                    activate_processing_ordering(connection, legacy_writers_drained=True)
            print("Applied explicit owner-local migration/capability operation")
            return 0
        settings = KafkaSettings.from_env()
        if args.command == "create-local-topics":
            print(json.dumps({"topics": create_local_topics(settings, contracts=registry)}))
            return 0
        stop = threading.Event()
        for signum in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(signum, lambda *_: stop.set())
        if args.command == "relay":
            from .postgres import OutboxRelay, connect
            from .producer import KafkaPublisher

            dsn = os.environ.get("PROCESSING_DATABASE_URL") or os.environ.get("DATABASE_URL")
            if not dsn:
                raise ConfigurationError("PROCESSING_DATABASE_URL is required")
            mode = args.mode or os.environ.get("OUTBOX_EXECUTION_MODE", "legacy")
            if mode not in {"legacy", "ordered"}:
                raise ConfigurationError("Unsupported Outbox mode")
            with KafkaPublisher(settings, "outbox", contracts=registry) as publisher:
                relay_type = OutboxRelay
                if mode == "ordered":
                    from .ordering import OrderedOutboxRelay

                    relay_type = OrderedOutboxRelay
                runner = relay_type(
                    lambda: connect(dsn),
                    publisher,
                    lease_seconds=float(os.environ.get("OUTBOX_CLAIM_LEASE_SECONDS", "120")),
                    batch_size=int(os.environ.get("OUTBOX_BATCH_SIZE", "10")),
                    poll_interval_seconds=float(os.environ.get("OUTBOX_POLL_INTERVAL_MS", "250"))
                    / 1000,
                )
                if args.once:
                    print(json.dumps({"published": runner.run_once()}))
                else:
                    runner.run(stop)
            return 0
        if args.role is None:
            raise ConfigurationError("A consumer service role is required")
        args.processing_mode = args.processing_mode or os.environ.get(
            "PROCESSING_EXECUTION_MODE", "serial"
        )
        if args.processing_mode not in {"serial", "keyed"}:
            raise ConfigurationError("Unsupported Processing mode")
        if args.processing_mode == "keyed" and args.role != "processing":
            raise ConfigurationError("Keyed mode is Processing-only")
        roles = {
            "processing": ["processing"],
            "business": ["business-live", "business-prediction", "business-alert"],
            "storage": ["storage-accepted", "storage-rejected"],
        }[args.role]
        if args.archive_outcomes:
            if args.role != "storage":
                raise ConfigurationError("Outcome archive is a storage-only option")
            roles.append("storage-outcomes")
        if args.handler_factory:
            module, separator, name = args.handler_factory.partition(":")
            if not separator or not module or not name:
                raise ConfigurationError("Handler factory must be module:function")
            factory = getattr(importlib.import_module(module), name)
            handlers = {role: factory(settings, role, registry) for role in roles}
        elif args.role == "storage":
            from .archive import S3RecordArchive

            handlers = {role: S3RecordArchive.from_env(settings) for role in roles}
        else:
            raise ConfigurationError("Application consumers require a durable handler factory")
        from .service import serve_consumers

        serve_consumers(
            settings,
            handlers,
            stop,
            contracts=registry,
            keyed_limits=KeyedSettings.from_env() if args.processing_mode == "keyed" else None,
        )
        return 0
    except Exception as exc:
        # Never print config values, raw messages or chained provider tracebacks.
        print(f"Kafka command failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
